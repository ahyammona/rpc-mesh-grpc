use std::collections::HashMap;

/// How the router should pick a node for a given RPC method.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// Lowest rolling-average latency — for reads where the user/bot is waiting.
    Fastest,
    /// Node closest to the cluster tip — for slot/blockhash calls where staleness matters.
    LowestSlotLag,
    /// Lowest error rate — for simulateTransaction where correctness matters most.
    MostReliable,
    /// Lowest cost per compute unit — for expensive calls like getProgramAccounts.
    Cheapest,
    /// Fire to all healthy nodes simultaneously — always used for sendTransaction.
    Multipath,
    /// Most stable WebSocket connection — for subscription methods.
    StableWs,
}

impl std::fmt::Display for RoutingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fastest       => write!(f, "fastest"),
            Self::LowestSlotLag => write!(f, "lowest_slot_lag"),
            Self::MostReliable  => write!(f, "most_reliable"),
            Self::Cheapest      => write!(f, "cheapest"),
            Self::Multipath     => write!(f, "multipath"),
            Self::StableWs      => write!(f, "stable_ws"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RequestProfile {
    pub strategy:   RoutingStrategy,
    pub timeout_ms: u64,
    /// Whether to retry on transient failure (not for multipath — dedup handles that).
    pub retry:      bool,
    /// If true, fall back aggressively on failure rather than returning an error.
    pub critical:   bool,
}

/// Maps every known Solana RPC method name to a routing profile.
/// Unknown methods fall back to the safe default (fastest, 2s timeout).
pub struct Classifier {
    profiles: HashMap<&'static str, RequestProfile>,
    default:  RequestProfile,
}

impl Classifier {
    pub fn new() -> Self {
        let mut p: HashMap<&'static str, RequestProfile> = HashMap::new();

        // ── Fastest: read calls where the caller is waiting ────────────────────
        let fast = |timeout_ms, critical| RequestProfile {
            strategy: RoutingStrategy::Fastest,
            timeout_ms,
            retry: true,
            critical,
        };
        p.insert("getAccountInfo",            fast(800,  true));
        p.insert("getBalance",                fast(500,  false));
        p.insert("getMultipleAccounts",       fast(800,  true));
        p.insert("getTokenAccountBalance",    fast(500,  false));
        p.insert("getTokenAccountsByOwner",   fast(1000, false));
        p.insert("getSignaturesForAddress",   fast(1500, false));
        p.insert("getTransaction",            fast(2000, false));
        p.insert("getRecentPrioritizationFees", RequestProfile {
            strategy: RoutingStrategy::Fastest,
            // was 300ms — bumped to 700ms (2× observed ~330ms provider latency)
            timeout_ms: 700,
            retry: false,   // time-sensitive — stale retry worse than one miss
            critical: true,
        });

        // ── LowestSlotLag: freshness of slot state matters ─────────────────────
        let lag = |timeout_ms| RequestProfile {
            strategy: RoutingStrategy::LowestSlotLag,
            timeout_ms,
            retry: true,
            critical: true,
        };
        // was 300ms — bumped to 700ms (2× observed ~330ms provider latency)
        p.insert("getSlot",            lag(700));
        p.insert("getBlockHeight",     lag(700));
        // was 500ms — bumped to 900ms; stale blockhash = failed tx
        p.insert("getLatestBlockhash", lag(900));
        p.insert("getEpochInfo",       lag(900));

        // ── MostReliable: accuracy over speed ─────────────────────────────────
        p.insert("simulateTransaction", RequestProfile {
            strategy: RoutingStrategy::MostReliable,
            timeout_ms: 3000,
            retry: true,
            critical: true,
        });

        // ── Cheapest: expensive calls — cost matters more than speed ───────────
        let cheap = |timeout_ms| RequestProfile {
            strategy: RoutingStrategy::Cheapest,
            timeout_ms,
            retry: false,   // too expensive to retry blindly
            critical: false,
        };
        p.insert("getProgramAccounts",      cheap(10_000));
        p.insert("getLargestAccounts",      cheap(5_000));
        p.insert("getTokenLargestAccounts", cheap(5_000));

        // ── Multipath: sendTransaction always fires to all healthy nodes ────────
        p.insert("sendTransaction", RequestProfile {
            strategy: RoutingStrategy::Multipath,
            timeout_ms: 5000,
            retry: false,
            critical: true,
        });

        // ── StableWs: subscriptions — uptime of the WS connection matters ──────
        let ws = |strategy| RequestProfile {
            strategy,
            timeout_ms: 0,  // persistent, no call-level timeout
            retry: true,    // always reconnect
            critical: true,
        };
        p.insert("logsSubscribe",    ws(RoutingStrategy::StableWs));
        p.insert("accountSubscribe", ws(RoutingStrategy::StableWs));
        p.insert("slotSubscribe",    ws(RoutingStrategy::LowestSlotLag));
        p.insert("programSubscribe", ws(RoutingStrategy::StableWs));

        Self {
            profiles: p,
            default: RequestProfile {
                strategy: RoutingStrategy::Fastest,
                timeout_ms: 2000,
                retry: true,
                critical: false,
            },
        }
    }

    pub fn classify(&self, method: &str) -> &RequestProfile {
        if let Some(profile) = self.profiles.get(method) {
            return profile;
        }
        tracing::warn!(method, "unknown RPC method — using default profile");
        &self.default
    }
}

impl Default for Classifier {
    fn default() -> Self { Self::new() }
}

/// Convert the proto RpcMethodSet bool flags into the list of JSON-RPC method
/// name strings the caller actually wants to invoke.
pub fn methods_from_flags(methods: &crate::proto::RpcMethodSet) -> Vec<&'static str> {
    let mut out = Vec::new();
    if methods.get_account_info               { out.push("getAccountInfo"); }
    if methods.get_balance                    { out.push("getBalance"); }
    if methods.get_multiple_accounts          { out.push("getMultipleAccounts"); }
    if methods.get_program_accounts           { out.push("getProgramAccounts"); }
    if methods.get_slot                       { out.push("getSlot"); }
    if methods.get_block_height               { out.push("getBlockHeight"); }
    if methods.get_latest_blockhash           { out.push("getLatestBlockhash"); }
    if methods.get_recent_prioritization_fees { out.push("getRecentPrioritizationFees"); }
    if methods.simulate_transaction           { out.push("simulateTransaction"); }
    if methods.get_token_accounts_by_owner    { out.push("getTokenAccountsByOwner"); }
    if methods.get_signatures_for_address     { out.push("getSignaturesForAddress"); }
    if methods.get_transaction                { out.push("getTransaction"); }
    if methods.logs_subscribe                 { out.push("logsSubscribe"); }
    if methods.account_subscribe              { out.push("accountSubscribe"); }
    if methods.slot_subscribe                 { out.push("slotSubscribe"); }
    out
}