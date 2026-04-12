//! Server configuration — infra-only.
//!
//! The server itself no longer owns provider URLs. Providers are defined in
//! each client's own `config.toml` (`rpc-client/`, `grpc-client/`).
//!
//! To supply providers to the server at runtime you have two options:
//!
//! 1. **Environment variables** (recommended for Docker / CI):
//!    ```
//!    MESH_PROVIDERS__0__ID=helius-rpc \
//!    MESH_PROVIDERS__0__KIND=rpc \
//!    MESH_PROVIDERS__0__URL=https://mainnet.helius-rpc.com/?api-key=YOUR_KEY \
//!    cargo run
//!    ```
//!
//! 2. **`providers.toml`** — an optional file you create next to the server
//!    binary (or in the working directory).  Copy the example below to get
//!    started:
//!
//!    ```toml
//!    # providers.toml  (server-side, optional)
//!    [[providers]]
//!    id   = "helius-rpc"
//!    kind = "rpc"
//!    url  = "https://mainnet.helius-rpc.com/?api-key=YOUR_KEY"
//!
//!    [[providers]]
//!    id            = "helius-geyser"
//!    kind          = "geyser"
//!    grpc_endpoint = "https://atlas-mainnet.helius-rpc.com:2083"
//!    x_token       = "YOUR_KEY"
//!    ```
//!
//! Precedence (highest → lowest):
//!   env vars (`MESH_*`) → `providers.toml` → `config.toml` defaults

use serde::Deserialize;

// ─────────────────────────────────────────────────────────────────────────────
// Per-provider config
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderConfig {
    pub id: String,

    /// Transport kind.
    ///   "rpc"    — plain JSON-RPC HTTP (default)
    ///   "geyser" — Yellowstone Dragon's Mouth gRPC geyser stream
    #[serde(default)]
    pub kind: Option<String>,

    /// HTTP JSON-RPC endpoint — used when kind = "rpc" (or unset).
    ///
    /// Supported providers and their URL patterns:
    ///   Helius:   https://mainnet.helius-rpc.com/?api-key=<KEY>
    ///   Shyft:    https://rpc.shyft.to?api_key=<KEY>
    ///   QuickNode: https://<name>.solana-mainnet.quiknode.pro/<KEY>/
    ///   Triton:   https://<pool>.rpcpool.com/<TOKEN>/
    ///   Alchemy:  https://solana-mainnet.g.alchemy.com/v2/<KEY>
    ///   Local:    http://127.0.0.1:8899
    #[serde(default)]
    pub url: String,

    /// Yellowstone gRPC endpoint — used when kind = "geyser".
    /// Format: "https://host:port" (TLS) or "http://host:port" (plaintext).
    ///
    /// Supported providers:
    ///   Helius:    https://atlas-mainnet.helius-rpc.com:2083
    ///   Shyft:     https://grpc.ny.shyft.to:10000
    ///   Triton:    https://<pool>.rpcpool.com:10000
    ///   Solana VS: https://basic.grpc.solanavibestation.com
    ///   Local:     http://127.0.0.1:10000
    #[serde(default)]
    pub grpc_endpoint: Option<String>,

    /// Auth token injected as the `x-token` gRPC metadata header.
    /// Required by Helius, Shyft, and Triton geyser endpoints.
    /// Leave unset for unauthenticated local / public endpoints.
    #[serde(default)]
    pub x_token: Option<String>,

    /// Optional per-provider cost hint (compute-units per request).
    /// Used by the Cheapest routing strategy. Default: 1.0.
    #[serde(default = "default_cost")]
    pub cost_per_cu: f64,
}

fn default_cost() -> f64 { 1.0 }

// ─────────────────────────────────────────────────────────────────────────────
// Top-level server config — infra only
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub listen_addr:       String,
    pub probe_interval_ms: u64,
    /// Providers loaded from `providers.toml` or env vars.
    /// Empty by default — the server logs a warning if none are configured.
    #[serde(default)]
    pub providers: Vec<ProviderConfig>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Load configuration
// ─────────────────────────────────────────────────────────────────────────────

pub fn load() -> anyhow::Result<AppConfig> {
    let cfg = config::Config::builder()
        // Server infra defaults from config.toml (listen addr, probe interval).
        .add_source(config::File::with_name("config").required(false))
        // Optional providers.toml — users drop this next to the binary to
        // define their provider set without touching server config.
        .add_source(config::File::with_name("providers").required(false))
        // Env-var overrides: MESH_LISTEN_ADDR, MESH_PROBE_INTERVAL_MS,
        // MESH_PROVIDERS__0__ID, MESH_PROVIDERS__0__URL, etc.
        .add_source(config::Environment::with_prefix("MESH").separator("__"))
        .set_default("listen_addr",       "[::1]:50051")?
        .set_default("probe_interval_ms", 500u64)?
        .set_default("providers",         Vec::<config::Value>::new())?
        .build()?;

    Ok(cfg.try_deserialize()?)
}