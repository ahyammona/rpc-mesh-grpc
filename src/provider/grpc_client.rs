use std::time::Instant;

use tonic::{
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig},
    Request,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use anyhow::Context;

use crate::geyser_proto::{
    geyser_client::GeyserClient,
    CommitmentLevel,
    GetVersionRequest,
    SubscribeRequest,
    SubscribeRequestFilterAccounts,
    SubscribeRequestFilterSlots,
    SubscribeRequestFilterTransactions,
    SubscribeUpdate,
};

// ─────────────────────────────────────────────────────────────────────────────
// GeyserProviderClient — connects to a Yellowstone Dragon's Mouth endpoint
// (Helius, Shyft, Triton) and exposes:
//
//   • subscribe()  — open a streaming subscription with typed filters
//   • get_slot()   — cheaply probe current slot (used by the health prober)
//   • version()    — fetch provider version string
//
// Auth: providers authenticate via an `x-token` metadata header.
// TLS:  all production endpoints (https:// / port 443) require TLS.
//       Local / non-TLS endpoints should be prefixed with `http://`.
// ─────────────────────────────────────────────────────────────────────────────

/// A cheap-to-clone handle to a Yellowstone geyser endpoint.
/// The inner `Channel` is Arc-backed; cloning this is effectively free.
#[derive(Clone)]
pub struct GeyserProviderClient {
    pub endpoint: String,
    /// Optional `x-token` auth value, set from config.
    token:   Option<String>,
    channel: Channel,
}

// ── Connection ────────────────────────────────────────────────────────────────

impl GeyserProviderClient {
    /// Connect (lazily) to a geyser endpoint.
    ///
    /// `endpoint` examples:
    ///   `https://mainnet.helius-rpc.com:443`                      (Helius)
    ///   `https://rpc.shyft.to:443`                                (Shyft)
    ///   `https://solana-yellowstone-grpc.publicnode.com:443`      (Triton)
    ///   `http://[::1]:10000`                                      (local/test)
    ///
    /// `token` — provider auth token injected as `x-token` metadata:
    ///   Helius  → your Helius API key
    ///   Shyft   → your Shyft x-api-key
    ///   Triton  → the token from your rpcpool plan
    pub async fn connect(endpoint: &str, token: Option<String>) -> anyhow::Result<Self> {
        let use_tls = endpoint.starts_with("https://");

        let channel = if use_tls {
            Channel::from_shared(endpoint.to_string())
                .with_context(|| format!("invalid geyser endpoint: {endpoint}"))?
                .tls_config(ClientTlsConfig::new().with_native_roots())
                .context("TLS config failed")?
                .connect_lazy()
        } else {
            Channel::from_shared(endpoint.to_string())
                .with_context(|| format!("invalid geyser endpoint: {endpoint}"))?
                .connect_lazy()
        };

        Ok(Self {
            endpoint: endpoint.to_string(),
            token,
            channel,
        })
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    fn raw_client(&self) -> GeyserClient<Channel> {
        GeyserClient::new(self.channel.clone())
            // Allow up to 64 MiB messages — geyser block updates can be large.
            .max_decoding_message_size(64 * 1024 * 1024)
    }

    /// Inject `x-token` into a tonic `Request` if this provider requires auth.
    fn attach_token<T>(&self, mut req: Request<T>) -> anyhow::Result<Request<T>> {
        if let Some(ref tok) = self.token {
            let val: MetadataValue<tonic::metadata::Ascii> =
                tok.parse().context("x-token contains non-ASCII characters")?;
            req.metadata_mut().insert("x-token", val);
        }
        Ok(req)
    }
}

// ── Public API ────────────────────────────────────────────────────────────────

impl GeyserProviderClient {
    /// Open a Yellowstone geyser subscription stream.
    ///
    /// Returns a `tonic::Streaming<SubscribeUpdate>` the caller drives with
    /// `.message().await`.  The mesh `server.rs` GeyserSubscribe handler
    /// wraps this and forwards events to the downstream client.
    ///
    /// Build the request with `GeyserSubscribeRequest` (below) or manually.
    ///
    /// # How it works
    /// Yellowstone uses a **bidirectional** stream: the client sends filter
    /// updates on one half, the server streams events on the other.  We send
    /// the initial filter then park the sender in a background task so the
    /// server never sees an EOF (which would close the stream).  When the
    /// caller drops the returned `Streaming`, the channel closes naturally and
    /// the background task exits.
    pub async fn subscribe(
        &self,
        request: SubscribeRequest,
    ) -> anyhow::Result<tonic::Streaming<SubscribeUpdate>> {
        let (tx, rx) = mpsc::channel::<SubscribeRequest>(8);
        tx.send(request)
            .await
            .context("failed to enqueue initial SubscribeRequest")?;

        // Keep `tx` alive in a background task so the bidi stream stays open.
        tokio::spawn(async move {
            let _keep_alive = tx;
            std::future::pending::<()>().await
        });

        let req = self.attach_token(Request::new(ReceiverStream::new(rx)))?;

        let stream = self
            .raw_client()
            .subscribe(req)
            .await
            .map_err(|status| {
                anyhow::anyhow!(
                    "GeyserSubscribe failed to {} — gRPC {:?} (code {}): {}",
                    self.endpoint,
                    status.code(),
                    status.code() as i32,
                    status.message(),
                )
            })?
            .into_inner();

        Ok(stream)
    }

    /// Lightweight slot probe used by the registry health prober.
    ///
    /// Opens a minimal slot subscription, reads the first `Slot` update,
    /// then drops the stream.  Returns `(latency_ms, slot)` — same shape as
    /// `RpcClient::get_slot()` so the prober can treat all providers uniformly.
    pub async fn get_slot(&self) -> anyhow::Result<(u64, u64)> {
        let start = Instant::now();

        let req = GeyserSubscribeRequest::new()
            .slots()
            .commitment(CommitmentLevel::Confirmed)
            .build();

        let mut stream = self.subscribe(req).await?;

        // Drain until we get a real Slot update (skip Ping frames).
        let slot = loop {
            match tokio::time::timeout(
                std::time::Duration::from_secs(5),
                stream.message(),
            )
            .await
            {
                Ok(Ok(Some(update))) => {
                    use crate::geyser_proto::subscribe_update::UpdateOneof;
                    if let Some(UpdateOneof::Slot(s)) = update.update_oneof {
                        break s.slot;
                    }
                    // Ping / other — keep waiting
                }
                Ok(Ok(None)) => {
                    anyhow::bail!("geyser stream closed unexpectedly from {}", self.endpoint)
                }
                Ok(Err(e)) => {
                    anyhow::bail!(
                        "geyser stream error from {} — gRPC {:?} (code {}): {}",
                        self.endpoint,
                        e.code(),
                        e.code() as i32,
                        e.message(),
                    )
                }
                Err(_) => {
                    anyhow::bail!("geyser probe timed out from {}", self.endpoint)
                }
            }
        };

        Ok((start.elapsed().as_millis() as u64, slot))
    }

    /// Fetch the provider's version string (cheap unary call — useful for
    /// startup diagnostics and confirming auth works).
    pub async fn version(&self) -> anyhow::Result<String> {
        let req = self.attach_token(Request::new(GetVersionRequest {}))?;
        let resp = self
            .raw_client()
            .get_version(req)
            .await
            .map_err(|status| {
                anyhow::anyhow!(
                    "GetVersion failed to {} — gRPC {:?} (code {}): {}",
                    self.endpoint,
                    status.code(),
                    status.code() as i32,
                    status.message(),
                )
            })?
            .into_inner();
        Ok(resp.version)
    }
}

// ── Subscription builder ──────────────────────────────────────────────────────

/// Ergonomic builder for `SubscribeRequest`.
///
/// Each filter gets an auto-generated label.  You can also add multiple
/// account / transaction filters in one request by calling the method more
/// than once with different arguments — each call inserts a new keyed entry.
///
/// # Example
/// ```rust
/// // Subscribe to all token-program account changes + all slots,
/// // excluding failed transactions, at Confirmed commitment.
/// let req = GeyserSubscribeRequest::new()
///     .accounts(
///         vec![],  // any account …
///         vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into()], // … owned by SPL Token
///     )
///     .transactions(
///         vec!["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA".into()],
///         vec![], false, false,
///     )
///     .slots()
///     .commitment(CommitmentLevel::Confirmed)
///     .build();
/// ```
#[derive(Default)]
pub struct GeyserSubscribeRequest {
    inner:         SubscribeRequest,
    label_counter: u32,
}

impl GeyserSubscribeRequest {
    pub fn new() -> Self {
        Self::default()
    }

    fn label(&mut self, prefix: &str) -> String {
        self.label_counter += 1;
        format!("{prefix}_{}", self.label_counter)
    }

    /// Subscribe to account data changes.
    /// `accounts` — specific pubkeys (base58); empty = match all accounts.
    /// `owners`   — filter by program owner pubkey; empty = any owner.
    pub fn accounts(mut self, accounts: Vec<String>, owners: Vec<String>) -> Self {
        let label = self.label("acct");
        self.inner.accounts.insert(
            label,
            SubscribeRequestFilterAccounts {
                account: accounts,
                owner:   owners,
                ..Default::default()
            },
        );
        self
    }

    /// Subscribe to slot status updates (processed / confirmed / finalized).
    pub fn slots(mut self) -> Self {
        let label = self.label("slot");
        self.inner.slots.insert(
            label,
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
            },
        );
        self
    }

    /// Subscribe to transactions.
    ///
    /// `include`  — account pubkeys that must appear in the transaction.
    /// `exclude`  — account pubkeys that must NOT appear.
    /// `votes`    — if true, include vote transactions.
    /// `failed`   — if true, include failed transactions.
    pub fn transactions(
        mut self,
        include: Vec<String>,
        exclude: Vec<String>,
        votes:   bool,
        failed:  bool,
    ) -> Self {
        let label = self.label("tx");
        self.inner.transactions.insert(
            label,
            SubscribeRequestFilterTransactions {
                vote:            Some(votes),
                failed:          Some(failed),
                account_include: include,
                account_exclude: exclude,
                ..Default::default()
            },
        );
        self
    }

    /// Set the commitment level applied to all filters in this request.
    pub fn commitment(mut self, level: CommitmentLevel) -> Self {
        self.inner.commitment = Some(level as i32);
        self
    }

    pub fn build(self) -> SubscribeRequest {
        self.inner
    }
}

// ── Convenience re-exports used by server.rs ──────────────────────────────────
pub use crate::geyser_proto::{
    subscribe_update::UpdateOneof as GeyserUpdateKind,
    CommitmentLevel as GeyserCommitmentLevel,
    SubscribeUpdate  as GeyserUpdate,
};