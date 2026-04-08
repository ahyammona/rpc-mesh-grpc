pub mod grpc_client;
pub mod rpc_client;

use anyhow::Context;
use serde_json::Value;

use crate::config::ProviderConfig;
use grpc_client::{GeyserProviderClient, GeyserSubscribeRequest, GeyserUpdate};
use rpc_client::RpcClient;

// ─────────────────────────────────────────────────────────────────────────────
// Provider — unified enum over every backend transport.
//
//   Rpc(RpcClient)            — plain JSON-RPC HTTP (Helius, Triton, etc.)
//   Geyser(GeyserProviderClient) — Yellowstone gRPC geyser stream
//                                  (Helius, Shyft, Triton Dragon's Mouth)
//
// The rest of the system (registry prober, executor, server) only touches
// the Provider enum — it never cares which transport is underneath.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub enum Provider {
    Rpc(RpcClient),
    Geyser(GeyserProviderClient),
}

impl Provider {
    /// Construct a `Provider` from config.
    ///
    /// `kind = "rpc"`    → JSON-RPC HTTP provider  (requires `url`)
    /// `kind = "geyser"` → Yellowstone geyser node (requires `grpc_endpoint`;
    ///                      `x_token` is optional but needed for Helius/Shyft/Triton)
    pub async fn from_config(cfg: &ProviderConfig) -> anyhow::Result<Self> {
        match cfg.kind.as_deref().unwrap_or("rpc") {
            "geyser" => {
                let endpoint = cfg
                    .grpc_endpoint
                    .as_deref()
                    .with_context(|| {
                        format!(
                            "provider '{}': grpc_endpoint is required for kind=\"geyser\"",
                            cfg.id
                        )
                    })?;
                let client =
                    GeyserProviderClient::connect(endpoint, cfg.x_token.clone()).await?;
                Ok(Self::Geyser(client))
            }
            _ => Ok(Self::Rpc(RpcClient::new(&cfg.url))),
        }
    }

    // ── Health probe — same interface for all transports ─────────────────────

    /// Probe the provider and return `(latency_ms, slot)`.
    /// Used by `registry::probe_loop_provider` every `probe_interval_ms`.
    pub async fn get_slot(&self) -> anyhow::Result<u64> {
        match self {
            Self::Rpc(c) => c.get_slot().await,
            Self::Geyser(g) => {
                let (_lat, slot) = g.get_slot().await?;
                Ok(slot)
            }
        }
    }

    // ── JSON-RPC call — only for Rpc providers ────────────────────────────────

    /// Fire a JSON-RPC call and return the `result` value.
    /// For `Geyser` providers this is not applicable; callers should use
    /// `subscribe_geyser` instead.
    pub async fn call(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        match self {
            Self::Rpc(c) => c.call(method, params).await,
            Self::Geyser(_) => anyhow::bail!(
                "Provider::call() is not supported on a geyser provider — use subscribe_geyser()"
            ),
        }
    }

    // ── Geyser streaming ──────────────────────────────────────────────────────

    /// Open a Yellowstone geyser subscription on this provider.
    ///
    /// Returns an error if called on a non-geyser provider — the mesh server
    /// only routes `GeyserSubscribe` RPCs to providers where `is_geyser()` is
    /// true, so this should never be hit in practice.
    pub async fn subscribe_geyser(
        &self,
        request: crate::geyser_proto::SubscribeRequest,
    ) -> anyhow::Result<tonic::Streaming<GeyserUpdate>> {
        match self {
            Self::Geyser(g) => g.subscribe(request).await,
            Self::Rpc(_) => anyhow::bail!(
                "subscribe_geyser() called on an RPC provider — configure kind=\"geyser\" instead"
            ),
        }
    }

    /// Build a subscription request using the ergonomic builder and open it.
    ///
    /// Convenience wrapper so `server.rs` doesn't need to import the builder.
    pub async fn subscribe_geyser_with(
        &self,
        build: impl FnOnce(GeyserSubscribeRequest) -> GeyserSubscribeRequest,
    ) -> anyhow::Result<tonic::Streaming<GeyserUpdate>> {
        let req = build(GeyserSubscribeRequest::new()).build();
        self.subscribe_geyser(req).await
    }

    // ── Introspection ─────────────────────────────────────────────────────────

    pub fn kind_label(&self) -> &'static str {
        match self {
            Self::Rpc(_)    => "rpc",
            Self::Geyser(_) => "geyser",
        }
    }

    /// Returns true if this provider can serve geyser subscriptions.
    pub fn is_geyser(&self) -> bool {
        matches!(self, Self::Geyser(_))
    }

    /// Returns true if this provider can serve JSON-RPC calls.
    pub fn is_rpc(&self) -> bool {
        matches!(self, Self::Rpc(_))
    }
}
