use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderConfig {
    pub id: String,

    /// Transport kind.
    ///   "rpc"    — plain JSON-RPC HTTP (default)
    ///   "geyser" — Yellowstone Dragon's Mouth gRPC geyser stream
    #[serde(default)]
    pub kind: Option<String>,

    /// HTTP JSON-RPC endpoint — used when kind = "rpc" (or unset).
    #[serde(default)]
    pub url: String,

    /// Yellowstone gRPC endpoint — used when kind = "geyser".
    /// Format: "https://host:port" (TLS) or "http://host:port" (plaintext).
    ///
    /// Helius:  https://mainnet.helius-rpc.com:443
    /// Shyft:   https://rpc.shyft.to:443
    /// Triton:  https://<your-pool>.rpcpool.com:443
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

#[derive(Debug, Deserialize)]
pub struct AppConfig {
    pub listen_addr:       String,
    pub probe_interval_ms: u64,
    pub providers:         Vec<ProviderConfig>,
}

pub fn load() -> anyhow::Result<AppConfig> {
    let cfg = config::Config::builder()
        .add_source(config::File::with_name("config").required(false))
        .add_source(config::Environment::with_prefix("MESH"))
        .set_default("listen_addr", "[::1]:50051")?
        .set_default("probe_interval_ms", 500u64)?
        .set_default(
            "providers",
            vec![
                std::collections::HashMap::from([
                    ("id".to_string(),   "helius-rpc".to_string()),
                    ("kind".to_string(), "rpc".to_string()),
                    ("url".to_string(),  "https://mainnet.helius-rpc.com/?api-key=YOUR_KEY".to_string()),
                ]),
            ],
        )?
        .build()?;

    Ok(cfg.try_deserialize()?)
}
