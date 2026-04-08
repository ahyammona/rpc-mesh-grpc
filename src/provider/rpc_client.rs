use serde_json::{json, Value};
use uuid::Uuid;

/// A thin async JSON-RPC HTTP client for a single Solana RPC provider.
/// Clone is cheap — the inner reqwest::Client is Arc-backed.
#[derive(Clone)]
pub struct RpcClient {
    url:    String,
    client: reqwest::Client,
}

impl RpcClient {
    pub fn new(url: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .expect("Failed to build HTTP client");
        Self { url: url.to_string(), client }
    }

    /// Fire a JSON-RPC 2.0 request and return the `result` field on success,
    /// or an error if the response contains an `error` field or the HTTP call fails.
    pub async fn call(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id":      Uuid::new_v4().to_string(),
            "method":  method,
            "params":  params,
        });

        let resp = self.client
            .post(&self.url)
            .json(&body)
            .send()
            .await?
            .json::<Value>()
            .await?;

        if let Some(err) = resp.get("error") {
            anyhow::bail!("JSON-RPC error: {}", err);
        }

        resp.get("result")
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("Missing 'result' in JSON-RPC response"))
    }

    pub async fn get_slot(&self) -> anyhow::Result<u64> {
        let result = self.call("getSlot", json!([{"commitment": "confirmed"}])).await?;
        result.as_u64().ok_or_else(|| anyhow::anyhow!("getSlot: unexpected type"))
    }
}