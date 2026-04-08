use std::{collections::HashMap, sync::Arc, time::Instant};
use serde_json::Value;
use dashmap::DashMap;
use crate::{
    mesh::scorer::NodeMetrics,
    provider::Provider,
    proto::{LandingResult, MeshResponse},
};

/// Fires a single call to one provider node and records the result into the
/// shared metrics registry.  Works transparently for both RPC and gRPC providers.
pub async fn send_to_node(
    node_id:    &str,
    provider:   &Provider,
    method:     &str,
    params:     Value,
    timeout_ms: u64,
    nodes:      &Arc<DashMap<String, NodeMetrics>>,
) -> anyhow::Result<MeshResponse> {
    let start = Instant::now();

    let result = tokio::time::timeout(
        std::time::Duration::from_millis(timeout_ms),
        provider.call(method, params),
    )
    .await;

    let latency = start.elapsed().as_millis() as u64;

    match result {
        Ok(Ok(json_result)) => {
            if let Some(mut m) = nodes.get_mut(node_id) {
                m.record(latency as f64, true);
            }
            let slot = json_result
                .get("context")
                .and_then(|c| c.get("slot"))
                .and_then(|s| s.as_u64())
                .unwrap_or(0);

            Ok(MeshResponse {
                result:     serde_json::to_vec(&json_result).unwrap_or_default(),
                routed_to:  node_id.to_string(),
                latency_ms: latency,
                slot,
                error:      String::new(),
            })
        }
        Ok(Err(e)) => {
            if let Some(mut m) = nodes.get_mut(node_id) {
                m.record(latency as f64, false);
            }
            Err(anyhow::anyhow!("error from {node_id}: {e}"))
        }
        Err(_timeout) => {
            if let Some(mut m) = nodes.get_mut(node_id) {
                m.record(timeout_ms as f64, false);
            }
            Err(anyhow::anyhow!("timeout from {node_id} after {timeout_ms}ms"))
        }
    }
}

/// Fires a transaction to all provided nodes simultaneously.
pub async fn multipath_send(
    node_ids:      Vec<String>,
    providers:     Arc<HashMap<String, Provider>>,
    serialized_tx: &[u8],
    nodes:         Arc<DashMap<String, NodeMetrics>>,
) -> Vec<LandingResult> {
    let tx_b58 = bs58::encode(serialized_tx).into_string();

    let mut handles = Vec::new();
    for node_id in node_ids {
        let provider = providers.get(&node_id).cloned();
        let nodes    = nodes.clone();
        let tx_b58   = tx_b58.clone();
        let node_id  = node_id.clone();

        handles.push(tokio::spawn(async move {
            let Some(provider) = provider else {
                return LandingResult {
                    node_id,
                    success: false,
                    latency_ms: 0,
                    error: "provider not found".into(),
                };
            };

            let start  = Instant::now();
            let params = serde_json::json!([tx_b58, {"encoding": "base58"}]);
            match tokio::time::timeout(
                std::time::Duration::from_millis(5000),
                provider.call("sendTransaction", params),
            )
            .await
            {
                Ok(Ok(_)) => {
                    let latency = start.elapsed().as_millis() as u64;
                    if let Some(mut m) = nodes.get_mut(&node_id) {
                        m.record(latency as f64, true);
                    }
                    LandingResult { node_id, success: true, latency_ms: latency, error: String::new() }
                }
                Ok(Err(e)) => {
                    let latency = start.elapsed().as_millis() as u64;
                    if let Some(mut m) = nodes.get_mut(&node_id) {
                        m.record(latency as f64, false);
                    }
                    LandingResult { node_id, success: false, latency_ms: latency, error: e.to_string() }
                }
                Err(_) => {
                    if let Some(mut m) = nodes.get_mut(&node_id) {
                        m.record(5000.0, false);
                    }
                    LandingResult { node_id, success: false, latency_ms: 5000, error: "timeout".into() }
                }
            }
        }));
    }

    let mut results = Vec::new();
    for h in handles {
        if let Ok(r) = h.await { results.push(r); }
    }
    results
}
