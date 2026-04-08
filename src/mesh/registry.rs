use dashmap::DashMap;
use std::{sync::Arc, time::{Duration, Instant}};
use tracing::{info, warn};
use crate::{mesh::scorer::NodeMetrics, provider::Provider};

/// Background loop that probes all registered providers every `interval`.
/// Uses get_slot() as the probe — cheap, returns latency + slot position.
/// Only probes RPC providers — geyser providers use a separate keepalive probe
/// so that upstream geyser failures never poison RPC node health metrics.
/// Computes slot_lag against the highest slot seen (cluster tip proxy).
pub async fn probe_loop_provider(
    nodes:    Arc<DashMap<String, NodeMetrics>>,
    clients:  Vec<(String, Provider)>,
    interval: Duration,
) {
    let rpc_clients: Vec<_> = clients.iter()
        .filter(|(_, p)| p.is_rpc())
        .cloned()
        .collect();

    let geyser_clients: Vec<_> = clients.iter()
        .filter(|(_, p)| p.is_geyser())
        .cloned()
        .collect();

    info!(
        "Health prober started — {} RPC + {} Geyser providers, interval {}ms",
        rpc_clients.len(),
        geyser_clients.len(),
        interval.as_millis()
    );

    // Geyser providers start healthy — their status is updated by the
    // streaming server when a subscribe succeeds or fails, not by this prober.
    // This prevents unreachable upstream endpoints from poisoning the scorer
    // and blocking geyser routing entirely.
    for (id, _) in &geyser_clients {
        if let Some(mut m) = nodes.get_mut(id) {
            m.error_rate = 0.0;
            m.latency_ms = 200.0;
            m.slot_lag   = 0;
        }
    }

    loop {
        let probe_start = Instant::now();
        let mut results: Vec<(String, u64, f64, bool)> = Vec::new();
        let mut cluster_tip: u64 = 0;

        // Fire RPC probes concurrently
        let mut handles = Vec::new();
        for (id, provider) in &rpc_clients {
            let id       = id.clone();
            let provider = provider.clone();
            handles.push(tokio::spawn(async move {
                let start = Instant::now();
                match provider.get_slot().await {
                    Ok(slot) => {
                        let lat = start.elapsed().as_millis() as f64;
                        (id, slot, lat, true)
                    }
                    Err(e) => {
                        warn!(node = %id, err = %e, "Provider probe failed");
                        (id, 0u64, 9999.0, false)
                    }
                }
            }));
        }

        for handle in handles {
            if let Ok((id, slot, lat, ok)) = handle.await {
                if slot > cluster_tip { cluster_tip = slot; }
                results.push((id, slot, lat, ok));
            }
        }

        // Update RPC metrics now that we know the cluster tip
        for (id, slot, latency, success) in results {
            let lag = cluster_tip.saturating_sub(slot);
            if let Some(mut metrics) = nodes.get_mut(&id) {
                metrics.set_slot_lag(lag);
                metrics.record(latency, success);
                tracing::debug!(
                    node       = %id,
                    latency_ms = %latency,
                    slot_lag   = %lag,
                    status     = %metrics.status,
                    "Probe result"
                );
            }
        }

        // Geyser keepalive: attempt a quick get_slot ping to measure latency,
        // but only update latency — never record failures that would flip status.
        for (id, provider) in &geyser_clients {
            let id       = id.clone();
            let provider = provider.clone();
            let nodes_c  = nodes.clone();
            tokio::spawn(async move {
                let start = Instant::now();
                if let Ok(_slot) = provider.get_slot().await {
                    let lat = start.elapsed().as_millis() as f64;
                    if let Some(mut m) = nodes_c.get_mut(&id) {
                        // Only update latency on success — don't touch error_rate
                        let alpha = 0.25;
                        m.latency_ms = alpha * lat + (1.0 - alpha) * m.latency_ms;
                        m.last_seen  = Instant::now();
                        tracing::debug!(node = %id, latency_ms = %lat, "Geyser keepalive ok");
                    }
                }
                // Silently ignore failures — geyser health is managed by the stream server
            });
        }

        let elapsed = probe_start.elapsed();
        if let Some(remaining) = interval.checked_sub(elapsed) {
            tokio::time::sleep(remaining).await;
        }
    }
}