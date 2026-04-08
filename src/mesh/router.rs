use dashmap::DashMap;
use std::sync::Arc;
use crate::mesh::{
    classifier::RoutingStrategy,
    scorer::{NodeMetrics, NodeStatus},
};
use crate::provider::Provider;
use std::collections::HashMap;

/// Routes requests to the best node based on live scoring.
/// All reads from the DashMap are concurrent and lock-free at the shard level.
pub struct Router {
    pub nodes: Arc<DashMap<String, NodeMetrics>>,
}

impl Router {
    pub fn new(nodes: Arc<DashMap<String, NodeMetrics>>) -> Self {
        Self { nodes }
    }

    /// Select the single best node for a given routing strategy.
    /// Returns None if no healthy node is available.
    pub fn select(&self, strategy: &RoutingStrategy) -> Option<String> {
        self.nodes
            .iter()
            .filter(|e| e.status != NodeStatus::Down)
            .max_by(|a, b| {
                a.score_for(strategy)
                    .partial_cmp(&b.score_for(strategy))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|e| e.key().clone())
    }

    /// Return all healthy nodes — used for Multipath (sendTransaction).
    pub fn all_healthy(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|e| e.status == NodeStatus::Healthy)
            .map(|e| e.key().clone())
            .collect()
    }

    /// Fallback: return the single node with highest overall composite score,
    /// regardless of strategy. Used when the primary selection fails.
    pub fn best_fallback(&self) -> Option<String> {
        self.nodes
            .iter()
            .filter(|e| e.status != NodeStatus::Down)
            .max_by(|a, b| {
                let sa = composite_score(&a);
                let sb = composite_score(&b);
                sa.partial_cmp(&sb).unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|e| e.key().clone())
    }
       pub fn select_rpc(
        &self,
        strategy: &RoutingStrategy,
        clients: &HashMap<String, Provider>,
    ) -> Option<String> {
        self.nodes
            .iter()
            .filter(|e| {
                e.status != NodeStatus::Down
                    && clients.get(e.key()).map_or(false, |p| p.is_rpc())
            })
            .max_by(|a, b| {
                a.score_for(strategy)
                    .partial_cmp(&b.score_for(strategy))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|e| e.key().clone())
    }

    pub fn best_rpc_fallback(&self, clients: &HashMap<String, Provider>) -> Option<String> {
        self.nodes
            .iter()
            .filter(|e| {
                e.status != NodeStatus::Down
                    && clients.get(e.key()).map_or(false, |p| p.is_rpc())
            })
            .max_by(|a, b| {
                composite_score(&a)
                    .partial_cmp(&composite_score(&b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|e| e.key().clone())
    }
}

fn composite_score(e: &dashmap::mapref::multiple::RefMulti<String, NodeMetrics>) -> f64 {
    let latency  = 1.0 / (e.latency_ms + 1.0);
    let lag      = 1.0 / (e.slot_lag as f64 + 1.0);
    let reliable = 1.0 - e.error_rate;
    let cost     = 1.0 / (e.cost_per_cu + 0.001);
    latency * 0.40 + lag * 0.35 + reliable * 0.15 + cost * 0.10
}