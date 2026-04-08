use std::time::Instant;
use crate::mesh::classifier::RoutingStrategy;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeStatus {
    Healthy,
    Degraded,
    Down,
}

impl std::fmt::Display for NodeStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Healthy  => write!(f, "healthy"),
            Self::Degraded => write!(f, "degraded"),
            Self::Down     => write!(f, "down"),
        }
    }
}

/// Live metrics for a single provider node, maintained via exponential moving average.
/// All updates are lock-free at the DashMap shard level.
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    /// Rolling average latency in milliseconds (EMA, α=0.25).
    pub latency_ms:   f64,
    /// How many slots behind the cluster tip this node is.
    pub slot_lag:     u64,
    /// Rolling error rate 0.0–1.0 (EMA, α=0.25).
    pub error_rate:   f64,
    /// Estimated cost per compute unit (provider-specific, set from config).
    pub cost_per_cu:  f64,
    /// WebSocket disconnect rate 0.0–1.0 (EMA, α=0.1).
    pub ws_drop_rate: f64,
    /// Timestamp of last successful probe.
    pub last_seen:    Instant,
    pub status:       NodeStatus,
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self {
            latency_ms:   200.0,
            slot_lag:     0,
            error_rate:   0.0,
            cost_per_cu:  1.0,
            ws_drop_rate: 0.0,
            last_seen:    Instant::now(),
            status:       NodeStatus::Healthy,
        }
    }
}

impl NodeMetrics {
    /// Compute a 0.0–1.0 score optimised for the given routing strategy.
    /// Higher is better. Down nodes always score 0.
    pub fn score_for(&self, strategy: &RoutingStrategy) -> f64 {
        if self.status == NodeStatus::Down {
            return 0.0;
        }

        match strategy {
            RoutingStrategy::Fastest => {
                // 70% latency, 30% reliability
                let latency = 1.0 / (self.latency_ms + 1.0);
                let reliable = 1.0 - self.error_rate;
                latency * 0.7 + reliable * 0.3
            }
            RoutingStrategy::LowestSlotLag => {
                // 70% slot freshness, 30% latency
                let lag     = 1.0 / (self.slot_lag as f64 + 1.0);
                let latency = 1.0 / (self.latency_ms + 1.0);
                lag * 0.7 + latency * 0.3
            }
            RoutingStrategy::MostReliable => {
                // pure reliability — error rate is the only metric
                1.0 - self.error_rate
            }
            RoutingStrategy::Cheapest => {
                // pure cost — lower cost_per_cu wins
                1.0 / (self.cost_per_cu + 0.001)
            }
            RoutingStrategy::StableWs => {
                // 80% ws stability, 20% latency
                let stable  = 1.0 - self.ws_drop_rate;
                let latency = 1.0 / (self.latency_ms + 1.0);
                stable * 0.8 + latency * 0.2
            }
            RoutingStrategy::Multipath => {
                // for multipath we include all healthy nodes — score is binary
                if self.status == NodeStatus::Healthy { 1.0 } else { 0.0 }
            }
        }
    }

    /// Update metrics from a completed probe or request.
    /// Uses EMA so recent observations dominate without throwing away history.
    pub fn record(&mut self, latency_ms: f64, success: bool) {
        let alpha = 0.25;
        self.latency_ms = alpha * latency_ms + (1.0 - alpha) * self.latency_ms;
        self.error_rate = alpha * (if success { 0.0 } else { 1.0 })
                        + (1.0 - alpha) * self.error_rate;
        self.last_seen = Instant::now();
        self.recompute_status();
    }

    /// Record a WebSocket disconnect event.
    pub fn record_ws_drop(&mut self) {
        let alpha = 0.1;  // slower decay for ws stability
        self.ws_drop_rate = alpha * 1.0 + (1.0 - alpha) * self.ws_drop_rate;
    }

    /// Update slot lag and recompute status.
    pub fn set_slot_lag(&mut self, lag: u64) {
        self.slot_lag = lag;
        self.recompute_status();
    }

    fn recompute_status(&mut self) {
        self.status = if self.error_rate > 0.6 {
            NodeStatus::Down
        } else if self.slot_lag > 10 || self.error_rate > 0.2 {
            NodeStatus::Degraded
        } else {
            NodeStatus::Healthy
        };
    }
}