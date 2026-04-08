use std::{collections::HashMap, pin::Pin, sync::Arc, time::{Duration, Instant}};
use dashmap::DashMap;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tonic::{Request, Response, Status};

use crate::{
    geyser_proto::{
        geyser_server::Geyser,
        GetVersionRequest, GetVersionResponse,
        SubscribeRequest  as GeyserSubscribeRequest,
        SubscribeUpdate   as GeyserSubscribeUpdate,
    },
    mesh::{
        classifier::{Classifier, RoutingStrategy, methods_from_flags},
        executor::{multipath_send, send_to_node},
        router::Router,
        scorer::NodeMetrics,
    },
    proto::{
        rpc_mesh_server::RpcMesh,
        HealthRequest, MeshRequest, MeshResponse,
        NodeHealth, RoutingEvent, RoutingWatchRequest,
        TransactionRequest, TransactionResponse,
    },
    provider::Provider,
};

type BoxStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send>>;

#[derive(Clone)]
pub struct MeshServer {
    router:     Arc<Router>,
    classifier: Arc<Classifier>,
    clients:    Arc<HashMap<String, Provider>>,
    nodes:      Arc<DashMap<String, NodeMetrics>>,
    routing_tx: broadcast::Sender<RoutingEvent>,
    health_tx:  broadcast::Sender<NodeHealth>,
}

impl MeshServer {
    pub fn new(
        router:     Arc<Router>,
        classifier: Arc<Classifier>,
        clients:    Arc<HashMap<String, Provider>>,
        nodes:      Arc<DashMap<String, NodeMetrics>>,
    ) -> Self {
        let (routing_tx, _) = broadcast::channel(256);
        let (health_tx,  _) = broadcast::channel(256);

        // Background task: push NodeHealth snapshots every 500ms.
        let health_tx_c = health_tx.clone();
        let nodes_c     = nodes.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                for entry in nodes_c.iter() {
                    let m = entry.value();
                    let _ = health_tx_c.send(NodeHealth {
                        node_id:    entry.key().clone(),
                        latency_ms: m.latency_ms as u64,
                        slot_lag:   m.slot_lag,
                        error_rate: m.error_rate as f32,
                        cost_score: (1.0 / (m.cost_per_cu + 0.001)) as f32,
                        status:     m.status.to_string(),
                    });
                }
            }
        });

        Self { router, classifier, clients, nodes, routing_tx, health_tx }
    }

    fn emit_routing(
        &self, method: &str, strategy: &RoutingStrategy,
        node_id: &str, latency_ms: u64, success: bool, slot: u64,
    ) {
        let _ = self.routing_tx.send(RoutingEvent {
            method:     method.to_string(),
            strategy:   strategy.to_string(),
            node_id:    node_id.to_string(),
            latency_ms, success, slot,
        });
    }

    // ── Geyser routing helper ─────────────────────────────────────────────────

    /// Select the best geyser-capable provider using the same scoring used for
    /// RPC — `StableWs` strategy (lowest ws_drop_rate + latency), which maps
    /// naturally to a long-lived streaming connection.
    ///
    /// Falls back to `LowestSlotLag` if no StableWs candidate is available
    /// (all providers equally stable at startup, so slot freshness wins).
    fn select_geyser_node(&self) -> Option<String> {
        // Prefer nodes that (a) are geyser providers AND (b) score best on
        // StableWs.  The DashMap already has all providers; we filter by kind.
        let geyser_ids: std::collections::HashSet<String> = self
            .clients
            .iter()
            .filter(|(_, p)| p.is_geyser())
            .map(|(id, _)| id.clone())
            .collect();

        if geyser_ids.is_empty() {
            return None;
        }

        // Score only geyser nodes.
        self.nodes
            .iter()
            .filter(|e| {
                geyser_ids.contains(e.key())
                    && e.status != crate::mesh::scorer::NodeStatus::Down
            })
            .max_by(|a, b| {
                // Primary: StableWs (ws_drop_rate + latency)
                let sa = a.score_for(&RoutingStrategy::StableWs);
                let sb = b.score_for(&RoutingStrategy::StableWs);
                // Secondary tiebreak: LowestSlotLag
                let ta = a.score_for(&RoutingStrategy::LowestSlotLag);
                let tb = b.score_for(&RoutingStrategy::LowestSlotLag);
                (sa + ta * 0.1)
                    .partial_cmp(&(sb + tb * 0.1))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|e| e.key().clone())
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// RpcMesh service — handles JSON-RPC routing (unchanged)
// ═════════════════════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl RpcMesh for MeshServer {

    // ── Send ──────────────────────────────────────────────────────────────────
    async fn send(
        &self,
        request: Request<MeshRequest>,
    ) -> Result<Response<MeshResponse>, Status> {
        let req = request.into_inner();

        let method_set = req.methods.unwrap_or_default();
        let methods    = methods_from_flags(&method_set);

        if methods.is_empty() {
            return Err(Status::invalid_argument("No method flags set in RpcMethodSet"));
        }

        let primary_method = methods[0];
        let profile        = self.classifier.classify(primary_method);
        let strategy       = &profile.strategy;

        let node_id = self.router
            .select_rpc(strategy, &self.clients)
            .or_else(|| self.router.best_rpc_fallback(&self.clients))
            .ok_or_else(|| Status::unavailable("No healthy RPC nodes available"))?;
        let client = self.clients
            .get(&node_id)
            .ok_or_else(|| Status::internal(format!("No client for node {node_id}")))?;

        let params = build_params(&req, primary_method);

        match send_to_node(&node_id, client, primary_method, params, profile.timeout_ms, &self.nodes).await {
            Ok(resp) => {
                self.emit_routing(primary_method, strategy, &node_id, resp.latency_ms, true, resp.slot);
                Ok(Response::new(resp))
            }
            Err(e) if profile.critical => {
                if let Some(fallback) = self.router.best_rpc_fallback(&self.clients) {
                    if fallback != node_id {
                        if let Some(fb_client) = self.clients.get(&fallback) {
                            let params = build_params(&req, primary_method);
                            if let Ok(resp) = send_to_node(
                                &fallback, fb_client, primary_method, params,
                                profile.timeout_ms, &self.nodes,
                            ).await {
                                self.emit_routing(primary_method, strategy, &fallback, resp.latency_ms, true, resp.slot);
                                return Ok(Response::new(resp));
                            }
                        }
                    }
                }
                Err(Status::internal(e.to_string()))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }

    // ── SendTransaction ───────────────────────────────────────────────────────
    async fn send_transaction(
        &self,
        request: Request<TransactionRequest>,
    ) -> Result<Response<TransactionResponse>, Status> {
        let req = request.into_inner();
        let node_ids = if req.preferred_nodes.is_empty() {
            self.router.all_healthy()
        } else {
            req.preferred_nodes.clone()
        };

        if node_ids.is_empty() {
            return Err(Status::unavailable("No healthy nodes for sendTransaction"));
        }

        let results = multipath_send(
            node_ids, self.clients.clone(), &req.serialized_tx, self.nodes.clone(),
        ).await;

        let signature = results.iter()
            .find(|r| r.success)
            .map(|_| "pending".to_string())
            .unwrap_or_default();

        Ok(Response::new(TransactionResponse { signature, results }))
    }

    // ── WatchHealth ───────────────────────────────────────────────────────────
    type WatchHealthStream = BoxStream<NodeHealth>;

    async fn watch_health(
        &self,
        _: Request<HealthRequest>,
    ) -> Result<Response<Self::WatchHealthStream>, Status> {
        let rx = self.health_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|r| r.ok()).map(Ok);
        Ok(Response::new(Box::pin(stream)))
    }

    // ── WatchRouting ──────────────────────────────────────────────────────────
    type WatchRoutingStream = BoxStream<RoutingEvent>;

    async fn watch_routing(
        &self,
        _: Request<RoutingWatchRequest>,
    ) -> Result<Response<Self::WatchRoutingStream>, Status> {
        let rx = self.routing_tx.subscribe();
        let stream = BroadcastStream::new(rx).filter_map(|r| r.ok()).map(Ok);
        Ok(Response::new(Box::pin(stream)))
    }
}

// ═════════════════════════════════════════════════════════════════════════════
// Geyser service — routes Yellowstone Dragon's Mouth subscriptions
// ═════════════════════════════════════════════════════════════════════════════

#[tonic::async_trait]
impl Geyser for MeshServer {

    // ── Subscribe — the core geyser routing entrypoint ────────────────────────
    //
    // Flow:
    //   1. Client sends a SubscribeRequest with account/tx/slot filters.
    //   2. We score all geyser-capable providers (StableWs + LowestSlotLag)
    //      and pick the best healthy one — same logic as RPC routing.
    //   3. We open a real Yellowstone stream to that provider.
    //   4. We forward every SubscribeUpdate from the provider back to the client.
    //   5. If the provider stream dies, we automatically failover to the next
    //      best geyser node and resume streaming — the client never sees a gap.
    //
    // Auth: the client's original request filters are preserved verbatim and
    // forwarded to the upstream provider.  Only the `x-token` header is added
    // server-side (from the provider's config), so the client never needs to
    // know which upstream they're hitting.
    type SubscribeStream = BoxStream<GeyserSubscribeUpdate>;

    async fn subscribe(
        &self,
        request: Request<tonic::Streaming<GeyserSubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // Read the first filter from the client's bidi stream.
        // We use only the initial request for routing — if the client sends
        // filter updates later, they're discarded (the upstream bidi stream
        // is already open). Full bidi re-routing is a future extension.
        let mut inbound = request.into_inner();
        let client_req = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("failed to read SubscribeRequest: {e}")))?
            .ok_or_else(|| Status::invalid_argument("empty SubscribeRequest stream"))?;

        // Pick the best geyser provider.
        let node_id = self
            .select_geyser_node()
            .ok_or_else(|| {
                Status::unavailable(
                    "No healthy geyser providers available. \
                     Add kind=\"geyser\" entries in config.toml.",
                )
            })?;

        let provider = self
            .clients
            .get(&node_id)
            .ok_or_else(|| Status::internal(format!("No client for geyser node {node_id}")))?
            .clone();

        tracing::info!(
            node = %node_id,
            accounts  = client_req.accounts.len(),
            slots     = client_req.slots.len(),
            txns      = client_req.transactions.len(),
            "GeyserSubscribe: routing to node"
        );

        // Clone what we need to move into the streaming task.
        let clients    = self.clients.clone();
        let nodes      = self.nodes.clone();
        let routing_tx = self.routing_tx.clone();
        let req_clone  = client_req.clone();

        // Build the output stream.
        // We use an mpsc so we can failover without the client noticing.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<GeyserSubscribeUpdate, Status>>(256);

        tokio::spawn(async move {
            let mut current_node = node_id.clone();
            let mut current_provider = provider;
            let mut backoff_ms: u64 = 100; // start at 100ms, cap at 5s

            'outer: loop {
                let start = Instant::now();

                // Open the stream to the chosen provider.
                let mut stream = match current_provider.subscribe_geyser(req_clone.clone()).await {
                    Ok(s)  => s,
                    Err(e) => {
                        tracing::warn!(node = %current_node, err = %e, "GeyserSubscribe: open failed");
                        // Record a WS drop only — do NOT call record(9999, false).
                        // The registry prober intentionally never fails geyser nodes
                        // so that a temporarily unreachable upstream doesn't poison the
                        // scorer and lock out all geyser routing.  An open-failure here
                        // is the same class of event: transient connectivity, not a
                        // sustained quality degradation.  Using record_ws_drop keeps the
                        // error_rate clean while still penalising ws_drop_rate, which
                        // drives the StableWs score down and causes the router to prefer
                        // the other provider on the next selection.
                        if let Some(mut m) = nodes.get_mut(&current_node) {
                            m.record_ws_drop();
                        }
                        if let Some((fb_id, fb_prov)) =
                            pick_geyser_failover(&clients, &nodes, &current_node)
                        {
                            tracing::info!(from = %current_node, to = %fb_id, delay_ms = %backoff_ms, "GeyserSubscribe: failing over");
                            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                            backoff_ms = (backoff_ms * 2).min(5_000);
                            current_node     = fb_id;
                            current_provider = fb_prov;
                            continue 'outer;
                        }
                        let _ = tx.send(Err(Status::unavailable(format!(
                            "All geyser providers failed: {e}"
                        )))).await;
                        break;
                    }
                };

                // Forward events.
                loop {
                    match stream.message().await {
                        Ok(Some(update)) => {
                            // Update latency metric on every slot event.
                            if let Some(crate::geyser_proto::subscribe_update::UpdateOneof::Slot(ref s)) =
                                update.update_oneof
                            {
                                let lat = start.elapsed().as_millis() as f64;
                                if let Some(mut m) = nodes.get_mut(&current_node) {
                                    m.record(lat, true);
                                    m.set_slot_lag(0); // geyser is always at tip
                                }
                                // Stream is alive and delivering — reset backoff.
                                backoff_ms = 100;
                                let _ = routing_tx.send(RoutingEvent {
                                    method:     "geyser.Subscribe".into(),
                                    strategy:   "stable_ws".into(),
                                    node_id:    current_node.clone(),
                                    latency_ms: lat as u64,
                                    success:    true,
                                    slot:       s.slot,
                                });
                            }

                            if tx.send(Ok(update)).await.is_err() {
                                // Client disconnected — clean shutdown.
                                tracing::debug!(node = %current_node, "GeyserSubscribe: client disconnected");
                                break 'outer;
                            }
                        }
                        Ok(None) => {
                            // Provider closed the stream gracefully — failover.
                            tracing::warn!(node = %current_node, "GeyserSubscribe: stream ended, failing over");
                            if let Some(mut m) = nodes.get_mut(&current_node) {
                                m.record_ws_drop();
                            }
                            if let Some((fb_id, fb_prov)) =
                                pick_geyser_failover(&clients, &nodes, &current_node)
                            {
                                tracing::info!(from = %current_node, to = %fb_id, delay_ms = %backoff_ms, "GeyserSubscribe: failover");
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                backoff_ms = (backoff_ms * 2).min(5_000);
                                current_node     = fb_id;
                                current_provider = fb_prov;
                                continue 'outer;
                            }
                            let _ = tx.send(Err(Status::unavailable("All geyser providers exhausted"))).await;
                            break 'outer;
                        }
                        Err(e) => {
                            tracing::warn!(node = %current_node, err = %e, "GeyserSubscribe: stream error, failing over");
                            if let Some(mut m) = nodes.get_mut(&current_node) {
                                m.record(9999.0, false);
                                m.record_ws_drop();
                            }
                            if let Some((fb_id, fb_prov)) =
                                pick_geyser_failover(&clients, &nodes, &current_node)
                            {
                                tracing::info!(from = %current_node, to = %fb_id, delay_ms = %backoff_ms, "GeyserSubscribe: failover");
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                backoff_ms = (backoff_ms * 2).min(5_000);
                                current_node     = fb_id;
                                current_provider = fb_prov;
                                continue 'outer;
                            }
                            let _ = tx.send(Err(Status::unavailable(format!("Geyser stream error: {e}")))).await;
                            break 'outer;
                        }
                    }
                }
            }
        });

        let out_stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(out_stream)))
    }

    // ── GetVersion — forward to the best geyser node ──────────────────────────
    async fn get_version(
        &self,
        _: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        let node_id = self
            .select_geyser_node()
            .ok_or_else(|| Status::unavailable("No geyser providers configured"))?;

        let provider = self
            .clients
            .get(&node_id)
            .ok_or_else(|| Status::internal("provider not found"))?;

        match provider {
            crate::provider::Provider::Geyser(g) => {
                let version = g
                    .version()
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(GetVersionResponse { version }))
            }
            _ => Err(Status::internal("selected node is not a geyser provider")),
        }
    }
}

// ── Failover helper ───────────────────────────────────────────────────────────

/// Pick the next best geyser node, excluding `exclude_id`.
fn pick_geyser_failover(
    clients:    &Arc<HashMap<String, Provider>>,
    nodes:      &Arc<DashMap<String, crate::mesh::scorer::NodeMetrics>>,
    exclude_id: &str,
) -> Option<(String, Provider)> {
    nodes
        .iter()
        .filter(|e| {
            e.key() != exclude_id
                && e.status != crate::mesh::scorer::NodeStatus::Down
                && clients
                    .get(e.key())
                    .map(|p| p.is_geyser())
                    .unwrap_or(false)
        })
        .max_by(|a, b| {
            a.score_for(&RoutingStrategy::StableWs)
                .partial_cmp(&b.score_for(&RoutingStrategy::StableWs))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .and_then(|e| {
            let id = e.key().clone();
            clients.get(&id).map(|p| (id, p.clone()))
        })
}

// ── JSON-RPC param builder (unchanged) ───────────────────────────────────────

fn build_params(req: &MeshRequest, method: &str) -> serde_json::Value {
    use serde_json::json;
    match method {
        "getAccountInfo" | "getBalance" => {
            let addr = req.account_include.first().cloned().unwrap_or_default();
            json!([addr, {"encoding": "base64", "commitment": "confirmed"}])
        }
        "getMultipleAccounts" => {
            json!([req.account_include, {"encoding": "base64", "commitment": "confirmed"}])
        }
        "getProgramAccounts" => {
            let program_id = req.account_include.first().cloned().unwrap_or_default();
            let mut config = json!({"encoding": "base64", "commitment": "confirmed"});
            if !req.account_require.is_empty() {
                config["filters"] = json!(req.account_require);
            }
            json!([program_id, config])
        }
        "getSlot" | "getBlockHeight" => json!([{"commitment": "confirmed"}]),
        "getLatestBlockhash"          => json!([{"commitment": "confirmed"}]),
        "getRecentPrioritizationFees" => json!([req.account_include]),
        "getTokenAccountsByOwner" => {
            let owner = req.account_include.first().cloned().unwrap_or_default();
            json!([owner, {"mint": req.account_require.first().cloned().unwrap_or_default()}, {"encoding": "jsonParsed"}])
        }
        "getSignaturesForAddress" => {
            let addr = req.account_include.first().cloned().unwrap_or_default();
            let mut config = json!({"commitment": "finalized", "limit": 10});
            if req.slot > 0 { config["minContextSlot"] = json!(req.slot); }
            json!([addr, config])
        }
        _ => json!([]),
    }
}