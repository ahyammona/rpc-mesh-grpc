use std::{sync::Arc, time::Duration};
use dashmap::DashMap;
use tonic::transport::Server;
use tracing::info;

mod config;
mod mesh;
mod provider;

use mesh::{
    classifier::Classifier,
    registry,
    router::Router,
    scorer::NodeMetrics,
    server::MeshServer,
};
use provider::Provider;

pub mod proto {
    tonic::include_proto!("rpcmesh.v1");
}

pub mod geyser_proto {
    tonic::include_proto!("geyser");
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "rpc_mesh=info".into()),
        )
        .init();

    let cfg = config::load()?;
    info!("Loaded {} providers", cfg.providers.len());

    let nodes: Arc<DashMap<String, NodeMetrics>> = Arc::new(DashMap::new());

    // Build providers and seed the registry.
    let mut probe_providers: Vec<(String, Provider)> = Vec::new();
    for p in &cfg.providers {
        nodes.insert(p.id.clone(), NodeMetrics::default());
        let provider = Provider::from_config(p).await?;
        info!(
            id   = %p.id,
            kind = %provider.kind_label(),
            "Registered provider"
        );
        probe_providers.push((p.id.clone(), provider));
    }

    // Background health prober — works for both RPC and geyser providers.
    let nodes_for_prober = nodes.clone();
    let probe_interval   = Duration::from_millis(cfg.probe_interval_ms);
    tokio::spawn(async move {
        registry::probe_loop_provider(nodes_for_prober, probe_providers, probe_interval).await;
    });

    let router     = Arc::new(Router::new(nodes.clone()));
    let classifier = Arc::new(Classifier::new());

    // Build execution clients (one per provider).
    let mut exec_providers = std::collections::HashMap::new();
    for p in &cfg.providers {
        exec_providers.insert(p.id.clone(), Provider::from_config(p).await?);
    }
    let exec_providers = Arc::new(exec_providers);

    let svc = MeshServer::new(router, classifier, exec_providers, nodes.clone());

    let addr = cfg.listen_addr.parse()?;
    info!("rpc-mesh listening on {addr}  (RpcMesh + Geyser services)");

    Server::builder()
        // RpcMesh service — JSON-RPC routing
        .add_service(proto::rpc_mesh_server::RpcMeshServer::new(svc.clone()))
        // Geyser service — Yellowstone Dragon's Mouth routing
        .add_service(geyser_proto::geyser_server::GeyserServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
