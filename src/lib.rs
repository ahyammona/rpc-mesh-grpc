pub mod config;
pub mod mesh;
pub mod provider;

pub mod proto {
    tonic::include_proto!("rpcmesh.v1");
}

pub mod geyser_proto {
    tonic::include_proto!("geyser");
}
