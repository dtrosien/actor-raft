extern crate core;

pub mod app;
#[cfg(feature = "client")]
pub mod raft_client;
#[cfg(feature = "server")]
pub mod raft_server;
pub mod snapshot;

pub mod raft_server_rpc {
    tonic::include_proto!("raft_server_proto"); // The string specified here must match the proto package name
}
pub mod raft_client_rpc {
    tonic::include_proto!("raft_client_proto"); // The string specified here must match the proto package name
}
