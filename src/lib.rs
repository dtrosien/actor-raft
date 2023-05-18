extern crate core;

pub mod raft_client;
pub mod raft_server;

pub mod raft_rpc {
    tonic::include_proto!("raft_proto"); // The string specified here must match the proto package name
}
