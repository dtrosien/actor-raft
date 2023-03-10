extern crate core;

mod actors;
mod config;
pub mod raft;
mod rpc;
mod db;

mod raft_rpc {
    tonic::include_proto!("raft_proto"); // The string specified here must match the proto package name
}
