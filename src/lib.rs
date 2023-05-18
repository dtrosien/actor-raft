extern crate core;

mod actors;
mod config;
mod db;
pub mod raft;
pub mod raft_handles;
mod rpc;
mod state_meta;

pub mod raft_rpc {
    tonic::include_proto!("raft_proto"); // The string specified here must match the proto package name
}
