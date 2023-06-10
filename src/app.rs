use crate::raft_server_rpc::append_entries_request::Entry;
use std::error::Error;
use std::fmt::Debug;
use tokio::task::JoinHandle;

pub trait App: Send + Sync + Debug {
    // todo maybe self as mut if possible
    fn run(&self, entry: Entry) -> JoinHandle<Result<AppResult, Box<dyn Error + Send + Sync>>>;
    fn query(
        &self,
        payload: Vec<u8>,
    ) -> JoinHandle<Result<AppResult, Box<dyn Error + Send + Sync>>>;
}

#[derive(Debug, Clone)]
pub struct AppResult {
    pub success: bool,
    pub payload: Vec<u8>,
}
