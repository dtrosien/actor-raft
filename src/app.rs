use crate::raft_server_rpc::append_entries_request::Entry;
use futures_util::future::BoxFuture;
use std::error::Error;
use std::fmt::Debug;

pub trait App: Send + Sync + Debug {
    fn run(
        &mut self,
        entry: Entry,
    ) -> BoxFuture<'_, Result<AppResult, Box<dyn Error + Send + Sync>>>;

    fn query(
        &self,
        payload: Vec<u8>,
    ) -> BoxFuture<'_, Result<AppResult, Box<dyn Error + Send + Sync>>>;
}

#[derive(Debug, Clone)]
pub struct AppResult {
    pub success: bool,
    pub payload: Vec<u8>,
}
