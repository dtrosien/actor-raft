use crate::raft_server_rpc::append_entries_request::Entry;
use futures_util::future::BoxFuture;
use std::error::Error;
use std::fmt::Debug;
use tokio::task::JoinHandle;

pub trait App: Send + Sync + Debug {
    // todo return future or make async with async trait crate
    fn run(&mut self, entry: Entry) -> JoinHandle<Result<AppResult, Box<dyn Error + Send + Sync>>>;
    fn query(
        &self,
        payload: Vec<u8>,
    ) -> JoinHandle<Result<AppResult, Box<dyn Error + Send + Sync>>>;

    // todo [crit feature]separate take_snapshot and install_snapshot?
    fn snapshot(&self) -> BoxFuture<'static, Result<AppResult, Box<dyn Error + Send + Sync>>>;
}

#[derive(Debug, Clone)]
pub struct AppResult {
    pub success: bool,
    pub payload: Vec<u8>,
}
