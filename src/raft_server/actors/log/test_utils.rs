use crate::app::{App, AppResult};
use crate::raft_server_rpc::append_entries_request::Entry;
use std::error::Error;
use std::fmt::Debug;
use tracing::info;

#[derive(Debug)]
pub struct TestApp {}

impl App for TestApp {
    #[tracing::instrument(ret, level = "debug")]
    fn run(&self, entry: Entry) -> Result<AppResult, Box<(dyn Error + Send + Sync)>> {
        let msg = entry.payload.as_str();
        info!("the following payload was executed in TestApp: {}", msg);

        let result_payload = bincode::serialize("successful execution").unwrap();
        let result = AppResult {
            success: true,
            payload: result_payload,
        };

        Ok(result)
    }
}
