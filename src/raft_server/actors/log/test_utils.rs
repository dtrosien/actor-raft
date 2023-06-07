use crate::app::{App, AppResult};
use crate::raft_server_rpc::append_entries_request::Entry;
use std::error::Error;
use std::fmt::Debug;
use tokio::task::JoinHandle;
use tracing::info;

#[derive(Debug)]
pub struct TestApp {}

impl App for TestApp {
    #[tracing::instrument(ret, level = "debug")]
    fn run(&self, entry: Entry) -> Result<AppResult, Box<(dyn Error + Send + Sync)>> {
        let msg: String = bincode::deserialize(&entry.payload).unwrap();
        info!("the following payload was executed in TestApp: {}", msg);

        let result_payload = bincode::serialize("successful execution").unwrap();
        let result = AppResult {
            success: true,
            payload: result_payload,
        };

        Ok(result)
    }

    fn query(
        &self,
        payload: Vec<u8>,
    ) -> JoinHandle<Result<AppResult, Box<dyn Error + Send + Sync>>> {
        tokio::spawn(async move {
            let input: String = bincode::deserialize(&payload).unwrap();
            let answer = format!("successful query: {}", input);
            let output = bincode::serialize(&answer).unwrap();
            let result = AppResult {
                success: true,
                payload: output,
            };
            Ok(result)
        })
    }
}
