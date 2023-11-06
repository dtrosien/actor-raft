use crate::app::{App, AppResult};
use crate::raft_server_rpc::append_entries_request::Entry;
use futures_util::future::BoxFuture;
use std::error::Error;
use std::fmt::Debug;
use tracing::info;

#[derive(Debug)]
pub struct TestApp {}

impl App for TestApp {
    fn run(
        &mut self,
        entry: Entry,
    ) -> BoxFuture<'_, Result<AppResult, Box<dyn Error + Send + Sync>>> {
        let future = async move {
            let msg: String = bincode::deserialize(&entry.payload).unwrap();

            info!("the following payload was executed in TestApp: {}", msg);

            let result_payload = bincode::serialize("successful execution").unwrap();
            let result = AppResult {
                success: true,
                payload: result_payload,
            };

            Ok(result)
        };
        Box::pin(future)
    }

    fn query(
        &self,
        payload: Vec<u8>,
    ) -> BoxFuture<'_, Result<AppResult, Box<dyn Error + Send + Sync>>> {
        let future = async move {
            let input: String = bincode::deserialize(payload.clone().as_slice()).unwrap();
            let answer = format!("successful query: {}", input);
            let output = bincode::serialize(&answer).unwrap();
            let result = AppResult {
                success: true,
                payload: output,
            };
            Ok(result)
        };
        Box::pin(future)
    }
}
