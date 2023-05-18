use crate::raft_rpc::append_entries_request::Entry;
use crate::raft_server::raft_node::App;
use std::error::Error;
use std::fmt::Debug;

#[derive(Debug)]
pub struct TestApp {}

impl App for TestApp {
    #[tracing::instrument(ret, level = "debug")]
    fn run(&self, entry: Entry) -> Result<bool, Box<dyn Error + Send + Sync>> {
        println!("hey there");
        Ok(true)
    }
}