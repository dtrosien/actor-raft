use actor_raft::raft_node::raft::App;
use actor_raft::raft_rpc::append_entries_request::Entry;
use std::error::Error;

#[derive(Debug)]
pub struct IntegrationTestApp {}

impl App for IntegrationTestApp {
    #[tracing::instrument(ret, level = "debug")]
    fn run(&self, entry: Entry) -> Result<bool, Box<dyn Error + Send + Sync>> {
        println!("hey there");
        Ok(true)
    }
}
