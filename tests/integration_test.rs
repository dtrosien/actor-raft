use actor_raft::raft_node::raft::ServerState::Leader;
use actor_raft::raft_node::raft::{App, RaftBuilder};
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

#[tokio::test]
async fn test_add() {
    let app = Box::new(IntegrationTestApp {});
    let raft = RaftBuilder::new(app)
        .with_id(1)
        .with_initial_state(Leader)
        .with_channel_capacity(10)
        .with_election_timeout_range(200, 500)
        .build()
        .await;
}
