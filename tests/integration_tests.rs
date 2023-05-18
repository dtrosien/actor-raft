use crate::common::IntegrationTestApp;
use actor_raft::raft_node::raft::RaftBuilder;
use actor_raft::raft_node::raft::ServerState::Leader;

mod common;

#[tokio::test]
async fn election_test() {
    let app = Box::new(IntegrationTestApp {});
    let raft = RaftBuilder::new(app)
        .with_id(1)
        .with_initial_state(Leader)
        .with_channel_capacity(10)
        .with_election_timeout_range(200, 500)
        .build()
        .await;
}

#[tokio::test]
async fn leader_test() {
    let app = Box::new(IntegrationTestApp {});
    let raft = RaftBuilder::new(app)
        .with_id(1)
        .with_initial_state(Leader)
        .with_channel_capacity(10)
        .with_election_timeout_range(200, 500)
        .build()
        .await;
}
