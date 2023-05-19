use crate::common::{get_test_db_paths, IntegrationTestApp};
use actor_raft::raft_server::config::NodeConfig;
use actor_raft::raft_server::raft_node::RaftNodeBuilder;
use actor_raft::raft_server::raft_node::ServerState::{Candidate, Follower, Leader};

mod common;

#[tokio::test]
async fn election_test() {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let node_conf1 = NodeConfig {
        id: 1,
        ip: "[::1]".to_string(),
        port: 40055,
    };
    let node_conf2 = NodeConfig {
        id: 2,
        ip: "[::1]".to_string(),
        port: 40056,
    };
    let node_conf3 = NodeConfig {
        id: 3,
        ip: "[::1]".to_string(),
        port: 40057,
    };

    let mut db_paths = get_test_db_paths(9).await;

    let app1 = Box::new(IntegrationTestApp {});
    let mut raft_node1 = RaftNodeBuilder::new(app1)
        .with_id(1)
        .with_port(40055)
        .with_nodes(vec![node_conf2.clone(), node_conf3.clone()])
        .with_log_db_path(db_paths.pop().unwrap().as_str())
        .with_term_db_path(db_paths.pop().unwrap().as_str())
        .with_vote_db_path(db_paths.pop().unwrap().as_str())
        .with_initial_state(Candidate)
        .build()
        .await;

    let app2 = Box::new(IntegrationTestApp {});
    let mut raft_node2 = RaftNodeBuilder::new(app2)
        .with_id(2)
        .with_port(40056)
        .with_nodes(vec![node_conf1.clone(), node_conf3.clone()])
        .with_log_db_path(db_paths.pop().unwrap().as_str())
        .with_term_db_path(db_paths.pop().unwrap().as_str())
        .with_vote_db_path(db_paths.pop().unwrap().as_str())
        .with_initial_state(Follower)
        .build()
        .await;

    let app3 = Box::new(IntegrationTestApp {});
    let mut raft_node3 = RaftNodeBuilder::new(app3)
        .with_id(3)
        .with_port(40057)
        .with_nodes(vec![node_conf1.clone(), node_conf2.clone()])
        .with_log_db_path(db_paths.pop().unwrap().as_str())
        .with_term_db_path(db_paths.pop().unwrap().as_str())
        .with_vote_db_path(db_paths.pop().unwrap().as_str())
        .with_initial_state(Follower)
        .build()
        .await;

    raft_node1.start_rpc_server().await;
    raft_node2.start_rpc_server().await;
    raft_node3.start_rpc_server().await;
    // todo server start besser machen

    let i1 = raft_node1.server_handle.take();
    let i2 = raft_node2.server_handle.take();
    let i3 = raft_node3.server_handle.take();

    // tokio::join!(
    //     raft_node1.run(),
    //     raft_node2.run(),
    //     raft_node3.run(),
    //     i1.unwrap(),
    //     i2.unwrap(),
    //     i3.unwrap()
    // );
}

#[tokio::test]
async fn leader_test() {
    let app = Box::new(IntegrationTestApp {});
    let raft_node = RaftNodeBuilder::new(app)
        .with_id(1)
        .with_initial_state(Follower)
        .with_channel_capacity(10)
        .with_election_timeout_range(200, 500)
        .build()
        .await;
}
