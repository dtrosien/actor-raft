use crate::common::{get_test_db_paths, get_test_port, IntegrationTestApp};
use actor_raft::raft_server::config::NodeConfig;
use actor_raft::raft_server::raft_handles::RaftHandles;
use actor_raft::raft_server::raft_node::ServerState::{Candidate, Follower, Leader};
use actor_raft::raft_server::raft_node::{RaftNode, RaftNodeBuilder};
use actor_raft::raft_server_rpc::append_entries_request::Entry;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tracing::{debug, info};

mod common;

#[tokio::test]
async fn election_test() {
    let port1 = get_test_port().await;
    let port2 = get_test_port().await;
    let port3 = get_test_port().await;

    let node_conf1 = NodeConfig {
        id: 1,
        ip: "[::1]".to_string(),
        port: port1,
    };
    let node_conf2 = NodeConfig {
        id: 2,
        ip: "[::1]".to_string(),
        port: port2,
    };
    let node_conf3 = NodeConfig {
        id: 3,
        ip: "[::1]".to_string(),
        port: port3,
    };

    let mut db_paths = get_test_db_paths(9).await;

    let s_shutdown = broadcast::channel(1).0;

    let app1 = Box::new(IntegrationTestApp {});
    let mut raft_node1 = RaftNodeBuilder::new(app1)
        .with_id(1)
        .with_port(port1)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
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
        .with_port(port2)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
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
        .with_port(port3)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
        .with_nodes(vec![node_conf1.clone(), node_conf2.clone()])
        .with_log_db_path(db_paths.pop().unwrap().as_str())
        .with_term_db_path(db_paths.pop().unwrap().as_str())
        .with_vote_db_path(db_paths.pop().unwrap().as_str())
        .with_initial_state(Follower)
        .build()
        .await;

    let r_handle1 = raft_node1.get_handles();
    let r_handle2 = raft_node2.get_handles();
    let r_handle3 = raft_node3.get_handles();

    let handles = vec![r_handle1.clone(), r_handle2, r_handle3];

    for h in handles.clone() {
        h.term_store.reset_term().await;
        h.log_store.reset_log().await;
        h.initiator.reset_voted_for().await;
    }

    let i1 = raft_node1.get_node_server_handle().unwrap();
    let i2 = raft_node2.get_node_server_handle().unwrap();
    let i3 = raft_node3.get_node_server_handle().unwrap();

    let t1 = tokio::spawn(async move {
        raft_node1.run_n_times(1).await;
    });
    let t2 = tokio::spawn(async move {
        raft_node2.run_n_times(1).await;
    });
    let t3 = tokio::spawn(async move {
        raft_node3.run_n_times(1).await;
    });

    tokio::try_join!(i1, i2, i3, t1, t2, t3).unwrap();
    assert_eq!(r_handle1.state_store.get_state().await, Leader);
}

#[tokio::test]
async fn replication_test() {
    let port1 = get_test_port().await;
    let port2 = get_test_port().await;
    let port3 = get_test_port().await;

    let node_conf1 = NodeConfig {
        id: 1,
        ip: "[::1]".to_string(),
        port: port1,
    };
    let node_conf2 = NodeConfig {
        id: 2,
        ip: "[::1]".to_string(),
        port: port2,
    };
    let node_conf3 = NodeConfig {
        id: 3,
        ip: "[::1]".to_string(),
        port: port3,
    };

    let mut db_paths = get_test_db_paths(9).await;

    let s_shutdown = broadcast::channel(1).0;

    let app1 = Box::new(IntegrationTestApp {});
    let mut raft_node1 = RaftNodeBuilder::new(app1)
        .with_id(1)
        .with_port(port1)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
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
        .with_port(port2)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
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
        .with_port(port3)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
        .with_nodes(vec![node_conf1.clone(), node_conf2.clone()])
        .with_log_db_path(db_paths.pop().unwrap().as_str())
        .with_term_db_path(db_paths.pop().unwrap().as_str())
        .with_vote_db_path(db_paths.pop().unwrap().as_str())
        .with_initial_state(Follower)
        .build()
        .await;

    let r_handle1 = raft_node1.get_handles();
    let r_handle2 = raft_node2.get_handles();
    let r_handle3 = raft_node3.get_handles();

    let handles = vec![r_handle1.clone(), r_handle2.clone(), r_handle3.clone()];

    for h in handles.clone() {
        h.term_store.reset_term().await;
        h.log_store.reset_log().await;
        h.initiator.reset_voted_for().await;
    }

    let i1 = raft_node1.get_node_server_handle().unwrap();
    let i2 = raft_node2.get_node_server_handle().unwrap();
    let i3 = raft_node3.get_node_server_handle().unwrap();

    let t1 = tokio::spawn(async move {
        raft_node1.run_continuously().await;
    });
    let t2 = tokio::spawn(async move {
        raft_node2.run_continuously().await;
    });
    let t3 = tokio::spawn(async move {
        raft_node3.run_continuously().await;
    });

    let t4 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        s_shutdown.send(()).unwrap();
    });

    let t5 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let entry = r_handle1.create_entry("test".to_string()).await.unwrap();
        r_handle1.append_entry(entry).await;
        let index = r_handle1.log_store.get_last_log_index().await;
        info!("log index: {}", index);
    });

    tokio::try_join!(i1, i2, i3, t1, t2, t3, t4, t5).unwrap();

    match r_handle2.log_store.read_last_entry().await {
        None => {
            panic!()
        }
        Some(_) => {}
    }
    match r_handle3.log_store.read_last_entry().await {
        None => {
            panic!()
        }
        Some(_) => {}
    }
}

#[ignore]
#[tokio::test]
async fn failover_test() {
    // todo [crucial test] write test: replicate entry -> shutdown one server -> replicate entry -> restart server -> all match
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber).unwrap();

    let (mut nodes, mut handles, s_shutdown) = prepare_nodes().await;

    // todo fix prepare function ... if not fixable copy from replication test

    let mut raft_node1 = nodes.pop().unwrap();
    let mut raft_node2 = nodes.pop().unwrap();
    let mut raft_node3 = nodes.pop().unwrap();

    let r_handle1 = handles.pop().unwrap();
    let r_handle2 = handles.pop().unwrap();
    let r_handle3 = handles.pop().unwrap();

    let i1 = raft_node1.get_node_server_handle().unwrap();
    let i2 = raft_node2.get_node_server_handle().unwrap();
    let i3 = raft_node3.get_node_server_handle().unwrap();

    for h in handles.clone() {
        info!(
            "term node{}: {}",
            h.config.id,
            h.term_store.get_term().await
        );
    }
    for h in handles.clone() {
        info!(
            "log index node{}: {}",
            h.config.id,
            h.log_store.get_last_log_index().await
        );
    }

    let t1 = tokio::spawn(async move {
        raft_node1.run_continuously().await;
    });
    let t2 = tokio::spawn(async move {
        raft_node2.run_continuously().await;
    });
    let t3 = tokio::spawn(async move {
        raft_node3.run_continuously().await;
    });

    let t4 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1000)).await;
        s_shutdown.send(()).unwrap();
    });

    let t5 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let entry = r_handle1.create_entry("test".to_string()).await.unwrap();
        r_handle1.append_entry(entry).await;
        let index = r_handle1.log_store.get_last_log_index().await;
        info!("log index: {}", index);
    });

    tokio::try_join!(i1, i2, i3, t1, t2, t3, t4, t5).unwrap();

    match r_handle2.log_store.read_last_entry().await {
        None => {
            panic!()
        }
        Some(_) => {}
    }
    match r_handle3.log_store.read_last_entry().await {
        None => {
            panic!()
        }
        Some(_) => {}
    }
}

async fn prepare_nodes() -> (Vec<RaftNode>, Vec<RaftHandles>, Sender<()>) {
    let port1 = get_test_port().await;
    let port2 = get_test_port().await;
    let port3 = get_test_port().await;

    let node_conf1 = NodeConfig {
        id: 1,
        ip: "[::1]".to_string(),
        port: port1,
    };
    let node_conf2 = NodeConfig {
        id: 2,
        ip: "[::1]".to_string(),
        port: port2,
    };
    let node_conf3 = NodeConfig {
        id: 3,
        ip: "[::1]".to_string(),
        port: port3,
    };

    let mut db_paths = get_test_db_paths(9).await;

    let s_shutdown = broadcast::channel(1).0;

    let app1 = Box::new(IntegrationTestApp {});
    let mut raft_node1 = RaftNodeBuilder::new(app1)
        .with_id(1)
        .with_port(port1)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
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
        .with_port(port2)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
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
        .with_port(port3)
        .with_shutdown(s_shutdown.clone())
        .with_client_service_enabled(false)
        .with_nodes(vec![node_conf1.clone(), node_conf2.clone()])
        .with_log_db_path(db_paths.pop().unwrap().as_str())
        .with_term_db_path(db_paths.pop().unwrap().as_str())
        .with_vote_db_path(db_paths.pop().unwrap().as_str())
        .with_initial_state(Follower)
        .build()
        .await;

    let handles = vec![
        raft_node1.get_handles(),
        raft_node2.get_handles(),
        raft_node3.get_handles(),
    ];

    for h in handles.clone() {
        h.term_store.reset_term().await;
        h.log_store.reset_log().await;
        h.initiator.reset_voted_for().await;
    }

    let nodes = vec![raft_node1, raft_node2, raft_node3];

    (nodes, handles, s_shutdown)
}
