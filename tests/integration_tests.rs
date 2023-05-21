use crate::common::{enable_tracing, get_test_db_paths, get_test_port, IntegrationTestApp};
use actor_raft::raft_server::config::NodeConfig;
use actor_raft::raft_server::raft_handles::RaftHandles;
use actor_raft::raft_server::raft_node::ServerState::{Candidate, Leader};
use actor_raft::raft_server::raft_node::{RaftNode, RaftNodeBuilder};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tracing::{debug, info};

mod common;

#[tokio::test]
async fn election_test() {
    enable_tracing().await;

    // prepare nodes

    let (mut nodes, handles, _s_shutdown) = prepare_cluster(3).await;

    let mut raft_node3 = nodes.pop().unwrap();
    let mut raft_node2 = nodes.pop().unwrap();
    let mut raft_node1 = nodes.pop().unwrap();

    let r_handle1 = handles.get(0).unwrap();

    // run rpc server

    let s1 = raft_node1.get_node_server_handle().unwrap();
    let s2 = raft_node2.get_node_server_handle().unwrap();
    let s3 = raft_node3.get_node_server_handle().unwrap();

    // run each node in own task

    let t1 = tokio::spawn(async move {
        raft_node1.run_n_times(1).await;
    });
    let t2 = tokio::spawn(async move {
        raft_node2.run_n_times(1).await;
    });
    let t3 = tokio::spawn(async move {
        raft_node3.run_n_times(1).await;
    });

    tokio::try_join!(s1, s2, s3, t1, t2, t3).unwrap();

    // first node from prepare cluster is expected to be leader
    assert_eq!(r_handle1.state_store.get_state().await, Leader);
}

#[tokio::test]
async fn replication_test() {
    enable_tracing().await;

    // prepare nodes

    let (mut nodes, mut handles, s_shutdown) = prepare_cluster(3).await;

    let mut raft_node3 = nodes.pop().unwrap();
    let mut raft_node2 = nodes.pop().unwrap();
    let mut raft_node1 = nodes.pop().unwrap();

    let r_handle3 = handles.pop().unwrap();
    let r_handle2 = handles.pop().unwrap();
    let r_handle1 = handles.pop().unwrap();

    // run rpc server

    let s1 = raft_node1.get_node_server_handle().unwrap();
    let s2 = raft_node2.get_node_server_handle().unwrap();
    let s3 = raft_node3.get_node_server_handle().unwrap();

    // run each node in own task

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

    // create entry in first node (first node from prepare cluster is expected to be leader)
    // and call append_entry to save to local log and replicate

    let t5 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let entry = r_handle1.create_entry("test".to_string()).await.unwrap();
        r_handle1.append_entry(entry).await;
        let index = r_handle1.log_store.get_last_log_index().await;
        assert_eq!(index, 1);
    });

    tokio::try_join!(s1, s2, s3, t1, t2, t3, t4, t5).unwrap();

    // check if entry was replicated correctly
    match r_handle2.log_store.read_last_entry().await {
        None => {
            panic!()
        }
        Some(entry) => {
            assert_eq!(entry.index, 1)
        }
    }
    match r_handle3.log_store.read_last_entry().await {
        None => {
            panic!()
        }
        Some(entry) => {
            assert_eq!(entry.index, 1)
        }
    }
}

#[tokio::test]
async fn failover_test() {
    // todo [crucial test] write test: replicate entry -> shutdown one server -> replicate entry -> restart server -> all match
    enable_tracing().await;

    // prepare test

    let (mut nodes, mut handles, s_shutdown) = prepare_cluster(3).await;

    let mut raft_node3 = nodes.pop().unwrap();
    let mut raft_node2 = nodes.pop().unwrap();
    let mut raft_node1 = nodes.pop().unwrap();

    let r_handle3 = handles.pop().unwrap();
    let r_handle2 = handles.pop().unwrap();
    let r_handle1 = handles.pop().unwrap();

    let s1 = raft_node1.get_node_server_handle().unwrap();
    let s2 = raft_node2.get_node_server_handle().unwrap();
    let s3 = raft_node3.get_node_server_handle().unwrap();

    // begin test
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

    tokio::try_join!(s1, s2, s3, t1, t2, t3, t4, t5).unwrap();

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

// first node (id=0) will be leader after first run iteration. (this will be the last node to pop from Vec!)
async fn prepare_cluster(num_nodes: u16) -> (Vec<RaftNode>, Vec<RaftHandles>, Sender<()>) {
    let mut ports: Vec<u16> = Vec::new();
    let mut node_configs: Vec<NodeConfig> = Vec::new();
    let mut raft_nodes: Vec<RaftNode> = Vec::new();
    let mut handles: Vec<RaftHandles> = Vec::new();
    let s_shutdown = broadcast::channel(1).0;
    let mut db_paths = get_test_db_paths(3 * num_nodes).await;

    for i in 0..num_nodes {
        let port = get_test_port().await;
        let node_config = NodeConfig {
            id: i as u64,
            ip: "[::1]".to_string(),
            port,
        };
        ports.push(port);
        node_configs.push(node_config);
    }

    // prepare raft nodes
    for i in 0..num_nodes {
        let app = Box::new(IntegrationTestApp {});

        let mut other_nodes = node_configs.clone();
        other_nodes.remove(i as usize);

        let raft_node = if i == 0 {
            RaftNodeBuilder::new(app)
                .with_id(i as u64)
                .with_port(*ports.get(i as usize).unwrap())
                .with_shutdown(s_shutdown.clone())
                .with_client_service_enabled(false)
                .with_nodes(other_nodes)
                .with_log_db_path(db_paths.pop().unwrap().as_str())
                .with_term_db_path(db_paths.pop().unwrap().as_str())
                .with_vote_db_path(db_paths.pop().unwrap().as_str())
                .with_initial_state(Candidate)
                .build()
                .await
        } else {
            RaftNodeBuilder::new(app)
                .with_id(i as u64)
                .with_port(*ports.get(i as usize).unwrap())
                .with_shutdown(s_shutdown.clone())
                .with_client_service_enabled(false)
                .with_nodes(other_nodes)
                .with_log_db_path(db_paths.pop().unwrap().as_str())
                .with_term_db_path(db_paths.pop().unwrap().as_str())
                .with_vote_db_path(db_paths.pop().unwrap().as_str())
                .build()
                .await
        };
        handles.push(raft_node.get_handles());
        raft_nodes.push(raft_node);
    }

    // reset dbs from previous runs
    for h in handles.clone() {
        h.term_store.reset_term().await;
        h.log_store.reset_log().await;
        h.initiator.reset_voted_for().await;
    }

    (raft_nodes, handles, s_shutdown)
}
