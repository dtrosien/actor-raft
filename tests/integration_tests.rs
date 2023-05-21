use crate::common::{
    enable_tracing, get_test_db_paths, get_test_port, prepare_cluster, IntegrationTestApp,
};
use actor_raft::raft_server::config::{Config, NodeConfig};
use actor_raft::raft_server::raft_handles::RaftHandles;
use actor_raft::raft_server::raft_node::ServerState::{Candidate, Follower, Leader};
use actor_raft::raft_server::raft_node::{RaftNode, RaftNodeBuilder};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tracing::info;

mod common;

#[tokio::test]
async fn election_test() {
    enable_tracing().await;

    // prepare nodes

    let (mut nodes, handles, _shutdown_receivers) = prepare_cluster(3, true).await;

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

    let (mut nodes, mut handles, shutdown_receivers) = prepare_cluster(3, true).await;

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
        // shutdown all nodes
        shutdown_receivers.iter().for_each(|x| {
            x.send(()).unwrap();
        });
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

    let (mut nodes, mut handles, shutdown_receivers) = prepare_cluster(3, true).await;

    let mut raft_node3 = nodes.pop().unwrap();
    let mut raft_node2 = nodes.pop().unwrap();
    let mut raft_node1 = nodes.pop().unwrap();

    let r_handle3 = handles.pop().unwrap();
    let r_handle2 = handles.pop().unwrap();
    let r_handle1 = handles.pop().unwrap();

    let s1 = raft_node1.get_node_server_handle().unwrap();
    let s2 = raft_node2.get_node_server_handle().unwrap();
    let s3 = raft_node3.get_node_server_handle().unwrap();

    let node1_config = raft_node1.get_config();
    let s_shutdown: Sender<()> = broadcast::channel(1).0;

    let mut shutdown_receivers_clone = shutdown_receivers.clone();
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
        shutdown_receivers.iter().for_each(|x| {
            let _ = x.send(());
        });
    });

    // replicate one entry from node 1
    let t5 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let entry = r_handle1.create_entry("test".to_string()).await.unwrap();
        r_handle1.append_entry(entry).await;
        let index = r_handle1.log_store.get_last_log_index().await;
        assert_eq!(index, 1);

        shutdown_receivers_clone
            .pop_back()
            .unwrap()
            .send(())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;

        // let failover_node = prepare_node_from_config(node1_config, s_shutdown).await;
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
