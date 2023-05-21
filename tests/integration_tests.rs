use crate::common::{
    enable_tracing, get_test_db_paths, get_test_port, prepare_cluster, prepare_node_from_config,
    IntegrationTestApp,
};
use actor_raft::raft_server::config::{Config, NodeConfig};
use actor_raft::raft_server::raft_handles::RaftHandles;
use actor_raft::raft_server::raft_node::ServerState::{Candidate, Follower, Leader};
use actor_raft::raft_server::raft_node::{RaftNode, RaftNodeBuilder};
use actor_raft::raft_server_rpc::append_entries_request::Entry;
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tracing::{info, warn};

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

    // thread to shutdown all servers
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
    enable_tracing().await;

    // prepare nodes

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

    // required clone of node 1 config for restarting node 1 as "failover node"
    let node1_config = raft_node1.get_config();

    // shutdown hooks for starting nodes
    let mut shutdown_receivers_clone = shutdown_receivers.clone();

    // shutdown hooks for restarted node
    let failover_node_shutdown: Sender<()> = broadcast::channel(1).0;
    let failover_node_shutdown_clone = failover_node_shutdown.clone();

    // begin test

    let t1 = tokio::spawn(async move {
        raft_node1.run_continuously().await;
    });
    let t2 = tokio::spawn(async move {
        raft_node2.run_continuously().await;
    });
    let t3 = tokio::spawn(async move {
        raft_node3.run_continuously().await;
    });

    // thread to shutdown all servers
    let t4 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(7000)).await;
        shutdown_receivers.iter().for_each(|x| {
            let _ = x.send(());
        });
        failover_node_shutdown_clone.send(()).unwrap();
    });

    // replicate one entry from node 1
    let t5 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        let entry = r_handle1.create_entry("test".to_string()).await.unwrap();
        r_handle1.append_entry(entry).await;
        let index = r_handle1.log_store.get_last_log_index().await;
        assert_eq!(index, 1);
        tokio::time::sleep(Duration::from_millis(600)).await;
        info!("test if first entry is replicated");
        match r_handle2.log_store.read_last_entry().await {
            None => {
                panic!()
            }
            Some(entry) => {
                assert_eq!(entry.index, 1);
            }
        }
        match r_handle3.log_store.read_last_entry().await {
            None => {
                panic!()
            }
            Some(entry) => {
                assert_eq!(entry.index, 1);
            }
        }

        info!("shutdown node1");

        shutdown_receivers_clone
            .pop_back()
            .unwrap()
            .send(())
            .unwrap();
        tokio::time::sleep(Duration::from_millis(2000)).await;

        match r_handle2.create_entry("test1".to_string()).await {
            None => {}
            Some(entry) => {
                r_handle2.append_entry(entry).await;
            }
        };
        match r_handle3.create_entry("test1".to_string()).await {
            None => {}
            Some(entry) => {
                r_handle3.append_entry(entry).await;
            }
        };

        tokio::time::sleep(Duration::from_millis(2000)).await;

        match r_handle2.log_store.read_last_entry().await {
            None => {
                panic!("second entry is missing in node2")
            }
            Some(entry) => {
                assert_eq!(entry.index, 2);
            }
        }
        match r_handle3.log_store.read_last_entry().await {
            None => {
                panic!("second entry is missing in node3")
            }
            Some(entry) => {
                assert_eq!(entry.index, 2);
            }
        }

        match r_handle3.log_store.read_entry(2).await {
            None => {
                panic!("second entry is missing in node3")
            }
            Some(entry) => {
                assert_eq!(entry.index, 2);
            }
        }

        info!("prepare node 1 as failover node again ");
        let mut failover_node =
            prepare_node_from_config(node1_config, failover_node_shutdown.clone()).await;
        let failover_handles = failover_node.get_handles();

        let _ = tokio::spawn(async move {
            info!("run failover node ");
            failover_node.run_continuously().await;
        })
        .await;

        match failover_handles.log_store.read_last_entry().await {
            None => {
                panic!("No entry in failover node")
            }
            Some(entry) => {
                assert_eq!(entry.index, 2);
            }
        }
    });

    tokio::try_join!(s1, s2, s3, t1, t2, t3, t4, t5).unwrap();
}
