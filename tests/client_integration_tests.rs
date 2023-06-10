use crate::common::{
    enable_tracing, get_test_db_paths, get_test_port, prepare_cluster, prepare_node_from_config,
    IntegrationTestApp,
};
use actor_raft::raft_server_rpc::EntryType::Command;
use std::time::Duration;
mod common;

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

    // run each node in own task

    let t1 = tokio::spawn(async move { raft_node1.execute().await });
    let t2 = tokio::spawn(async move { raft_node2.execute().await });
    let t3 = tokio::spawn(async move { raft_node3.execute().await });

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
        let entry = r_handle1
            .create_entry(bincode::serialize("test").unwrap(), Command)
            .await
            .unwrap();
        r_handle1.append_entry(entry).await;
        let index = r_handle1.log_store.get_last_log_index().await;
        assert_eq!(index, 1);
    });

    tokio::try_join!(t1, t2, t3, t4, t5).unwrap();

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
