use actor_raft::app::{App, AppResult};
use actor_raft::raft_server::config::{Config, NodeConfig};
use actor_raft::raft_server::raft_handles::RaftHandles;
use actor_raft::raft_server::raft_node::ServerState::{Candidate, Follower};
use actor_raft::raft_server::raft_node::{RaftNode, RaftNodeBuilder};
use actor_raft::raft_server_rpc::append_entries_request::Entry;
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::error::Error;
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tracing::{info, Subscriber};

#[derive(Debug)]
pub struct IntegrationTestApp {}

impl App for IntegrationTestApp {
    #[tracing::instrument(ret, level = "debug")]
    fn run(&self, entry: Entry) -> Result<AppResult, Box<(dyn Error + Send + Sync)>> {
        let msg = entry.payload.as_str();
        info!("the following payload was executed in TestApp: {}", msg);

        let result_payload = bincode::serialize("successful execution").unwrap();
        let result = AppResult {
            success: true,
            payload: result_payload,
        };

        Ok(result)
    }

    fn query(
        &self,
        payload: Vec<u8>,
    ) -> JoinHandle<Result<AppResult, Box<dyn Error + Send + Sync>>> {
        let result = AppResult {
            success: true,
            payload: SLED_DUMMY.clone().into_bytes(),
        };
        let a = Arc::new(result);

        tokio::spawn(async move {
            let result = AppResult {
                success: true,
                payload: SLED_DUMMY.clone().into_bytes(),
            };
            Ok(result)
        })
    }
}
static SLED_DUMMY: Lazy<String> = Lazy::new(|| String::from("Dummy Val"));

// global var used to offer unique dbs for each store in unit tests to prevent concurrency issues while testing
static DB_COUNTER: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(0));

// get number from GLOBAL_DB_COUNTER
pub async fn get_test_db_paths(amount: u16) -> Vec<String> {
    let mut i = DB_COUNTER.lock().await;
    let mut paths = Vec::new();
    for _n in 0..amount {
        *i += 1;
        paths.push(format!("databases/int-test-db{}", *i))
    }
    paths
}

// global var used to offer unique ports for each rpc call in unit tests starting from port number 50060
static GLOBAL_PORT_COUNTER: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(50060));
// get port from GLOBAL_PORT_COUNTER
pub async fn get_test_port() -> u16 {
    let mut i = GLOBAL_PORT_COUNTER.lock().await;
    *i += 1;
    *i
}

// used to set a global tracing setting for integration tests
static TRACING_ENABLED: Lazy<Mutex<bool>> = Lazy::new(|| Mutex::new(false));
pub async fn enable_tracing() {
    let mut tracing_guard = TRACING_ENABLED.lock().await;
    if !*tracing_guard {
        // to disable integration logging comment out the following lines
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_target(true)
            .with_line_number(true)
            .with_thread_ids(true)
            .init();
        *tracing_guard = true
    }
}

// used to create test clusters for integration testing
// first node (id=0) will be leader after first run iteration when enabled. (this will be the last node to pop from Vec!)
pub async fn prepare_cluster(
    num_nodes: u16,
    predetermined_leader: bool,
) -> (Vec<RaftNode>, Vec<RaftHandles>, VecDeque<Sender<()>>) {
    let mut ports: Vec<u16> = Vec::new();
    let mut node_configs: Vec<NodeConfig> = Vec::new();
    let mut raft_nodes: Vec<RaftNode> = Vec::new(); // todo switch to map?
    let mut handles: Vec<RaftHandles> = Vec::new();
    let mut db_paths = get_test_db_paths(3 * num_nodes).await;
    let mut shutdown_receivers: VecDeque<Sender<()>> = VecDeque::new();

    for i in 0..num_nodes {
        let s_shutdown = broadcast::channel(1).0;
        let port = get_test_port().await;
        let node_config = NodeConfig {
            id: i as u64,
            ip: "[::1]".to_string(),
            port,
        };
        ports.push(port);
        node_configs.push(node_config);
        shutdown_receivers.push_front(s_shutdown);
    }

    // clone receivers to be able to return them later
    let mut copy_of_receivers = shutdown_receivers.clone();

    // prepare raft nodes
    for i in 0..num_nodes {
        let app = Box::new(IntegrationTestApp {});

        let mut other_nodes = node_configs.clone();
        other_nodes.remove(i as usize);

        let raft_node = if i == 0 && predetermined_leader {
            RaftNodeBuilder::new(app)
                .with_id(i as u64)
                .with_port(*ports.get(i as usize).unwrap())
                .with_shutdown(copy_of_receivers.pop_back().unwrap())
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
                .with_shutdown(copy_of_receivers.pop_back().unwrap())
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

    (raft_nodes, handles, shutdown_receivers)
}

// prepares a node from a (previous) node config.
// IMPORTANT: since sled db does not support closing and opening dbs rapidly
// for integration testing a new db will be created for the "same" node
// (https://github.com/spacejam/sled/issues/1234)
pub async fn prepare_node_from_config(config: Config, s_shutdown: Sender<()>) -> RaftNode {
    let app = Box::new(IntegrationTestApp {});
    let mut db_paths = get_test_db_paths(3).await;
    let node = RaftNodeBuilder::new(app)
        .with_id(config.id)
        .with_port(config.port)
        .with_shutdown(s_shutdown)
        .with_client_service_enabled(false)
        .with_nodes(config.nodes)
        .with_log_db_path(db_paths.pop().unwrap().as_str())
        .with_term_db_path(db_paths.pop().unwrap().as_str())
        .with_vote_db_path(db_paths.pop().unwrap().as_str())
        .with_initial_state(Follower)
        .build()
        .await;
    let h = node.get_handles();
    h.term_store.reset_term().await;
    h.log_store.reset_log().await;
    h.initiator.reset_voted_for().await;
    node
}
