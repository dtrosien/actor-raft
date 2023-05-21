use actor_raft::raft_server::raft_node::App;
use actor_raft::raft_server_rpc::append_entries_request::Entry;
use once_cell::sync::Lazy;
use std::error::Error;
use tokio::sync::Mutex;
use tracing::Subscriber;

#[derive(Debug)]
pub struct IntegrationTestApp {}

impl App for IntegrationTestApp {
    #[tracing::instrument(ret, level = "debug")]
    fn run(&self, entry: Entry) -> Result<bool, Box<dyn Error + Send + Sync>> {
        println!("hey there");
        Ok(true)
    }
}

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
