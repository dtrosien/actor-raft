use actor_raft::raft_server::raft_node::App;
use actor_raft::raft_server_rpc::append_entries_request::Entry;
use std::error::Error;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;

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
