use crate::actors::log::executor::App;
use crate::raft_rpc::append_entries_request::Entry;
use once_cell::sync::Lazy;
use std::error::Error;
use tokio::sync::Mutex;

// global var used to offer unique dbs for each log store in unit tests to prevent concurrency issues while testing
static DB_COUNTER: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(0));
// get number from GLOBAL_DB_COUNTER
pub async fn get_test_db() -> String {
    let mut i = DB_COUNTER.lock().await;
    *i += 1;
    format!("databases/executor-test-db_{}", *i)
}

pub struct TestApp {}

impl App for TestApp {
    fn run(&self, entry: Entry) -> Result<bool, Box<dyn Error + Send + Sync>> {
        println!("hey there");
        Ok(true)
    }
}
