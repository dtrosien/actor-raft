use once_cell::sync::Lazy;
use tokio::sync::Mutex;

// global var used to offer unique dbs for each store in unit tests to prevent concurrency issues while testing
static DB_COUNTER: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(0));
// get number from GLOBAL_DB_COUNTER
pub async fn get_test_db() -> String {
    let mut i = DB_COUNTER.lock().await;
    *i += 1;
    format!("databases/test-db{}", *i)
}

// todo more static counter to remove pressure from a single instance?
