use once_cell::sync::Lazy;
use tokio::sync::Mutex;

// global var used to offer unique dbs for each store in unit tests to prevent concurrency issues while testing
#[cfg(test)]
static DB_COUNTER: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(0));

// get number from GLOBAL_DB_COUNTER
#[cfg(test)]
pub async fn get_test_db_paths(amount: u16) -> Vec<String> {
    let mut i = DB_COUNTER.lock().await;
    let mut paths = Vec::new();
    for _n in 0..amount {
        *i += 1;
        paths.push(format!("databases/test-db{}", *i))
    }
    paths
}
