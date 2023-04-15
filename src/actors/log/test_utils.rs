use crate::actors::log::executor::App;
use crate::raft_rpc::append_entries_request::Entry;
use once_cell::sync::Lazy;
use std::error::Error;
use tokio::sync::Mutex;

#[cfg(test)]
pub struct TestApp {}
#[cfg(test)]
impl App for TestApp {
    fn run(&self, entry: Entry) -> Result<bool, Box<dyn Error + Send + Sync>> {
        println!("hey there");
        Ok(true)
    }
}
