use crate::actors::state_store::ServerState;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub id: u64,
    pub ip: String,
    pub port: u16,
    pub log_db_path: String,
    pub term_db_path: String,
    pub vote_db_path: String,
    pub channel_capacity: u16,
    pub timeout: u64,
    pub election_timeout_range: (u64, u64),
    pub initial_state: ServerState,
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
pub struct Node {
    pub id: u64,
    pub ip: String,
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            id: 0,
            ip: "".to_string(),
            port: 0,
            log_db_path: "databases/log-db".to_string(),
            term_db_path: "databases/term-db".to_string(),
            vote_db_path: "databases/vote-db".to_string(),
            channel_capacity: 20,
            timeout: 20,
            election_timeout_range: (10, 20),
            initial_state: ServerState::Follower,
            nodes: vec![],
        }
    }
}

impl Config {
    pub fn new() -> Self {
        Config::default()
    }
}

#[cfg(test)]
pub async fn get_test_config() -> Config {
    use crate::db::test_utils::get_test_db_paths;
    use crate::rpc::test_utils::get_test_port;

    let mut nodes = Vec::new();
    for n in 1..=4 {
        let node = Node {
            id: n,
            ip: "[::1]".to_string(),
            port: get_test_port().await,
        };
        nodes.push(node)
    }
    let mut db_paths = get_test_db_paths(3).await;
    Config {
        id: 0,
        ip: "[::1]".to_string(),
        port: get_test_port().await,
        log_db_path: db_paths.pop().unwrap(),
        term_db_path: db_paths.pop().unwrap(),
        vote_db_path: db_paths.pop().unwrap(),
        channel_capacity: 8,
        timeout: 20,
        election_timeout_range: (10, 30),
        initial_state: ServerState::Follower,
        nodes,
    }
}
