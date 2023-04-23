use crate::rpc::test_utils::get_test_port;
use serde::{Deserialize, Serialize};

//todo use config lib like i.e config-rs

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub id: u64,
    pub ip: String,
    pub port: u16,
    pub db_path: String,
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
pub struct Node {
    pub id: u64,
    pub ip: String,
    pub port: u16,
}

impl Config {
    pub fn new() -> Self {
        Config {
            id: 0,
            ip: "".to_string(),
            port: 0,
            db_path: "databases/log-db".to_string(),
            nodes: vec![],
        }
    }
    pub async fn for_test() -> Self {
        let mut nodes = Vec::new();
        for n in 1..=4 {
            let node = Node {
                id: n,
                ip: "[::1]".to_string(),
                port: get_test_port().await,
            };
            nodes.push(node)
        }
        Config {
            id: 0,
            ip: "[::1]".to_string(),
            port: get_test_port().await,
            db_path: "databases/log-db".to_string(),
            nodes,
        }
    }
}

//todo sinnvolle defaults definieren
impl Default for Config {
    fn default() -> Self {
        Config::new()
    }
}
