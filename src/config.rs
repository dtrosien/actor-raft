use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub id: u64,
    pub fqdn: String,
    pub port: u16,
    pub state_store_path: String,
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
pub struct Node {
    pub id: u64,
    pub fqdn: String,
    pub port: u16,
}

impl Config {
    pub fn new() -> Self {
        Config {
            id: 0,
            fqdn: "".to_string(),
            port: 0,
            state_store_path: "".to_string(),
            nodes: vec![],
        }
    }

    pub fn for_test() -> Self {
        let mut nodes = Vec::new();
        for n in 1..=4 {
            let node = Node {
                id: n,
                fqdn: format!("node{n}"),
                port: 0,
            };
            nodes.push(node)
        }
        Config {
            id: 0,
            fqdn: "node0".to_string(),
            port: 0,
            state_store_path: "".to_string(),
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
