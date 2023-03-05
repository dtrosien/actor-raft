use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs;

//todo use config lib like i.e config-rs

#[derive(Serialize, Deserialize, Clone)]
pub struct Config {
    pub id: u64,
    pub ip: String,
    pub port: u16,
    pub state_store_path: String,
    pub nodes: Vec<Node>,
}

#[derive(Serialize, Deserialize, Clone, Eq, Hash, PartialEq)]
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
            state_store_path: "".to_string(),
            nodes: vec![],
        }
    }

    pub fn for_test() -> Self {
        let mut nodes = Vec::new();
        for n in 1..=4 {
            let node = Node {
                id: n,
                ip: "[::1]".to_string(),
                port: (50060 + n) as u16,
            };
            nodes.push(node)
        }
        Config {
            id: 0,
            ip: "[::1]".to_string(),
            port: 50060,
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
