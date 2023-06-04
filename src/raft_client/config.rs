use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub nodes: HashMap<u64, NodeConfig>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
pub struct NodeConfig {
    pub id: u64,
    pub ip: String,
    pub port: u16,
}

impl Config {
    pub fn new() -> Self {
        Config {
            nodes: Default::default(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            nodes: Default::default(),
        }
    }
}
