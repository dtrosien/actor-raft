use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Config {
    pub nodes: HashMap<u64, NodeConfig>,
    pub delay: Duration,
    pub max_retries: u8,
}

#[derive(Serialize, Deserialize, Debug, Clone, Eq, Hash, PartialEq)]
pub struct NodeConfig {
    pub id: u64,
    pub ip: String,
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            nodes: Default::default(),
            delay: Duration::from_millis(100),
            max_retries: 4,
        }
    }
}
