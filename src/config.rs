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
}

//todo sinnvolle defaults definieren
impl Default for Config {
    fn default() -> Self {
        Config::new()
    }
}
