use crate::raft_client::config::{Config, NodeConfig};
use crate::raft_client_rpc::raft_client_rpc_client::RaftClientRpcClient;
use crate::raft_client_rpc::{
    ClientQueryReply, ClientQueryRequest, ClientRequestReply, ClientRequestRequest,
    RegisterClientReply, RegisterClientRequest,
};
use rand::Rng;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::{IntoRequest, Request};
use tracing::warn;

struct Client {
    id: Option<u64>,
    sequence_num: Option<u64>,
    leader_id: Option<u64>,
    config: Config,
    tonic_client: RaftClientRpcClient<Channel>,
}

impl Client {
    pub async fn query(
        &mut self,
        query_payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
        let query = self.build_query(query_payload).await;
        let mut retries_left = self.config.max_retries;
        loop {
            retries_left -= 1;
            match self.send_query(&query).await {
                Ok(opt) => return Ok(opt),
                Err(e) => {
                    warn!(
                        "error: , retry send query, retries left: {}",
                        (retries_left)
                    );
                    if retries_left == 0 {
                        return Err(e);
                    };
                    tokio::time::sleep(self.config.delay).await;
                }
            };
        }
    }

    pub async fn command(
        &mut self,
        command_payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
        let command = self.build_command(command_payload).await;

        let mut retries_left = self.config.max_retries;
        loop {
            retries_left -= 1;
            match self.send_command(&command).await {
                Ok(opt) => return Ok(opt),
                Err(e) => {
                    warn!(
                        "error: , retry send command, retries left: {}",
                        (retries_left)
                    );
                    if retries_left == 0 {
                        return Err(e);
                    };
                    tokio::time::sleep(self.config.delay).await;
                }
            };
        }
    }

    // todo use this method in command and query if client id is not set
    async fn register(&mut self) -> Result<Option<u64>, Box<dyn Error + Send + Sync>> {
        let register_request = self.build_register_request().await;
        let mut retries_left = self.config.max_retries;
        loop {
            retries_left -= 1;
            match self.send_register_request(&register_request).await {
                Ok(opt) => return Ok(opt),
                Err(e) => {
                    warn!(
                        "error: , retry send command, retries left: {}",
                        (retries_left)
                    );
                    if retries_left == 0 {
                        return Err(e);
                    };
                    tokio::time::sleep(self.config.delay).await;
                }
            };
        }
    }

    async fn send_query(
        &mut self,
        query: &ClientQueryRequest,
    ) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
        let request = Request::new(query.clone());
        let response = self.tonic_client.client_query(request).await?;
        let response_arguments = response.into_inner();

        // todo [feature] not necessary when using Error with status in response
        if !response_arguments.status && response_arguments.leader_hint.is_none() {
            return Err("command could not be processed by raft node")?;
        }

        if let Some(leader_hint) = response_arguments.leader_hint {
            self.update_leader(leader_hint).await;
            return Err("wrong node, got leader hint")?;
        }

        Ok(response_arguments.response)
    }

    async fn send_command(
        &mut self,
        command: &ClientRequestRequest,
    ) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
        let request = Request::new(command.clone());
        let response = self.tonic_client.client_request(request).await?;
        let response_arguments = response.into_inner();

        // todo [feature] not necessary when using Error with status in response
        if !response_arguments.status && response_arguments.leader_hint.is_none() {
            return Err("command could not be processed by raft node")?; 
        }

        if let Some(leader_hint) = response_arguments.leader_hint {
            self.update_leader(leader_hint).await;
            return Err("wrong node, got leader hint")?;
        }

        Ok(response_arguments.response)
    }

    async fn send_register_request(
        &mut self,
        register_request: &RegisterClientRequest,
    ) -> Result<Option<u64>, Box<dyn Error + Send + Sync>> {
        let request = Request::new(register_request.clone());
        let response = self.tonic_client.register_client(request).await?;
        let response_arguments = response.into_inner();
        
        // todo [feature] not necessary when using Error with status in response
        if !response_arguments.status && response_arguments.leader_hint.is_none() {
            return Err("register_request could not be processed by raft node")?;
        }

        if let Some(leader_hint) = response_arguments.leader_hint {
            self.update_leader(leader_hint).await;
            return Err("wrong node, got leader hint")?;
        }

        Ok(response_arguments.client_id)
    }

    // build commands /////////////
    async fn build_query(&self, payload: Vec<u8>) -> ClientQueryRequest {
        ClientQueryRequest { query: payload }
    }
    async fn build_command(&self, payload: Vec<u8>) -> ClientRequestRequest {
        ClientRequestRequest {
            client_id: 0,    // todo impl of registration needed
            sequence_num: 0, // todo impl of registration needed
            command: payload,
        }
    }
    async fn build_register_request(&self) -> RegisterClientRequest {
        RegisterClientRequest {}
    }

    async fn update_leader(&mut self, leader_id: u64) {
        self.leader_id = Some(leader_id);
        self.tonic_client = init_tonic_client(
            self.config
                .nodes
                .get(&leader_id)
                .cloned()
                .expect("leader id was not found in config"),
        )
        .await;
    }

    // todo [feature/ refactoring] not working probably because dependent on mut self
    async fn retry<I, T, F, Fut>(
        &mut self,
        max_tries: u8,
        delay: Duration,
        request: &I,
        mut f: F,
    ) -> Result<Option<T>, Box<dyn Error + Send + Sync>>
    where
        F: FnMut(&mut Self, &I) -> Fut,
        Fut: Future<Output = Result<Option<T>, Box<dyn Error + Send + Sync>>>,
    {
        let mut retries_left = max_tries;
        loop {
            retries_left -= 1;
            match f(self, request).await {
                Ok(opt) => return Ok(opt),
                Err(e) => {
                    warn!(
                        "error: , retry send request, retries left: {}",
                        (retries_left)
                    );
                    if retries_left == 0 {
                        return Err(e);
                    };
                    tokio::time::sleep(delay).await;
                }
            };
        }
    }
}

struct ClientBuilder {
    id: Option<u64>,
    leader_id: Option<u64>,
    config: Config,
}

impl ClientBuilder {
    pub fn new() -> Self {
        ClientBuilder {
            id: None,
            leader_id: None,
            config: Config::default(),
        }
    }

    /// the port in NodeConfig must be the Client Service Port
    pub fn with_nodes(&mut self, nodes: Vec<NodeConfig>) -> &mut ClientBuilder {
        nodes.iter().for_each(|node| {
            self.config.nodes.insert(node.id, node.clone());
        });
        self
    }

    pub fn with_node(&mut self, node: NodeConfig) -> &mut ClientBuilder {
        self.config.nodes.insert(node.id, node);
        self
    }
    pub fn with_delay(&mut self, delay: Duration) -> &mut ClientBuilder {
        self.config.delay = delay;
        self
    }
    pub fn with_max_retries(&mut self, max_retries: u8) -> &mut ClientBuilder {
        self.config.max_retries = max_retries;
        self
    }

    pub async fn build(&self) -> Client {
        let leader = get_random_node(&self.config.nodes).expect("no nodes found in config");
        let tonic_client = init_tonic_client(leader.clone()).await;

        Client {
            id: self.id,
            sequence_num: None,
            leader_id: Some(leader.id),
            config: self.config.clone(),
            tonic_client,
        }
    }
}

fn get_random_node(nodes: &HashMap<u64, NodeConfig>) -> Option<NodeConfig> {
    if nodes.is_empty() {
        return None;
    }
    let rand_pos = rand::thread_rng().gen_range(0..nodes.keys().len() - 1);
    let keys: Vec<u64> = nodes.keys().cloned().collect();

    let rand_id = keys
        .get(rand_pos)
        .cloned()
        .expect("empty pos in key vec must not happen");

    nodes.get(&rand_id).cloned()
}

async fn init_tonic_client(node_config: NodeConfig) -> RaftClientRpcClient<Channel> {
    let ip = node_config.ip.clone();
    let port = node_config.port;
    let uri = format!("https://{ip}:{port}");
    let channel = Channel::builder(uri.parse().expect("could not parse leader uri"))
        .connect_timeout(Duration::from_millis(50))
        .connect()
        .await
        .expect("could not build channel for tonic client");

    RaftClientRpcClient::new(channel)
}
