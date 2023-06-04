use crate::raft_client::config::{Config, NodeConfig};
use crate::raft_client_rpc::raft_client_rpc_client::RaftClientRpcClient;
use crate::raft_client_rpc::{
    ClientQueryReply, ClientQueryRequest, ClientRequestReply, ClientRequestRequest,
    RegisterClientReply, RegisterClientRequest,
};
use rand::Rng;
use std::collections::HashMap;
use std::error::Error;
use std::time::Duration;
use tonic::transport::Channel;
use tonic::Request;

struct Client {
    id: Option<u64>,
    leader_id: Option<u64>,
    config: Config,
    tonic_client: RaftClientRpcClient<Channel>,
}

impl Client {
    pub async fn send_query(
        &self,
        query: Request<ClientQueryRequest>,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        todo!()
    }

    pub async fn send_command(
        &mut self,
        command: ClientRequestRequest,
    ) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
        let request = Request::new(command.clone());
        let response = self.tonic_client.client_request(request).await?;
        let response_arguments = response.into_inner();

        if !response_arguments.status && response_arguments.leader_hint.is_none() {
            panic!("command could not be processed by raft node")
        }
        if let Some(leader_hint) = response_arguments.leader_hint {
            self.update_leader(leader_hint).await;
            // self.send_command(command).await; // todo cancel retry after x repeats, think about how to handle recusion + build commands private because otherwise sequence num could be wrong etc
        }

        todo!()
    }

    async fn register(&self, register_request: RegisterClientRequest) {
        todo!()
    }

    pub async fn build_query(&self, payload: Vec<u8>) -> ClientQueryRequest {
        ClientQueryRequest { query: payload }
    }
    pub async fn build_command(&self, payload: Vec<u8>) -> ClientRequestRequest {
        ClientRequestRequest {
            client_id: 0,                   // todo impl of registration needed
            sequence_num: 0,                // todo impl of registration needed
            command: "payload".to_string(), // todo switch to vec<u8> payload
        }
    }
    async fn build_register_request(&self) -> RegisterClientRequest {
        todo!()
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
}

pub trait ClientRequest {}

impl ClientRequest for ClientRequestRequest {}

impl ClientRequest for ClientQueryRequest {}

impl ClientRequest for RegisterClientRequest {}

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

    pub async fn build(&self) -> Client {
        let leader = get_random_node(&self.config.nodes).expect("no nodes found in config");
        let tonic_client = init_tonic_client(leader.clone()).await;

        Client {
            id: self.id,
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
