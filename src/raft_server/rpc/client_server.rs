use tonic::{Request, Response, Status};
use crate::raft_client_rpc::{ClientQueryReply, ClientQueryRequest, ClientRequestReply, ClientRequestRequest, RegisterClientReply, RegisterClientRequest};
use crate::raft_client_rpc::raft_client_rpc_server::RaftClientRpc;
use crate::raft_server::raft_handles::RaftHandles;

// todo rename service_server

#[derive(Debug)]
pub struct RaftClientServer {
    handles: RaftHandles,
}

impl RaftClientServer {
    pub fn new(handles: RaftHandles) -> Self {
        RaftClientServer { handles }
    }
}

#[tonic::async_trait]
impl RaftClientRpc for RaftClientServer{
    async fn client_request(&self, request: Request<ClientRequestRequest>) -> Result<Response<ClientRequestReply>, Status> {
        todo!()
    }

    async fn register_client(&self, request: Request<RegisterClientRequest>) -> Result<Response<RegisterClientReply>, Status> {
        todo!()
    }

    async fn client_query(&self, request: Request<ClientQueryRequest>) -> Result<Response<ClientQueryReply>, Status> {
        todo!()
    }
}
