use crate::app::AppResult;
use crate::raft_client_rpc::raft_client_rpc_server::RaftClientRpc;
use crate::raft_client_rpc::{
    ClientQueryReply, ClientQueryRequest, ClientRequestReply, ClientRequestRequest,
    RegisterClientReply, RegisterClientRequest,
};
use crate::raft_server::raft_handles::RaftHandles;

use tonic::{Request, Response, Status};
use tracing::info;

// todo [idea] rename service_server

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
impl RaftClientRpc for RaftClientServer {
    async fn client_request(
        &self,
        request: Request<ClientRequestRequest>,
    ) -> Result<Response<ClientRequestReply>, Status> {
        let rpc_arguments = request.into_inner();

        // creat entry with index
        match self.handles.create_entry(rpc_arguments.command).await {
            None => {
                deny_client_request(1) // todo [crucial] return leader id / address
            }
            Some(entry) => {
                let index = entry.index;

                // append entry to local log and replicate
                self.handles.append_entry(entry).await;

                // wait until log was applied and get result
                match self.handles.wait_for_execution_notification(index).await {
                    // todo [feature/bug?] maybe wrap with timeout
                    None => deny_client_request(1), // todo [crucial] return leader id / address
                    Some(result) => accept_client_request(1, result), // todo [crucial] return leader id / address
                }
            }
        }
    }

    async fn register_client(
        &self,
        request: Request<RegisterClientRequest>,
    ) -> Result<Response<RegisterClientReply>, Status> {
        todo!("feature")
    }

    async fn client_query(
        &self,
        request: Request<ClientQueryRequest>,
    ) -> Result<Response<ClientQueryReply>, Status> {
        todo!("feature")
    }
}

fn deny_client_request(leader_id: u64) -> Result<Response<ClientRequestReply>, Status> {
    let reply = ClientRequestReply {
        status: false,
        response: "".to_string(),
        leader_hint: leader_id,
    };
    Ok(Response::new(reply))
}

fn accept_client_request(
    leader_id: u64,
    result: AppResult,
) -> Result<Response<ClientRequestReply>, Status> {
    let payload: String = bincode::deserialize(&result.payload).unwrap(); // todo remove after switch to binary, see below
    let reply = ClientRequestReply {
        status: result.success,
        response: payload,
        leader_hint: leader_id,
    };
    Ok(Response::new(reply))
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn client_request_test() {
        // todo
    }
}
