use crate::raft::CoreHandles;
use crate::raft_rpc::raft_rpc_server::RaftRpc;
use crate::raft_rpc::{
    AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
};
use tonic::{Request, Response, Status};

pub struct RaftServer {
    core: CoreHandles,
}
#[tonic::async_trait]
impl RaftRpc for RaftServer {
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        // todo implementation

        let reply = AppendEntriesReply {
            term: 1,
            success: true,
        };
        Ok(Response::new(reply))
    }

    async fn request_votes(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        // todo implementation

        let reply = RequestVoteReply {
            term: 1,
            vote_granted: true,
        };
        Ok(Response::new(reply))
    }
}
