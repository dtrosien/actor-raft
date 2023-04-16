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
        let rpc_arguments = request.into_inner();
        let rpc_term = rpc_arguments.term;
        let current_term = self.core.term_store.get_term().await;

        if rpc_term < current_term {
            // todo why not <= ?
            let reply = RequestVoteReply {
                term: current_term,
                vote_granted: false,
            };
            return Ok(Response::new(reply));
        }

        let rpc_candidate_id = rpc_arguments.candidate_id;
        let rpc_last_log_index = rpc_arguments.last_log_index;
        // let rpc_last_log_term = rpc_arguments.last_log_term;

        let voted_for = self.core.initiator.get_voted_for().await;
        let last_log_index = self.core.log_store.get_last_log_index().await;
        // let last_log_term = self.core.log_store.get_last_log_term().await;

        let vote_granted_id = match voted_for {
            None => true,
            Some(id) => id == rpc_candidate_id,
        };

        let vote_granted_log = rpc_last_log_index >= last_log_index;

        let vote_granted = vote_granted_id && vote_granted_log;

        let reply = RequestVoteReply {
            term: current_term,
            vote_granted,
        };
        Ok(Response::new(reply))
    }
}
