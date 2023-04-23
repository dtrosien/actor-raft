use crate::raft::CoreHandles;

use crate::raft_rpc::raft_rpc_server::RaftRpc;
use crate::raft_rpc::{
    AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
};
use std::collections::VecDeque;
use tonic::{Request, Response, Status};

#[derive(Debug)]
pub struct RaftServer {
    core: CoreHandles,
}

#[tonic::async_trait]
impl RaftRpc for RaftServer {
    #[tracing::instrument(ret, level = "debug")]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let rpc_arguments = request.into_inner();
        let rpc_term = rpc_arguments.term;
        let current_term = self.core.term_store.get_term().await;

        let entries = VecDeque::from(rpc_arguments.entries);

        // step 1
        if rpc_term < current_term {
            let reply = AppendEntriesReply {
                term: current_term,
                success: false,
            };
            return Ok(Response::new(reply));
        }

        // step 2
        // todo this step in log_store? check first prevs in memory before reading from disk + refactor
        match self
            .core
            .log_store
            .read_entry(rpc_arguments.prev_log_index)
            .await
        {
            None => return deny_append_request(current_term),
            Some(prev_entry) => {
                if prev_entry.term != rpc_arguments.prev_log_term {
                    return deny_append_request(current_term);
                }
            }
        }

        // step 3 & 4
        self.core.log_store.append_entries(entries.clone()).await;

        // step 5
        // todo change to Option(entry) or entries and remove leader commmit from entry
        self.core
            .executor
            .commit_log(entries.front().unwrap().clone())
            .await;
        self.core
            .executor
            .apply_log()
            .await
            .expect("TODO: panic message");

        let reply = AppendEntriesReply {
            term: current_term,
            success: true,
        };
        Ok(Response::new(reply))
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn request_votes(
        &self,
        request: Request<RequestVoteRequest>,
    ) -> Result<Response<RequestVoteReply>, Status> {
        let rpc_arguments = request.into_inner();

        let current_term = self.core.term_store.get_term().await;

        if rpc_arguments.term < current_term {
            return deny_vote_request(current_term);
        }

        // let rpc_last_log_term = rpc_arguments.last_log_term; todo not needed?

        let voted_for = self.core.initiator.get_voted_for().await;
        let last_log_index = self.core.log_store.get_last_log_index().await;
        // let last_log_term = self.core.log_store.get_last_log_term().await; todo not needed?

        let vote_granted_id = match voted_for {
            None => true,
            Some(id) => id == rpc_arguments.candidate_id,
        };

        let vote_granted_log = rpc_arguments.last_log_index >= last_log_index;

        let vote_granted = vote_granted_id && vote_granted_log;

        let reply = RequestVoteReply {
            term: current_term,
            vote_granted,
        };
        Ok(Response::new(reply))
    }
}

fn deny_append_request(term: u64) -> Result<Response<AppendEntriesReply>, Status> {
    let reply = AppendEntriesReply {
        term,
        success: false,
    };
    Ok(Response::new(reply))
}

fn deny_vote_request(term: u64) -> Result<Response<RequestVoteReply>, Status> {
    let reply = RequestVoteReply {
        term,
        vote_granted: false,
    };
    Ok(Response::new(reply))
}
