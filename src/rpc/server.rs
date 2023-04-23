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
        if !self
            .core
            .log_store
            .previous_entry_match(rpc_arguments.prev_log_index, rpc_arguments.prev_log_term)
            .await
        {
            return deny_append_request(current_term);
        }

        // step 3 & 4
        self.core.log_store.append_entries(entries.clone()).await;

        // step 5
        self.core
            .executor
            .commit_log(entries.back().cloned(), rpc_arguments.leader_commit)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::log::replication::worker::StateMeta;
    use crate::actors::log::test_utils::TestApp;
    use crate::actors::term_store::TermStoreHandle;
    use crate::actors::watchdog::WatchdogHandle;
    use crate::config::Config;
    use crate::db::test_utils::get_test_db_paths;
    use crate::raft_rpc::append_entries_request::Entry;
    use std::time::Duration;

    #[tokio::test]
    async fn append_entry_test() {
        let mut db_paths = get_test_db_paths(3).await;
        let wd = WatchdogHandle::default();
        let term_store = TermStoreHandle::new(wd.clone(), db_paths.pop().unwrap());
        let state_meta = StateMeta {
            previous_log_index: 0, // todo why couldnt this be set to zero inside actor
            previous_log_term: 0,  // todo why couldnt this be set to zero inside actor
            term: 0,
            leader_id: 0,
            leader_commit: 0, // todo why couldnt this be set to zero inside actor
        };
        let core = CoreHandles::new(
            wd,
            Config::for_test().await,
            Box::new(TestApp {}),
            term_store,
            state_meta,
            db_paths.pop().unwrap(),
            db_paths.pop().unwrap(),
        );
        let raft_server = RaftServer { core };

        raft_server.core.log_store.reset_log().await;

        let entry1 = Entry {
            index: 1,
            term: 0,
            payload: "some payload".to_string(),
        };
        let entry2 = Entry {
            index: 2,
            term: 1,
            payload: "some payload".to_string(),
        };
        let entry3 = Entry {
            index: 3,
            term: 2,
            payload: "some payload".to_string(),
        };
        let entry4 = Entry {
            index: 4,
            term: 2,
            payload: "some payload".to_string(),
        };
        let entry5 = Entry {
            index: 5,
            term: 2,
            payload: "some payload".to_string(),
        };

        let entries = vec![entry1, entry2, entry3, entry4, entry5];

        let msg1 = AppendEntriesRequest {
            term: 2,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries,
            leader_commit: 5,
        };
        let request1 = Request::new(msg1);
        raft_server
            .append_entries(request1)
            .await
            .expect("TODO: panic message");

        assert_eq!(raft_server.core.log_store.get_last_log_index().await, 5);
        assert_eq!(raft_server.core.executor.get_last_applied().await, 5);

        let entry6 = Entry {
            index: 6,
            term: 2,
            payload: "some payload".to_string(),
        };

        let msg2 = AppendEntriesRequest {
            term: 2,
            leader_id: 0,
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![entry6],
            leader_commit: 6,
        };
        let request2 = Request::new(msg2);

        raft_server
            .append_entries(request2)
            .await
            .expect("TODO: panic message");

        assert_eq!(raft_server.core.log_store.get_last_log_index().await, 6);

        assert_eq!(raft_server.core.executor.get_last_applied().await, 6);

        let msg3 = AppendEntriesRequest {
            term: 2,
            leader_id: 0,
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 6,
        };
        let request3 = Request::new(msg3);

        raft_server
            .append_entries(request3)
            .await
            .expect("TODO: panic message");

        assert_eq!(raft_server.core.log_store.get_last_log_index().await, 6);

        assert_eq!(raft_server.core.executor.get_last_applied().await, 6);

        //todo further testing
    }
}
