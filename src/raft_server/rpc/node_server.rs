use crate::raft_server::raft_handles::RaftHandles;
use crate::raft_server_rpc::raft_server_rpc_server::RaftServerRpc;
use crate::raft_server_rpc::{
    AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
};
use std::collections::VecDeque;
use tonic::{Request, Response, Status};
use tracing::warn;

#[derive(Debug)]
pub struct RaftNodeServer {
    handles: RaftHandles,
}

impl RaftNodeServer {
    pub fn new(handles: RaftHandles) -> Self {
        RaftNodeServer { handles }
    }
}

#[tonic::async_trait]
impl RaftServerRpc for RaftNodeServer {
    #[tracing::instrument(ret, level = "debug")]
    async fn append_entries(
        &self,
        request: Request<AppendEntriesRequest>,
    ) -> Result<Response<AppendEntriesReply>, Status> {
        let rpc_arguments = request.into_inner();

        let entries = VecDeque::from(rpc_arguments.entries);

        // reset timeout
        self.handles.state_timer.register_heartbeat().await;

        // step 1
        let (term_ok, current_term) = self
            .handles
            .term_store
            .check_term_and_reply(rpc_arguments.term)
            .await;

        if !term_ok {
            let reply = AppendEntriesReply {
                term: current_term,
                success: false,
            };
            warn!(
                "term not ok! current term = {}, rpc term= {}",
                current_term, rpc_arguments.term
            );
            return Ok(Response::new(reply));
        }

        // step 2
        if !self
            .handles
            .log_store
            .last_entry_match(rpc_arguments.prev_log_index, rpc_arguments.prev_log_term)
            .await
        {
            let li = self.handles.log_store.get_last_log_index().await; // todo [refactor] integrate warning in function above?
            let lt = self.handles.log_store.get_last_log_term().await;
            warn!(
                "last entry does not match: rpc prev_log_index = {}, rpc prev_log_term= {}, own prev_log_index: {}, own prev_log_term {}",
                rpc_arguments.prev_log_index, rpc_arguments.prev_log_term, li,lt
            );
            return deny_append_request(current_term);
        }

        // step 3 & 4
        self.handles.log_store.append_entries(entries.clone()).await;

        // step 5
        self.handles
            .executor
            .commit_log(entries.back().cloned(), rpc_arguments.leader_commit)
            .await;

        let _ = self.handles.executor.apply_log().await;

        // update leader_id
        self.handles
            .state_store
            .set_leader_id(Some(rpc_arguments.leader_id))
            .await;

        //send reply
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

        // reset timeout
        // self.handles.state_timer.register_heartbeat().await; // todo should be removed to prevent dead locks

        // step 1: reply false if term < current_term
        let (term_ok, current_term) = self
            .handles
            .term_store
            .check_term_and_reply(rpc_arguments.term)
            .await;

        if !term_ok {
            return deny_vote_request(current_term);
        }

        // step 2: if voted_for is none or candidate_id,
        // and candidates log is at least as up to date as receivers log, grant vote
        let voted_for = self.handles.initiator.get_voted_for().await;
        let last_log_index = self.handles.log_store.get_last_log_index().await;

        let vote_granted_id = match voted_for {
            None => true,
            Some(id) => id == rpc_arguments.candidate_id,
        };

        let vote_granted_log = rpc_arguments.last_log_index >= last_log_index;

        let vote_granted = vote_granted_id && vote_granted_log;

        if vote_granted {
            self.handles
                .initiator
                .set_voted_for(rpc_arguments.candidate_id)
                .await;
        }

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
    use crate::raft_server::actors::log::log_store::LogStoreHandle;
    use crate::raft_server::actors::log::test_utils::TestApp;
    use crate::raft_server::actors::state_store::StateStoreHandle;
    use crate::raft_server::actors::term_store::TermStoreHandle;
    use crate::raft_server::actors::watchdog::WatchdogHandle;
    use crate::raft_server::config::get_test_config;
    use crate::raft_server::db::test_utils::get_test_db_paths;
    use crate::raft_server::state_meta::StateMeta;
    use crate::raft_server_rpc::append_entries_request::Entry;
    use crate::raft_server_rpc::EntryType;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn append_entry_test() {
        let mut db_paths = get_test_db_paths(3).await;
        let state_store = StateStoreHandle::default();
        let wd = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(wd.clone(), db_paths.pop().unwrap());
        let log_store = LogStoreHandle::new(db_paths.pop().unwrap());

        log_store.reset_log().await;
        term_store.reset_term().await;

        let state_meta = StateMeta {
            last_log_index: 0, // only matters for replicator and voter
            last_log_term: 0,  //only matters for replicator and voter
            term: 0,
            id: 0,
            leader_commit: 0,
        };

        let config = get_test_config().await;
        let handles = RaftHandles::build(
            state_store,
            wd,
            config,
            Arc::new(Mutex::new(TestApp {})),
            term_store,
            log_store,
            state_meta,
        );
        let raft_server = RaftNodeServer { handles };

        raft_server.handles.initiator.reset_voted_for().await;

        let payload = bincode::serialize("some payload").unwrap();

        // 1st request

        let entry1 = Entry {
            index: 1,
            term: 0,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
            session_info: None,
        };
        let entry2 = Entry {
            index: 2,
            term: 1,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
            session_info: None,
        };
        let entry3 = Entry {
            index: 3,
            term: 2,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
            session_info: None,
        };
        let entry4 = Entry {
            index: 4,
            term: 2,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
            session_info: None,
        };
        let entry5 = Entry {
            index: 5,
            term: 2,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
            session_info: None,
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

        let reply = raft_server
            .append_entries(request1)
            .await
            .unwrap()
            .into_inner();

        assert!(reply.success);
        assert_eq!(reply.term, 2);

        assert_eq!(raft_server.handles.log_store.get_last_log_index().await, 5);
        assert_eq!(raft_server.handles.executor.get_last_applied().await, 5);

        // 2nd request

        let entry6 = Entry {
            index: 6,
            term: 2,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
            session_info: None,
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

        let reply2 = raft_server
            .append_entries(request2)
            .await
            .unwrap()
            .into_inner();

        assert!(reply2.success);
        assert_eq!(reply2.term, 2);

        assert_eq!(raft_server.handles.log_store.get_last_log_index().await, 6);

        assert_eq!(raft_server.handles.executor.get_last_applied().await, 6);

        // 3rd request

        let msg3 = AppendEntriesRequest {
            term: 4,
            leader_id: 3, // changed to check if hint is updated after leader change
            prev_log_index: 5,
            prev_log_term: 2,
            entries: vec![],
            leader_commit: 6,
        };
        let request3 = Request::new(msg3);

        let reply3 = raft_server
            .append_entries(request3)
            .await
            .unwrap()
            .into_inner();

        assert!(reply3.success);
        assert_eq!(reply3.term, 4);
        assert_eq!(raft_server.handles.log_store.get_last_log_index().await, 6);

        assert_eq!(raft_server.handles.executor.get_last_applied().await, 6);
        assert_eq!(
            raft_server.handles.state_store.get_leader_id().await,
            Some(3)
        );

        assert_eq!(
            raft_server
                .handles
                .log_store
                .read_last_entry()
                .await
                .unwrap()
                .payload,
            payload
        );
    }

    #[tokio::test]
    async fn request_votes_test() {
        let mut db_paths = get_test_db_paths(3).await;
        let state_store = StateStoreHandle::default();
        let wd = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(wd.clone(), db_paths.pop().unwrap());
        let log_store = LogStoreHandle::new(db_paths.pop().unwrap());

        log_store.reset_log().await;
        term_store.reset_term().await;

        let state_meta = StateMeta {
            last_log_index: 0, // only matters for replicator
            last_log_term: 0,  // only matters for replicator
            term: 0,
            id: 0,
            leader_commit: 0,
        };
        let config = get_test_config().await;
        let handles = RaftHandles::build(
            state_store,
            wd,
            config,
            Arc::new(Mutex::new(TestApp {})),
            term_store,
            log_store,
            state_meta,
        );
        let raft_server = RaftNodeServer { handles };

        raft_server.handles.initiator.reset_voted_for().await;

        assert_eq!(raft_server.handles.term_store.get_term().await, 0);

        // grant vote since voted for is none and term > current term
        let msg = RequestVoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };

        let request = Request::new(msg);

        let reply = raft_server
            .request_votes(request)
            .await
            .unwrap()
            .into_inner();

        assert!(reply.vote_granted);
        assert_eq!(reply.term, 1);

        // deny vote since term < than current term
        let msg2 = RequestVoteRequest {
            term: 0,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let request2 = Request::new(msg2);

        let reply2 = raft_server
            .request_votes(request2)
            .await
            .unwrap()
            .into_inner();

        assert!(!reply2.vote_granted);
        assert_eq!(reply2.term, 1);

        // deny vote for id 2, since id 1 was already granted
        let msg3 = RequestVoteRequest {
            term: 1,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        };

        let request3 = Request::new(msg3);

        let reply3 = raft_server
            .request_votes(request3)
            .await
            .unwrap()
            .into_inner();

        assert!(!reply3.vote_granted);
        assert_eq!(reply3.term, 1);

        // grant vote for id 1 since it was already granted before (term cannot be checked here)
        let msg4 = RequestVoteRequest {
            term: 1,
            candidate_id: 1,
            last_log_index: 0,
            last_log_term: 0,
        };

        let request4 = Request::new(msg4);

        let reply4 = raft_server
            .request_votes(request4)
            .await
            .unwrap()
            .into_inner();

        assert!(reply4.vote_granted);
        assert_eq!(reply4.term, 1);
    }
}
