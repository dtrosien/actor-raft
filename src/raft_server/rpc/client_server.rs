use crate::app::AppResult;
use crate::raft_client_rpc::raft_client_rpc_server::RaftClientRpc;
use crate::raft_client_rpc::{
    ClientQueryReply, ClientQueryRequest, ClientRequestReply, ClientRequestRequest,
    RegisterClientReply, RegisterClientRequest,
};
use crate::raft_server::raft_handles::RaftHandles;

use crate::raft_server::raft_node::ServerState;
use crate::raft_server_rpc::{EntryType, SessionInfo};
use tonic::{Request, Response, Status};
use tracing::{error, info, warn};

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

        let session = SessionInfo {
            client_id: rpc_arguments.client_id,
            sequence_num: rpc_arguments.sequence_num,
        };

        info!(
            "got client request for session id:{}, seq:{}",
            session.client_id, session.sequence_num
        );

        // check if request already in client store to prevent enforce only once semantic
        //todo client store

        // creat entry with index
        match self
            .handles
            .create_entry(rpc_arguments.command, EntryType::Command, Some(session))
            .await
        {
            None => {
                warn!("no entry was created");
                let leader_id = self.handles.state_store.get_leader_id().await;
                deny_client_request(leader_id)
            }
            Some(entry) => {
                let index = entry.index;

                // append entry to local log and replicate
                self.handles.append_entry(entry).await;

                // wait until log was applied and get result
                // todo [feature/bug?] maybe wrap with timeout
                match self.handles.wait_for_execution_notification(index).await {
                    None => deny_client_request(None),
                    Some(result) => accept_client_request(None, result),
                }
            }
        }
    }

    async fn register_client(
        &self,
        request: Request<RegisterClientRequest>,
    ) -> Result<Response<RegisterClientReply>, Status> {
        let rpc_arguments = request.into_inner();
        info!("got client registration request");

        let leader_id = self.handles.state_store.get_leader_id().await;

        // step 1 reply false with leader hint if not leader
        if self.handles.state_store.get_state().await != ServerState::Leader {
            return deny_client_registration(leader_id);
        }

        // step 2 append register command to log, replicate and commit it

        match self
            .handles
            .create_entry(vec![], EntryType::Registration, None) // todo session ifo  payload optional if empty vec creates problems
            .await
        {
            None => {
                warn!("no entry was created");
                deny_client_registration(leader_id)
            }
            Some(entry) => {
                let index = entry.index;

                // append entry to local log and replicate
                self.handles.append_entry(entry).await;

                // wait until log was applied and get result
                // todo [feature/bug?] maybe wrap with timeout
                match self.handles.wait_for_execution_notification(index).await {
                    None => deny_client_registration(None),
                    Some(result) => {
                        let client_id: u64 = bincode::deserialize(&result.payload).unwrap();
                        accept_client_registration(None, client_id)
                    }
                }
            }
        }
    }

    async fn client_query(
        &self,
        request: Request<ClientQueryRequest>,
    ) -> Result<Response<ClientQueryReply>, Status> {
        let leader_id = self.handles.state_store.get_leader_id().await;

        // step 1
        if self.handles.state_store.get_state().await != ServerState::Leader {
            return deny_client_query(leader_id);
        }

        // step 2 + 3
        let current_term = self.handles.term_store.get_term().await;
        let read_index = self.handles.executor.get_commit_index().await;

        let committed_entry_term = self.handles.executor.get_commit_term().await;

        if current_term != committed_entry_term {
            error!(
                "client_query: current term != commited entry term: {} | {}",
                current_term, committed_entry_term
            );
            return deny_client_query(leader_id);
        }

        // step 4
        self.handles.send_heartbeat().await;

        // step 5
        if self.handles.executor.get_last_applied().await < read_index {
            self.handles
                .wait_for_execution_notification(read_index)
                .await;
        };

        // execute query and return result
        if let Ok(app_result) = self
            .handles
            .app
            .lock()
            .await
            .query(request.into_inner().query)
            .await
        {
            accept_client_query(leader_id, app_result)
        } else {
            deny_client_query(leader_id)
        }
    }
}

fn deny_client_request(leader_id: Option<u64>) -> Result<Response<ClientRequestReply>, Status> {
    let reply = ClientRequestReply {
        status: false,
        response: None,
        leader_hint: leader_id,
    };
    Ok(Response::new(reply))
}

fn accept_client_request(
    leader_id: Option<u64>,
    result: AppResult,
) -> Result<Response<ClientRequestReply>, Status> {
    let reply = ClientRequestReply {
        status: result.success,
        response: Some(result.payload),
        leader_hint: leader_id,
    };
    Ok(Response::new(reply))
}

fn deny_client_query(leader_id: Option<u64>) -> Result<Response<ClientQueryReply>, Status> {
    let reply = ClientQueryReply {
        status: false,
        response: None,
        leader_hint: leader_id,
    };
    Ok(Response::new(reply))
}

fn accept_client_query(
    leader_id: Option<u64>,
    result: AppResult,
) -> Result<Response<ClientQueryReply>, Status> {
    let reply = ClientQueryReply {
        status: result.success,
        response: Some(result.payload),
        leader_hint: leader_id,
    };
    Ok(Response::new(reply))
}

fn deny_client_registration(
    leader_id: Option<u64>,
) -> Result<Response<RegisterClientReply>, Status> {
    let reply = RegisterClientReply {
        status: false,
        client_id: None,
        leader_hint: leader_id,
    };
    Ok(Response::new(reply))
}

fn accept_client_registration(
    leader_id: Option<u64>,
    client_id: u64,
) -> Result<Response<RegisterClientReply>, Status> {
    let reply = RegisterClientReply {
        status: true,
        client_id: Some(client_id),
        leader_hint: leader_id,
    };
    Ok(Response::new(reply))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_client_rpc::raft_client_rpc_server::RaftClientRpc;
    use crate::raft_client_rpc::ClientRequestRequest;
    use crate::raft_server::actors::log::log_store::LogStoreHandle;
    use crate::raft_server::actors::log::test_utils::TestApp;
    use crate::raft_server::actors::state_store::StateStoreHandle;
    use crate::raft_server::actors::term_store::TermStoreHandle;
    use crate::raft_server::actors::watchdog::WatchdogHandle;
    use crate::raft_server::config::get_test_config;
    use crate::raft_server::db::test_utils::get_test_db_paths;
    use crate::raft_server::raft_handles::RaftHandles;
    use crate::raft_server::raft_node::ServerState;
    use crate::raft_server::rpc::client_server::RaftClientServer;
    use crate::raft_server::state_meta::StateMeta;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn client_request_accept_test() {
        // init test setup
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

        let mut config = get_test_config().await;
        // to avoid state change from leader
        config.state_timeout = 10000;
        let handles = RaftHandles::build(
            state_store,
            wd,
            config,
            Arc::new(Mutex::new(TestApp {})),
            term_store,
            log_store,
            state_meta,
        );
        let client_server = RaftClientServer { handles };

        client_server.handles.initiator.reset_voted_for().await;

        client_server
            .handles
            .replicator
            .register_workers_at_executor()
            .await;

        client_server
            .handles
            .state_store
            .change_state(ServerState::Leader)
            .await;

        // start test

        let command = bincode::serialize("some payload").unwrap();

        let msg1 = ClientRequestRequest {
            client_id: 0,
            sequence_num: 0,
            command,
        };
        let request1 = Request::new(msg1);

        //  fake replication since no servers are running
        let repl_fake = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let repl1 = client_server
                .handles
                .executor
                .register_replication_success(1, 1)
                .await;
            let repl2 = client_server
                .handles
                .executor
                .register_replication_success(2, 1)
                .await;
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert_eq!(client_server.handles.executor.get_commit_index().await, 1);
        };

        let response = tokio::join!(client_server.client_request(request1), repl_fake).0;

        let request_reply = response.unwrap().into_inner();

        let reply_payload: String = bincode::deserialize(&request_reply.response.unwrap()).unwrap();

        assert_eq!(reply_payload, "successful execution"); // this string is set in TestApp
        assert_eq!(request_reply.leader_hint, None) // when leader no hint is given
    }

    #[tokio::test]
    async fn client_request_denied_test() {
        // init test setup
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

        let mut config = get_test_config().await;
        // to avoid state change from leader
        config.state_timeout = 10000;
        let handles = RaftHandles::build(
            state_store,
            wd,
            config,
            Arc::new(Mutex::new(TestApp {})),
            term_store,
            log_store,
            state_meta,
        );
        let client_server = RaftClientServer { handles };

        client_server.handles.initiator.reset_voted_for().await;

        let leader_id = Some(4);

        client_server
            .handles
            .state_store
            .set_leader_id(leader_id)
            .await;

        // start test

        let command = bincode::serialize("some payload").unwrap();

        let msg1 = ClientRequestRequest {
            client_id: 0,
            sequence_num: 0,
            command,
        };
        let request1 = Request::new(msg1);
        let response = client_server.client_request(request1).await;
        let request_reply = response.unwrap().into_inner();

        assert!(!request_reply.status);
        assert_eq!(request_reply.response, None);
        assert_eq!(request_reply.leader_hint, leader_id) // when leader no hint is given
    }

    #[tokio::test]
    async fn client_query_accept_test() {
        // init test setup
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

        let mut config = get_test_config().await;
        // to avoid state change from leader
        config.state_timeout = 10000;
        let handles = RaftHandles::build(
            state_store,
            wd,
            config,
            Arc::new(Mutex::new(TestApp {})),
            term_store,
            log_store,
            state_meta,
        );
        let client_server = RaftClientServer { handles };

        client_server.handles.initiator.reset_voted_for().await;

        client_server
            .handles
            .replicator
            .register_workers_at_executor()
            .await;

        client_server
            .handles
            .state_store
            .change_state(ServerState::Leader)
            .await;

        // start test

        let query = bincode::serialize("TestQuery").unwrap();

        let msg1 = ClientQueryRequest { query };
        let request1 = Request::new(msg1);

        let response = client_server.client_query(request1).await;

        let request_reply = response.unwrap().into_inner();

        let reply_payload: String = bincode::deserialize(&request_reply.response.unwrap()).unwrap();

        assert_eq!(reply_payload, "successful query: TestQuery"); // this string is set in TestApp
        assert_eq!(request_reply.leader_hint, None) // when leader no hint is given
    }

    #[tokio::test]
    async fn client_query_denied_test() {
        // init test setup
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

        let mut config = get_test_config().await;
        // to avoid state change from leader
        config.state_timeout = 10000;
        let handles = RaftHandles::build(
            state_store,
            wd,
            config,
            Arc::new(Mutex::new(TestApp {})),
            term_store,
            log_store,
            state_meta,
        );
        let client_server = RaftClientServer { handles };

        client_server.handles.initiator.reset_voted_for().await;

        let leader_id = Some(4);

        client_server
            .handles
            .state_store
            .set_leader_id(leader_id)
            .await;

        // start test

        let msg1 = ClientQueryRequest { query: vec![] };
        let request1 = Request::new(msg1);
        let response = client_server.client_query(request1).await;
        let request_reply = response.unwrap().into_inner();

        assert!(!request_reply.status);
        assert_eq!(request_reply.response, None);
        assert_eq!(request_reply.leader_hint, leader_id) // when leader no hint is given
    }
}
