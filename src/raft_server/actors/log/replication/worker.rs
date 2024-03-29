use crate::raft_server::actors::log::executor::ExecutorHandle;
use crate::raft_server::actors::log::log_store::LogStoreHandle;
use crate::raft_server::actors::term_store::TermStoreHandle;
use crate::raft_server::config::NodeConfig;
use crate::raft_server::rpc::node_client;
use crate::raft_server_rpc::append_entries_request::Entry;
use crate::raft_server_rpc::AppendEntriesRequest;

use std::collections::VecDeque;
use std::error::Error;

use crate::raft_server::rpc::node_client::NodeClient;
use crate::raft_server::state_meta::StateMeta;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

#[derive(Debug)]
struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    term_store: TermStoreHandle,
    log_store: LogStoreHandle,
    executor: ExecutorHandle,
    node: NodeConfig,
    uri: String,
    state_meta: StateMeta,
    entries_cache: VecDeque<Entry>,
    client: Option<NodeClient>,
    reload: bool,
}

#[derive(Debug)]
enum WorkerMsg {
    GetNode {
        respond_to: oneshot::Sender<NodeConfig>,
    },
    GetStateMeta {
        respond_to: oneshot::Sender<StateMeta>,
    },
    GetCachedEntries {
        respond_to: oneshot::Sender<VecDeque<Entry>>,
    },
    ReplicateEntry {
        entry: Entry,
    },
    AddToReplicationBatch {
        entry: Entry,
    },
    FlushReplicationBatch,
    SetStateMeta {
        respond_to: oneshot::Sender<()>,
        state_meta: StateMeta,
    },
}

impl Worker {
    #[tracing::instrument(ret, level = "debug")]
    fn new(
        receiver: mpsc::Receiver<WorkerMsg>,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        executor: ExecutorHandle,
        node: NodeConfig,
        state_meta: StateMeta,
    ) -> Self {
        let ip = node.ip.clone();
        let port = node.port;
        let uri = format!("https://{ip}:{port}");
        Worker {
            receiver,
            term_store,
            log_store,
            executor,
            node,
            uri,
            state_meta,
            entries_cache: Default::default(),
            client: None,
            reload: false,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::GetNode { respond_to } => {
                let _ = respond_to.send(self.node.clone());
            }
            WorkerMsg::GetStateMeta { respond_to } => {
                let _ = respond_to.send(self.state_meta.clone());
            }
            WorkerMsg::GetCachedEntries { respond_to } => {
                let _ = respond_to.send(self.entries_cache.clone());
            }
            WorkerMsg::ReplicateEntry { entry } => self.replicate_entry(entry).await,
            WorkerMsg::AddToReplicationBatch { entry } => {
                self.add_to_replication_batch(entry).await
            }
            WorkerMsg::FlushReplicationBatch => self.flush_replication_batch().await,
            WorkerMsg::SetStateMeta {
                respond_to,
                state_meta,
            } => {
                let _ = {
                    self.state_meta = state_meta;
                    respond_to.send(())
                };
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn replicate_entry(&mut self, entry: Entry) {
        self.add_to_replication_batch(entry).await;
        self.flush_replication_batch().await;
    }

    // one of the biggest deviations compared to the original paper. the match and the next index
    // are not needed here. among other things because of the event based triggering by passing
    // entries to the worker and keeping them in the cache. maybe a last index has to be
    // created in case the cache is cleared
    // todo [refactor] clean up + nicer way of handling broken connections (build client)
    #[tracing::instrument(ret, level = "debug")]
    async fn send_append_request(
        &mut self,
        request: AppendEntriesRequest,
        last_entry_index: u64,
        last_entry_term: u64,
    ) {
        if self.init_client_if_necessary().await.is_err() {
            error!("unable to build client for node {} with address: {},{} for replication, retry with next request ",
                    self.node.id, self.node.ip, self.node.port);
            return;
        };

        match self
            .client
            .as_mut()
            .expect("client must exist")
            .append_entry(request)
            .await
        {
            Ok(response) => {
                self.term_store.check_term(response.term).await;
                if response.success_or_granted {
                    self.state_meta.last_log_index = last_entry_index;
                    self.state_meta.last_log_term = last_entry_term;
                    self.state_meta.leader_commit = self
                        .executor
                        .register_replication_success(self.node.id, last_entry_index)
                        .await;
                    self.entries_cache.clear();
                    self.reload = false;
                } else {
                    // new try with next heartbeat
                    warn!(
                        "log not up to date for {}, try to append previous entry with index : {}, term: {}",
                        self.node.id, self.state_meta.last_log_index,self.state_meta.last_log_term
                    );
                    self.append_previous_entry_to_log_cache().await;
                }
            }
            Err(_) => {
                // nothing to do, request will be retried with next heartbeat
                // todo [later feature] implement dead node logic to prevent unnecessary rpc calls
                error!(
                    "error while sending append entry rpc to node {} with address: {},{}",
                    self.node.id, self.node.ip, self.node.port
                );
                self.client.take();
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn init_client_if_necessary(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.client.is_some() {
            return Ok(());
        }
        info!("init node client for replication");
        NodeClient::build(self.node.ip.clone(), self.node.port)
            .await
            .map(|x| {
                self.client.replace(x);
            })
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn append_previous_entry_to_log_cache(&mut self) {
        // todo [refactor] clean up

        if !self.reload {
            if let Some(entry) = self
                .log_store
                .read_entry(self.state_meta.last_log_index)
                .await
            {
                self.entries_cache.push_back(entry.clone());
            }
            self.reload = true;
        }

        match self
            .log_store
            .read_previous_entry(self.state_meta.last_log_index)
            .await
        {
            Some(previous_entry) => {
                info!("previous entry found with index: {}", previous_entry.index);
                // add entry to back of the cache queue
                self.entries_cache.push_back(previous_entry.clone());
                // update log metadata
                self.state_meta.last_log_index = previous_entry.index;
                self.state_meta.last_log_term = previous_entry.term;
            }
            None => {
                // check if beginning of log was reached
                if self.state_meta.last_log_index <= 1 {
                    self.state_meta.last_log_index = 0;
                    self.state_meta.last_log_term = 0;
                } else {
                    // this should not happen
                    panic!(
                        "no previous entry found for index: {} in worker for node{} of leader {}",
                        self.state_meta.last_log_index, self.node.id, self.state_meta.id
                    )
                }
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn add_to_replication_batch(&mut self, entry: Entry) {
        self.entries_cache.push_front(entry);
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn flush_replication_batch(&mut self) {
        let request = self.build_append_request().await;

        match self.entries_cache.front() {
            None => {
                self.send_append_request(
                    request,
                    self.state_meta.last_log_index,
                    self.state_meta.last_log_term,
                )
                .await;
            }
            Some(last_entry) => {
                self.send_append_request(request, last_entry.index, last_entry.term)
                    .await;
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn build_append_request(&self) -> AppendEntriesRequest {
        AppendEntriesRequest {
            term: self.state_meta.term,
            leader_id: self.state_meta.id,
            prev_log_index: self.state_meta.last_log_index, // be not confused by naming: the current last entry will be the previous entry of the upcoming request
            prev_log_term: self.state_meta.last_log_term,
            entries: self.entries_cache.iter().rev().cloned().collect(),
            leader_commit: self.state_meta.leader_commit,
        }
    }
}

#[derive(Clone, Debug)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerMsg>,
}

impl WorkerHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        executor: ExecutorHandle,
        node: NodeConfig,
        state_meta: StateMeta,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Worker::new(receiver, term_store, log_store, executor, node, state_meta);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_node(&self) -> NodeConfig {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetNode { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn get_state_meta(&self) -> StateMeta {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetStateMeta { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn get_cached_entries(&self) -> VecDeque<Entry> {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetCachedEntries { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn replicate_entry(&self, entry: Entry) {
        let msg = WorkerMsg::ReplicateEntry { entry };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn add_to_replication_batch(&self, entry: Entry) {
        let msg = WorkerMsg::AddToReplicationBatch { entry };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn flush_replication_batch(&self) {
        let msg = WorkerMsg::FlushReplicationBatch;
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn set_state_meta(&self, state_meta: StateMeta) {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::SetStateMeta {
            respond_to: send,
            state_meta,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_server::actors::client_store::ClientStoreHandle;
    use crate::raft_server::actors::log::test_utils::TestApp;
    use crate::raft_server::actors::watchdog::WatchdogHandle;
    use crate::raft_server::db::test_utils::get_test_db_paths;
    use crate::raft_server::rpc::utils::test::{
        get_test_port, start_test_server, TestServerFalse, TestServerTrue,
    };
    use crate::raft_server::state_meta::StateMeta;
    use crate::raft_server_rpc::EntryType;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::{broadcast, Mutex};

    #[tokio::test]
    async fn get_node_test() {
        let (_port, node, log_store, executor, term_store, mut _error_recv) =
            prepare_test_dependencies().await;

        // initialize state
        let meta = StateMeta {
            last_log_index: 0,
            last_log_term: 1,
            term: 1,
            id: 0,
            leader_commit: 0,
        };

        let worker = WorkerHandle::new(term_store, log_store, executor, node.clone(), meta);

        assert_eq!(worker.get_node().await.id, node.id);
    }

    #[tokio::test]
    async fn replication_success_test() {
        // setup of required actor dependencies
        let (port, node, log_store, executor, term_store, mut error_recv) =
            prepare_test_dependencies().await;

        // initialize state
        let meta = StateMeta {
            last_log_index: 0,
            last_log_term: 1,
            term: 1,
            id: 0,
            leader_commit: 0,
        };

        let entry = Entry {
            index: 1,
            term: 1,
            entry_type: i32::from(EntryType::Command),
            payload: bincode::serialize("some payload").unwrap(),
            session_info: None,
        };

        log_store.append_entry(entry.clone()).await;
        executor.register_worker(node.id).await;
        let worker = WorkerHandle::new(term_store, log_store, executor, node, meta);

        // start actual test

        assert_eq!(worker.get_state_meta().await.last_log_index, 0);
        assert_eq!(worker.get_state_meta().await.last_log_term, 1);
        assert_eq!(worker.get_state_meta().await.leader_commit, 0);

        let test_future = async {
            // sleep necessary to make sure that server is up
            tokio::time::sleep(Duration::from_millis(15)).await;
            worker.replicate_entry(entry).await;
            // sleep necessary to make sure that entry is processed
            tokio::time::sleep(Duration::from_millis(15)).await;
        };

        tokio::select! {
            _ = start_test_server(port, TestServerTrue {}) => panic!("server returned first"),
            _ = error_recv.recv() => panic!("exit was fired, probably because of term error"),
            _ = test_future => {
                assert_eq!(worker.get_state_meta().await.last_log_index, 1);
                assert_eq!(worker.get_state_meta().await.last_log_term, 1);
                assert_eq!(worker.get_state_meta().await.leader_commit, 1);
                assert!(worker.get_cached_entries().await.is_empty())
                }
        }
    }

    #[tokio::test]
    async fn batch_replication_success_test() {
        // setup of required actor dependencies
        let (port, node, log_store, executor, term_store, mut error_recv) =
            prepare_test_dependencies().await;

        // initialize state
        let meta = StateMeta {
            last_log_index: 0,
            last_log_term: 1,
            term: 1,
            id: 0,
            leader_commit: 0,
        };

        executor.register_worker(node.id).await;
        let worker = WorkerHandle::new(term_store, log_store.clone(), executor, node, meta);

        // start actual test

        assert!(worker.get_cached_entries().await.is_empty());
        let payload = bincode::serialize("some payload").unwrap();
        for i in 1..=100 {
            let entry = Entry {
                index: i,
                term: 1,
                entry_type: i32::from(EntryType::Command),
                payload: payload.clone(),
                session_info: None,
            };
            worker.add_to_replication_batch(entry.clone()).await;
            log_store.append_entry(entry.clone()).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert_eq!(worker.get_state_meta().await.last_log_index, 0);
        assert_eq!(worker.get_state_meta().await.last_log_term, 1);
        assert_eq!(worker.get_state_meta().await.leader_commit, 0);
        assert_eq!(worker.get_cached_entries().await.len(), 100);

        let test_future = async {
            // sleep necessary to make sure that server is up
            tokio::time::sleep(Duration::from_millis(15)).await;
            worker.flush_replication_batch().await;
            // sleep necessary to make sure that entry is processed
            tokio::time::sleep(Duration::from_millis(15)).await;
        };

        tokio::select! {
            _ = start_test_server(port, TestServerTrue {}) => panic!("server returned first"),
            _ = error_recv.recv() => panic!("exit was fired, probably because of term error"),
            _ = test_future => {
                assert_eq!(worker.get_state_meta().await.last_log_index, 100);
                assert_eq!(worker.get_state_meta().await.last_log_term, 1);
                assert_eq!(worker.get_state_meta().await.leader_commit, 100);
                assert!(worker.get_cached_entries().await.is_empty())
                }
        }
    }

    #[tokio::test]
    async fn replication_fail_test() {
        // setup of required actor dependencies
        let (port, node, log_store, executor, term_store, mut error_recv) =
            prepare_test_dependencies().await;

        // initialize state
        let meta = StateMeta {
            last_log_index: 10,
            last_log_term: 1,
            term: 1,
            id: 0,
            leader_commit: 10,
        };

        executor.register_worker(node.id).await;
        let worker = WorkerHandle::new(term_store, log_store.clone(), executor, node, meta);

        // start actual test

        assert!(worker.get_cached_entries().await.is_empty());

        let payload = bincode::serialize("some payload").unwrap();
        // old entries not in cache
        for i in 1..=10 {
            let entry = Entry {
                index: i,
                term: 1,
                entry_type: i32::from(EntryType::Command),
                payload: payload.clone(),
                session_info: None,
            };
            log_store.append_entry(entry.clone()).await;
        }

        // new entries still in cache
        for i in 11..=15 {
            let entry = Entry {
                index: i,
                term: 1,
                entry_type: i32::from(EntryType::Command),
                payload: payload.clone(),
                session_info: None,
            };
            log_store.append_entry(entry.clone()).await;
            worker.add_to_replication_batch(entry).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert_eq!(worker.get_state_meta().await.last_log_index, 10);
        assert_eq!(worker.get_state_meta().await.last_log_term, 1);
        assert_eq!(worker.get_state_meta().await.leader_commit, 10);
        assert_eq!(worker.get_cached_entries().await.len(), 5);

        let test_future = async {
            // sleep necessary to make sure that server is up
            tokio::time::sleep(Duration::from_millis(15)).await;

            // flush twice to simulate two retries
            worker.flush_replication_batch().await;
            worker.flush_replication_batch().await;

            // sleep necessary to make sure that entry is processed
            tokio::time::sleep(Duration::from_millis(15)).await;
        };

        tokio::select! {
            _ = start_test_server(port, TestServerFalse {}) => panic!("server returned first"),
            _ = error_recv.recv() => panic!("exit was fired, probably because of term error"),
            _ = test_future => {
                assert_eq!(worker.get_state_meta().await.last_log_index, 8);
                assert_eq!(worker.get_state_meta().await.last_log_term, 1);
                assert_eq!(worker.get_state_meta().await.leader_commit, 10);
                assert_eq!(worker.get_cached_entries().await.len(), 8);
                assert_eq!(worker.get_cached_entries().await.pop_front().unwrap().index ,15);
                assert_eq!(worker.get_cached_entries().await.pop_back().unwrap().index, 8);
                }
        }
    }

    async fn prepare_test_dependencies() -> (
        u16,
        NodeConfig,
        LogStoreHandle,
        ExecutorHandle,
        TermStoreHandle,
        broadcast::Receiver<()>,
    ) {
        let wd = WatchdogHandle::default();
        let error_recv = wd.get_exit_receiver().await;
        let mut test_db_paths = get_test_db_paths(2).await;

        let term_store = TermStoreHandle::new(wd.clone(), test_db_paths.pop().unwrap());
        term_store.reset_term().await;

        // term must be at least 1 since mock server replies 1
        term_store.increment_term().await;

        let app = Arc::new(Mutex::new(TestApp {}));
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;
        let client_store = ClientStoreHandle::new();
        let executor = ExecutorHandle::new(
            log_store.clone(),
            client_store,
            term_store.get_term().await,
            app,
        );
        // test server connection
        let port = get_test_port().await;
        let node = NodeConfig {
            id: 0,
            ip: "[::1]".to_string(),
            port,
        };

        (port, node, log_store, executor, term_store, error_recv)
    }
}
