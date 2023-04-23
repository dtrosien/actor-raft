use crate::actors::log::executor::ExecutorHandle;
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::worker::{StateMeta, WorkerHandle};
use crate::actors::term_store::TermStoreHandle;

use crate::config::Config;
use crate::raft_rpc::append_entries_request::Entry;
use std::collections::BTreeMap;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct Replicator {
    receiver: mpsc::Receiver<ReplicatorMsg>,
    term: u64,
    executor: ExecutorHandle,
    workers: BTreeMap<u64, WorkerHandle>,
}

#[derive(Debug)]
enum ReplicatorMsg {
    SetTerm { term: u64 },
    GetTerm { respond_to: oneshot::Sender<u64> },
    ReplicateEntry { entry: Entry },
    AddToBatch { entry: Entry },
    FlushBatch,
    RegisterWorkers,
}

impl Replicator {
    #[tracing::instrument(ret, level = "debug")]
    fn new(
        //todo check if it is better to get init values from Handles and make new() async
        receiver: mpsc::Receiver<ReplicatorMsg>,
        executor: ExecutorHandle,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        config: Config,
        state_meta: StateMeta,
    ) -> Self {
        let workers = config
            .nodes
            .into_iter()
            .map(|node| {
                (
                    node.id,
                    WorkerHandle::new(
                        term_store.clone(),
                        log_store.clone(),
                        executor.clone(),
                        node,
                        state_meta.clone(),
                    ),
                )
            })
            .collect();

        Replicator {
            receiver,
            term: state_meta.term,
            executor,
            workers,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: ReplicatorMsg) {
        match msg {
            ReplicatorMsg::SetTerm { term } => self.term = term,
            ReplicatorMsg::GetTerm { respond_to } => {
                let _ = respond_to.send(self.term);
            }
            ReplicatorMsg::ReplicateEntry { entry } => self.replicate_entry(entry).await,
            ReplicatorMsg::AddToBatch { entry } => self.add_to_batch(entry).await,
            ReplicatorMsg::FlushBatch => self.flush_batch().await,
            ReplicatorMsg::RegisterWorkers => self.register_workers_at_executor().await,
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn replicate_entry(&self, entry: Entry) {
        for worker in self.workers.values() {
            worker.replicate_entry(entry.clone()).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn add_to_batch(&self, entry: Entry) {
        for worker in self.workers.values() {
            worker.add_to_replication_batch(entry.clone()).await;
        }
    }

    // can be used for sending heartbeats
    #[tracing::instrument(ret, level = "debug")]
    async fn flush_batch(&self) {
        for worker in self.workers.values() {
            worker.flush_replication_batch().await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn register_workers_at_executor(&self) {
        for worker in self.workers.keys() {
            self.executor.register_worker(*worker).await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct ReplicatorHandle {
    sender: mpsc::Sender<ReplicatorMsg>,
}

impl ReplicatorHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(
        executor: ExecutorHandle,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        config: Config,
        state_meta: StateMeta,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Replicator::new(
            receiver, executor, term_store, log_store, config, state_meta,
        );
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn replicate_entry(&self, entry: Entry) {
        let msg = ReplicatorMsg::ReplicateEntry { entry };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn add_to_batch(&self, entry: Entry) {
        let msg = ReplicatorMsg::AddToBatch { entry };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn flush_batch(&self) {
        let msg = ReplicatorMsg::FlushBatch;
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn register_workers_at_executor(&self) {
        let msg = ReplicatorMsg::RegisterWorkers;
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn set_term(&self, term: u64) {
        let msg = ReplicatorMsg::SetTerm { term };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_term(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ReplicatorMsg::GetTerm { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::log::log_store::LogStoreHandle;
    use crate::actors::log::test_utils::TestApp;
    use crate::actors::term_store::TermStoreHandle;
    use crate::actors::watchdog::WatchdogHandle;
    use crate::db::test_utils::get_test_db_paths;
    use crate::rpc::test_utils::{start_test_server, TestServerFalse, TestServerTrue};
    use std::time::Duration;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn term_test() {
        let (config, state_meta, replicator, log_store, executor, term_store, mut error_recv) =
            prepare_test_dependencies().await;
        let state_meta = StateMeta {
            previous_log_index: 0,
            previous_log_term: 0,
            term: 0,
            leader_id: 0,
            leader_commit: 0,
        };

        let replicator = ReplicatorHandle::new(executor, term_store, log_store, config, state_meta);
        replicator.set_term(1).await;
        assert_eq!(replicator.get_term().await, 1);
    }

    #[tokio::test]
    async fn replication_test() {
        let (config, state_meta, replicator, log_store, executor, term_store, mut error_recv) =
            prepare_test_dependencies().await;

        let entry = Entry {
            index: 1,
            term: 1,
            leader_commit: 0,
            payload: "".to_string(),
        };

        log_store.append_entry(entry.clone()).await;

        let test_future = async {
            // sleep required to make sure that server is up
            tokio::time::sleep(Duration::from_millis(15)).await;
            replicator.replicate_entry(entry).await;
            // sleep required to make sure that worker can process entry
            tokio::time::sleep(Duration::from_millis(15)).await;
        };

        // wait for test future or servers to return
        tokio::select! {
            _ = error_recv.recv() => panic!("exit was fired, probably because of term error"),
            _ = start_test_server(config.nodes[0].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[0].id),
            _ = start_test_server(config.nodes[1].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[1].id),
            _ = start_test_server(config.nodes[2].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[2].id),
            _ = start_test_server(config.nodes[3].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[3].id),
            _ = test_future => {
                assert_eq!(executor.get_commit_index().await,1);
                }
        }
    }

    #[tokio::test]
    async fn batch_replication_test() {
        let (config, state_meta, replicator, log_store, executor, term_store, mut error_recv) =
            prepare_test_dependencies().await;

        for i in 1..=100 {
            let entry = Entry {
                index: i,
                term: 1,
                leader_commit: 0,
                payload: "".to_string(),
            };
            replicator.add_to_batch(entry.clone()).await;
            log_store.append_entry(entry.clone()).await;
        }

        let test_future = async {
            // sleep required to make sure that server is up
            tokio::time::sleep(Duration::from_millis(15)).await;
            replicator.flush_batch().await;
            // sleep required to make sure that worker can process entry
            tokio::time::sleep(Duration::from_millis(15)).await;
        };

        // wait for test future or servers to return
        tokio::select! {
            _ = error_recv.recv() => panic!("exit was fired, probably because of term error"),
            _ = start_test_server(config.nodes[0].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[0].id),
            _ = start_test_server(config.nodes[1].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[1].id),
            _ = start_test_server(config.nodes[2].port, TestServerFalse {}) => panic!("server {} returned first",config.nodes[2].id),
            _ = start_test_server(config.nodes[3].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[3].id),
            _ = test_future => {
                assert_eq!(executor.get_commit_index().await,100);
                }
        }
    }

    async fn prepare_test_dependencies() -> (
        Config,
        StateMeta,
        ReplicatorHandle,
        LogStoreHandle,
        ExecutorHandle,
        TermStoreHandle,
        broadcast::Receiver<()>,
    ) {
        let wd = WatchdogHandle::default();
        let error_recv = wd.get_exit_receiver().await;

        let mut test_db_paths = get_test_db_paths(2).await;

        let term_store = TermStoreHandle::new(wd.clone(), test_db_paths.pop().unwrap());
        // term must be at least 1 since mock server replies 1
        term_store.increment_term().await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;
        let app = Box::new(TestApp {});
        let executor = ExecutorHandle::new(log_store.clone(), 1, app);

        let config = Config::for_test().await;

        let state_meta = StateMeta {
            previous_log_index: 0,
            previous_log_term: 0,
            term: 1,
            leader_id: 0,
            leader_commit: 0,
        };

        let replicator = ReplicatorHandle::new(
            executor.clone(),
            term_store.clone(),
            log_store.clone(),
            config.clone(),
            state_meta.clone(),
        );

        replicator.register_workers_at_executor().await;

        (
            config, state_meta, replicator, log_store, executor, term_store, error_recv,
        )
    }
}
