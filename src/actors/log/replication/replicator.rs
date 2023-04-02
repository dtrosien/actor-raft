use crate::actors::log::executor::ExecutorHandle;
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::worker::{InitialStateMeta, WorkerHandle};
use crate::actors::term_store::TermStoreHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Config;
use crate::raft_rpc::append_entries_request::Entry;
use std::collections::BTreeMap;
use tokio::sync::{mpsc, oneshot};

struct Replicator {
    receiver: mpsc::Receiver<ReplicatorMsg>,
    term: u64,
    executor: ExecutorHandle,
    workers: BTreeMap<u64, WorkerHandle>,
}

enum ReplicatorMsg {
    SetTerm { term: u64 },
    GetTerm { respond_to: oneshot::Sender<u64> },
    ReplicateEntry { entry: Entry },
}

impl Replicator {
    fn new(
        //todo check if it is better to get init values from Handles and make new() async
        receiver: mpsc::Receiver<ReplicatorMsg>,
        executor: ExecutorHandle,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        config: Config,
        state_meta: InitialStateMeta,
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

    async fn handle_message(&mut self, msg: ReplicatorMsg) {
        match msg {
            ReplicatorMsg::SetTerm { term } => self.term = term,
            ReplicatorMsg::GetTerm { respond_to } => {
                let _ = respond_to.send(self.term);
            }
            ReplicatorMsg::ReplicateEntry { entry } => self.replicate_entry(entry).await,
        }
    }

    async fn replicate_entry(&self, entry: Entry) {}
}

#[derive(Clone)]
pub struct ReplicatorHandle {
    sender: mpsc::Sender<ReplicatorMsg>,
}

impl ReplicatorHandle {
    pub fn new(
        executor: ExecutorHandle,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        config: Config,
        state_meta: InitialStateMeta,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Replicator::new(
            receiver, executor, term_store, log_store, config, state_meta,
        );
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn replicate_entry(&self, entry: Entry) {
        let msg = ReplicatorMsg::ReplicateEntry { entry };
        let _ = self.sender.send(msg).await;
    }

    pub async fn set_term(&self, term: u64) {
        let msg = ReplicatorMsg::SetTerm { term };
        let _ = self.sender.send(msg).await;
    }

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
    use crate::actors::log::test_utils::{get_test_db, TestApp};
    use crate::actors::term_store::TermStoreHandle;

    #[tokio::test]
    async fn term_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        let app = Box::new(TestApp {});
        let executor = ExecutorHandle::new(log_store.clone(), 0, app);
        let term_store = TermStoreHandle::default();
        let config = Config::for_test().await;
        let last_log_index = log_store.get_last_log_index().await;
        let leader_commit = executor.get_commit_index().await;
        let state_meta = InitialStateMeta {
            last_log_index,
            previous_log_index: 0,
            previous_log_term: 0,
            term: 0,
            leader_id: 0,
            leader_commit,
        };

        let replicator = ReplicatorHandle::new(executor, term_store, log_store, config, state_meta);
        replicator.set_term(1).await;
        assert_eq!(replicator.get_term().await, 1);
    }
}
