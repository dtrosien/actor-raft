use crate::actors::log::executor::ExecutorHandle;
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::worker::WorkerHandle;
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
        receiver: mpsc::Receiver<ReplicatorMsg>,
        term: u64,
        executor: ExecutorHandle,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        config: Config,
        last_log_index: u64,
    ) -> Self {
        let workers = config
            .nodes
            .into_iter()
            .map(|node| {
                (
                    node.id,
                    WorkerHandle::new(term_store.clone(), log_store.clone(), node, last_log_index),
                )
            })
            .collect();

        Replicator {
            receiver,
            term,
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
        term: u64,
        executor: ExecutorHandle,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        config: Config,
        last_log_index: u64,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Replicator::new(
            receiver,
            term,
            executor,
            term_store,
            log_store,
            config,
            last_log_index,
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

        let replicator =
            ReplicatorHandle::new(0, executor, term_store, log_store, config, last_log_index);
        replicator.set_term(1).await;
        assert_eq!(replicator.get_term().await, 1);
    }
}
