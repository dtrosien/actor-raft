use crate::actors::log::executor::ExecutorHandle;
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::term_store::TermStoreHandle;
use crate::config::Node;
use crate::raft_rpc::append_entries_request::Entry;
use crate::raft_rpc::AppendEntriesRequest;
use crate::rpc::client;
use crate::rpc::client::Reply;
use std::time::Duration;

use std::collections::VecDeque;
use std::error::Error;
use tokio::sync::{mpsc, oneshot};

struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    term_store: TermStoreHandle,
    log_store: LogStoreHandle,
    executor: ExecutorHandle,
    node: Node,
    next_index: u64,
    match_index: u64,
    previous_log_index: u64,
    previous_log_term: u64,
    term: u64,
    leader_id: u64,
    leader_commit: u64,
    entries_cache: VecDeque<Entry>,
}

enum WorkerMsg {
    GetNode { respond_to: oneshot::Sender<Node> },
    ReplicateEntry { entry: Entry },
}

impl Worker {
    fn new(
        receiver: mpsc::Receiver<WorkerMsg>,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        executor: ExecutorHandle,
        node: Node,
        state_meta: InitialStateMeta,
    ) -> Self {
        Worker {
            receiver,
            term_store,
            log_store,
            executor,
            node,
            next_index: state_meta.last_log_index + 1,
            match_index: 0,
            previous_log_index: state_meta.previous_log_index,
            previous_log_term: state_meta.previous_log_term,
            term: state_meta.term,
            leader_id: state_meta.leader_id,
            leader_commit: state_meta.leader_commit,
            entries_cache: Default::default(),
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::GetNode { respond_to } => {
                let _ = respond_to.send(self.node.clone());
            }
            WorkerMsg::ReplicateEntry { entry } => self.replicate_entry(entry).await,
        }
    }

    async fn replicate_entry(&mut self, entry: Entry) {
        self.entries_cache.push_front(entry);

        //todo collect logs and then send batch
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(5)) => {},
            _ = tokio::time::sleep(Duration::from_millis(5)) => {},
        }

        let request = AppendEntriesRequest {
            term: self.term,
            leader_id: self.leader_id,
            prev_log_index: self.previous_log_index,
            prev_log_term: self.previous_log_term,
            entries: Vec::from(self.entries_cache.clone()),
            leader_commit: self.leader_commit,
        };

        if let Some(last_entry) = self.entries_cache.front() {
            self.send_append_request(request, last_entry.index, last_entry.term)
                .await;
        }

        //todo implementation
    }

    pub async fn send_append_request(
        &mut self,
        request: AppendEntriesRequest,
        last_entry_index: u64,
        last_entry_term: u64,
    ) {
        let ip = self.node.ip.clone();
        let port = self.node.port;
        let uri = format!("https://{ip}:{port}");

        match client::append_entry(uri, request).await {
            Ok(response) => {
                self.term_store.check_term(response.term).await;
                if response.success_or_granted {
                    self.next_index = last_entry_index + 1;
                    self.match_index = last_entry_index;
                    self.previous_log_index = last_entry_index; //todo get rid of duplicates
                    self.previous_log_term = last_entry_term;
                    self.executor
                        .register_replication_success(self.node.id, last_entry_index)
                        .await;
                }
            }
            Err(_) => {
                //todo implementation

                println!("wrong")
            }
        }
    }

    pub async fn send_heartbeat() {}

    async fn catch_up_log(&self) {

        //todo implement case when follower does not have current entries in log (via log_store)
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerMsg>,
}

impl WorkerHandle {
    pub fn new(
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        executor: ExecutorHandle,
        node: Node,
        state_meta: InitialStateMeta,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Worker::new(receiver, term_store, log_store, executor, node, state_meta);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn get_node(&self) -> Node {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetNode { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[derive(Clone)]
pub struct InitialStateMeta {
    pub last_log_index: u64,
    pub previous_log_index: u64,
    pub previous_log_term: u64,
    pub term: u64,
    pub leader_id: u64,
    pub leader_commit: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::log::test_utils::{get_test_db, TestApp};

    #[tokio::test]
    async fn id_test() {
        let term_store = TermStoreHandle::default();
        let log_store = LogStoreHandle::new(get_test_db().await);
        let app = Box::new(TestApp {});

        let executor = ExecutorHandle::new(log_store.clone(), 0, app);
        let node = Node {
            id: 0,
            ip: "".to_string(),
            port: 0,
        };

        let worker = WorkerHandle::new(
            term_store,
            log_store.clone(),
            executor,
            node.clone(),
            InitialStateMeta {
                last_log_index: 0,
                previous_log_index: 0,
                previous_log_term: 0,
                term: 0,
                leader_id: 0,
                leader_commit: 0,
            },
        );
        assert_eq!(worker.get_node().await.id, node.id);
    }
}
