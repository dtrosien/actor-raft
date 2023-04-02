use crate::actors::log::executor::ExecutorHandle;
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::term_store::TermStoreHandle;
use crate::config::Node;
use crate::raft_rpc::append_entries_request::Entry;
use crate::raft_rpc::AppendEntriesRequest;
use crate::rpc::client;
use tokio::sync::{mpsc, oneshot};

struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    term_store: TermStoreHandle,
    log_store: LogStoreHandle,
    executor: ExecutorHandle,
    node: Node,
    next_index: u64,
    match_index: u64,
    term: u64,
    leader_id: u64,
    leader_commit: u64,
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
        last_log_index: u64,
        term: u64,
        leader_id: u64,
        leader_commit: u64,
    ) -> Self {
        Worker {
            receiver,
            term_store,
            log_store,
            executor,
            node,
            next_index: last_log_index + 1,
            match_index: 0,
            term,
            leader_id,
            leader_commit,
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

    async fn replicate_entry(&self, entry: Entry) {
        let ip = self.node.ip.clone();
        let port = self.node.port;
        let uri = format!("https://{ip}:{port}");

        let request = AppendEntriesRequest {
            term: 0,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };

        let response = client::append_entry(uri, request).await;

        let _ = self.term_store.check_term(0).await;
        //todo implementation
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
        last_log_index: u64,
        term: u64,
        leader_id: u64,
        leader_commit: u64,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Worker::new(
            receiver,
            term_store,
            log_store,
            executor,
            node,
            last_log_index,
            term,
            leader_id,
            leader_commit,
        );
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
            0,
            0,
            0,
            0,
        );
        assert_eq!(worker.get_node().await.id, node.id);
    }
}
