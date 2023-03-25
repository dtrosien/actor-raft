use crate::actors::election::counter::CounterHandle;
use crate::actors::term_store::TermStoreHandle;
use crate::config::Node;
use crate::raft_rpc::RequestVoteRequest;
use crate::rpc;

use tokio::sync::{mpsc, oneshot};

struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    term_store: TermStoreHandle,
    counter: CounterHandle,
    node: Node,
}

enum WorkerMsg {
    RequestVote { request: RequestVoteRequest },
    GetNode { respond_to: oneshot::Sender<Node> },
}

impl Worker {
    fn new(
        receiver: mpsc::Receiver<WorkerMsg>,
        term: TermStoreHandle,
        counter: CounterHandle,
        node: Node,
    ) -> Self {
        Worker {
            receiver,
            term_store: term,
            counter,
            node,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::RequestVote { request } => {
                let vote = self.request_vote(request).await;
                self.counter.register_vote(vote).await;
            }
            WorkerMsg::GetNode { respond_to } => {
                let _ = respond_to.send(self.node.clone());
            }
        }
    }

    async fn request_vote(&self, request: RequestVoteRequest) -> Option<bool> {
        let ip = self.node.ip.clone();
        let port = self.node.port;
        let uri = format!("https://{ip}:{port}");

        match rpc::client::request_vote(uri, request).await {
            Ok(reply) => {
                // this call is non blocking and might fire a term error
                self.term_store.check_term(reply.term).await;
                Some(reply.success_or_granted)
            }
            Err(_) => Some(false),
        }
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerMsg>,
}

impl WorkerHandle {
    pub fn new(term_store: TermStoreHandle, counter: CounterHandle, node: Node) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut worker = Worker::new(receiver, term_store, counter, node);
        tokio::spawn(async move { worker.run().await });

        Self { sender }
    }

    pub async fn get_node(&self) -> Node {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetNode { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn request_vote(&self, request: RequestVoteRequest) {
        let msg = WorkerMsg::RequestVote { request };
        let _ = self.sender.send(msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::watchdog::WatchdogHandle;
    use crate::rpc::test_tools::{get_test_port, start_test_server, TestServerTrue};
    use std::time::Duration;

    #[tokio::test]
    async fn get_node_test() {
        let watchdog = WatchdogHandle::default();
        let term_store = TermStoreHandle::new(watchdog.clone());
        let votes_required: u64 = 3;
        let counter = CounterHandle::new(watchdog, votes_required).await;
        let node = Node {
            id: 0,
            ip: "".to_string(),
            port: 0,
        };
        let worker = WorkerHandle::new(term_store, counter, node.clone());

        assert_eq!(worker.get_node().await.id, node.id);
    }

    #[tokio::test]
    async fn request_vote_test() {
        // initialise test setup
        let watchdog = WatchdogHandle::default();
        let term_store = TermStoreHandle::new(watchdog.clone());
        let votes_required: u64 = 3;
        let counter = CounterHandle::new(watchdog, votes_required).await;
        let port = get_test_port().await;
        let node = Node {
            id: 0,
            ip: "[::1]".to_string(),
            port,
        };
        let worker = WorkerHandle::new(term_store, counter.clone(), node);
        let request = RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };

        // start test
        assert_eq!(counter.clone().get_votes_received().await, 0);

        let test_future = async {
            // sleep necessary to make sure that server is up
            tokio::time::sleep(Duration::from_millis(15)).await;
            worker.request_vote(request).await;
            // sleep necessary to make sure that vote is processed before getting it
            tokio::time::sleep(Duration::from_millis(15)).await;
        };

        tokio::select! {
            _ = start_test_server(port, TestServerTrue {}) => panic!("server returned first"),
            _ = test_future => {
                assert_eq!(counter.clone().get_votes_received().await, 1);
                }
        }
    }
}
