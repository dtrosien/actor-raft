use crate::actors::election::counter::CounterHandle;
use crate::actors::term::TermHandle;
use crate::config::Node;
use crate::raft_rpc::RequestVoteRequest;
use crate::rpc;
use crate::rpc::client::Reply;
use tokio::sync::{mpsc, oneshot};

struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    term: TermHandle,
    counter: CounterHandle,
    node: Node,
}

enum WorkerMsg {
    RequestVote,
    GetNode { respond_to: oneshot::Sender<Node> },
}

impl Worker {
    fn new(
        receiver: mpsc::Receiver<WorkerMsg>,
        term: TermHandle,
        counter: CounterHandle,
        node: Node,
    ) -> Self {
        Worker {
            receiver,
            term,
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
            WorkerMsg::RequestVote => {
                let vote = self.request_vote().await;
                self.counter.register_vote(vote).await;
            }
            WorkerMsg::GetNode { respond_to } => {
                let _ = respond_to.send(self.node.clone());
            }
        }
    }

    async fn request_vote(&self) -> Option<bool> {
        //todo adjust input params
        let request = RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
        let uri = "".to_string();

        match rpc::client::request_vote(uri, request).await {
            Ok(reply) => {
                //todo term check here correct?
                self.term.check_term(reply.term).await;
                Some(reply.success_or_granted)
            }
            Err(_) => Some(true), //todo fix tests
        }
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerMsg>,
}

impl WorkerHandle {
    pub fn new(term: TermHandle, counter: CounterHandle, node: Node) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut worker = Worker::new(receiver, term, counter, node);
        tokio::spawn(async move { worker.run().await });

        Self { sender }
    }

    pub async fn get_node(&self) -> Node {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetNode { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn request_vote(&self) {
        let msg = WorkerMsg::RequestVote;
        let _ = self.sender.send(msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::watchdog::WatchdogHandle;
    use std::time::Duration;

    #[tokio::test]
    async fn get_node_test() {
        let watchdog = WatchdogHandle::default();
        let term = TermHandle::new(watchdog.clone());
        let votes_required: u64 = 3;
        let counter = CounterHandle::new(watchdog, votes_required).await;
        let node = Node {
            id: 0,
            fqdn: "".to_string(),
            port: 0,
        };
        let worker = WorkerHandle::new(term, counter, node.clone());

        assert_eq!(worker.get_node().await.id, node.id);
    }

    #[tokio::test]
    async fn request_vote_test() {
        let watchdog = WatchdogHandle::default();
        let term = TermHandle::new(watchdog.clone());
        let votes_required: u64 = 3;
        let counter = CounterHandle::new(watchdog, votes_required).await;
        let node = Node {
            id: 0,
            fqdn: "".to_string(),
            port: 0,
        };
        let worker = WorkerHandle::new(term, counter.clone(), node);

        assert_eq!(counter.clone().get_votes_received().await, 0);
        worker.request_vote().await;
        // sleep necessary to make sure that vote is processed before getting it
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(counter.clone().get_votes_received().await, 1);
    }
}
