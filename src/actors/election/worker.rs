use crate::actors::election::counter::CounterHandle;
use crate::actors::term::TermHandle;
use crate::config::Node;
use tokio::sync::mpsc;

struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    term: TermHandle,
    counter: CounterHandle,
    node: Node,
}

enum WorkerMsg {
    RequestVote,
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
                let vote = self.send_request_vote();
                self.counter.register_vote(vote).await;
            }
        }
    }

    fn send_request_vote(&self) -> Option<bool> {
        //todo call rpc function
        Some(true)
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

    pub async fn request_vote(&self) {
        let msg = WorkerMsg::RequestVote;
        let _ = self.sender.send(msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::watchdog::WatchdogHandle;

    #[tokio::test]
    async fn request_vote_test() {
        let watchdog = WatchdogHandle::default();
        let term = TermHandle::new(watchdog.clone());
        let counter = CounterHandle::new(watchdog).await;
        let node = Node {
            id: 0,
            fqdn: "".to_string(),
            port: 0,
        };
        let worker = WorkerHandle::new(term, counter, node);
        //todo write test
    }
}
