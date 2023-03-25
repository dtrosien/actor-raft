use crate::actors::log::executor::ExecutorHandle;
use tokio::sync::{mpsc, oneshot};

struct Replicator {
    receiver: mpsc::Receiver<ReplicatorMsg>,
    term: u64,
    executor: ExecutorHandle,
}

enum ReplicatorMsg {
    SetTerm { term: u64 },
    GetTerm { respond_to: oneshot::Sender<u64> },
}

impl Replicator {
    fn new(receiver: mpsc::Receiver<ReplicatorMsg>, term: u64, executor: ExecutorHandle) -> Self {
        Replicator {
            receiver,
            term,
            executor,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: ReplicatorMsg) {
        match msg {
            ReplicatorMsg::SetTerm { term } => self.term = term,
            ReplicatorMsg::GetTerm { respond_to } => {
                let _ = respond_to.send(self.term);
            }
        }
    }
}

#[derive(Clone)]
pub struct ReplicatorHandle {
    sender: mpsc::Sender<ReplicatorMsg>,
}

impl ReplicatorHandle {
    pub fn new(term: u64, executor: ExecutorHandle) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Replicator::new(receiver, term, executor);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
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

    #[tokio::test]
    async fn term_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        let app = Box::new(TestApp {});
        let executor = ExecutorHandle::new(log_store, 0, app);
        let replicator = ReplicatorHandle::new(0, executor);
        replicator.set_term(1).await;
        assert_eq!(replicator.get_term().await, 1);
    }
}
