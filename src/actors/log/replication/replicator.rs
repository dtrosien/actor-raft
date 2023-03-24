use tokio::sync::{mpsc, oneshot};

struct Replicator {
    receiver: mpsc::Receiver<ReplicatorMsg>,
    term: u64,
}

enum ReplicatorMsg {
    SetTerm { term: u64 },
    GetTerm { respond_to: oneshot::Sender<u64> },
}

impl Replicator {
    fn new(receiver: mpsc::Receiver<ReplicatorMsg>, term: u64) -> Self {
        Replicator { receiver, term }
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
    pub fn new(term: u64) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Replicator::new(receiver, term);
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

    #[tokio::test]
    async fn term_test() {
        let replicator = ReplicatorHandle::new(0);
        replicator.set_term(1).await;
        assert_eq!(replicator.get_term().await, 1);
    }
}
