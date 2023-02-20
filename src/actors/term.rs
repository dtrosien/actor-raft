use tokio::sync::{mpsc, oneshot};

struct Term {
    receiver: mpsc::Receiver<TermMsg>,
    term: u64,
}

enum TermMsg {
    GetTerm { respond_to: oneshot::Sender<u64> },
}

impl Term {
    fn new(receiver: mpsc::Receiver<TermMsg>) -> Self {
        Term { receiver, term: 0 }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: TermMsg) {
        match msg {
            TermMsg::GetTerm { respond_to } => {
                let _ = respond_to.send(self.term);
            }
        }
    }
}

#[derive(Clone)]
pub struct TermHandle {
    sender: mpsc::Sender<TermMsg>,
}

impl TermHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut term = Term::new(receiver);
        tokio::spawn(async move { term.run().await });

        Self { sender }
    }

    pub async fn get_term(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = TermMsg::GetTerm { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

impl Default for TermHandle {
    fn default() -> Self {
        TermHandle::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_term_test() {
        let actor = TermHandle::new();
        assert_eq!(actor.get_term().await, 0);
    }
}
