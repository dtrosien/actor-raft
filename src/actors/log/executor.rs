use tokio::sync::{mpsc, oneshot};

struct Executor {
    receiver: mpsc::Receiver<ExecutorMsg>,
    id: u64,
}

enum ExecutorMsg {
    GetId { respond_to: oneshot::Sender<u64> },
}

impl Executor {
    fn new(receiver: mpsc::Receiver<ExecutorMsg>) -> Self {
        Executor { receiver, id: 1 }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: ExecutorMsg) {
        match msg {
            ExecutorMsg::GetId { respond_to } => {
                let _ = respond_to.send(self.id);
            }
        }
    }
}

#[derive(Clone)]
pub struct ExecutorHandle {
    sender: mpsc::Sender<ExecutorMsg>,
}

impl ExecutorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Executor::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn get_id(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetId { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

impl Default for ExecutorHandle {
    fn default() -> Self {
        ExecutorHandle::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn id_test() {
        let executor = ExecutorHandle::new();
        assert_eq!(executor.get_id().await, 1);
    }
}
