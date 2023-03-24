use tokio::sync::{mpsc, oneshot};

struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    id: u64,
}

enum WorkerMsg {
    GetId { respond_to: oneshot::Sender<u64> },
}

impl Worker {
    fn new(receiver: mpsc::Receiver<WorkerMsg>) -> Self {
        Worker { receiver, id: 1 }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::GetId { respond_to } => {
                let _ = respond_to.send(self.id);
            }
        }
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerMsg>,
}

impl WorkerHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Worker::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn get_id(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetId { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

impl Default for WorkerHandle {
    fn default() -> Self {
        WorkerHandle::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn id_test() {
        let worker = WorkerHandle::new();
        assert_eq!(worker.get_id().await, 1);
    }
}
