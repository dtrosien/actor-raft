use tokio::sync::{mpsc, oneshot};

struct Actor {
    receiver: mpsc::Receiver<ActorMsg>,
    id: u64,
}

enum ActorMsg {
    GetId { respond_to: oneshot::Sender<u64> },
}

impl Actor {
    fn new(receiver: mpsc::Receiver<ActorMsg>) -> Self {
        Actor { receiver, id: 1 }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: ActorMsg) {
        match msg {
            ActorMsg::GetId { respond_to } => {
                // The `let _ =` ignores any errors when sending.
                //
                // This can happen if the `select!` macro is used
                // to cancel waiting for the response.
                let _ = respond_to.send(self.id);
            }
        }
    }
}

#[derive(Clone)]
pub struct ActorHandle {
    sender: mpsc::Sender<ActorMsg>,
}

impl ActorHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Actor::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn get_id(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ActorMsg::GetId { respond_to: send };

        // Ignore send errors. If this send fails, so does the
        // recv.await below. There's no reason to check for the
        // same failure twice.
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

impl Default for ActorHandle {
    fn default() -> Self {
        ActorHandle::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn id_test() {
        let actor = ActorHandle::new();
        assert_eq!(actor.get_id().await, 1);
    }
}
