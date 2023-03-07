use tokio::sync::{mpsc, oneshot};

struct LogStore {
    receiver: mpsc::Receiver<LogStoreMsg>,
    last_log_index: u64,
}

enum LogStoreMsg {
    GetLastLogIndex { respond_to: oneshot::Sender<u64> },
}

impl LogStore {
    fn new(receiver: mpsc::Receiver<LogStoreMsg>) -> Self {
        LogStore {
            receiver,
            last_log_index: 0,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: LogStoreMsg) {
        match msg {
            LogStoreMsg::GetLastLogIndex { respond_to } => {
                // The `let _ =` ignores any errors when sending.
                //
                // This can happen if the `select!` macro is used
                // to cancel waiting for the response.
                let _ = respond_to.send(self.last_log_index);
            }
        }
    }
}

#[derive(Clone)]
pub struct LogStoreHandle {
    sender: mpsc::Sender<LogStoreMsg>,
}

impl LogStoreHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = LogStore::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn get_last_log_index(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::GetLastLogIndex { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

impl Default for LogStoreHandle {
    fn default() -> Self {
        LogStoreHandle::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_last_log_index_test() {
        let log_store = LogStoreHandle::new();
        assert_eq!(log_store.get_last_log_index().await, 0);
    }
}
