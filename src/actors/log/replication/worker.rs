use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Node;
use tokio::sync::{mpsc, oneshot};

struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    watchdog: WatchdogHandle,
    log_store: LogStoreHandle,
    node: Node,
}

enum WorkerMsg {
    GetNode { respond_to: oneshot::Sender<Node> },
}

impl Worker {
    fn new(
        receiver: mpsc::Receiver<WorkerMsg>,
        watchdog: WatchdogHandle,
        log_store: LogStoreHandle,
        node: Node,
    ) -> Self {
        Worker {
            receiver,
            watchdog,
            log_store,
            node,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::GetNode { respond_to } => {
                let _ = respond_to.send(self.node.clone());
            }
        }
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerMsg>,
}

impl WorkerHandle {
    pub fn new(watchdog: WatchdogHandle, log_store: LogStoreHandle, node: Node) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Worker::new(receiver, watchdog, log_store, node);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn get_node(&self) -> Node {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetNode { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::log::test_utils::get_test_db;

    #[tokio::test]
    async fn id_test() {
        let wd = WatchdogHandle::default();
        let log_store = LogStoreHandle::new(get_test_db().await);

        let node = Node {
            id: 0,
            ip: "".to_string(),
            port: 0,
        };

        let worker = WorkerHandle::new(wd, log_store, node.clone());
        assert_eq!(worker.get_node().await.id, node.id);
    }
}
