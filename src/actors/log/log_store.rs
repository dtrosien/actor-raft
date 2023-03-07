use crate::raft_rpc::append_entries_request::Entry;
use tokio::sync::{mpsc, oneshot};

struct LogStore {
    receiver: mpsc::Receiver<LogStoreMsg>,
    last_log_index: u64,
    last_log_term: u64,
    previous_log_index: u64,
    previous_log_term: u64,
}

enum LogStoreMsg {
    GetLastLogIndex { respond_to: oneshot::Sender<u64> },
    GetLastLogTerm { respond_to: oneshot::Sender<u64> },
    GetPreviousLogIndex { respond_to: oneshot::Sender<u64> },
    GetPreviousLogTerm { respond_to: oneshot::Sender<u64> },
    AppendEntry { entry: Entry },
    AppendEntries { entries: Vec<Entry> },
}

impl LogStore {
    fn new(receiver: mpsc::Receiver<LogStoreMsg>) -> Self {
        LogStore {
            receiver,
            last_log_index: 0, //todo proper init
            last_log_term: 0,
            previous_log_index: 0,
            previous_log_term: 0,
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
                let _ = respond_to.send(self.last_log_index);
            }
            LogStoreMsg::GetLastLogTerm { respond_to } => {
                let _ = respond_to.send(self.last_log_term);
            }
            LogStoreMsg::GetPreviousLogIndex { respond_to } => {
                let _ = respond_to.send(self.previous_log_index);
            }
            LogStoreMsg::GetPreviousLogTerm { respond_to } => {
                let _ = respond_to.send(self.previous_log_term);
            }
            LogStoreMsg::AppendEntry { entry } => self.append_entries(vec![entry]),
            LogStoreMsg::AppendEntries { entries } => self.append_entries(entries),
        }
    }

    fn append_entries(&self, entries: Vec<Entry>) {
        //todo implement
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

    pub async fn append_entry(&self, entry: Entry) {
        let msg = LogStoreMsg::AppendEntry { entry };
        let _ = self.sender.send(msg).await;
    }

    pub async fn append_entries(&self, entries: Vec<Entry>) {
        let msg = LogStoreMsg::AppendEntries { entries };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_last_log_index(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::GetLastLogIndex { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn get_last_log_term(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::GetLastLogTerm { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn get_previous_log_index(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::GetPreviousLogIndex { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn get_previous_log_term(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::GetPreviousLogTerm { respond_to: send };
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
