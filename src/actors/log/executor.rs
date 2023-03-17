use crate::raft_rpc::append_entries_request::Entry;
use std::cmp::min;
use tokio::sync::{mpsc, oneshot};

struct Executor {
    receiver: mpsc::Receiver<ExecutorMsg>,
    commit_index: u64,
    last_applied: u64,
}

enum ExecutorMsg {
    GetCommitIndex { respond_to: oneshot::Sender<u64> },
    CommitLog { entry: Entry },
    ApplyLog { entry: Entry },
}

impl Executor {
    fn new(receiver: mpsc::Receiver<ExecutorMsg>) -> Self {
        Executor {
            receiver,
            commit_index: 0,
            last_applied: 0,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: ExecutorMsg) {
        match msg {
            ExecutorMsg::GetCommitIndex { respond_to } => {
                let _ = respond_to.send(self.commit_index);
            }
            ExecutorMsg::CommitLog { entry } => self.commit_log(entry),
            ExecutorMsg::ApplyLog { entry } => self.apply_log(entry),
        }
    }

    fn commit_log(&mut self, entry: Entry) {
        if entry.leader_commit > self.commit_index {
            self.commit_index = min(entry.leader_commit, entry.index);
        }
    }

    fn apply_log(&self, entry: Entry) {}
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

    pub async fn commit_log(&self, entry: Entry) {
        let msg = ExecutorMsg::CommitLog { entry };
        let _ = self.sender.send(msg).await;
    }

    pub async fn apply_log(&self, entry: Entry) {
        let msg = ExecutorMsg::ApplyLog { entry };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_commit_index(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetCommitIndex { respond_to: send };

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
    async fn get_commit_index_test() {
        let executor = ExecutorHandle::new();
        assert_eq!(executor.get_commit_index().await, 0);
    }

    #[tokio::test]
    async fn apply_log_test() {
        let executor = ExecutorHandle::new();
        assert_eq!(executor.get_commit_index().await, 0);
    }
}
