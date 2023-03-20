use crate::actors::log::log_store::LogStoreHandle;
use crate::raft_rpc::append_entries_request::Entry;

use std::cmp::min;
use std::collections::BTreeMap;
use std::error::Error;
use tokio::sync::{mpsc, oneshot};

pub trait App: Send + Sync {
    fn run(&self, entry: Entry) -> Result<bool, Box<dyn Error + Send + Sync>>;
}

struct Executor {
    receiver: mpsc::Receiver<ExecutorMsg>,
    commit_index: u64,
    last_applied: u64,
    num_workers: u64,
    max_repl_indices: BTreeMap<u64, u64>,
    log_store: LogStoreHandle,
    app: Box<dyn App>,
}

enum ExecutorMsg {
    GetCommitIndex {
        respond_to: oneshot::Sender<u64>,
    },
    SetCommitIndex {
        index: u64,
    },
    GetLastApplied {
        respond_to: oneshot::Sender<u64>,
    },
    SetLastApplied {
        index: u64,
    },
    GetNumWorkers {
        respond_to: oneshot::Sender<u64>,
    },
    SetNumWorkers {
        num: u64,
    },
    CommitLog {
        entry: Entry,
    },
    ApplyLog {
        respond_to: oneshot::Sender<Result<bool, Box<dyn Error + Send + Sync>>>,
    },
}

impl Executor {
    fn new(
        receiver: mpsc::Receiver<ExecutorMsg>,
        log_store: LogStoreHandle,
        app: Box<dyn App>,
    ) -> Self {
        Executor {
            receiver,
            commit_index: 0,
            last_applied: 0,
            num_workers: 0,
            max_repl_indices: Default::default(),
            log_store,
            app,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: ExecutorMsg) {
        match msg {
            ExecutorMsg::GetCommitIndex { respond_to } => {
                let _ = respond_to.send(self.commit_index);
            }
            ExecutorMsg::SetCommitIndex { index } => self.commit_index = index,
            ExecutorMsg::GetLastApplied { respond_to } => {
                let _ = respond_to.send(self.last_applied);
            }
            ExecutorMsg::SetLastApplied { index } => self.last_applied = index,
            ExecutorMsg::GetNumWorkers { respond_to } => {
                let _ = respond_to.send(self.num_workers);
            }
            ExecutorMsg::SetNumWorkers { num } => self.num_workers = num,
            ExecutorMsg::CommitLog { entry } => self.commit_log(entry).await,
            ExecutorMsg::ApplyLog { respond_to } => {
                let _ = respond_to.send(self.apply_log().await);
            }
        }
    }

    async fn commit_log(&mut self, entry: Entry) {
        if entry.leader_commit > self.commit_index {
            self.commit_index = min(entry.leader_commit, entry.index);
        }
    }

    async fn apply_log(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        //todo think about reply type and how to use it and if it is really necessary
        //todo maybe add a second fn which doesnt wait and reply (to not block the main thread)
        let mut reply = false;
        while self.last_applied < self.commit_index {
            let entry_to_be_applied = self.last_applied + 1;
            if let Some(entry) = self.log_store.read_entry(entry_to_be_applied).await {
                reply = self.app.run(entry)?;
            };
            self.last_applied = entry_to_be_applied;
        }
        Ok(reply)
    }

    async fn register_worker(&mut self, worker_id: u64) {
        //sets key to id and value to 0 if not exists , else returns value
        self.max_repl_indices.entry(worker_id).or_insert(0);
    }

    // used in leader state
    async fn register_replication_success(&mut self, worker_id: u64, index: u64) -> u64 {
        if self.max_repl_indices.contains_key(&worker_id) {
            self.max_repl_indices.insert(worker_id, index);

            //todo get min value from btree (see last step in rules for leader)
            //todo write tests
        }
        self.commit_index
    }
}

async fn test() -> Result<(), Box<dyn Error + Send + Sync>> {
    Ok(())
}

#[derive(Clone)]
pub struct ExecutorHandle {
    sender: mpsc::Sender<ExecutorMsg>,
}

impl ExecutorHandle {
    pub fn new(log_store: LogStoreHandle, app: Box<dyn App>) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Executor::new(receiver, log_store, app);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn commit_log(&self, entry: Entry) {
        let msg = ExecutorMsg::CommitLog { entry };
        let _ = self.sender.send(msg).await;
    }

    pub async fn apply_log(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::ApplyLog { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn get_commit_index(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetCommitIndex { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    async fn set_commit_index(&self, index: u64) {
        let msg = ExecutorMsg::SetCommitIndex { index };
        let _ = self.sender.send(msg).await;
    }

    async fn set_last_applied(&self, index: u64) {
        let msg = ExecutorMsg::SetLastApplied { index };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_last_applied(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetLastApplied { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn set_num_workers(&self, num: u64) {
        let msg = ExecutorMsg::SetNumWorkers { num };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_num_workers(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetNumWorkers { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use tokio::sync::Mutex;

    // global var used to offer unique dbs for each log store in unit tests to prevent concurrency issues while testing
    static DB_COUNTER: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(0));
    // get number from GLOBAL_DB_COUNTER
    pub async fn get_test_db() -> String {
        let mut i = DB_COUNTER.lock().await;
        *i += 1;
        format!("databases/executor-test-db_{}", *i)
    }

    struct TestApp {}

    impl App for TestApp {
        fn run(&self, entry: Entry) -> Result<bool, Box<dyn Error + Send + Sync>> {
            println!("hey there");
            Ok(true)
        }
    }

    #[tokio::test]
    async fn get_commit_index_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        let app = Box::new(TestApp {});
        let executor = ExecutorHandle::new(log_store, app);

        assert_eq!(executor.get_commit_index().await, 0);
    }

    #[tokio::test]
    async fn commit_log_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        let app = Box::new(TestApp {});
        let executor = ExecutorHandle::new(log_store, app);

        // index is lower than leader commit -> index wins
        let entry1 = Entry {
            index: 1,
            term: 0,
            leader_commit: 2,
            payload: "".to_string(),
        };
        executor.commit_log(entry1).await;
        assert_eq!(executor.get_commit_index().await, 1);

        //leader commit is lower than index -> leader commit wins
        let entry2 = Entry {
            index: 4,
            term: 0,
            leader_commit: 2,
            payload: "".to_string(),
        };
        executor.commit_log(entry2).await;
        assert_eq!(executor.get_commit_index().await, 2);
    }

    #[tokio::test]
    async fn apply_log_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        log_store.reset_log().await;
        let entry1 = Entry {
            index: 1,
            term: 1,
            leader_commit: 2,
            payload: "".to_string(),
        };
        let entry2 = Entry {
            index: 2,
            term: 1,
            leader_commit: 2,
            payload: "".to_string(),
        };
        log_store.append_entry(entry1).await;
        log_store.append_entry(entry2).await;

        let app = Box::new(TestApp {});
        let executor = ExecutorHandle::new(log_store, app);

        //test initial state
        assert!(!executor.apply_log().await.unwrap());

        //test that nothing happens if commit index is behind (should not happen)
        executor.set_commit_index(1).await;
        executor.set_last_applied(2).await;
        assert!(!executor.apply_log().await.unwrap());
        assert_eq!(executor.get_last_applied().await, 2);
        assert_eq!(executor.get_commit_index().await, 1);

        //test that last_applied increases until commit_index is reached and logs get applied
        executor.set_commit_index(2).await;
        executor.set_last_applied(0).await;
        assert!(executor.apply_log().await.unwrap());
        assert_eq!(executor.get_last_applied().await, 2);
        assert_eq!(executor.get_commit_index().await, 2);
    }
}
