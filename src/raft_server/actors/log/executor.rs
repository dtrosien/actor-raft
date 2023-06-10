use crate::raft_server::actors::log::log_store::LogStoreHandle;
use crate::raft_server_rpc::append_entries_request::Entry;

use crate::app::{App, AppResult};
use crate::raft_server::state_meta::StateMeta;
use crate::raft_server_rpc::EntryType;

use std::cmp::min;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::collections::HashMap;
use std::error::Error;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, oneshot, Mutex};

#[derive(Debug)]
struct Executor {
    receiver: mpsc::Receiver<ExecutorMsg>,
    applied_sender: broadcast::Sender<(u64, AppResult)>,
    // todo sender to client store
    commit_index: u64,
    commit_term: u64, // term of last committed entry, used for queries to prevent db reads
    last_applied: u64,
    num_workers: u64,
    current_term: u64,
    match_index: HashMap<u64, u64>,
    log_store: LogStoreHandle,
    app: Arc<Mutex<dyn App>>,
}

#[derive(Debug)]
enum ExecutorMsg {
    GetAppliedReceiver {
        respond_to: oneshot::Sender<broadcast::Receiver<(u64, AppResult)>>,
    },
    RegisterWorker {
        worker_id: u64,
    },
    RegisterReplSuccess {
        worker_id: u64,
        index: u64,
        respond_to: oneshot::Sender<u64>,
    },
    CommitLog {
        entry: Option<Entry>,
        leader_commit: u64,
    },
    ApplyLog {
        respond_to: oneshot::Sender<Result<bool, Box<dyn Error + Send + Sync>>>,
    },
    GetCommitIndex {
        respond_to: oneshot::Sender<u64>,
    },
    SetCommitIndex {
        index: u64,
    },
    GetCommitTerm {
        respond_to: oneshot::Sender<u64>,
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
    SetStateMeta {
        respond_to: oneshot::Sender<()>,
        state_meta: StateMeta,
    },
}

impl Executor {
    #[tracing::instrument(ret, level = "debug")]
    fn new(
        receiver: mpsc::Receiver<ExecutorMsg>,
        log_store: LogStoreHandle,
        current_term: u64,
        app: Arc<Mutex<dyn App>>,
    ) -> Self {
        let (applied_sender, _) = broadcast::channel(8);
        Executor {
            receiver,
            applied_sender,
            commit_index: 0,
            commit_term: 0,
            last_applied: 0,
            num_workers: 0,
            current_term,
            match_index: Default::default(),
            log_store,
            app,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: ExecutorMsg) {
        match msg {
            ExecutorMsg::GetAppliedReceiver { respond_to } => {
                let _ = respond_to.send(self.applied_sender.subscribe());
            }
            ExecutorMsg::CommitLog {
                entry,
                leader_commit,
            } => self.commit_log(entry, leader_commit).await,
            ExecutorMsg::ApplyLog { respond_to } => {
                let _ = respond_to.send(self.apply_log().await);
            }
            ExecutorMsg::RegisterWorker { worker_id } => self.register_worker(worker_id).await,
            ExecutorMsg::RegisterReplSuccess {
                worker_id,
                index,
                respond_to,
            } => {
                let _ = respond_to.send(self.register_replication_success(worker_id, index).await);
            }
            ExecutorMsg::GetCommitIndex { respond_to } => {
                let _ = respond_to.send(self.commit_index);
            }
            ExecutorMsg::SetCommitIndex { index } => self.commit_index = index,
            ExecutorMsg::GetCommitTerm { respond_to } => {
                let _ = respond_to.send(self.commit_term);
            }
            ExecutorMsg::GetLastApplied { respond_to } => {
                let _ = respond_to.send(self.last_applied);
            }
            ExecutorMsg::SetLastApplied { index } => self.last_applied = index,
            ExecutorMsg::GetNumWorkers { respond_to } => {
                let _ = respond_to.send(self.num_workers);
            }
            ExecutorMsg::SetNumWorkers { num } => self.num_workers = num,
            ExecutorMsg::SetStateMeta {
                respond_to,
                state_meta,
            } => {
                let _ = {
                    self.current_term = state_meta.term;
                    respond_to.send(())
                };
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn commit_log(&mut self, entry: Option<Entry>, leader_commit: u64) {
        if let Some(entry) = entry {
            if leader_commit > self.commit_index {
                self.commit_index = min(leader_commit, entry.index);
            }
            // term of last committed entry, used for queries to prevent db reads (not in original raft paper)
            if self.commit_index >= entry.index {
                self.commit_term = entry.term;
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn apply_log(&mut self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let mut reply = false;
        while self.last_applied < self.commit_index {
            let entry_to_be_applied = self.last_applied + 1;
            if let Some(entry) = self.log_store.read_entry(entry_to_be_applied).await {
                let result: AppResult = match EntryType::from_i32(entry.entry_type) {
                    Some(EntryType::Command) => self.app.lock().await.run(entry).await?,
                    Some(EntryType::Registration) => self.register_client(entry).await?,
                    Some(EntryType::MembershipChange) => todo!(),
                    Some(EntryType::InstallSnapshot) => self.app.lock().await.snapshot().await?, // todo [feature] correct impl of triggering snapshots and sending snapshots (different things !!!)
                    _ => {
                        panic!()
                    }
                };

                reply = result.success;
                let _ = self.applied_sender.send((entry_to_be_applied, result));
                // todo handle error
            };
            self.last_applied = entry_to_be_applied;
        }
        Ok(reply)
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn register_client(
        &mut self,
        entry: Entry,
    ) -> Result<AppResult, Box<dyn Error + Send + Sync>> {
        todo!()
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn register_worker(&mut self, worker_id: u64) {
        if let Vacant(_) = self.match_index.entry(worker_id) {
            self.num_workers += 1;
            self.match_index.insert(worker_id, 0);
        }
    }

    // used in leader state (see last paragraph for leader in raft paper)
    #[tracing::instrument(ret, level = "debug")]
    async fn register_replication_success(&mut self, worker_id: u64, index: u64) -> u64 {
        if let Occupied(mut entry) = self.match_index.entry(worker_id) {
            entry.insert(index);
            if index > self.commit_index {
                let potential_commit_index =
                    new_commit_index(&mut self.match_index, self.commit_index, self.num_workers);

                //todo [performance] is the termcheck possible without reading the log from disk?
                if let Some(entry) = self.log_store.read_entry(potential_commit_index).await {
                    if entry.term == self.current_term {
                        self.commit_index = potential_commit_index;
                        self.commit_term = entry.term;
                        // in leader mode the executer triggers application itself
                        let _ = self.apply_log().await; // todo [performance] read twice from disc in this function, needs improvement
                    }
                }
            }
        }
        self.commit_index
    }
}

#[derive(Clone, Debug)]
pub struct ExecutorHandle {
    sender: mpsc::Sender<ExecutorMsg>,
}

impl ExecutorHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(log_store: LogStoreHandle, current_term: u64, app: Arc<Mutex<dyn App>>) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = Executor::new(receiver, log_store, current_term, app);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_applied_receiver(&self) -> broadcast::Receiver<(u64, AppResult)> {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetAppliedReceiver { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("watchdog task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn commit_log(&self, entry: Option<Entry>, leader_commit: u64) {
        let msg = ExecutorMsg::CommitLog {
            entry,
            leader_commit,
        };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn apply_log(&self) -> Result<bool, Box<dyn Error + Send + Sync>> {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::ApplyLog { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn register_worker(&self, worker_id: u64) {
        let msg = ExecutorMsg::RegisterWorker { worker_id };
        let _ = self.sender.send(msg).await;
    }

    //reply last commit
    #[tracing::instrument(ret, level = "debug")]
    pub async fn register_replication_success(&self, worker_id: u64, index: u64) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::RegisterReplSuccess {
            worker_id,
            index,
            respond_to: send,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_commit_index(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetCommitIndex { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn set_commit_index(&self, index: u64) {
        let msg = ExecutorMsg::SetCommitIndex { index };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_commit_term(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetCommitTerm { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn set_last_applied(&self, index: u64) {
        let msg = ExecutorMsg::SetLastApplied { index };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_last_applied(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetLastApplied { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn set_num_workers(&self, num: u64) {
        let msg = ExecutorMsg::SetNumWorkers { num };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_num_workers(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::GetNumWorkers { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn set_state_meta(&self, state_meta: StateMeta) {
        let (send, recv) = oneshot::channel();
        let msg = ExecutorMsg::SetStateMeta {
            respond_to: send,
            state_meta,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[tracing::instrument(ret, level = "debug")]
fn new_commit_index(
    match_index: &mut HashMap<u64, u64>,
    last_commit_index: u64,
    num_worker: u64,
) -> u64 {
    let mut count_map: HashMap<u64, u64> = HashMap::new();
    //todo [performance] is there a better way than O(n^2)?
    for (_id, max_index) in match_index.iter() {
        for index in last_commit_index..=*max_index {
            if count_map.contains_key(&index) {
                let new_count = count_map.get(&index).unwrap() + 1;
                count_map.insert(index, new_count);
            } else {
                count_map.insert(index, 1);
            }
        }
    }

    let mut commit_index = 0_u64;
    for (index, count) in count_map {
        if count.ge(&calculate_required_replicas(num_worker)) && commit_index < index {
            commit_index = index;
        };
    }

    commit_index
}

//exclude leader (-> insert only number of other nodes)
#[tracing::instrument(ret, level = "debug")]
fn calculate_required_replicas(num_worker: u64) -> u64 {
    if num_worker % 2 == 0 {
        num_worker / 2
    } else {
        (num_worker + 1) / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_server::actors::log::test_utils::TestApp;
    use crate::raft_server::db::test_utils::get_test_db_paths;
    use crate::raft_server_rpc::EntryType;
    use std::sync::Arc;

    #[tokio::test]
    async fn get_commit_index_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        let app = Arc::new(Mutex::new(TestApp {}));
        let executor = ExecutorHandle::new(log_store, 0, app);

        assert_eq!(executor.get_commit_index().await, 0);
    }

    #[tokio::test]
    async fn commit_log_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        let app = Arc::new(Mutex::new(TestApp {}));
        let executor = ExecutorHandle::new(log_store, 0, app);
        let payload = bincode::serialize("some payload").unwrap();

        // index is lower than leader commit -> index wins
        let entry1 = Entry {
            index: 1,
            term: 0,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
        };
        executor.commit_log(Some(entry1), 2).await;
        assert_eq!(executor.get_commit_index().await, 1);

        //leader commit is lower than index -> leader commit wins
        let entry2 = Entry {
            index: 4,
            term: 0,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
        };
        executor.commit_log(Some(entry2), 2).await;
        assert_eq!(executor.get_commit_index().await, 2);

        executor.commit_log(None, 2).await;
        assert_eq!(executor.get_commit_index().await, 2);
    }

    #[tokio::test]
    async fn apply_log_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;
        let payload = bincode::serialize("some payload").unwrap();
        let entry1 = Entry {
            index: 1,
            term: 1,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
        };
        let entry2 = Entry {
            index: 2,
            term: 1,
            entry_type: i32::from(EntryType::Command),
            payload: payload.clone(),
        };
        log_store.append_entry(entry1).await;
        log_store.append_entry(entry2).await;

        let app = Arc::new(Mutex::new(TestApp {}));
        let executor = ExecutorHandle::new(log_store, 0, app);

        let mut applied_receiver = executor.get_applied_receiver().await;

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

        let received_notification = applied_receiver.recv().await.unwrap();
        assert_eq!(received_notification.0, 1);
        let received_notification = applied_receiver.recv().await.unwrap();
        assert_eq!(received_notification.0, 2);

        let payload: String = bincode::deserialize(&received_notification.1.payload).unwrap();
        assert_eq!(payload, "successful execution"); // must match value set in TestApp
    }

    #[tokio::test]
    async fn new_commit_index_test() {
        let mut match_index: HashMap<u64, u64> = HashMap::new();

        assert_eq!(new_commit_index(&mut match_index, 0, 5), 0);

        match_index.insert(1, 1);
        match_index.insert(2, 2);
        match_index.insert(3, 3);
        match_index.insert(5, 5);
        match_index.insert(6, 5);

        assert_eq!(new_commit_index(&mut match_index, 0, 5), 3);

        match_index.insert(1, 4999994);
        match_index.insert(2, 4999999);
        match_index.insert(3, 5000000);
        match_index.insert(5, 5000001);
        match_index.insert(6, 5000001);
        assert_eq!(new_commit_index(&mut match_index, 4999994, 5), 5000000);

        //todo [note] interesting for thesis: performance testing and when compaction must happen - is it really necessary when counting starts with last commit?
    }

    #[tokio::test]
    async fn register_replication_success_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;
        let app = Arc::new(Mutex::new(TestApp {}));
        let executor = ExecutorHandle::new(log_store.clone(), 0, app);

        // needed for term check in log
        let payload = bincode::serialize("some payload").unwrap();
        for i in 1..=5 {
            let entry = Entry {
                index: i,
                term: 0,
                entry_type: i32::from(EntryType::Command),
                payload: payload.clone(),
            };
            log_store.append_entry(entry).await;
        }

        executor.register_worker(1).await;
        executor.register_worker(2).await;
        executor.register_worker(4).await;

        executor.register_replication_success(4, 1).await;
        executor.register_replication_success(1, 1).await;
        executor.register_replication_success(4, 2).await;
        executor.register_replication_success(4, 3).await;
        //not registered, so must not be count
        executor.register_replication_success(3, 999).await;
        executor.register_replication_success(2, 2).await;
        executor.register_replication_success(2, 999).await;
        executor.register_replication_success(1, 4).await;

        // since id3 is not registered, 4 is the next highest index which 2 nodes have registered
        assert_eq!(executor.get_commit_index().await, 4);
    }
}
