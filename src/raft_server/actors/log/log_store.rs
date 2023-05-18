use crate::raft_server::db::raft_db::RaftDb;
use crate::raft_server_rpc::append_entries_request::Entry;
use std::collections::VecDeque;
use std::error::Error;

use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct LogStore {
    receiver: mpsc::Receiver<LogStoreMsg>,
    last_log_index: u64,
    last_log_term: u64,
    next_log_index: u64,
    db: RaftDb,
}

#[derive(Debug)]
enum LogStoreMsg {
    GetLastLogIndex {
        respond_to: oneshot::Sender<u64>,
    },
    GetLastLogTerm {
        respond_to: oneshot::Sender<u64>,
    },
    GetAndIncrementNextLogIndex {
        respond_to: oneshot::Sender<u64>,
    },
    AppendEntry {
        respond_to: oneshot::Sender<Option<u64>>,
        entry: Entry,
    },
    AppendEntries {
        respond_to: oneshot::Sender<VecDeque<Option<u64>>>,
        entries: VecDeque<Entry>,
    },
    ReadEntry {
        respond_to: oneshot::Sender<Option<Entry>>,
        index: u64,
    },
    ReadPreviousEntry {
        respond_to: oneshot::Sender<Option<Entry>>,
        index: u64,
    },
    ReadLastEntry {
        respond_to: oneshot::Sender<Option<Entry>>,
    },
    ResetLog {
        respond_to: oneshot::Sender<()>,
    },
    LastEntryMatch {
        respond_to: oneshot::Sender<bool>,
        index: u64,
        term: u64,
    },
}

impl LogStore {
    #[tracing::instrument(ret, level = "debug")]
    fn new(receiver: mpsc::Receiver<LogStoreMsg>, db_path: String) -> Self {
        let db = RaftDb::new(db_path);
        let (last_log_index, last_log_term) = unwrap_index_and_term(db.read_last_entry());
        let next_log_index = last_log_index + 1;
        LogStore {
            receiver,
            last_log_index,
            last_log_term,
            next_log_index,
            db,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: LogStoreMsg) {
        match msg {
            LogStoreMsg::GetLastLogIndex { respond_to } => {
                let _ = respond_to.send(self.last_log_index);
            }
            LogStoreMsg::GetLastLogTerm { respond_to } => {
                let _ = respond_to.send(self.last_log_term);
            }
            LogStoreMsg::GetAndIncrementNextLogIndex { respond_to } => {
                let _ = respond_to.send(self.get_and_increment_next_log_index().await);
            }
            LogStoreMsg::AppendEntry { respond_to, entry } => {
                let _ = respond_to.send(self.append_entry_and_flush(entry).await);
            }
            LogStoreMsg::AppendEntries {
                respond_to,
                entries,
            } => {
                let _ = respond_to.send(self.append_entries_and_flush(entries).await);
            }
            LogStoreMsg::ReadLastEntry { respond_to } => {
                let _ = respond_to.send(self.read_last_entry().await);
            }
            LogStoreMsg::ReadEntry { respond_to, index } => {
                let _ = respond_to.send(self.read_entry(index).await);
            }
            LogStoreMsg::ReadPreviousEntry { respond_to, index } => {
                let _ = respond_to.send(self.read_previous_entry(index).await);
            }
            LogStoreMsg::ResetLog { respond_to } => {
                let _ = respond_to.send(self.reset_log().await);
            }
            LogStoreMsg::LastEntryMatch {
                respond_to,
                index,
                term,
            } => {
                let _ = respond_to.send(self.last_entry_match(index, term).await);
            }
        }
    }

    //todo [test] test if return values are in correct order
    #[tracing::instrument(ret, level = "debug")]
    async fn append_entries_and_flush(
        &mut self,
        entries: VecDeque<Entry>,
    ) -> VecDeque<Option<u64>> {
        let mut reply = VecDeque::new();
        for entry in entries {
            reply.push_back(self.append_entry(entry).await);
        }
        self.db.flush_entries().await.expect("Database corrupted");
        reply
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn append_entry_and_flush(&mut self, entry: Entry) -> Option<u64> {
        let index = self.append_entry(entry).await;
        self.db.flush_entries().await.expect("Database corrupted");
        index
    }

    // covers step 3. and 4. of the append entries rpc in the raft paper
    #[tracing::instrument(ret, level = "debug")]
    async fn append_entry(&mut self, entry: Entry) -> Option<u64> {
        let entry_term = entry.term;
        let entry_index = entry.index;
        //update latest entry meta data
        self.last_log_index = entry_index;
        self.last_log_term = entry_term;
        self.next_log_index = entry_index + 1;
        //write to db
        match self.db.store_entry(entry).await {
            Ok(result) => {
                if let Some(old_entry) = result {
                    if entry_term.ne(&old_entry.term) {
                        //todo [feature] think of better error handling for last index, just dropping complete db is not possible (commit index etc)
                        let last_index = self
                            .db
                            .read_last_entry()
                            .expect("Error reading DB")
                            .expect("Error: last index is none")
                            .index;
                        // delete all following entries (the current one was already exchanged)
                        self.db
                            .delete_entries(entry_index + 1, last_index)
                            .await
                            .expect("Error when deleting wrong entries: must not happen");
                    }
                }
                Some(entry_index)
            }
            Err(_) => None,
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn read_last_entry(&self) -> Option<Entry> {
        match self.db.read_last_entry() {
            Ok(option) => option,
            Err(_) => None,
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn read_entry(&self, index: u64) -> Option<Entry> {
        match self.db.read_entry(index) {
            Ok(option) => option,
            Err(_) => None,
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn read_previous_entry(&self, index: u64) -> Option<Entry> {
        match self.db.read_previous_entry(index) {
            Ok(option) => option,
            Err(_) => None,
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn reset_log(&mut self) {
        self.db
            .clear_db()
            .await
            .expect("log_store db seems to be corrupted, delete manually");
        self.last_log_term = 0;
        self.last_log_index = 0;
        self.next_log_index = 1;
    }

    // this method covers step 2 of the append entries rpc receiver implementation of the raft paper
    #[tracing::instrument(ret, level = "debug")]
    async fn last_entry_match(&mut self, index: u64, term: u64) -> bool {
        if (index == self.last_log_index) && (term == self.last_log_term) {
            return true;
        };
        match self.read_entry(index).await {
            None => false,
            Some(prev_entry) => prev_entry.term == term,
        }
    }

    async fn get_and_increment_next_log_index(&mut self) -> u64 {
        let index = self.next_log_index;
        self.next_log_index += 1;
        index
    }
}

#[tracing::instrument(ret, level = "debug")]
fn unwrap_index_and_term(
    wrapped_entry: Result<Option<Entry>, Box<dyn Error + Send + Sync>>,
) -> (u64, u64) {
    if let Ok(Some(entry)) = wrapped_entry {
        (entry.index, entry.term)
    } else {
        (0, 0)
    }
}

#[derive(Clone, Debug)]
pub struct LogStoreHandle {
    sender: mpsc::Sender<LogStoreMsg>,
}

impl LogStoreHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(db_path: String) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = LogStore::new(receiver, db_path);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn append_entry(&self, entry: Entry) -> Option<u64> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::AppendEntry {
            respond_to: send,
            entry,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn append_entries(&self, entries: VecDeque<Entry>) -> VecDeque<Option<u64>> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::AppendEntries {
            respond_to: send,
            entries,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn read_last_entry(&self) -> Option<Entry> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::ReadLastEntry { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn read_entry(&self, index: u64) -> Option<Entry> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::ReadEntry {
            respond_to: send,
            index,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn read_previous_entry(&self, index: u64) -> Option<Entry> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::ReadPreviousEntry {
            respond_to: send,
            index,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn last_entry_match(&self, index: u64, term: u64) -> bool {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::LastEntryMatch {
            respond_to: send,
            index,
            term,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn reset_log(&self) {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::ResetLog { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_last_log_index(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::GetLastLogIndex { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_last_log_term(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::GetLastLogTerm { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_and_increment_next_log_index(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::GetAndIncrementNextLogIndex { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_server::db::test_utils::get_test_db_paths;

    #[tokio::test]
    async fn append_entry_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;
        let entry1 = Entry {
            index: 1,
            term: 0,
            payload: "some payload".to_string(),
        };
        let entry2 = Entry {
            index: 2,
            term: 1,
            payload: "some payload".to_string(),
        };
        let entry3 = Entry {
            index: 3,
            term: 2,
            payload: "some payload".to_string(),
        };
        let entries = VecDeque::from(vec![entry1.clone(), entry2.clone(), entry3.clone()]);
        let mut indices = log_store.append_entries(entries).await;

        //check correct order in reply
        assert_eq!(indices.pop_front().unwrap().unwrap(), entry1.index);
        assert_eq!(indices.pop_front().unwrap().unwrap(), entry2.index);
        assert_eq!(indices.pop_front().unwrap().unwrap(), entry3.index);

        //check correctness of meta data and stored entries
        assert_eq!(entry3, log_store.read_last_entry().await.unwrap());
        assert_eq!(entry2, log_store.read_previous_entry(3).await.unwrap());
        assert_eq!(entry3, log_store.read_previous_entry(5).await.unwrap());
        assert_eq!(log_store.get_last_log_index().await, 3);
        // assert_eq!(log_store.get_previous_log_index().await, 2);
        assert_eq!(log_store.get_last_log_term().await, 2);
        //   assert_eq!(log_store.get_previous_log_term().await, 1);

        // write entry with existing index but newer term and check correctness of meta data and stored entries
        let entry4 = Entry {
            index: 2,
            term: 4,
            payload: "some payload".to_string(),
        };
        let index = log_store.append_entry(entry4.clone()).await;
        assert_eq!(entry4.clone().index, index.unwrap());
        assert_eq!(entry4, log_store.read_last_entry().await.unwrap());
        assert_eq!(entry1, log_store.read_entry(1).await.unwrap());
        assert_eq!(log_store.get_last_log_index().await, 2);
        //  assert_eq!(log_store.get_previous_log_index().await, 1);
        assert_eq!(log_store.get_last_log_term().await, 4);
        //  assert_eq!(log_store.get_previous_log_term().await, 0);
    }

    #[tokio::test]
    async fn get_last_log_index_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;
        assert_eq!(log_store.get_last_log_index().await, 0);
    }

    #[tokio::test]
    async fn get_last_log_term_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;
        assert_eq!(log_store.get_last_log_term().await, 0);
    }

    #[tokio::test]
    async fn get_and_increment_next_log_index_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;
        assert_eq!(log_store.get_and_increment_next_log_index().await, 1);
        assert_eq!(log_store.get_and_increment_next_log_index().await, 2);
        assert_eq!(log_store.get_and_increment_next_log_index().await, 3);
    }

    #[tokio::test]
    async fn get_previous_entry_match_test() {
        let mut test_db_paths = get_test_db_paths(1).await;
        let log_store = LogStoreHandle::new(test_db_paths.pop().unwrap());
        log_store.reset_log().await;

        assert!(log_store.last_entry_match(0, 0).await);
        assert!(!log_store.last_entry_match(1, 0).await);

        let entry1 = Entry {
            index: 1,
            term: 1,
            payload: "some payload".to_string(),
        };

        log_store.append_entry(entry1).await;

        assert!(log_store.last_entry_match(1, 1).await);

        let entry2 = Entry {
            index: 2,
            term: 1,
            payload: "some payload".to_string(),
        };
        log_store.append_entry(entry2).await;

        // check succeeds since both log store attributes term and index matches
        assert!(log_store.last_entry_match(2, 1).await);

        // this check also succeed since now the entry from DB is read which matches index and term
        assert!(log_store.last_entry_match(1, 1).await);
    }
}
