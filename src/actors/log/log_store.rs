use crate::db::raft_db::RaftDb;
use crate::raft_rpc::append_entries_request::Entry;
use std::collections::VecDeque;
use std::error::Error;
use tokio::sync::{mpsc, oneshot};

struct LogStore {
    receiver: mpsc::Receiver<LogStoreMsg>,
    last_log_index: u64,
    last_log_term: u64,
    previous_log_index: u64,
    previous_log_term: u64,
    db: RaftDb,
}

enum LogStoreMsg {
    GetLastLogIndex {
        respond_to: oneshot::Sender<u64>,
    },
    GetLastLogTerm {
        respond_to: oneshot::Sender<u64>,
    },
    GetPreviousLogIndex {
        respond_to: oneshot::Sender<u64>,
    },
    GetPreviousLogTerm {
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
}

impl LogStore {
    fn new(receiver: mpsc::Receiver<LogStoreMsg>, db_path: String) -> Self {
        let db = RaftDb::new(db_path);
        let (last_log_index, last_log_term) = unwrap_index_and_term(db.read_last_entry());
        let (previous_log_index, previous_log_term) =
            unwrap_index_and_term(db.read_previous_entry(last_log_index));
        LogStore {
            receiver,
            last_log_index,
            last_log_term,
            previous_log_index,
            previous_log_term,
            db,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: LogStoreMsg) {
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
        }
    }

    //todo change return to Vec<option<u64>> like with singe entry
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

    async fn append_entry_and_flush(&mut self, entry: Entry) -> Option<u64> {
        let index = self.append_entry(entry).await;
        self.db.flush_entries().await.expect("Database corrupted");
        index
    }

    // covers step 3. and 4. of the append entries rpc in the raft paper
    async fn append_entry(&mut self, entry: Entry) -> Option<u64> {
        let entry_term = entry.term;
        let entry_index = entry.index;
        //update previous entry meta data
        self.previous_log_index = self.last_log_index;
        self.previous_log_term = self.last_log_term;
        //update latest entry meta data
        self.last_log_index = entry_index;
        self.last_log_term = entry_term;

        //write to db

        match self.db.store_entry(entry).await {
            Ok(result) => {
                if let Some(old_entry) = result {
                    if entry_term.ne(&old_entry.term) {
                        //todo: think of better error handling for last index, just dropping complete db is not possible (commit index etc)
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
                        // correct previous entry meta data
                        (self.previous_log_index, self.previous_log_term) =
                            unwrap_index_and_term(self.db.read_previous_entry(entry_index));
                    }
                }
                Some(entry_index)
            }
            Err(_) => None,
        }
        //todo send updated last log meta to election initiator or initiator reads the values?
    }

    async fn read_last_entry(&self) -> Option<Entry> {
        match self.db.read_last_entry() {
            Ok(option) => option,
            Err(_) => None,
        }
    }

    async fn read_entry(&self, index: u64) -> Option<Entry> {
        match self.db.read_entry(index) {
            Ok(option) => option,
            Err(_) => None,
        }
    }

    async fn read_previous_entry(&self, index: u64) -> Option<Entry> {
        match self.db.read_previous_entry(index) {
            Ok(option) => option,
            Err(_) => None,
        }
    }

    async fn reset_log(&mut self) {
        self.db
            .clear_db()
            .await
            .expect("log_store db seems to be corrupted, delete manually");
        self.previous_log_term = 0;
        self.previous_log_index = 0;
        self.last_log_term = 0;
        self.last_log_index = 0;
        //todo trigger executor to reset commit?
    }
}

fn unwrap_index_and_term(
    wrapped_entry: Result<Option<Entry>, Box<dyn Error + Send + Sync>>,
) -> (u64, u64) {
    if let Ok(Some(entry)) = wrapped_entry {
        (entry.index, entry.term)
    } else {
        (0, 0)
    }
}

#[derive(Clone)]
pub struct LogStoreHandle {
    sender: mpsc::Sender<LogStoreMsg>,
}

impl LogStoreHandle {
    pub fn new(db_path: String) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = LogStore::new(receiver, db_path);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    pub async fn append_entry(&self, entry: Entry) -> Option<u64> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::AppendEntry {
            respond_to: send,
            entry,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn append_entries(&self, entries: VecDeque<Entry>) -> VecDeque<Option<u64>> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::AppendEntries {
            respond_to: send,
            entries,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn read_last_entry(&self) -> Option<Entry> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::ReadLastEntry { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn read_entry(&self, index: u64) -> Option<Entry> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::ReadEntry {
            respond_to: send,
            index,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn read_previous_entry(&self, index: u64) -> Option<Entry> {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::ReadPreviousEntry {
            respond_to: send,
            index,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn reset_log(&self) {
        let (send, recv) = oneshot::channel();
        let msg = LogStoreMsg::ResetLog { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_utils::get_test_db;
    use once_cell::sync::Lazy;
    use tokio::sync::Mutex;

    #[tokio::test]
    async fn append_entry_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        log_store.reset_log().await;
        let entry1 = Entry {
            index: 1,
            term: 0,
            leader_commit: 0,
            payload: "some payload".to_string(),
        };
        let entry2 = Entry {
            index: 2,
            term: 1,
            leader_commit: 0,
            payload: "some payload".to_string(),
        };
        let entry3 = Entry {
            index: 3,
            term: 2,
            leader_commit: 0,
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
        assert_eq!(log_store.get_previous_log_index().await, 2);
        assert_eq!(log_store.get_last_log_term().await, 2);
        assert_eq!(log_store.get_previous_log_term().await, 1);

        // write entry with existing index but newer term and check correctness of meta data and stored entries
        let entry4 = Entry {
            index: 2,
            term: 4,
            leader_commit: 0,
            payload: "some payload".to_string(),
        };
        let index = log_store.append_entry(entry4.clone()).await;
        assert_eq!(entry4.clone().index, index.unwrap());
        assert_eq!(entry4, log_store.read_last_entry().await.unwrap());
        assert_eq!(entry1, log_store.read_entry(1).await.unwrap());
        assert_eq!(log_store.get_last_log_index().await, 2);
        assert_eq!(log_store.get_previous_log_index().await, 1);
        assert_eq!(log_store.get_last_log_term().await, 4);
        assert_eq!(log_store.get_previous_log_term().await, 0);
    }

    #[tokio::test]
    async fn get_last_log_index_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        log_store.reset_log().await;
        assert_eq!(log_store.get_last_log_index().await, 0);
    }

    #[tokio::test]
    async fn get_last_log_term_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        log_store.reset_log().await;
        assert_eq!(log_store.get_last_log_term().await, 0);
    }

    #[tokio::test]
    async fn get_previous_log_index_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        log_store.reset_log().await;
        assert_eq!(log_store.get_previous_log_index().await, 0);
    }

    #[tokio::test]
    async fn get_previous_log_term_test() {
        let log_store = LogStoreHandle::new(get_test_db().await);
        log_store.reset_log().await;
        assert_eq!(log_store.get_previous_log_term().await, 0);
    }
}
