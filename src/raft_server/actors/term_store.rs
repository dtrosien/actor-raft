use crate::raft_server::actors::watchdog::WatchdogHandle;
use crate::raft_server::db::raft_db::RaftDb;
use std::cmp::Ordering;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct TermStore {
    receiver: mpsc::Receiver<TermMsg>,
    watchdog: WatchdogHandle,
    db: RaftDb,
    current_term: u64,
}

#[derive(Debug)]
enum TermMsg {
    Get {
        respond_to: oneshot::Sender<u64>,
    },
    CheckTermAndReply {
        respond_to: oneshot::Sender<(bool, u64)>,
        term: u64,
    },
    CheckTerm {
        term: u64,
    },
    Set {
        term: u64,
    },
    Increment,
    Reset {
        respond_to: oneshot::Sender<()>,
    },
}

impl TermStore {
    #[tracing::instrument(ret, level = "debug")]
    fn new(receiver: mpsc::Receiver<TermMsg>, watchdog: WatchdogHandle, db_path: String) -> Self {
        let db = RaftDb::new(db_path);
        let current_term = db
            .read_current_term()
            .expect("term_store db seems to be corrupted")
            .unwrap_or(0);
        TermStore {
            receiver,
            watchdog,
            db,
            current_term,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: TermMsg) {
        match msg {
            TermMsg::Get { respond_to } => {
                let _ = respond_to.send(self.current_term);
            }
            TermMsg::Set { term } => self.set_term(term).await,
            TermMsg::Increment => self.increment_term().await,
            TermMsg::CheckTermAndReply { respond_to, term } => {
                let _ = respond_to.send(self.check_term_and_reply(term).await);
            }
            TermMsg::CheckTerm { term } => self.check_term(term).await,
            TermMsg::Reset { respond_to } => {
                let _ = {
                    self.reset_term().await;
                    respond_to.send(())
                };
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn check_term(&self, term: u64) {
        if term.cmp(&self.current_term) == Ordering::Greater {
            self.watchdog.term_error().await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn check_term_and_reply(&mut self, term: u64) -> (bool, u64) {
        match term.cmp(&self.current_term) {
            Ordering::Less => (false, self.current_term),
            Ordering::Equal => (true, self.current_term),
            Ordering::Greater => {
                self.current_term = term;
                self.watchdog.term_error().await;
                (true, self.current_term)
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn increment_term(&mut self) {
        self.current_term += 1;
        self.db
            .store_current_term(self.current_term)
            .await
            .expect("term_store db seems to be corrupted");
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn set_term(&mut self, term: u64) {
        self.current_term = term;
        self.db
            .store_current_term(self.current_term)
            .await
            .expect("term_store db seems to be corrupted");
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn reset_term(&mut self) {
        self.current_term = 0;
        self.db
            .clear_db()
            .await
            .expect("term_store db seems to be corrupted, delete manually")
    }
}

#[derive(Clone, Debug)]
pub struct TermStoreHandle {
    sender: mpsc::Sender<TermMsg>,
}

impl TermStoreHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(watchdog: WatchdogHandle, db_path: String) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut term_store = TermStore::new(receiver, watchdog, db_path);
        tokio::spawn(async move { term_store.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_term(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = TermMsg::Get { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn set_term(&self, term: u64) {
        let msg = TermMsg::Set { term };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn check_term_and_reply(&self, term: u64) -> (bool, u64) {
        let (send, recv) = oneshot::channel();
        let msg = TermMsg::CheckTermAndReply {
            respond_to: send,
            term,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn check_term(&self, term: u64) {
        let msg = TermMsg::CheckTerm { term };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn increment_term(&self) {
        let msg = TermMsg::Increment;
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn reset_term(&self) {
        let (send, recv) = oneshot::channel();
        let msg = TermMsg::Reset { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_server::db::test_utils::get_test_db_paths;

    #[tokio::test]
    async fn get_term_test() {
        let watchdog = WatchdogHandle::default();
        let mut test_db_paths = get_test_db_paths(1).await;

        let term_store = TermStoreHandle::new(watchdog, test_db_paths.pop().unwrap());
        term_store.reset_term().await;

        assert_eq!(term_store.get_term().await, 0);
    }

    #[tokio::test]
    async fn set_term_test() {
        let watchdog = WatchdogHandle::default();
        let mut test_db_paths = get_test_db_paths(1).await;

        let term_store = TermStoreHandle::new(watchdog, test_db_paths.pop().unwrap());
        term_store.reset_term().await;

        let new_term: u64 = 1;
        term_store.set_term(new_term).await;
        assert_eq!(new_term, term_store.get_term().await);
    }

    #[tokio::test]
    async fn check_term_and_reply_test() {
        let watchdog = WatchdogHandle::default();
        let mut test_db_paths = get_test_db_paths(1).await;

        let term_store = TermStoreHandle::new(watchdog, test_db_paths.pop().unwrap());
        term_store.reset_term().await;

        term_store.set_term(2).await;
        let correct_term: u64 = 2;
        let smaller_term: u64 = 1;
        let bigger_term: u64 = 3;

        assert_eq!(
            term_store.check_term_and_reply(correct_term).await,
            (true, 2)
        );
        assert_eq!(
            term_store.check_term_and_reply(smaller_term).await,
            (false, 2)
        );
        assert_eq!(
            term_store.check_term_and_reply(bigger_term).await,
            (true, 3)
        );
    }
}
