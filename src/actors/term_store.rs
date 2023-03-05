use crate::actors::watchdog::WatchdogHandle;
use std::cmp::Ordering;
use tokio::sync::{mpsc, oneshot};

struct TermStore {
    receiver: mpsc::Receiver<TermMsg>,
    watchdog: WatchdogHandle,
    current_term: u64,
}

enum TermMsg {
    Get {
        respond_to: oneshot::Sender<u64>,
    },
    CheckTermAndReply {
        respond_to: oneshot::Sender<Option<bool>>,
        term: u64,
    },
    CheckTerm {
        term: u64,
    },
    Set {
        term: u64,
    },
    Increment,
}

impl TermStore {
    fn new(receiver: mpsc::Receiver<TermMsg>, watchdog: WatchdogHandle) -> Self {
        TermStore {
            receiver,
            watchdog,
            current_term: 0,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: TermMsg) {
        match msg {
            TermMsg::Get { respond_to } => {
                let _ = respond_to.send(self.current_term);
            }
            TermMsg::Set { term } => self.current_term = term,
            TermMsg::Increment => self.current_term += 1,
            TermMsg::CheckTermAndReply { respond_to, term } => match term.cmp(&self.current_term) {
                Ordering::Less => {
                    let _ = respond_to.send(Some(false));
                }
                Ordering::Equal => {
                    let _ = respond_to.send(Some(true));
                }
                Ordering::Greater => {
                    self.watchdog.term_error().await;
                    let _ = respond_to.send(None);
                }
            },
            TermMsg::CheckTerm { term } => {
                if term.cmp(&self.current_term) == Ordering::Greater {
                    self.watchdog.term_error().await;
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct TermStoreHandle {
    sender: mpsc::Sender<TermMsg>,
}

impl TermStoreHandle {
    pub fn new(watchdog: WatchdogHandle) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut term_store = TermStore::new(receiver, watchdog);
        tokio::spawn(async move { term_store.run().await });

        Self { sender }
    }

    pub async fn get_term(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = TermMsg::Get { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    async fn set_term(&self, term: u64) {
        let msg = TermMsg::Set { term };
        let _ = self.sender.send(msg).await;
    }

    pub async fn check_term_and_reply(&self, term: u64) -> Option<bool> {
        let (send, recv) = oneshot::channel();
        let msg = TermMsg::CheckTermAndReply {
            respond_to: send,
            term,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn check_term(&self, term: u64) {
        let msg = TermMsg::CheckTerm { term };
        let _ = self.sender.send(msg).await;
    }

    pub async fn increment_term(&self) {
        let msg = TermMsg::Increment;
        let _ = self.sender.send(msg).await;
    }
}

impl Default for TermStoreHandle {
    fn default() -> Self {
        let watchdog = WatchdogHandle::default();
        TermStoreHandle::new(watchdog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_term_test() {
        let term_store = TermStoreHandle::default();
        assert_eq!(term_store.get_term().await, 0);
    }

    #[tokio::test]
    async fn set_term_test() {
        let term_store = TermStoreHandle::default();
        let new_term: u64 = 1;
        term_store.set_term(new_term).await;
        assert_eq!(new_term, term_store.get_term().await);
    }

    #[tokio::test]
    async fn check_term_and_reply_test() {
        let term_store = TermStoreHandle::default();
        term_store.set_term(2).await;
        let correct_term: u64 = 2;
        let smaller_term: u64 = 1;
        let bigger_term: u64 = 3;

        assert_eq!(
            term_store.check_term_and_reply(correct_term).await,
            Some(true)
        );
        assert_eq!(
            term_store.check_term_and_reply(smaller_term).await,
            Some(false)
        );
        assert_eq!(term_store.check_term_and_reply(bigger_term).await, None);
    }
}
