use crate::actors::watchdog::WatchdogHandle;
use std::cmp::Ordering;
use tokio::sync::{mpsc, oneshot};

struct Term {
    receiver: mpsc::Receiver<TermMsg>,
    watchdog: WatchdogHandle,
    current_term: u64,
}

enum TermMsg {
    Get {
        respond_to: oneshot::Sender<u64>,
    },
    CheckTerm {
        respond_to: oneshot::Sender<Option<bool>>,
        term: u64,
    },
    Set {
        term: u64,
    },
}

impl Term {
    fn new(receiver: mpsc::Receiver<TermMsg>, watchdog: WatchdogHandle) -> Self {
        Term {
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
            TermMsg::CheckTerm { respond_to, term } => match term.cmp(&self.current_term) {
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
        }
    }
}

#[derive(Clone)]
pub struct TermHandle {
    sender: mpsc::Sender<TermMsg>,
}

impl TermHandle {
    pub fn new(watchdog: WatchdogHandle) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut term = Term::new(receiver, watchdog);
        tokio::spawn(async move { term.run().await });

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

    pub async fn check_term(&self, term: u64) -> Option<bool> {
        let (send, recv) = oneshot::channel();
        let msg = TermMsg::CheckTerm {
            respond_to: send,
            term,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

impl Default for TermHandle {
    fn default() -> Self {
        let watchdog = WatchdogHandle::default();
        TermHandle::new(watchdog)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_term_test() {
        let term = TermHandle::default();
        assert_eq!(term.get_term().await, 0);
    }

    #[tokio::test]
    async fn set_term_test() {
        let term = TermHandle::default();
        let new_term: u64 = 1;
        term.set_term(new_term).await;
        assert_eq!(new_term, term.get_term().await);
    }

    #[tokio::test]
    async fn check_term_test() {
        let term = TermHandle::default();
        term.set_term(2).await;
        let correct_term: u64 = 2;
        let smaller_term: u64 = 1;
        let bigger_term: u64 = 3;

        assert_eq!(term.check_term(correct_term).await, Some(true));
        assert_eq!(term.check_term(smaller_term).await, Some(false));
        assert_eq!(term.check_term(bigger_term).await, None);
    }
}
