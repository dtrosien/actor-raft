use crate::actors::election::counter::CounterHandle;
use crate::actors::election::worker::WorkerHandle;
use crate::actors::term::TermHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::{Config, Node};
use std::collections::BTreeMap;
use tokio::sync::{mpsc, oneshot};

struct Initiator {
    receiver: mpsc::Receiver<InitiatorMsg>,
    term: TermHandle,
    counter: CounterHandle,
    workers: BTreeMap<u64, WorkerHandle>,
    voted_for: Option<u64>,
    id: u64,
    last_log_index: u64,
    last_log_term: u64,
}

enum InitiatorMsg {
    GetVotedFor {
        respond_to: oneshot::Sender<Option<u64>>,
    },
    SetLastLogMeta {
        last_log_index: u64,
        last_log_term: u64,
    },
    StartElection,
}

impl Initiator {
    async fn new(
        receiver: mpsc::Receiver<InitiatorMsg>,
        term: TermHandle,
        watchdog: WatchdogHandle,
        config: Config,
    ) -> Self {
        let votes_required = (config.nodes.len() / 2) as u64;
        let id = config.id;
        let counter = CounterHandle::new(watchdog, votes_required).await;
        let workers = config
            .nodes
            .into_iter()
            .map(|node| {
                (
                    node.id,
                    WorkerHandle::new(term.clone(), counter.clone(), node.clone()),
                )
            })
            .collect();
        Initiator {
            receiver,
            term,
            counter,
            workers,
            voted_for: None,
            id,
            last_log_index: 0,
            last_log_term: 0,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: InitiatorMsg) {
        match msg {
            InitiatorMsg::GetVotedFor { respond_to } => {
                let _ = respond_to.send(self.voted_for);
            }
            InitiatorMsg::SetLastLogMeta {
                last_log_index,
                last_log_term,
            } => {
                self.last_log_index = last_log_index;
                self.last_log_term = last_log_term;
            }
            InitiatorMsg::StartElection => self.start_election().await,
        }
    }

    async fn start_election(&mut self) {
        //increment current term
        self.term.increment_term().await;
        //vote for self
        self.voted_for = Some(self.id);
        for worker in self.workers.iter() {
            worker.1.request_vote().await;
        }
    }
}

#[derive(Clone)]
pub struct InitiatorHandle {
    sender: mpsc::Sender<InitiatorMsg>,
}

impl InitiatorHandle {
    pub async fn new(term: TermHandle, watchdog: WatchdogHandle, config: Config) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut initiator = Initiator::new(receiver, term, watchdog, config).await;
        tokio::spawn(async move { initiator.run().await });

        Self { sender }
    }

    pub async fn get_voted_for(&self) -> Option<u64> {
        let (send, recv) = oneshot::channel();
        let msg = InitiatorMsg::GetVotedFor { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn start_election(&self) {
        let msg = InitiatorMsg::StartElection;
        let _ = self.sender.send(msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_voted_for_test() {
        let watchdog = WatchdogHandle::default();
        let term = TermHandle::new(watchdog.clone());
        let config = Config::new();
        let initiator = InitiatorHandle::new(term, watchdog, config).await;
        assert_eq!(initiator.get_voted_for().await, None);
    }

    #[tokio::test]
    async fn start_election_test() {
        let watchdog = WatchdogHandle::default();
        let term = TermHandle::new(watchdog.clone());
        let config = Config::new();
        let initiator = InitiatorHandle::new(term, watchdog, config).await;

        //todo write test (add private fn to check worker status etc)
    }
}
