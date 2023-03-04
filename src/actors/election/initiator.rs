use crate::actors::election::counter::CounterHandle;
use crate::actors::election::worker::WorkerHandle;
use crate::actors::term_store::TermStoreHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::{Config, Node};
use std::collections::BTreeMap;
use tokio::sync::{mpsc, oneshot};

struct Initiator {
    receiver: mpsc::Receiver<InitiatorMsg>,
    term: TermStoreHandle,
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
    GetWorker {
        respond_to: oneshot::Sender<Option<WorkerHandle>>,
        id: u64,
    },
    GetCounter {
        respond_to: oneshot::Sender<CounterHandle>,
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
        term: TermStoreHandle,
        watchdog: WatchdogHandle,
        config: Config,
    ) -> Self {
        let votes_required = calculate_required_votes(config.nodes.len() as u64);
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
            InitiatorMsg::GetWorker { respond_to, id } => {
                let _ = respond_to.send(self.get_worker(id));
            }
            InitiatorMsg::GetCounter { respond_to } => {
                let _ = respond_to.send(self.counter.clone());
            }
            InitiatorMsg::SetLastLogMeta {
                last_log_index,
                last_log_term,
            } => self.set_last_log_meta(last_log_index, last_log_term),
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

    fn set_last_log_meta(&mut self, last_log_index: u64, last_log_term: u64) {
        self.last_log_index = last_log_index;
        self.last_log_term = last_log_term;
    }

    fn get_worker(&self, id: u64) -> Option<WorkerHandle> {
        self.workers.get(&id).cloned()
    }
}

#[derive(Clone)]
pub struct InitiatorHandle {
    sender: mpsc::Sender<InitiatorMsg>,
}

impl InitiatorHandle {
    pub async fn new(term: TermStoreHandle, watchdog: WatchdogHandle, config: Config) -> Self {
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

    async fn get_worker(&self, id: u64) -> Option<WorkerHandle> {
        let (send, recv) = oneshot::channel();
        let msg = InitiatorMsg::GetWorker {
            respond_to: send,
            id,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    async fn get_counter(&self) -> CounterHandle {
        let (send, recv) = oneshot::channel();
        let msg = InitiatorMsg::GetCounter { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    pub async fn start_election(&self) {
        let msg = InitiatorMsg::StartElection;
        let _ = self.sender.send(msg).await;
    }
}

//exclude candidate (-> insert only number of other nodes)
fn calculate_required_votes(nodes_num: u64) -> u64 {
    if nodes_num % 2 == 0 {
        nodes_num / 2
    } else {
        (nodes_num + 1) / 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn get_voted_for_test() {
        let watchdog = WatchdogHandle::default();
        let term = TermStoreHandle::new(watchdog.clone());
        let config = Config::new();
        let initiator = InitiatorHandle::new(term, watchdog, config).await;
        assert_eq!(initiator.get_voted_for().await, None);
    }

    #[tokio::test]
    async fn get_worker_test() {
        let watchdog = WatchdogHandle::default();
        let term = TermStoreHandle::new(watchdog.clone());
        let config = Config::for_test();
        let initiator = InitiatorHandle::new(term, watchdog, config).await;

        assert_eq!(
            initiator
                .get_worker(1)
                .await
                .expect("must not panic")
                .get_node()
                .await
                .id,
            1
        );
    }

    #[tokio::test]
    async fn start_election_test() {
        let watchdog = WatchdogHandle::default();
        let term = TermStoreHandle::new(watchdog.clone());

        let config = Config::for_test();
        let initiator = InitiatorHandle::new(term, watchdog, config.clone()).await;

        initiator.start_election().await;

        let counter = initiator.get_counter().await;

        assert_eq!(
            counter.get_votes_received().await,
            config.nodes.len() as u64
        );
    }

    #[tokio::test]
    async fn calculate_required_votes_test() {
        // only one server in total
        assert_eq!(calculate_required_votes(0), 0);
        // two servers total
        assert_eq!(calculate_required_votes(1), 1);
        // even number of other servers
        assert_eq!(calculate_required_votes(2), 1);
        assert_eq!(calculate_required_votes(10), 5);
        //odd number of other servers
        assert_eq!(calculate_required_votes(9), 5);
        assert_eq!(calculate_required_votes(11), 6);
    }
}
