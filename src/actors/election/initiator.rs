use crate::actors::election::counter::CounterHandle;
use crate::actors::election::worker::WorkerHandle;
use crate::actors::term_store::TermStoreHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::{Config, Node};
use crate::db::raft_db::RaftDb;
use crate::raft_rpc::RequestVoteRequest;
use crate::state_meta::StateMeta;
use std::collections::BTreeMap;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct Initiator {
    receiver: mpsc::Receiver<InitiatorMsg>,
    term_store: TermStoreHandle,
    counter: CounterHandle,
    workers: BTreeMap<u64, WorkerHandle>,
    db: RaftDb,
    voted_for: Option<u64>,
    id: u64,
    last_log_index: u64,
    last_log_term: u64,
}

#[derive(Debug)]
enum InitiatorMsg {
    GetVotedFor {
        respond_to: oneshot::Sender<Option<u64>>,
    },
    SetVotedFor {
        voted_for: u64,
    },
    GetWorker {
        respond_to: oneshot::Sender<Option<WorkerHandle>>,
        id: u64,
    },
    GetCounter {
        respond_to: oneshot::Sender<CounterHandle>,
    },
    SetLastLogMeta {
        respond_to: oneshot::Sender<()>,
        last_log_index: u64,
        last_log_term: u64,
    },
    StartElection,
    ResetVotedFor {
        respond_to: oneshot::Sender<()>,
    },
}

impl Initiator {
    #[tracing::instrument(ret, level = "debug")]
    fn new(
        receiver: mpsc::Receiver<InitiatorMsg>,
        term: TermStoreHandle,
        watchdog: WatchdogHandle,
        config: Config,
        state_meta: StateMeta,
        path: String,
    ) -> Self {
        let db = RaftDb::new(path);
        let voted_for = db
            .read_voted_for()
            .expect("vote_store db seems to be corrupted");
        let votes_required = calculate_required_votes(config.nodes.len() as u64);
        let id = config.id;
        let counter = CounterHandle::new(watchdog, votes_required);
        let workers = config
            .nodes
            .into_iter()
            .map(|node| {
                (
                    node.id,
                    WorkerHandle::new(term.clone(), counter.clone(), node),
                )
            })
            .collect();
        Initiator {
            receiver,
            term_store: term,
            counter,
            workers,
            db,
            voted_for,
            id,
            last_log_index: state_meta.previous_log_index,
            last_log_term: state_meta.previous_log_term,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: InitiatorMsg) {
        match msg {
            InitiatorMsg::GetVotedFor { respond_to } => {
                let _ = respond_to.send(self.voted_for);
            }
            InitiatorMsg::SetVotedFor { voted_for } => self.set_voted_for(voted_for).await,

            InitiatorMsg::GetWorker { respond_to, id } => {
                let _ = respond_to.send(self.get_worker(id));
            }
            InitiatorMsg::GetCounter { respond_to } => {
                let _ = respond_to.send(self.counter.clone());
            }
            InitiatorMsg::SetLastLogMeta {
                respond_to,
                last_log_index,
                last_log_term,
            } => {
                let _ = {
                    self.set_last_log_meta(last_log_index, last_log_term).await;
                    respond_to.send(())
                };
            }
            InitiatorMsg::StartElection => self.start_election().await,
            InitiatorMsg::ResetVotedFor { respond_to } => {
                let _ = {
                    self.reset_voted_for().await;
                    respond_to.send(())
                };
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn start_election(&mut self) {
        //increment current term
        self.term_store.increment_term().await;
        //vote for self
        self.voted_for = Some(self.id);

        //build request
        let request = RequestVoteRequest {
            term: self.term_store.get_term().await,
            candidate_id: self.id,
            last_log_index: self.last_log_index,
            last_log_term: self.last_log_term,
        };

        //send request to worker
        for worker in self.workers.iter() {
            worker.1.request_vote(request.clone()).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn set_last_log_meta(&mut self, last_log_index: u64, last_log_term: u64) {
        self.last_log_index = last_log_index;
        self.last_log_term = last_log_term;
    }

    #[tracing::instrument(ret, level = "debug")]
    fn get_worker(&self, id: u64) -> Option<WorkerHandle> {
        self.workers.get(&id).cloned()
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn reset_voted_for(&mut self) {
        self.voted_for = None;
        self.db
            .clear_db()
            .await
            .expect("vote_store db seems to be corrupted, delete manually")
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn set_voted_for(&mut self, voted_for: u64) {
        self.voted_for = Some(voted_for);
        self.db
            .store_voted_for(voted_for)
            .await
            .expect("term_store db seems to be corrupted");
    }
}

#[derive(Clone, Debug)]
pub struct InitiatorHandle {
    sender: mpsc::Sender<InitiatorMsg>,
}

impl InitiatorHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(
        term: TermStoreHandle,
        watchdog: WatchdogHandle,
        config: Config,
        state_meta: StateMeta,
        db_path: String,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut initiator = Initiator::new(receiver, term, watchdog, config, state_meta, db_path);
        tokio::spawn(async move { initiator.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_voted_for(&self) -> Option<u64> {
        let (send, recv) = oneshot::channel();
        let msg = InitiatorMsg::GetVotedFor { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn get_worker(&self, id: u64) -> Option<WorkerHandle> {
        let (send, recv) = oneshot::channel();
        let msg = InitiatorMsg::GetWorker {
            respond_to: send,
            id,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn get_counter(&self) -> CounterHandle {
        let (send, recv) = oneshot::channel();
        let msg = InitiatorMsg::GetCounter { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn start_election(&self) {
        let msg = InitiatorMsg::StartElection;
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn reset_voted_for(&self) {
        let (send, recv) = oneshot::channel();
        let msg = InitiatorMsg::ResetVotedFor { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn set_voted_for(&self, voted_for: u64) {
        let msg = InitiatorMsg::SetVotedFor { voted_for };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn set_last_log_meta(&self, last_log_index: u64, last_log_term: u64) {
        let (send, recv) = oneshot::channel();
        let msg = InitiatorMsg::SetLastLogMeta {
            respond_to: send,
            last_log_index,
            last_log_term,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

//exclude candidate (-> insert only number of other nodes)
#[tracing::instrument(ret, level = "debug")]
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
    use crate::config::get_test_config;
    use crate::db::test_utils::get_test_db_paths;
    use crate::rpc::test_utils::{start_test_server, TestServerTrue};
    use std::time::Duration;

    #[tokio::test]
    async fn get_voted_for_test() {
        let watchdog = WatchdogHandle::default();
        let mut test_db_paths = get_test_db_paths(2).await;
        let term_store = TermStoreHandle::new(watchdog.clone(), test_db_paths.pop().unwrap());
        let config = get_test_config().await;
        let state_meta = StateMeta {
            previous_log_index: 0,
            previous_log_term: 0,
            term: 0,
            id: 0,
            leader_commit: 0,
        };
        let initiator = InitiatorHandle::new(
            term_store,
            watchdog,
            config,
            state_meta,
            test_db_paths.pop().unwrap(),
        );
        initiator.reset_voted_for().await;

        assert_eq!(initiator.get_voted_for().await, None);

        initiator.set_voted_for(3).await;
        initiator.set_voted_for(4).await;

        assert_eq!(initiator.get_voted_for().await, Some(4))
    }

    #[tokio::test]
    async fn get_worker_test() {
        let watchdog = WatchdogHandle::default();
        let mut test_db_paths = get_test_db_paths(2).await;
        let term_store = TermStoreHandle::new(watchdog.clone(), test_db_paths.pop().unwrap());
        let config = get_test_config().await;

        let state_meta = StateMeta {
            previous_log_index: 0,
            previous_log_term: 0,
            term: 0,
            id: 0,
            leader_commit: 0,
        };
        let initiator = InitiatorHandle::new(
            term_store,
            watchdog,
            config,
            state_meta,
            test_db_paths.pop().unwrap(),
        );

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
        let mut test_db_paths = get_test_db_paths(2).await;
        let term_store = TermStoreHandle::new(watchdog.clone(), test_db_paths.pop().unwrap());
        term_store.reset_term().await;

        let config = get_test_config().await;
        let state_meta = StateMeta {
            previous_log_index: 0,
            previous_log_term: 0,
            term: 0,
            id: 0,
            leader_commit: 0,
        };
        let initiator = InitiatorHandle::new(
            term_store,
            watchdog,
            config.clone(),
            state_meta,
            test_db_paths.pop().unwrap(),
        );
        let counter = initiator.get_counter().await;

        let test_future = async {
            // sleep necessary to make sure that server is up
            tokio::time::sleep(Duration::from_millis(20)).await;
            initiator.start_election().await;
            // sleep necessary to make sure that vote is processed before getting the count
            tokio::time::sleep(Duration::from_millis(60)).await;
        };

        // wait for test future or servers to return
        tokio::select! {
            _ = start_test_server(config.nodes[0].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[0].id),
            _ = start_test_server(config.nodes[1].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[1].id),
            _ = start_test_server(config.nodes[2].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[2].id),
            _ = start_test_server(config.nodes[3].port, TestServerTrue {}) => panic!("server {} returned first",config.nodes[3].id),
            _ = test_future => {
                assert_eq!(
                    counter.get_votes_received().await,
                    config.nodes.len() as u64
                    );
                }
        }
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
