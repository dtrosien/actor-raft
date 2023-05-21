use crate::raft_server::actors::election::counter::CounterHandle;
use crate::raft_server::actors::election::worker::WorkerHandle;
use crate::raft_server::actors::term_store::TermStoreHandle;
use crate::raft_server::config::Config;
use crate::raft_server::db::raft_db::RaftDb;
use crate::raft_server::state_meta::StateMeta;
use crate::raft_server_rpc::RequestVoteRequest;
use std::collections::BTreeMap;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct Initiator {
    receiver: mpsc::Receiver<InitiatorMsg>,
    term_store: TermStoreHandle,
    workers: BTreeMap<u64, WorkerHandle>,
    counter: CounterHandle,
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
        counter: CounterHandle,
        config: Config,
        state_meta: StateMeta,
        path: String,
    ) -> Self {
        let db = RaftDb::new(path);
        let voted_for = db
            .read_voted_for()
            .expect("vote_store db seems to be corrupted");
        let id = config.id;
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
            workers,
            counter,
            db,
            voted_for,
            id,
            last_log_index: state_meta.last_log_index,
            last_log_term: state_meta.last_log_term,
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
        // start timer
        self.counter.start_election_timer().await;

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
        counter: CounterHandle,
        config: Config,
        state_meta: StateMeta,
        db_path: String,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut initiator = Initiator::new(receiver, term, counter, config, state_meta, db_path);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_server::actors::election::counter::calculate_required_votes;
    use crate::raft_server::actors::watchdog::WatchdogHandle;
    use crate::raft_server::config::get_test_config;
    use crate::raft_server::db::test_utils::get_test_db_paths;
    use crate::raft_server::rpc::utils::test::{start_test_server, TestServerTrue};
    use std::time::Duration;

    #[tokio::test]
    async fn get_voted_for_test() {
        let watchdog = WatchdogHandle::default();
        let mut test_db_paths = get_test_db_paths(2).await;
        let term_store = TermStoreHandle::new(watchdog.clone(), test_db_paths.pop().unwrap());
        let config = get_test_config().await;
        let state_meta = StateMeta {
            last_log_index: 0,
            last_log_term: 0,
            term: 0,
            id: 0,
            leader_commit: 0,
        };
        let votes_required = calculate_required_votes(config.nodes.len() as u64);
        let election_timeout = (150, 300);
        let counter = CounterHandle::new(watchdog, votes_required, election_timeout);
        let initiator = InitiatorHandle::new(
            term_store,
            counter,
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
            last_log_index: 0,
            last_log_term: 0,
            term: 0,
            id: 0,
            leader_commit: 0,
        };
        let votes_required = calculate_required_votes(config.nodes.len() as u64);
        let election_timeout = (150, 300);
        let counter = CounterHandle::new(watchdog, votes_required, election_timeout);
        let initiator = InitiatorHandle::new(
            term_store,
            counter,
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
            last_log_index: 0,
            last_log_term: 0,
            term: 0,
            id: 0,
            leader_commit: 0,
        };
        let votes_required = calculate_required_votes(config.nodes.len() as u64);
        let election_timeout = (150, 300);
        let counter = CounterHandle::new(watchdog, votes_required, election_timeout);
        let initiator = InitiatorHandle::new(
            term_store,
            counter.clone(),
            config.clone(),
            state_meta,
            test_db_paths.pop().unwrap(),
        );

        let test_future = async {
            // sleep necessary to make sure that server is up
            tokio::time::sleep(Duration::from_millis(30)).await;
            initiator.start_election().await;
            // sleep necessary to make sure that vote is processed before getting the count
            tokio::time::sleep(Duration::from_millis(70)).await;
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
}
