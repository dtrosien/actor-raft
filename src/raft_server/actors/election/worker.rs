use crate::raft_server::actors::election::counter::CounterHandle;
use crate::raft_server::actors::term_store::TermStoreHandle;
use crate::raft_server::config::NodeConfig;
use crate::raft_server::rpc;
use crate::raft_server_rpc::RequestVoteRequest;
use std::error::Error;

use crate::raft_server::rpc::node_client::NodeClient;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, warn};

#[derive(Debug)]
struct Worker {
    receiver: mpsc::Receiver<WorkerMsg>,
    term_store: TermStoreHandle,
    counter: CounterHandle,
    node: NodeConfig,
    client: Option<NodeClient>,
}

#[derive(Debug)]
enum WorkerMsg {
    RequestVote {
        request: RequestVoteRequest,
    },
    GetNode {
        respond_to: oneshot::Sender<NodeConfig>,
    },
}

impl Worker {
    #[tracing::instrument(ret, level = "debug")]
    fn new(
        receiver: mpsc::Receiver<WorkerMsg>,
        term: TermStoreHandle,
        counter: CounterHandle,
        node: NodeConfig,
    ) -> Self {
        Worker {
            receiver,
            term_store: term,
            counter,
            node,
            client: None,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: WorkerMsg) {
        match msg {
            WorkerMsg::RequestVote { request } => {
                let vote = self.request_vote(request).await;
                self.counter.register_vote(vote).await;
            }
            WorkerMsg::GetNode { respond_to } => {
                let _ = respond_to.send(self.node.clone());
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn request_vote(&mut self, request: RequestVoteRequest) -> Option<bool> {
        if self.init_client_if_necessary().await.is_err() {
            error!("unable to build client for node {} with address: {},{} for election, retry with next request ",
                    self.node.id, self.node.ip, self.node.port);
            return Some(false);
        };

        match self
            .client
            .as_mut()
            .expect("client must exist")
            .request_vote(request)
            .await
        {
            Ok(reply) => {
                // this call is non blocking and might fire a term error
                self.term_store.check_term(reply.term).await;
                Some(reply.success_or_granted)
            }
            Err(_) => {
                warn!("could not send vote request to node {}", self.node.id);
                self.client.take();
                Some(false)
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn init_client_if_necessary(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.client.is_some() {
            return Ok(());
        }
        info!("init node client for replication");
        NodeClient::build(self.node.ip.clone(), self.node.port)
            .await
            .map(|x| {
                self.client.replace(x);
            })
    }
}

#[derive(Clone, Debug)]
pub struct WorkerHandle {
    sender: mpsc::Sender<WorkerMsg>,
}

impl WorkerHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(term_store: TermStoreHandle, counter: CounterHandle, node: NodeConfig) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut worker = Worker::new(receiver, term_store, counter, node);
        tokio::spawn(async move { worker.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_node(&self) -> NodeConfig {
        let (send, recv) = oneshot::channel();
        let msg = WorkerMsg::GetNode { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn request_vote(&self, request: RequestVoteRequest) {
        let msg = WorkerMsg::RequestVote { request };
        let _ = self.sender.send(msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_server::actors::watchdog::WatchdogHandle;
    use crate::raft_server::db::test_utils::get_test_db_paths;
    use crate::raft_server::rpc::utils::test::{get_test_port, start_test_server, TestServerTrue};
    use std::time::Duration;

    #[tokio::test]
    async fn get_node_test() {
        let watchdog = WatchdogHandle::default();
        let mut test_db_paths = get_test_db_paths(1).await;
        let term_store = TermStoreHandle::new(watchdog.clone(), test_db_paths.pop().unwrap());
        let votes_required: u64 = 3;
        let election_timeout = (150, 300);
        let counter = CounterHandle::new(watchdog, votes_required, election_timeout);
        let node = NodeConfig {
            id: 0,
            ip: "".to_string(),
            port: 0,
        };
        let worker = WorkerHandle::new(term_store, counter, node.clone());

        assert_eq!(worker.get_node().await.id, node.id);
    }

    #[tokio::test]
    async fn request_vote_test() {
        // initialise test setup
        let watchdog = WatchdogHandle::default();
        let mut test_db_paths = get_test_db_paths(1).await;
        let term_store = TermStoreHandle::new(watchdog.clone(), test_db_paths.pop().unwrap());
        let votes_required: u64 = 3;
        let election_timeout = (150, 300);
        let counter = CounterHandle::new(watchdog, votes_required, election_timeout);
        let port = get_test_port().await;
        let node = NodeConfig {
            id: 0,
            ip: "[::1]".to_string(),
            port,
        };
        let worker = WorkerHandle::new(term_store, counter.clone(), node);
        let request = RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };

        // start test
        assert_eq!(counter.clone().get_votes_received().await, 0);

        let test_future = async {
            // sleep necessary to make sure that server is up
            tokio::time::sleep(Duration::from_millis(15)).await;
            worker.request_vote(request).await;
            // sleep necessary to make sure that vote is processed before getting it
            tokio::time::sleep(Duration::from_millis(15)).await;
        };

        tokio::select! {
            _ = start_test_server(port, TestServerTrue {}) => panic!("server returned first"),
            _ = test_future => {
                assert_eq!(counter.clone().get_votes_received().await, 1);
                }
        }
    }
}
