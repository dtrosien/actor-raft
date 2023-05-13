use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::test_utils::TestApp;
use crate::actors::state_store::{ServerState, StateStoreHandle};
use crate::actors::term_store::TermStoreHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::{Config, Node};
use crate::raft_handles::RaftHandles;
use crate::raft_rpc::raft_rpc_server::RaftRpcServer;
use crate::rpc::server::RaftServer;
use crate::state_meta::StateMeta;
use futures_util::FutureExt;

use crate::actors::log::executor::App;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tonic::transport::Server;
use tracing::info;

pub struct RaftBuilder {
    config: Config,
    s_shutdown: Sender<()>,
    app: Box<dyn App>,
}

impl RaftBuilder {
    pub fn new(app: Box<dyn App>) -> Self {
        let config = Config::default();
        let s_shutdown = broadcast::channel(1).0; // todo broadcast (capacity?) or better oneshot etc?
        RaftBuilder {
            config,
            s_shutdown,
            app,
        }
    }

    pub fn with_shutdown(&mut self, s_shutdown: broadcast::Sender<()>) -> &mut RaftBuilder {
        self.s_shutdown = s_shutdown;
        self
    }

    pub fn with_app(&mut self, app: Box<dyn App>) -> &mut RaftBuilder {
        self.app = app;
        self
    }

    pub fn with_node(&mut self, node: Node) -> &mut RaftBuilder {
        self.config.nodes.push(node);
        self
    }

    pub fn with_id(&mut self, id: u64) -> &mut RaftBuilder {
        self.config.id = id;
        self
    }
    pub fn with_ip(&mut self, ip: &str) -> &mut RaftBuilder {
        self.config.ip = ip.to_string();
        self
    }
    pub fn with_port(&mut self, port: u16) -> &mut RaftBuilder {
        self.config.port = port;
        self
    }

    pub fn with_log_db_path(&mut self, path: String) -> &mut RaftBuilder {
        self.config.log_db_path = path;
        self
    }
    pub fn with_term_db_path(&mut self, path: String) -> &mut RaftBuilder {
        self.config.term_db_path = path;
        self
    }
    pub fn with_vote_db_path(&mut self, path: String) -> &mut RaftBuilder {
        self.config.vote_db_path = path;
        self
    }
    pub fn with_channel_capacity(&mut self, capacity: u16) -> &mut RaftBuilder {
        self.config.channel_capacity = capacity;
        self
    }
    pub fn with_state_timeout(&mut self, timeout_ms: u64) -> &mut RaftBuilder {
        self.config.state_timeout = timeout_ms;
        self
    }
    pub fn with_heartbeat_interval(&mut self, hb_interval_ms: u64) -> &mut RaftBuilder {
        self.config.heartbeat_interval = hb_interval_ms;
        self
    }
    pub fn with_election_timeout_range(&mut self, from_ms: u64, to_ms: u64) -> &mut RaftBuilder {
        self.config.election_timeout_range = (from_ms, to_ms);
        self
    }
    pub fn with_initial_state(&mut self, state: ServerState) -> &mut RaftBuilder {
        self.config.initial_state = state;
        self
    }
    pub fn with_nodes(&mut self, nodes: Vec<Node>) -> &mut RaftBuilder {
        nodes
            .iter()
            .for_each(|node| self.config.nodes.push(node.clone()));
        self
    }

    pub async fn build(&self) -> Raft {
        Raft::build(self.config.clone(), self.s_shutdown.clone()).await
    }
}

// todo implement real console printer for default
impl Default for RaftBuilder {
    fn default() -> Self {
        let app = Box::new(TestApp {});
        RaftBuilder::new(app)
    }
}

pub struct Raft {
    state_store: StateStoreHandle,
    watchdog: WatchdogHandle,
    handles: RaftHandles,
    config: Config,
    s_shutdown: Sender<()>,
}

impl Raft {
    pub async fn build(config: Config, s_shutdown: broadcast::Sender<()>) -> Self {
        let state_store = StateStoreHandle::new(config.initial_state.clone());
        let watchdog = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(watchdog.clone(), config.term_db_path.clone());

        let log_store = LogStoreHandle::new(config.log_db_path.clone());
        let state_meta = StateMeta::build(config.id, log_store.clone(), term_store.clone()).await;

        let handles = RaftHandles::build(
            state_store.clone(),
            watchdog.clone(),
            config.clone(),
            Box::new(TestApp {}),
            term_store,
            log_store,
            state_meta,
        );

        Raft {
            state_store,
            watchdog,
            handles,
            config,
            s_shutdown,
        }
    }

    pub fn get_handles(&self) -> RaftHandles {
        self.handles.clone()
    }

    pub fn get_config(&self) -> Config {
        self.config.clone()
    }

    pub async fn run(&mut self) -> &mut Raft {
        // reset timer
        self.handles.state_timer.register_heartbeat().await;
        // register at watchdog to get notified when timeouts or term errors occurred
        let mut exit_state_r = self.watchdog.get_exit_receiver().await;

        match self.handles.state_store.get_state().await {
            ServerState::Leader => {
                info!("Run as Leader: start sending heartbeats");
                self.send_heartbeats().await;
            }
            ServerState::Candidate => {
                info!("Run as Candidate: start requesting votes");
                self.handles.request_votes().await;
            }
            ServerState::Follower => {
                info!("Run as Follower: start waiting for leader messages");
            }
        }
        // exit current state when triggered by watchdog
        let _switch_state_trigger = exit_state_r.recv().await;

        self.handles.reset_actor_states().await;
        info!(
            "Terminate current state. Next state: {:?}",
            self.state_store.get_state().await
        );
        self
    }

    pub async fn run_continuously(&mut self) {
        let r_shutdown = self.s_shutdown.subscribe();
        while r_shutdown.is_empty() {
            self.run().await;
        }
    }

    pub async fn start_rpc_server(&mut self) -> &Raft {
        let raft_rpc = RaftServer::new(self.handles.clone());
        let raft_service = RaftRpcServer::new(raft_rpc);

        let addr = format!("{}:{}", self.config.ip, self.config.port)
            .parse()
            .unwrap();

        let mut r_shutdown = self.s_shutdown.subscribe();

        tokio::spawn(async move {
            Server::builder()
                .add_service(raft_service)
                .serve_with_shutdown(addr, r_shutdown.recv().map(|_| ()))
                .await
                .unwrap();
            info!("tonic rpc server was shut down");
        });
        self
    }

    async fn send_heartbeats(&self) -> &Raft {
        let hb_interval = Duration::from_millis(self.config.heartbeat_interval);
        let exit_state_r = self.watchdog.get_exit_receiver().await;
        while exit_state_r.is_empty() {
            self.handles.send_heartbeat().await;
            // to prevent timeout leader must trigger his own timer
            self.handles.state_timer.register_heartbeat().await; // todo if possible maybe trigger this in append entry client (when follower answer)
            tokio::time::sleep(hb_interval).await;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::state_store::ServerState::Leader;
    use crate::config::get_test_config;
    // use tracing_test::traced_test;

    #[tokio::test]
    // #[traced_test]
    async fn run_test() {
        let s_shutdown = broadcast::channel(1).0;

        let config = get_test_config().await;
        let mut raft = Raft::build(config, s_shutdown).await;
        raft.start_rpc_server().await;
        raft.run().await.run().await;

        let rh = tokio::spawn(async move { raft.run_continuously().await });
        let hb_interval = Duration::from_millis(400);
        tokio::select! {
            _ = rh => {},
            _ = tokio::time::sleep(hb_interval) => {},

        }
    }

    #[tokio::test]
    async fn builder_test() {
        let app = Box::new(TestApp {});
        let raft = RaftBuilder::new(app)
            .with_id(1)
            .with_initial_state(Leader)
            .with_channel_capacity(10)
            .with_election_timeout_range(200, 500)
            .build()
            .await;

        let config = raft.get_config();
        let default_config = Config::default();

        //changed
        assert_eq!(config.id, 1);
        assert_eq!(config.initial_state, Leader);
        assert_eq!(config.channel_capacity, 10);
        assert_eq!(config.election_timeout_range, (200, 500));

        // unchanged
        assert_eq!(config.state_timeout, default_config.state_timeout);
        assert_eq!(config.port, default_config.port);
    }
}
