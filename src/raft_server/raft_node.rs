use crate::raft_server::actors::log::log_store::LogStoreHandle;
use crate::raft_server::actors::log::test_utils::TestApp;
use crate::raft_server::actors::state_store::StateStoreHandle;
use crate::raft_server::actors::term_store::TermStoreHandle;
use crate::raft_server::actors::watchdog::WatchdogHandle;
use crate::raft_server::config::{Config, NodeConfig};
use crate::raft_server::raft_handles::RaftHandles;
use crate::raft_server::state_meta::StateMeta;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::sync::Arc;

use crate::raft_server::rpc::utils::{init_client_server, init_node_server};

use crate::app::App;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Sender;
use tokio::task::JoinHandle;
use tracing::info;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum ServerState {
    Leader,
    Follower,
    Candidate,
}

pub struct RaftNodeBuilder {
    config: Config,
    s_shutdown: Sender<()>,
    app: Arc<dyn App>,
}

impl RaftNodeBuilder {
    pub fn new(app: Arc<dyn App>) -> Self {
        let config = Config::default();
        let s_shutdown = broadcast::channel(1).0; // todo [check] broadcast (capacity?)
        RaftNodeBuilder {
            config,
            s_shutdown,
            app,
        }
    }

    pub fn with_shutdown(&mut self, s_shutdown: broadcast::Sender<()>) -> &mut RaftNodeBuilder {
        self.s_shutdown = s_shutdown;
        self
    }

    pub fn with_client_service_enabled(&mut self, enable: bool) -> &mut RaftNodeBuilder {
        self.config.client_service_enabled = enable;
        self
    }

    pub fn with_rpc_server_enabled(&mut self, enable: bool) -> &mut RaftNodeBuilder {
        self.config.node_server_enabled = enable;
        self
    }

    pub fn with_app(&mut self, app: Arc<dyn App>) -> &mut RaftNodeBuilder {
        self.app = app;
        self
    }

    pub fn with_node(&mut self, node: NodeConfig) -> &mut RaftNodeBuilder {
        self.config.nodes.push(node);
        self
    }

    pub fn with_id(&mut self, id: u64) -> &mut RaftNodeBuilder {
        self.config.id = id;
        self
    }
    pub fn with_ip(&mut self, ip: &str) -> &mut RaftNodeBuilder {
        self.config.ip = ip.to_string();
        self
    }
    pub fn with_port(&mut self, port: u16) -> &mut RaftNodeBuilder {
        self.config.port = port;
        self
    }

    pub fn with_log_db_path(&mut self, path: &str) -> &mut RaftNodeBuilder {
        self.config.log_db_path = path.to_string();
        self
    }
    pub fn with_term_db_path(&mut self, path: &str) -> &mut RaftNodeBuilder {
        self.config.term_db_path = path.to_string();
        self
    }
    pub fn with_vote_db_path(&mut self, path: &str) -> &mut RaftNodeBuilder {
        self.config.vote_db_path = path.to_string();
        self
    }
    pub fn with_channel_capacity(&mut self, capacity: u16) -> &mut RaftNodeBuilder {
        self.config.channel_capacity = capacity;
        self
    }
    pub fn with_state_timeout(&mut self, timeout_ms: u64) -> &mut RaftNodeBuilder {
        self.config.state_timeout = timeout_ms;
        self
    }
    pub fn with_heartbeat_interval(&mut self, hb_interval_ms: u64) -> &mut RaftNodeBuilder {
        self.config.heartbeat_interval = hb_interval_ms;
        self
    }
    pub fn with_election_timeout_range(
        &mut self,
        from_ms: u64,
        to_ms: u64,
    ) -> &mut RaftNodeBuilder {
        self.config.election_timeout_range = (from_ms, to_ms);
        self
    }
    pub fn with_initial_state(&mut self, state: ServerState) -> &mut RaftNodeBuilder {
        self.config.initial_state = state;
        self
    }
    pub fn with_nodes(&mut self, nodes: Vec<NodeConfig>) -> &mut RaftNodeBuilder {
        nodes
            .iter()
            .for_each(|node| self.config.nodes.push(node.clone()));
        self
    }

    pub async fn build(&self) -> RaftNode {
        RaftNode::build(
            self.config.clone(),
            self.s_shutdown.clone(),
            self.config.node_server_enabled,
            self.config.client_service_enabled,
        )
        .await
    }
}

// todo [feature] implement real console printer for default
impl Default for RaftNodeBuilder {
    fn default() -> Self {
        let app = Arc::new(TestApp {});
        RaftNodeBuilder::new(app)
    }
}

pub struct RaftNode {
    state_store: StateStoreHandle,
    watchdog: WatchdogHandle,
    handles: RaftHandles,
    config: Config,
    s_shutdown: Sender<()>,
    node_server: Option<JoinHandle<()>>,
    client_server: Option<JoinHandle<()>>,
}

impl RaftNode {
    async fn build(
        config: Config,
        s_shutdown: broadcast::Sender<()>,
        node_server_enabled: bool,
        client_service_enabled: bool,
    ) -> Self {
        let state_store = StateStoreHandle::new(config.initial_state.clone());
        let watchdog = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(watchdog.clone(), config.term_db_path.clone());

        let log_store = LogStoreHandle::new(config.log_db_path.clone());
        let state_meta = StateMeta::build(config.id, log_store.clone(), term_store.clone()).await;

        let handles = RaftHandles::build(
            state_store.clone(),
            watchdog.clone(),
            config.clone(),
            Arc::new(TestApp {}),
            term_store,
            log_store,
            state_meta,
        );

        let node_server = if node_server_enabled {
            init_node_server(
                s_shutdown.subscribe(),
                handles.clone(),
                config.ip.clone(),
                config.port,
            )
            .await
        } else {
            None
        };
        let client_server = if client_service_enabled {
            init_client_server(
                s_shutdown.subscribe(),
                handles.clone(),
                config.ip.clone(),
                config.service_port,
            )
            .await
        } else {
            None
        };
        handles.register_replication_workers_at_executor().await;
        RaftNode {
            state_store,
            watchdog,
            handles,
            config,
            s_shutdown,
            node_server,
            client_server,
        }
    }

    /// returns handles to all raft actors
    pub fn get_handles(&self) -> RaftHandles {
        self.handles.clone()
    }

    /// returns the current configuration of the raft node
    pub fn get_config(&self) -> Config {
        self.config.clone()
    }

    /// returns a handle to the task in which the node rpc server is executed
    pub fn get_node_server_handle(&mut self) -> Option<JoinHandle<()>> {
        self.node_server.take()
    }
    /// returns a handle to the task in which the client rpc server is executed
    pub fn get_client_server_handle(&mut self) -> Option<JoinHandle<()>> {
        self.client_server.take()
    }

    /// runs all required services until a external shutdown signal is received
    pub async fn execute(&mut self) {
        if let (Some(n_hdl), Some(c_hdl)) = (
            self.get_node_server_handle(),
            self.get_client_server_handle(),
        ) {
            let runner = self.run_states_continuously();
            let _ = tokio::join!(n_hdl, c_hdl, runner);
        }
    }

    /// runs the current state until a state exit signal is received from watch dog
    async fn run_state(&mut self) -> &mut RaftNode {
        // reset timer
        self.handles.state_timer.register_heartbeat().await;
        // register at watchdog to get notified when timeouts or term errors occurred
        let mut r_exit_state = self.watchdog.get_exit_receiver().await;

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
        let _switch_state_trigger = r_exit_state.recv().await;

        self.handles.reset_actor_states().await;
        info!(
            "Terminate current state. Next state: {:?}",
            self.state_store.get_state().await
        );
        self
    }

    /// runs states continuously until a external shutdown signal is received
    pub async fn run_states_continuously(&mut self) {
        let r_shutdown = self.s_shutdown.subscribe();
        while r_shutdown.is_empty() {
            self.run_state().await;
        }
    }

    /// runs states until n state_exit signal from watchdog are received
    pub async fn run_states_n_times(&mut self, n: u64) -> &RaftNode {
        for i in 1..=n {
            info!("run n={}", i);
            self.run_state().await;
        }
        let _ = self.s_shutdown.send(());
        self
    }

    /// stops current node rpc server instance and starts a new one
    pub async fn restart_node_server(&mut self) -> &RaftNode {
        let r_shutdown = self.s_shutdown.subscribe();
        match self.node_server.take() {
            None => {}
            Some(server) => server.abort(),
        }
        self.node_server = init_node_server(
            r_shutdown,
            self.handles.clone(),
            self.config.ip.clone(),
            self.config.port,
        )
        .await;
        self
    }

    /// stops current client rpc server instance and starts a new one
    pub async fn restart_client_server(&mut self) -> &RaftNode {
        let r_shutdown = self.s_shutdown.subscribe();
        match self.client_server.take() {
            None => {}
            Some(server) => server.abort(),
        }
        self.client_server = init_client_server(
            r_shutdown,
            self.handles.clone(),
            self.config.ip.clone(),
            self.config.service_port,
        )
        .await;
        self
    }

    /// let the node send heartbeats to all other nodes until a state exit signal is received
    /// (this also triggers replication of appended entries)
    async fn send_heartbeats(&self) -> &RaftNode {
        // todo [feature] send no opt entry here
        let hb_interval = Duration::from_millis(self.config.heartbeat_interval);
        let r_exit_state = self.watchdog.get_exit_receiver().await;
        let r_shutdown = self.s_shutdown.subscribe();
        while r_exit_state.is_empty() && r_shutdown.is_empty() {
            info!("send heartbeat");
            self.handles.send_heartbeat().await;
            // to prevent timeout leader must trigger his own timer
            self.handles.state_timer.register_heartbeat().await; // todo [later feature] if possible maybe trigger this in append entry client (when follower answer)
            tokio::time::sleep(hb_interval).await;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_server::config::get_test_config;
    use crate::raft_server::raft_node::ServerState::Leader;

    #[tokio::test]
    async fn run_test() {
        let s_shutdown = broadcast::channel(1).0;

        let config = get_test_config().await;
        let mut raft_node = RaftNode::build(config, s_shutdown, true, true).await;
        raft_node.restart_node_server().await;
        raft_node.restart_client_server().await;
        raft_node.run_state().await.run_state().await;

        let rh = tokio::spawn(async move { raft_node.execute().await });
        let hb_interval = Duration::from_millis(50);
        tokio::select! {
            _ = rh => {panic!()},
            _ = tokio::time::sleep(hb_interval) => {},

        }
    }

    #[tokio::test]
    async fn builder_test() {
        let app = Arc::new(TestApp {});
        let raft_node = RaftNodeBuilder::new(app)
            .with_id(1)
            .with_initial_state(Leader)
            .with_channel_capacity(10)
            .with_election_timeout_range(200, 500)
            .build()
            .await;

        let config = raft_node.get_config();
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
