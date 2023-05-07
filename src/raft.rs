use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::test_utils::TestApp;
use crate::actors::state_store::{ServerState, StateStoreHandle};
use crate::actors::term_store::TermStoreHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Config;
use crate::raft_handles::RaftHandles;
use crate::state_meta::StateMeta;
use std::time::Duration;
use tracing::info;

#[derive(Debug)]
pub struct Raft {
    state_store: StateStoreHandle,
    watchdog: WatchdogHandle,
    term_store: TermStoreHandle,
    handles: RaftHandles,
    config: Config,
    shutdown: bool, // todo with shutdown hook channel like in tonic?
}

impl Raft {
    // todo implement with builder pattern
    pub async fn build(config: Config) -> Self {
        let state_store = StateStoreHandle::new();
        let watchdog = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(watchdog.clone(), config.term_db_path.clone());
        let handles = create_actors(
            config.clone(),
            state_store.clone(),
            watchdog.clone(),
            term_store.clone(),
        )
        .await;
        Raft {
            state_store,
            watchdog,
            term_store,
            handles,
            config,
            shutdown: false,
        }
    }

    pub fn get_handles(&self) -> RaftHandles {
        self.handles.clone()
    }

    pub async fn run(&mut self) {
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
        // exit current state when triggeren by watchdog
        let _switch_state_trigger = exit_state_r.recv().await;

        self.handles.reset_actor_states().await;
        info!(
            "Terminate current state. Next state: {:?}",
            self.state_store.get_state().await
        );
    }

    pub async fn run_continuously(&mut self) {
        loop {
            self.run().await;
            if self.shutdown {
                break;
            }
        }
    }

    async fn start_rpc_server() {
        // todo implement
    }

    async fn send_heartbeats(&self) {
        let hb_interval = Duration::from_millis(self.config.state_timeout / 2); // todo into config
        let mut exit_state_r = self.watchdog.get_exit_receiver().await;
        loop {
            let hb_future = async {
                self.handles.send_heartbeat().await;
                // to prevent timeout leader must trigger his own timer
                self.handles.state_timer.register_heartbeat().await; // todo if possible maybe trigger this in append entry client (when follower answer)
                tokio::time::sleep(hb_interval).await;
            };
            tokio::select! {
                _heartbeat= hb_future=> {},
                _switch_state_trigger = exit_state_r.recv() => {break},
            }
        }
    }
}

async fn create_actors(
    config: Config,
    state_store: StateStoreHandle,
    watchdog: WatchdogHandle,
    term_store: TermStoreHandle,
) -> RaftHandles {
    let log_store = LogStoreHandle::new(config.log_db_path.clone());
    let state_meta = StateMeta::build(config.id, log_store.clone(), term_store.clone()).await;

    RaftHandles::build(
        state_store,
        watchdog,
        config,
        Box::new(TestApp {}),
        term_store,
        log_store,
        state_meta,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::get_test_config;
    // use tracing_test::traced_test;

    #[tokio::test]
    // #[traced_test]
    async fn run_test() {
        let config = get_test_config().await;
        let mut raft = Raft::build(config).await;
        raft.run().await;
        raft.run().await;
    }
}
