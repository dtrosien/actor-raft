use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::worker::ReplicatorStateMeta;
use crate::actors::log::test_utils::TestApp;
use crate::actors::state_store::{ServerState, StateStoreHandle};
use crate::actors::term_store::TermStoreHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Config;
use crate::raft_handles::RaftHandles;
use std::time::Duration;
use tracing::info;

#[derive(Debug)]
pub struct Raft {
    state_store: StateStoreHandle,
    watchdog: WatchdogHandle,
    term_store: TermStoreHandle,
    handles: RaftHandles,
    config: Config,
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
        }
    }

    pub fn get_handles(&self) -> RaftHandles {
        self.handles.clone()
    }

    // todo think again about if this is really working like that
    pub async fn run(&mut self) {
        self.handles.state_timer.register_heartbeat().await;
        let mut exit_state_r = self.watchdog.get_exit_receiver().await;
        info!("{:?}", self.state_store.get_state().await);

        // todo more readable
        // will be triggered if timed out in candidate or follower state,
        // or when receiving higher term in leader state
        match self.handles.state_store.get_state().await {
            ServerState::Leader => {
                tokio::select! {
                    _leader = self.send_heartbeats() => {info!("send hb");}
                    _wait_for_timeout = exit_state_r.recv() =>{},

                }
            }
            ServerState::Follower => {
                let _wait_for_timeout = exit_state_r.recv().await;
            }
            ServerState::Candidate => {
                self.handles.request_votes().await;
                let _wait_for_timeout = exit_state_r.recv().await;
            }
        }

        info!("{:?}", self.state_store.get_state().await);
    }

    pub async fn run_continuously(&mut self) {
        //todo introduce complete shutdown ... shutdown should be renamed to exit/shutdown current state
        loop {
            self.run().await;
        }
    }

    async fn send_heartbeats(&self) {
        let hb_interval = Duration::from_millis(self.config.state_timeout / 2); // todo into config
        loop {
            self.handles.send_heartbeat().await;
            tokio::time::sleep(hb_interval).await;
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
    let previous_log_term = log_store.get_last_log_term().await;
    let previous_log_index = log_store.get_last_log_index().await;
    let term = term_store.get_term().await;
    let state_meta = ReplicatorStateMeta {
        previous_log_index,
        previous_log_term,
        term,
        leader_id: config.id,
        leader_commit: 0,
    };

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
    use crate::actors::log::test_utils::TestApp;
    use crate::config::get_test_config;

    #[tokio::test]
    async fn run_test() {
        let config = get_test_config().await;

        let mut raft = Raft::build(config).await;
        raft.run().await;
        raft.run().await;
    }
}
