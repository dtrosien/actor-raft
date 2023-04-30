use crate::actors::election::initiator::InitiatorHandle;
use crate::actors::log::executor::{App, ExecutorHandle};
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::replicator::ReplicatorHandle;
use crate::actors::log::replication::worker::ReplicatorStateMeta;
use std::collections::VecDeque;

use crate::actors::state_store::{ServerState, StateStoreHandle};
use crate::actors::term_store::TermStoreHandle;
use crate::actors::timer::TimerHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Config;
use crate::raft_rpc::append_entries_request::Entry;
use rand::Rng;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RaftHandles {
    // todo make attributes private when it is clear which funcs are needed in server
    pub state_store: StateStoreHandle,
    pub state_timer: TimerHandle,
    pub election_timer: TimerHandle, // todo maybe not needed here
    pub term_store: TermStoreHandle,
    // pub counter: CounterHandle, todo is initialized in Initiator, maybe better here?
    pub initiator: InitiatorHandle,
    pub log_store: LogStoreHandle,
    pub executor: ExecutorHandle,
    pub replicator: ReplicatorHandle,
}

impl RaftHandles {
    pub fn build(
        state_store: StateStoreHandle,
        watch_dog: WatchdogHandle,
        config: Config,
        app: Box<dyn App>,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        state_meta: ReplicatorStateMeta,
    ) -> Self {
        let state_timeout = Duration::from_millis(config.state_timeout);
        let election_timeout = Duration::from_millis(
            rand::thread_rng()
                .gen_range(config.election_timeout_range.0..config.election_timeout_range.1),
        );
        let state_timer = TimerHandle::new(watch_dog.clone(), state_timeout);
        let election_timer = TimerHandle::new(watch_dog.clone(), election_timeout);
        let initiator = InitiatorHandle::new(
            term_store.clone(),
            watch_dog,
            config.clone(),
            config.vote_db_path.clone(),
        );
        let executor = ExecutorHandle::new(log_store.clone(), state_meta.term, app);
        let replicator = ReplicatorHandle::new(
            executor.clone(),
            term_store.clone(),
            log_store.clone(),
            config,
            state_meta,
        );

        Self {
            state_store,
            state_timer,
            election_timer,
            term_store,
            initiator,
            log_store,
            executor,
            replicator,
        }
    }

    pub async fn register_heartbeat(&self) {
        self.state_timer.register_heartbeat().await;
    }

    pub async fn append_entries_to_local_log(
        &self,
        entries: VecDeque<Entry>,
    ) -> VecDeque<Option<u64>> {
        self.log_store.append_entries(entries).await
    }

    // append only in only in leader state  // todo else send to leader
    pub async fn append_entries(&self, entries: VecDeque<Entry>) {
        if self.state_store.get_state().await == ServerState::Leader {
            self.log_store.append_entries(entries.clone()).await;
            for entry in entries {
                self.replicator.add_to_batch(entry).await;
            }
            self.replicator.flush_batch().await;
        }
    }

    // only in leader state
    pub async fn send_heartbeat(&self) {
        if self.state_store.get_state().await == ServerState::Leader {
            self.replicator.flush_batch().await;
        }
    }

    // only in follower state
    pub async fn request_votes(&self) {
        if self.state_store.get_state().await == ServerState::Candidate {
            // todo counter needs timeout
            // todo voted for must be initialized after term change
            self.initiator.start_election().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actors::log::test_utils::TestApp;
    use crate::config::get_test_config;

    #[tokio::test]
    async fn build_test() {
        let config = get_test_config().await;
        let state_store = StateStoreHandle::new();
        let wd = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(wd.clone(), config.term_db_path.clone());
        let log_store = LogStoreHandle::new(config.log_db_path.clone());

        let previous_log_term = log_store.get_last_log_term().await;
        let previous_log_index = log_store.get_last_log_index().await;

        let state_meta = ReplicatorStateMeta {
            previous_log_index,
            previous_log_term,
            term: 0,
            leader_id: 0,
            leader_commit: 0, // todo why couldnt this be set to zero inside actor
        };

        let _handles = RaftHandles::build(
            state_store,
            wd,
            config,
            Box::new(TestApp {}),
            term_store,
            log_store,
            state_meta,
        );
    }
}
