use crate::actors::election::initiator::InitiatorHandle;
use crate::actors::log::executor::{App, ExecutorHandle};
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::replicator::ReplicatorHandle;
use std::collections::VecDeque;

use crate::actors::state_store::{ServerState, StateStoreHandle};
use crate::actors::term_store::TermStoreHandle;
use crate::actors::timer::TimerHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Config;
use crate::raft_rpc::append_entries_request::Entry;
use crate::state_meta::StateMeta;
use rand::Rng;
use std::time::Duration;
use tracing::info;

#[derive(Clone, Debug)]
pub struct RaftHandles {
    // todo make attributes private when it is clear which funcs are needed in server
    pub state_store: StateStoreHandle,
    pub state_timer: TimerHandle,
    //pub election_timer: TimerHandle, // todo maybe not needed here
    pub term_store: TermStoreHandle,
    // pub counter: CounterHandle, todo is initialized in Initiator, maybe better here?
    pub initiator: InitiatorHandle,
    pub log_store: LogStoreHandle,
    pub executor: ExecutorHandle,
    pub replicator: ReplicatorHandle,
    pub watchdog: WatchdogHandle,
    config: Config, // todo possible to get rid of here if election timer is handled differently?
}

impl RaftHandles {
    pub fn build(
        state_store: StateStoreHandle,
        watchdog: WatchdogHandle,
        config: Config,
        app: Box<dyn App>,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        state_meta: StateMeta,
    ) -> Self {
        let state_timeout = Duration::from_millis(config.state_timeout);
        let state_timer = TimerHandle::new(watchdog.clone(), state_timeout);

        // let election_timeout = Duration::from_millis(
        //     rand::thread_rng()
        //         .gen_range(config.election_timeout_range.0..config.election_timeout_range.1),
        // );
        //let election_timer = TimerHandle::new(watchdog.clone(), election_timeout);
        // todo create counter here
        let initiator = InitiatorHandle::new(
            term_store.clone(),
            watchdog.clone(),
            config.clone(),
            state_meta.clone(),
            config.vote_db_path.clone(),
        );
        let executor = ExecutorHandle::new(log_store.clone(), state_meta.term, app);
        let replicator = ReplicatorHandle::new(
            executor.clone(),
            term_store.clone(),
            log_store.clone(),
            config.clone(),
            state_meta,
        );

        Self {
            state_store,
            state_timer,
            term_store,
            initiator,
            log_store,
            executor,
            replicator,
            watchdog,
            config,
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

    // only possible in leader state because entry index must be correct
    // todo there must be an mechanism to reset actors in case of a leader switch e.g. the next index needs to be adjusted after a follower becomes a leader
    pub async fn create_entry(&self, payload: String) -> Option<Entry> {
        if self.state_store.get_state().await != ServerState::Leader {
            return None;
        }
        let index = self.log_store.get_and_increment_next_log_index().await;
        let term = self.term_store.get_term().await;
        Some(Entry {
            index,
            term,
            payload,
        })
    }

    // append only in leader state
    pub async fn append_entry(&self, entry: Entry) {
        if self.state_store.get_state().await == ServerState::Leader {
            self.log_store.append_entry(entry.clone()).await;
            self.replicator.replicate_entry(entry).await;
        }
    }

    // append only in leader state  // todo else send to leader
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
            info!("start election");
            self.initiator.start_election().await;
            let election_timeout = Duration::from_millis(rand::thread_rng().gen_range(
                self.config.election_timeout_range.0..self.config.election_timeout_range.1,
            ));
            TimerHandle::run_once(self.watchdog.clone(), election_timeout);
        }
    }

    // todo unit test (also in actors)
    pub async fn reset_actor_states(&self) {
        let state_meta = StateMeta::build(
            self.config.id,
            self.log_store.clone(),
            self.term_store.clone(),
        )
        .await;

        self.initiator.reset_voted_for().await;
        self.initiator
            .set_last_log_meta(state_meta.previous_log_index, state_meta.previous_log_term)
            .await;

        // todo counter reset votes

        self.replicator.set_state_meta(state_meta.clone()).await;

        self.executor.set_state_meta(state_meta.clone()).await;
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

        let state_meta = StateMeta {
            previous_log_index,
            previous_log_term,
            term: 0,
            id: 0,
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
