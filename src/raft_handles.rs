use crate::actors::election::initiator::InitiatorHandle;
use crate::actors::log::executor::{App, ExecutorHandle};
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::replicator::ReplicatorHandle;
use crate::actors::log::replication::worker::ReplicatorStateMeta;
use crate::actors::log::test_utils::TestApp;
use crate::actors::state_store::StateStoreHandle;
use crate::actors::term_store::TermStoreHandle;
use crate::actors::timer::TimerHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Config;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct RaftHandles {
    // todo make attributes private when it is clear which funcs are needed in server
    pub timeout_timer: TimerHandle,
    pub term_store: TermStoreHandle,
    // pub counter: CounterHandle, todo is initialized in Initiator, maybe better here?
    pub initiator: InitiatorHandle,
    pub log_store: LogStoreHandle,
    pub executor: ExecutorHandle,
    pub replicator: ReplicatorHandle,
}

impl RaftHandles {
    pub fn new(
        watch_dog: WatchdogHandle,
        config: Config,
        app: Box<dyn App>,
        term_store: TermStoreHandle,
        log_store: LogStoreHandle,
        state_meta: ReplicatorStateMeta,
        vote_db_path: String,
    ) -> Self {
        let timeout = Duration::from_millis(20);
        let timeout_timer = TimerHandle::new(watch_dog.clone(), timeout);
        let initiator =
            InitiatorHandle::new(term_store.clone(), watch_dog, config.clone(), vote_db_path);
        let executor = ExecutorHandle::new(log_store.clone(), state_meta.term, app);
        let replicator = ReplicatorHandle::new(
            executor.clone(),
            term_store.clone(),
            log_store.clone(),
            config,
            state_meta,
        );

        Self {
            timeout_timer,
            term_store,
            initiator,
            log_store,
            executor,
            replicator,
        }
    }

    pub async fn send_heartbeat(&self) {
        self.timeout_timer.send_heartbeat().await;
    }

    pub async fn append_entries_api(&self) {
        self.timeout_timer.send_heartbeat().await;
    }

    pub fn request_vote_api(&self) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test() {
        let wd = WatchdogHandle::default();
        let term_store = TermStoreHandle::new(wd.clone(), "databases/term_db".to_string());
        let log_store = LogStoreHandle::new("databases/log_db".to_string());

        let previous_log_term = log_store.get_last_log_term().await;
        let previous_log_index = log_store.get_last_log_index().await;

        let state_meta = ReplicatorStateMeta {
            previous_log_index,
            previous_log_term,
            term: 0,
            leader_id: 0,
            leader_commit: 0, // todo why couldnt this be set to zero inside actor
        };

        let handles = RaftHandles::new(
            wd,
            Config::default(),
            Box::new(TestApp {}),
            term_store,
            log_store,
            state_meta,
            "databases/vote_db".to_string(),
        );
    }
}
