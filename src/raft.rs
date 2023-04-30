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

#[derive(Debug)]
pub struct Raft {
    state_store: StateStoreHandle,
    watchdog: WatchdogHandle,
    term_store: TermStoreHandle,
    core: CoreHandles,
}

impl Raft {
    pub async fn build(log_db_path: String, vote_db_path: String, term_db_path: String) -> Self {
        let state_store = StateStoreHandle::new();
        let watchdog = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(watchdog.clone(), term_db_path);
        let core = create_actors(
            watchdog.clone(),
            term_store.clone(),
            log_db_path,
            vote_db_path,
        )
        .await;
        Raft {
            state_store,
            watchdog,
            term_store,
            core,
        }
    }

    pub fn get_handles(&self) -> CoreHandles {
        self.core.clone()
    }

    pub async fn run(&mut self) {
        let mut exit_state_r = self.watchdog.get_exit_receiver().await;
        println!("{:?}", self.state_store.get_state().await);

        exit_state_r.recv().await.expect("TODO: panic message");
        println!("{:?}", self.state_store.get_state().await);
    }

    pub async fn run_continuously(&mut self) {
        //todo introduce complete shutdown ... shutdown should be renamed to exit/shutdown current state
        loop {
            self.run().await;
        }
    }
}

async fn create_actors(
    watchdog: WatchdogHandle,
    term_store: TermStoreHandle,
    log_db_path: String,
    vote_db_path: String,
) -> CoreHandles {
    let config = Config::default();
    let log_store = LogStoreHandle::new(log_db_path);
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

    CoreHandles::new(
        watchdog,
        config,
        Box::new(TestApp {}),
        term_store,
        log_store,
        state_meta,
        vote_db_path,
    )
    // match self.state {
    //     State::Leader => ApiStruct {  },
    //     State::Follower => ApiStruct {  },
    //     State::Candidate => ApiStruct {  },
    // }
}

#[derive(Clone, Debug)]
pub struct CoreHandles {
    // todo make attributes private when it is clear which funcs are needed in server
    pub timer: TimerHandle,
    pub term_store: TermStoreHandle,
    // pub counter: CounterHandle, todo is initialized in Initiator, maybe better here?
    pub initiator: InitiatorHandle,
    pub log_store: LogStoreHandle,
    pub executor: ExecutorHandle,
    pub replicator: ReplicatorHandle,
}

impl CoreHandles {
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
        let timer = TimerHandle::new(watch_dog.clone(), timeout);
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
            timer,
            term_store,
            initiator,
            log_store,
            executor,
            replicator,
        }
    }

    pub async fn send_heartbeat(&self) {
        self.timer.send_heartbeat().await;
    }

    pub async fn append_entries_api(&self) {
        self.timer.send_heartbeat().await;
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

        let core = CoreHandles::new(
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
