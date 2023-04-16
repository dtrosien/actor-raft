use crate::actors::election::initiator::InitiatorHandle;
use crate::actors::log::executor::{App, ExecutorHandle};
use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::replicator::ReplicatorHandle;
use crate::actors::log::replication::worker::StateMeta;
use crate::actors::log::test_utils::TestApp;
use crate::actors::state_store::StateStoreHandle;
use crate::actors::term_store::TermStoreHandle;
use crate::actors::timer::TimerHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Config;
use std::time::Duration;

pub struct Raft {
    state_store: StateStoreHandle,
    watchdog: WatchdogHandle,
    term_store: TermStoreHandle,
    core: CoreHandles,
}

impl Raft {
    pub async fn build() -> Self {
        let state_store = StateStoreHandle::new();
        let watchdog = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(watchdog.clone(), "databases/term_db".to_string());
        let core = create_actors(watchdog.clone(), term_store.clone()).await;
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

async fn create_actors(watchdog: WatchdogHandle, term_store: TermStoreHandle) -> CoreHandles {
    let state_meta = StateMeta::new(term_store.get_term().await); // todo check if this small meta init is possible

    CoreHandles::new(
        watchdog,
        Config::default(),
        Box::new(TestApp {}),
        term_store,
        state_meta,
    )
    // match self.state {
    //     State::Leader => ApiStruct {  },
    //     State::Follower => ApiStruct {  },
    //     State::Candidate => ApiStruct {  },
    // }
}

#[derive(Clone)]
pub struct CoreHandles {
    // todo make attributes private when it is clear which funcs aare needed in server
    pub timer: TimerHandle,
    pub term_store: TermStoreHandle,
    // pub counter: CounterHandle, todo is initialized in Initiator, maybe better here?
    pub initiator: InitiatorHandle,
    pub log_store: LogStoreHandle,
    pub executor: ExecutorHandle,
    pub replicator: ReplicatorHandle,
}

impl CoreHandles {
    fn new(
        watch_dog: WatchdogHandle,
        config: Config,
        app: Box<dyn App>,
        term_store: TermStoreHandle,
        state_meta: StateMeta,
    ) -> Self {
        let timeout = Duration::from_millis(2);
        let timer = TimerHandle::new(watch_dog.clone(), timeout);
        let initiator = InitiatorHandle::new(
            term_store.clone(),
            watch_dog,
            config.clone(),
            "databases/vote_db".to_string(),
        );
        let log_store = LogStoreHandle::new("databases/log_db".to_string());
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
        let state_meta = StateMeta {
            previous_log_index: 0, // todo why couldnt this be set to zero inside actor
            previous_log_term: 0,  // todo why couldnt this be set to zero inside actor
            term: 0,
            leader_id: 0,
            leader_commit: 0, // todo why couldnt this be set to zero inside actor
        };

        let core = CoreHandles::new(
            wd,
            Config::default(),
            Box::new(TestApp {}),
            term_store,
            state_meta,
        );
    }
}
