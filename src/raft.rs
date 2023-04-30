use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::log::replication::worker::ReplicatorStateMeta;
use crate::actors::log::test_utils::TestApp;
use crate::actors::state_store::StateStoreHandle;
use crate::actors::term_store::TermStoreHandle;
use crate::actors::watchdog::WatchdogHandle;
use crate::config::Config;
use crate::raft_handles::RaftHandles;

#[derive(Debug)]
pub struct Raft {
    state_store: StateStoreHandle,
    watchdog: WatchdogHandle,
    term_store: TermStoreHandle,
    handles: RaftHandles,
}

impl Raft {
    pub async fn build(config: Config) -> Self {
        let state_store = StateStoreHandle::new();
        let watchdog = WatchdogHandle::new(state_store.clone());
        let term_store = TermStoreHandle::new(watchdog.clone(), config.term_db_path.clone());
        let handles = create_actors(
            config,
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
        }
    }

    pub fn get_handles(&self) -> RaftHandles {
        self.handles.clone()
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
