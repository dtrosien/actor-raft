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
use crate::raft_handles::RaftHandles;
use std::time::Duration;

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
        let term_store = TermStoreHandle::new(watchdog.clone(), config.term_db_path);
        let core = create_actors(
            watchdog.clone(),
            term_store.clone(),
            config.log_db_path,
            config.vote_db_path,
        )
        .await;
        Raft {
            state_store,
            watchdog,
            term_store,
            handles: core,
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
    watchdog: WatchdogHandle,
    term_store: TermStoreHandle,
    log_db_path: String,
    vote_db_path: String,
) -> RaftHandles {
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

    RaftHandles::new(
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
