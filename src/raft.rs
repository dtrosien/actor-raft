use crate::actors::state::StateHandle;
use crate::actors::timer::TimerHandle;
use crate::actors::watchdog::WatchdogHandle;
use std::time::Duration;

pub struct Raft {
    state: StateHandle,
    watchdog: WatchdogHandle,
    core: CoreHandles,
}

impl Raft {
    pub fn build() -> Self {
        let state = StateHandle::new();
        let watchdog = WatchdogHandle::new(state.clone());
        let core = create_actors(watchdog.clone());
        Raft {
            state,
            watchdog,
            core,
        }
    }

    pub fn get_handles(&self) -> CoreHandles {
        self.core.clone()
    }

    pub async fn run(&mut self) {
        let mut exit_state_r = self.watchdog.get_shutdown_sig().await;
        println!("{:?}", self.state.get_state().await);

        exit_state_r.recv().await.expect("TODO: panic message");
        println!("{:?}", self.state.get_state().await);
    }

    pub async fn run_continuously(&mut self) {
        //todo introduce complete shutdown ... shutdown should be renamed to exit/shutdown current state
        loop {
            self.run().await;
        }
    }
}

fn create_actors(watchdog: WatchdogHandle) -> CoreHandles {
    CoreHandles::new(watchdog)
    // match self.state {
    //     State::Leader => ApiStruct {  },
    //     State::Follower => ApiStruct {  },
    //     State::Candidate => ApiStruct {  },
    // }
}

#[derive(Clone)]
pub struct CoreHandles {
    timer: TimerHandle,
}

impl CoreHandles {
    fn new(watch_dog: WatchdogHandle) -> Self {
        let timeout = Duration::from_millis(2);
        let timer = TimerHandle::new(watch_dog, timeout);
        Self { timer }
    }

    pub async fn send_heartbeat(&self) {
        self.timer.send_heartbeat().await;
    }

    pub async fn append_entries_api(&self) {
        self.timer.send_heartbeat().await;
    }

    pub fn request_vote_api(&self) {}
}