use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub enum ServerState {
    Leader,
    Follower,
    Candidate,
}

#[derive(Debug)]
struct StateStore {
    receiver: mpsc::Receiver<StateMsg>,
    state: ServerState,
}

#[derive(Debug)]
enum StateMsg {
    ChangeState {
        state: ServerState,
    },
    GetState {
        respond_to: oneshot::Sender<ServerState>,
    },
}

impl StateStore {
    fn new(receiver: mpsc::Receiver<StateMsg>) -> Self {
        StateStore {
            receiver,
            state: ServerState::Follower, // todo read from config
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    fn handle_message(&mut self, msg: StateMsg) {
        match msg {
            StateMsg::ChangeState { state } => {
                self.state = state;
            }
            StateMsg::GetState { respond_to } => {
                respond_to
                    .send(self.state.clone())
                    .expect("Error get state");
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct StateStoreHandle {
    sender: mpsc::Sender<StateMsg>,
}

impl StateStoreHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut state_store = StateStore::new(receiver);

        tokio::spawn(async move { state_store.run().await });

        Self { sender }
    }

    pub async fn change_state(&self, state: ServerState) {
        let msg = StateMsg::ChangeState { state };
        let _ = self.sender.send(msg).await;
    }

    pub async fn get_state(&self) -> ServerState {
        let (send, recv) = oneshot::channel();
        let msg = StateMsg::GetState { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("state task has been killed")
    }
}

impl Default for StateStoreHandle {
    fn default() -> Self {
        StateStoreHandle::new()
    }
}
