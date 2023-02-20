use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub enum ServerState {
    Leader,
    Follower,
    Candidate,
}

struct State {
    receiver: mpsc::Receiver<StateMsg>,
    state: ServerState,
}
enum StateMsg {
    ChangeState {
        state: ServerState,
    },
    GetState {
        respond_to: oneshot::Sender<ServerState>,
    },
}

impl State {
    fn new(receiver: mpsc::Receiver<StateMsg>) -> Self {
        State {
            receiver,
            state: ServerState::Follower,
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
pub struct StateHandle {
    sender: mpsc::Sender<StateMsg>,
}

impl StateHandle {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut state = State::new(receiver);

        tokio::spawn(async move { state.run().await });

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

impl Default for StateHandle {
    fn default() -> Self {
        StateHandle::new()
    }
}
