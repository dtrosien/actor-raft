use crate::raft_server::raft_node::ServerState;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct StateStore {
    receiver: mpsc::Receiver<StateMsg>,
    state: ServerState,
    leader_id: Option<u64>, // todo [idea] maybe store somewhere else
}

#[derive(Debug)]
enum StateMsg {
    ChangeState {
        state: ServerState,
    },
    GetState {
        respond_to: oneshot::Sender<ServerState>,
    },
    GetLeaderId {
        respond_to: oneshot::Sender<Option<u64>>,
    },
    SetLeaderId {
        respond_to: oneshot::Sender<()>,
        leader_id: Option<u64>,
    },
}

impl StateStore {
    fn new(receiver: mpsc::Receiver<StateMsg>, init_state: ServerState) -> Self {
        StateStore {
            receiver,
            state: init_state,
            leader_id: None,
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
                self.leader_id = None;
                self.state = state;
            }
            StateMsg::GetState { respond_to } => {
                respond_to
                    .send(self.state.clone())
                    .expect("Error get state");
            }
            StateMsg::GetLeaderId { respond_to } => {
                respond_to
                    .send(self.leader_id)
                    .expect("Error get leader id");
            }
            StateMsg::SetLeaderId {
                respond_to,
                leader_id,
            } => {
                self.leader_id = leader_id;
                respond_to.send(()).expect("Error set leader id");
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct StateStoreHandle {
    sender: mpsc::Sender<StateMsg>,
}

impl StateStoreHandle {
    pub fn new(init_state: ServerState) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut state_store = StateStore::new(receiver, init_state);

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

    pub async fn get_leader_id(&self) -> Option<u64> {
        let (send, recv) = oneshot::channel();
        let msg = StateMsg::GetLeaderId { respond_to: send };
        let _ = self.sender.send(msg).await;
        recv.await.expect("state task has been killed")
    }

    pub async fn set_leader_id(&self, leader_id: Option<u64>) {
        let (send, recv) = oneshot::channel();
        let msg = StateMsg::SetLeaderId {
            respond_to: send,
            leader_id,
        };
        let _ = self.sender.send(msg).await;
        recv.await.expect("state task has been killed")
    }
}

impl Default for StateStoreHandle {
    fn default() -> Self {
        StateStoreHandle::new(ServerState::Follower)
    }
}
