use crate::raft_server::actors::state_store::StateStoreHandle;
use crate::raft_server::raft_node::ServerState;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::info;

#[derive(Debug)]
struct Watchdog {
    receiver: mpsc::Receiver<WatchdogMsg>,
    exit_sender: broadcast::Sender<()>,
    state_store: StateStoreHandle,
}

#[derive(Debug)]
enum WatchdogMsg {
    GetExitReceiver {
        respond_to: oneshot::Sender<broadcast::Receiver<()>>,
    },
    GetStateStoreHandle {
        respond_to: oneshot::Sender<StateStoreHandle>,
    },
    Timeout,
    TermError,
    ElectionWon,
}

impl Watchdog {
    #[tracing::instrument(ret, level = "debug")]
    fn new(receiver: mpsc::Receiver<WatchdogMsg>, state_store: StateStoreHandle) -> Self {
        let (exit_sender, _) = broadcast::channel(8);
        Watchdog {
            receiver,
            exit_sender,
            state_store,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: WatchdogMsg) {
        match msg {
            WatchdogMsg::GetExitReceiver { respond_to } => {
                let _ = respond_to.send(self.exit_sender.subscribe());
            }
            WatchdogMsg::GetStateStoreHandle { respond_to } => {
                let _ = respond_to.send(self.state_store.clone());
            }
            WatchdogMsg::Timeout => {
                self.state_store.change_state(ServerState::Candidate).await;
                let _ = self.exit_sender.send(());
            }
            WatchdogMsg::TermError => {
                self.state_store.change_state(ServerState::Follower).await;
                let _ = self.exit_sender.send(());
            }
            WatchdogMsg::ElectionWon => {
                self.state_store.change_state(ServerState::Leader).await;
                let _ = self.exit_sender.send(());
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct WatchdogHandle {
    sender: mpsc::Sender<WatchdogMsg>,
}

impl WatchdogHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(state_store: StateStoreHandle) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut watchdog = Watchdog::new(receiver, state_store);

        tokio::spawn(async move { watchdog.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_exit_receiver(&self) -> broadcast::Receiver<()> {
        let (send, recv) = oneshot::channel();
        let msg = WatchdogMsg::GetExitReceiver { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("watchdog task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn timeout(&self) {
        info!("Watchdog got timeout signal");
        let msg = WatchdogMsg::Timeout;
        self.sender
            .send(msg)
            .await
            .expect("watchdog task has been killed");
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn term_error(&self) {
        info!("Watchdog got term error signal");
        let msg = WatchdogMsg::TermError;
        self.sender
            .send(msg)
            .await
            .expect("watchdog task has been killed");
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn election_won(&self) {
        info!("Watchdog got election won signal");
        let msg = WatchdogMsg::ElectionWon;
        self.sender
            .send(msg)
            .await
            .expect("watchdog task has been killed");
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_state_store_handle(&self) -> StateStoreHandle {
        let (send, recv) = oneshot::channel();
        let msg = WatchdogMsg::GetStateStoreHandle { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("watchdog task has been killed")
    }
}

impl Default for WatchdogHandle {
    fn default() -> Self {
        let state_handle = StateStoreHandle::default();
        WatchdogHandle::new(state_handle)
    }
}
