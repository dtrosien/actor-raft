use crate::actors::state::{ServerState, StateHandle};
use tokio::sync::{broadcast, mpsc, oneshot};

struct Watchdog {
    receiver: mpsc::Receiver<WatchdogMsg>,
    exit_sender: broadcast::Sender<()>,
    state_handle: StateHandle,
}

#[derive(Debug)]
enum WatchdogMsg {
    GetExitReceiver {
        respond_to: oneshot::Sender<broadcast::Receiver<()>>,
    },
    GetStateHandle {
        respond_to: oneshot::Sender<StateHandle>,
    },
    ExitState,
}

impl Watchdog {
    fn new(receiver: mpsc::Receiver<WatchdogMsg>, state_handle: StateHandle) -> Self {
        let (exit_sender, _) = broadcast::channel(8);
        Watchdog {
            receiver,
            exit_sender,
            state_handle,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: WatchdogMsg) {
        match msg {
            WatchdogMsg::GetExitReceiver { respond_to } => {
                // let _ ignores errors when sending
                let _ = respond_to.send(self.exit_sender.subscribe());
            }
            WatchdogMsg::GetStateHandle { respond_to } => {
                let _ = respond_to.send(self.state_handle.clone());
            }
            WatchdogMsg::ExitState => {
                let _ = self.exit_sender.send(());
                self.state_handle.change_state(ServerState::Candidate).await;
            }
        }
    }
}

#[derive(Clone)]
pub struct WatchdogHandle {
    sender: mpsc::Sender<WatchdogMsg>,
}

impl WatchdogHandle {
    pub fn new(state: StateHandle) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut watchdog = Watchdog::new(receiver, state);

        tokio::spawn(async move { watchdog.run().await });

        Self { sender }
    }

    pub async fn get_shutdown_sig(&self) -> broadcast::Receiver<()> {
        let (send, recv) = oneshot::channel();
        let msg = WatchdogMsg::GetExitReceiver { respond_to: send };
        
        let _ = self.sender.send(msg).await;
        recv.await.expect("watchdog task has been killed")
    }

    pub async fn timeout(&self) {
        println!("Watchdog got timeout signal");
        let msg = WatchdogMsg::ExitState;
        self.sender
            .send(msg)
            .await
            .expect("watchdog task has been killed");
    }

    pub async fn get_state_handle(&self) -> StateHandle {
        let (send, recv) = oneshot::channel();
        let msg = WatchdogMsg::GetStateHandle { respond_to: send };
        
        let _ = self.sender.send(msg).await;
        recv.await.expect("watchdog task has been killed")
    }
}

impl Default for WatchdogHandle {
    fn default() -> Self {
        let state_handle = StateHandle::new();
        WatchdogHandle::new(state_handle)
    }
}