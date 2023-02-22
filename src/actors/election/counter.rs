use crate::actors::watchdog::WatchdogHandle;
use tokio::sync::mpsc;

struct Counter {
    receiver: mpsc::Receiver<CounterMsg>,
    watchdog: WatchdogHandle,
    votes_received: u64,
    votes_required: u64,
}

enum CounterMsg {
    RegisterVote { vote: Option<bool> },
}

impl Counter {
    async fn new(
        receiver: mpsc::Receiver<CounterMsg>,
        watchdog: WatchdogHandle,
        votes_required: u64,
    ) -> Self {
        Counter {
            receiver,
            watchdog,
            votes_received: 0,
            votes_required,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    async fn handle_message(&mut self, msg: CounterMsg) {
        match msg {
            CounterMsg::RegisterVote { vote } => {}
        }
    }

    async fn register_vote(&self) {
        //todo implement
    }
}

#[derive(Clone)]
pub struct CounterHandle {
    sender: mpsc::Sender<CounterMsg>,
}

impl CounterHandle {
    pub async fn new(watchdog: WatchdogHandle, votes_required: u64) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut counter = Counter::new(receiver, watchdog, votes_required).await;
        tokio::spawn(async move { counter.run().await });

        Self { sender }
    }

    pub async fn register_vote(&self, vote: Option<bool>) {
        let msg = CounterMsg::RegisterVote { vote };
        let _ = self.sender.send(msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_vote_test() {
        let watchdog = WatchdogHandle::default();
        let votes_required: u64 = 3;
        let counter = CounterHandle::new(watchdog, votes_required).await;
        let vote = Some(true);
        counter.register_vote(vote).await;
        //todo write test
    }
}
