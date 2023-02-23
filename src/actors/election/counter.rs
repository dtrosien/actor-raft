use crate::actors::watchdog::WatchdogHandle;
use tokio::sync::{mpsc, oneshot};

struct Counter {
    receiver: mpsc::Receiver<CounterMsg>,
    watchdog: WatchdogHandle,
    votes_received: u64,
    votes_required: u64,
}

enum CounterMsg {
    RegisterVote { vote: Option<bool> },
    GetVotesReceived { respond_to: oneshot::Sender<u64> },
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
            CounterMsg::RegisterVote { vote } => self.register_vote(vote).await,
            CounterMsg::GetVotesReceived { respond_to } => {
                let _ = respond_to.send(self.votes_received);
            }
        }
    }

    async fn register_vote(&mut self, vote: Option<bool>) {
        if let Some(vote) = vote {
            if vote {
                self.votes_received += 1;
                self.report_to_watchdog().await;
            }
        }
    }

    async fn report_to_watchdog(&self) {
        if self.votes_received.ge(&self.votes_required) {
            self.watchdog.election_won().await;
        }
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

    pub async fn get_votes_received(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = CounterMsg::GetVotesReceived { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn register_vote_test() {
        let watchdog = WatchdogHandle::default();
        let votes_required: u64 = 3;
        let counter = CounterHandle::new(watchdog, votes_required).await;
        let vote = Some(true);

        assert_eq!(counter.get_votes_received().await, 0);
        counter.register_vote(vote).await;
        assert_eq!(counter.get_votes_received().await, 1);
    }

    #[tokio::test]
    async fn election_won_test() {
        let watchdog = WatchdogHandle::default();
        let votes_required: u64 = 2;
        let counter = CounterHandle::new(watchdog.clone(), votes_required).await;
        let vote = Some(true);

        // first vote -> no exit, since not enough votes
        counter.register_vote(vote).await;
        let mut signal = watchdog.get_exit_receiver().await;
        tokio::select! {
        _ = signal.recv() => {panic!()},
        _ = tokio::time::sleep(Duration::from_millis(5))=> {}
        }

        // second vote -> required votes reached -> exit signal for state change must fire
        counter.register_vote(vote).await;
        tokio::select! {
        _ = signal.recv() => {},
        _ = tokio::time::sleep(Duration::from_millis(5))=> {panic!()}
        }
    }
}
