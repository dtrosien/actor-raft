use crate::raft_server::actors::timer::TimerHandle;
use crate::raft_server::actors::watchdog::WatchdogHandle;
use rand::Rng;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct Counter {
    receiver: mpsc::Receiver<CounterMsg>,
    watchdog: WatchdogHandle,
    votes_received: u64,
    votes_required: u64,
    election_timer: Option<TimerHandle>,
    election_timeout_range: (u64, u64),
}

#[derive(Debug)]
enum CounterMsg {
    RegisterVote { vote: Option<bool> },
    GetVotesReceived { respond_to: oneshot::Sender<u64> },
    ResetVotesReceived { respond_to: oneshot::Sender<()> },
    StartElectionTimer { respond_to: oneshot::Sender<()> },
}

impl Counter {
    #[tracing::instrument(ret, level = "debug")]
    fn new(
        receiver: mpsc::Receiver<CounterMsg>,
        watchdog: WatchdogHandle,
        votes_required: u64,
        election_timeout_range: (u64, u64),
    ) -> Self {
        Counter {
            receiver,
            watchdog,
            election_timeout_range,
            votes_received: 0,
            votes_required,
            election_timer: None,
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg).await;
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn handle_message(&mut self, msg: CounterMsg) {
        match msg {
            CounterMsg::RegisterVote { vote } => self.register_vote(vote).await,
            CounterMsg::GetVotesReceived { respond_to } => {
                let _ = respond_to.send(self.votes_received);
            }
            CounterMsg::ResetVotesReceived { respond_to } => {
                let _ = {
                    self.votes_received = 0;
                    respond_to.send(())
                };
            }
            CounterMsg::StartElectionTimer { respond_to } => {
                let _ = {
                    self.start_election_timer().await;
                    respond_to.send(())
                };
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn start_election_timer(&mut self) {
        let election_timeout = Duration::from_millis(
            rand::thread_rng()
                .gen_range(self.election_timeout_range.0..self.election_timeout_range.1),
        );
        self.election_timer = Some(TimerHandle::run_once(
            self.watchdog.clone(),
            election_timeout,
        ));
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn register_vote(&mut self, vote: Option<bool>) {
        if let Some(vote) = vote {
            if vote {
                self.votes_received += 1;
                self.check_if_won().await;
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    async fn check_if_won(&mut self) {
        if self.votes_received.ge(&self.votes_required) {
            match self.election_timer.take() {
                None => {}
                Some(timer) => {
                    timer.stop_timer().await;
                }
            }
            self.watchdog.election_won().await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct CounterHandle {
    sender: mpsc::Sender<CounterMsg>,
}

impl CounterHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(
        watchdog: WatchdogHandle,
        votes_required: u64,
        election_timeout: (u64, u64),
    ) -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut counter = Counter::new(receiver, watchdog, votes_required, election_timeout);
        tokio::spawn(async move { counter.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn register_vote(&self, vote: Option<bool>) {
        let msg = CounterMsg::RegisterVote { vote };
        let _ = self.sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_votes_received(&self) -> u64 {
        let (send, recv) = oneshot::channel();
        let msg = CounterMsg::GetVotesReceived { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn reset_votes_received(&self) {
        let (send, recv) = oneshot::channel();
        let msg = CounterMsg::ResetVotesReceived { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn start_election_timer(&self) {
        let (send, recv) = oneshot::channel();
        let msg = CounterMsg::StartElectionTimer { respond_to: send };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

//exclude candidate (-> insert only number of other nodes)
#[tracing::instrument(ret, level = "debug")]
pub fn calculate_required_votes(nodes_num: u64) -> u64 {
    if nodes_num % 2 == 0 {
        nodes_num / 2
    } else {
        (nodes_num + 1) / 2
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
        let election_timeout = (150, 300);
        let counter = CounterHandle::new(watchdog, votes_required, election_timeout);
        let vote = Some(true);

        assert_eq!(counter.get_votes_received().await, 0);
        counter.register_vote(vote).await;
        assert_eq!(counter.get_votes_received().await, 1);
    }

    #[tokio::test]
    async fn reset_votes_received_test() {
        let watchdog = WatchdogHandle::default();
        let votes_required: u64 = 3;
        let election_timeout = (150, 300);
        let counter = CounterHandle::new(watchdog, votes_required, election_timeout);
        let vote = Some(true);

        assert_eq!(counter.get_votes_received().await, 0);
        counter.register_vote(vote).await;
        counter.register_vote(vote).await;
        assert_eq!(counter.get_votes_received().await, 2);

        counter.reset_votes_received().await;
        assert_eq!(counter.get_votes_received().await, 0);
    }

    #[tokio::test]
    async fn election_won_test() {
        let watchdog = WatchdogHandle::default();
        let votes_required: u64 = 2;
        let election_timeout = (150, 300);
        let counter = CounterHandle::new(watchdog.clone(), votes_required, election_timeout);
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

    #[tokio::test]
    async fn election_timeout_test() {
        let watchdog = WatchdogHandle::default();
        let votes_required: u64 = 2;
        let election_timeout = (50, 80);
        let counter = CounterHandle::new(watchdog.clone(), votes_required, election_timeout);

        counter.start_election_timer().await;
        let mut signal = watchdog.get_exit_receiver().await;
        tokio::select! {
        _ = signal.recv() => {},
        _ = tokio::time::sleep(Duration::from_millis(100))=> {panic!()}
        }
    }
    

    #[tokio::test]
    async fn calculate_required_votes_test() {
        // only one server in total
        assert_eq!(calculate_required_votes(0), 0);
        // two servers total
        assert_eq!(calculate_required_votes(1), 1);
        // even number of other servers
        assert_eq!(calculate_required_votes(2), 1);
        assert_eq!(calculate_required_votes(10), 5);
        //odd number of other servers
        assert_eq!(calculate_required_votes(9), 5);
        assert_eq!(calculate_required_votes(11), 6);
    }
}
