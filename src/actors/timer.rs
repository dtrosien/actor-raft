use crate::actors::watchdog::WatchdogHandle;
use std::time::Duration;
use tokio::sync::mpsc;

#[derive(Debug)]
struct Timer {
    receiver: mpsc::Receiver<TimerMsg>,
    watchdog: WatchdogHandle,
    timeout: Duration,
}

#[derive(Debug)]
enum TimerMsg {
    Heartbeat,
}

impl Timer {
    #[tracing::instrument(ret, level = "debug")]
    fn new(
        receiver: mpsc::Receiver<TimerMsg>,
        watchdog: WatchdogHandle,
        timeout: Duration,
    ) -> Self {
        Timer {
            receiver,
            watchdog,
            timeout,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
            Some(msg) = self.receiver.recv() => {
            self.handle_message(msg);
            },
            _timeout = tokio::time::sleep(self.timeout)=> {
                println!("timeout");
               self.watchdog.timeout().await;
                    //todo break really needed?
                    //break
            }
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    fn handle_message(&mut self, msg: TimerMsg) {
        match msg {
            TimerMsg::Heartbeat => {
                println!("heartbeat")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TimerHandle {
    sender: mpsc::Sender<TimerMsg>,
}

impl TimerHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(watchdog: WatchdogHandle, timeout: Duration) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let mut timer = Timer::new(receiver, watchdog, timeout);

        tokio::spawn(async move { timer.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn send_heartbeat(&self) {
        let msg = TimerMsg::Heartbeat;
        let _ = self.sender.send(msg).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn heartbeat_test() {
        // since tokio only supports granularity on ms base, the test is expected only to pass up to 9ms sleep time

        let watchdog = WatchdogHandle::default();
        // was set from 10 to 20 since testing was to heavy
        let timer = TimerHandle::new(watchdog.clone(), Duration::from_millis(20));
        let mut signal = watchdog.get_exit_receiver().await;
        for n in 0..9 {
            timer.send_heartbeat().await;
            tokio::select! {
            _ = signal.recv() => {panic!()},
            _ = tokio::time::sleep(Duration::from_millis(n))=> {}
            }
        }
    }

    #[tokio::test]
    async fn timeout_test() {
        // asserts if the shutdown signal is send from the watchdog after a timeout
        let watchdog = WatchdogHandle::default();
        let _timer = TimerHandle::new(watchdog.clone(), Duration::from_millis(10));
        let mut signal = watchdog.get_exit_receiver().await;
        tokio::select! {
        _ = signal.recv() => {},
        _ = tokio::time::sleep(Duration::from_millis(20))=> {panic!()}
        }
    }
}
