use crate::actors::watchdog::WatchdogHandle;
use std::time::Duration;
use tokio::sync::mpsc;

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
                break
            }
            }
        }
    }

    fn handle_message(&mut self, msg: TimerMsg) {
        match msg {
            TimerMsg::Heartbeat => {
                println!("heartbeat")
            }
        }
    }
}

#[derive(Clone)]
pub struct TimerHandle {
    sender: mpsc::Sender<TimerMsg>,
}

impl TimerHandle {
    pub fn new(watchdog: WatchdogHandle, timeout: Duration) -> Self {
        let (sender, receiver) = mpsc::channel(1);
        let mut timer = Timer::new(receiver, watchdog, timeout);

        tokio::spawn(async move { timer.run().await });

        Self { sender }
    }

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
        let timer = TimerHandle::new(watchdog.clone(), Duration::from_millis(10));
        for n in 0..9 {
            timer.send_heartbeat().await;
            tokio::time::sleep(Duration::from_millis(n)).await;
            assert_eq!(watchdog.get_shutdown_sig().await.len(), 0);
        }
    }
    #[tokio::test]
    async fn timeout_test() {
        // asserts if the shutdown signal is send from the watchdog after a timeout
        let watchdog = WatchdogHandle::default();
        let _timer = TimerHandle::new(watchdog.clone(), Duration::from_millis(10));
        tokio::time::sleep(Duration::from_millis(11)).await;
        assert_eq!(watchdog.get_shutdown_sig().await.len(), 1);
    }
}
