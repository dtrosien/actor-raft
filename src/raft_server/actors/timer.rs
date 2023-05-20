use crate::raft_server::actors::watchdog::WatchdogHandle;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::info;

#[derive(Debug)]
struct Timer {
    hb_receiver: mpsc::Receiver<HeartbeatMsg>,
    stop_receiver: mpsc::Receiver<StopMsg>,
    watchdog: WatchdogHandle,
    timeout: Duration,
    run_once: bool,
}

#[derive(Debug)]
enum HeartbeatMsg {
    Heartbeat,
}

#[derive(Debug)]
enum StopMsg {
    Stop,
}

impl Timer {
    #[tracing::instrument(ret, level = "debug")]
    fn new(
        hb_receiver: mpsc::Receiver<HeartbeatMsg>,
        stop_receiver: mpsc::Receiver<StopMsg>,
        watchdog: WatchdogHandle,
        timeout: Duration,
        run_once: bool,
    ) -> Self {
        Timer {
            hb_receiver,
            stop_receiver,
            watchdog,
            timeout,
            run_once,
        }
    }

    async fn run(&mut self) {
        loop {
            tokio::select! {
            Some(msg) = self.hb_receiver.recv() => {
            self.handle_heartbeat_message(msg);
            },
            Some(_msg) = self.stop_receiver.recv() => {
                  info!("timer stopped");
                   break;
            },
            _timeout = tokio::time::sleep(self.timeout)=> {
               self.watchdog.timeout().await;
                 if self.run_once {info!("run once timeout");
                        break;}
                    else {info!("continuous timeout");}
            }
            }
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    fn handle_heartbeat_message(&mut self, msg: HeartbeatMsg) {
        match msg {
            HeartbeatMsg::Heartbeat => {
                info!(" heartbeat registered")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TimerHandle {
    hb_sender: mpsc::Sender<HeartbeatMsg>,
    stop_sender: mpsc::Sender<StopMsg>,
}

impl TimerHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(watchdog: WatchdogHandle, timeout: Duration) -> Self {
        let (hb_sender, hb_receiver) = mpsc::channel(1);
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        let mut timer = Timer::new(hb_receiver, stop_receiver, watchdog, timeout, false);

        tokio::spawn(async move { timer.run().await });

        Self {
            hb_sender,
            stop_sender,
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub fn run_once(watchdog: WatchdogHandle, timeout: Duration) -> Self {
        let (hb_sender, hb_receiver) = mpsc::channel(1);
        let (stop_sender, stop_receiver) = mpsc::channel(1);
        let mut timer = Timer::new(hb_receiver, stop_receiver, watchdog, timeout, true);
        tokio::spawn(async move { timer.run().await });
        Self {
            hb_sender,
            stop_sender,
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn register_heartbeat(&self) {
        let msg = HeartbeatMsg::Heartbeat;
        let _ = self.hb_sender.send(msg).await;
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn stop_timer(&self) {
        let msg = StopMsg::Stop;
        let _ = self.stop_sender.send(msg).await;
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
            timer.register_heartbeat().await;
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

    #[tokio::test]
    async fn run_once_timeout_test() {
        // asserts if the shutdown signal is send from the watchdog after a timeout
        let watchdog = WatchdogHandle::default();
        TimerHandle::run_once(watchdog.clone(), Duration::from_millis(10));
        let mut signal = watchdog.get_exit_receiver().await;
        tokio::select! {
        _ = signal.recv() => {},
        _ = tokio::time::sleep(Duration::from_millis(20))=> {panic!()}
        }
    }

    #[tokio::test]
    async fn stop_timer_test() {
        // asserts if the shutdown signal is send from the watchdog after a timeout
        let watchdog = WatchdogHandle::default();
        let timer = TimerHandle::new(watchdog.clone(), Duration::from_millis(5));
        let mut signal = watchdog.get_exit_receiver().await;
        timer.stop_timer().await;
        tokio::select! {
        _ = signal.recv() => {panic!()},
        _ = tokio::time::sleep(Duration::from_millis(10))=> {}
        }
    }
}
