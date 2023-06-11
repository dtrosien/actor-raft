use crate::app::AppResult;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
struct ClientStore {
    receiver: mpsc::Receiver<ClientStoreMsg>,
    results: HashMap<u64, (Option<u64>, Option<AppResult>)>,
}

#[derive(Debug)]
enum ClientStoreMsg {
    GetResult {
        client_id: u64,
        sequence_num: u64,
        respond_to: oneshot::Sender<Option<AppResult>>,
    },
    SetResult {
        client_id: u64,
        sequence_num: u64,
        result: AppResult,
        respond_to: oneshot::Sender<()>,
    },
    AddClient {
        client_id: u64,
        respond_to: oneshot::Sender<()>,
    },
    ClientExist {
        client_id: u64,
        respond_to: oneshot::Sender<bool>,
    },
}

impl ClientStore {
    #[tracing::instrument(ret, level = "debug")]
    fn new(receiver: mpsc::Receiver<ClientStoreMsg>) -> Self {
        ClientStore {
            receiver,
            results: HashMap::default(),
        }
    }

    async fn run(&mut self) {
        while let Some(msg) = self.receiver.recv().await {
            self.handle_message(msg);
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    fn handle_message(&mut self, msg: ClientStoreMsg) {
        match msg {
            ClientStoreMsg::GetResult {
                client_id,
                sequence_num,
                respond_to,
            } => {
                // todo [refactor] ugly code
                let app_result =
                    if let Some((Some(num), Some(result))) = self.results.get(&client_id) {
                        if *num == sequence_num {
                            Some(result.clone())
                        } else {
                            None
                        }
                    } else {
                        None
                    };

                let _ = respond_to.send(app_result);
            }
            ClientStoreMsg::SetResult {
                client_id,
                sequence_num,
                result,
                respond_to,
            } => {
                if self.results.contains_key(&client_id) {
                    self.results
                        .insert(client_id, (Some(sequence_num), Some(result)));
                }
                let _ = respond_to.send(());
            }
            ClientStoreMsg::AddClient {
                client_id,
                respond_to,
            } => {
                self.results.insert(client_id, (None, None));
                let _ = respond_to.send(());
            }
            ClientStoreMsg::ClientExist {
                client_id,
                respond_to,
            } => {
                let _ = respond_to.send(self.results.contains_key(&client_id));
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ClientStoreHandle {
    sender: mpsc::Sender<ClientStoreMsg>,
}

impl ClientStoreHandle {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mut actor = ClientStore::new(receiver);
        tokio::spawn(async move { actor.run().await });

        Self { sender }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn get_result(&self, client_id: u64, sequence_num: u64) -> Option<AppResult> {
        let (send, recv) = oneshot::channel();
        let msg = ClientStoreMsg::GetResult {
            client_id,
            sequence_num,
            respond_to: send,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn set_result(&self, client_id: u64, sequence_num: u64, result: AppResult) {
        let (send, recv) = oneshot::channel();
        let msg = ClientStoreMsg::SetResult {
            client_id,
            sequence_num,
            result,
            respond_to: send,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn add_client(&self, client_id: u64) {
        let (send, recv) = oneshot::channel();
        let msg = ClientStoreMsg::AddClient {
            client_id,
            respond_to: send,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn client_exist(&self, client_id: u64) -> bool {
        let (send, recv) = oneshot::channel();
        let msg = ClientStoreMsg::ClientExist {
            client_id,
            respond_to: send,
        };

        let _ = self.sender.send(msg).await;
        recv.await.expect("Actor task has been killed")
    }
}

impl Default for ClientStoreHandle {
    fn default() -> Self {
        ClientStoreHandle::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn set_get_result_test() {
        let client_store = ClientStoreHandle::new();

        client_store.add_client(1).await;

        assert!(client_store.client_exist(1).await);

        assert!(client_store.get_result(1, 1).await.is_none());

        let result = AppResult {
            success: true,
            payload: vec![],
        };

        client_store.set_result(1, 1, result.clone()).await;

        assert!(client_store.get_result(1, 1).await.is_some());
        assert!(client_store.get_result(1, 2).await.is_none());

        client_store.set_result(2, 1, result.clone()).await;
        assert!(client_store.get_result(2, 1).await.is_none());

        client_store.set_result(1, 2, result.clone()).await;
        assert!(client_store.get_result(1, 1).await.is_none());
        assert!(client_store.get_result(1, 2).await.is_some());
    }
}
