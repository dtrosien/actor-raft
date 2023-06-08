use crate::raft_server_rpc::raft_server_rpc_client::RaftServerRpcClient;
use crate::raft_server_rpc::{AppendEntriesRequest, RequestVoteRequest};
use std::error::Error;
use std::time::Duration;
use tonic::transport::Channel;
use tracing::info;

#[derive(Clone, Debug)]
pub struct Reply {
    pub term: u64,
    pub success_or_granted: bool,
}

#[derive(Debug, Clone)]
pub struct NodeClient {
    client: RaftServerRpcClient<Channel>,
}

impl NodeClient {
    #[tracing::instrument(ret, level = "debug")]
    pub async fn build(ip: String, port: u16) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let uri = format!("https://{ip}:{port}");

        let channel = Channel::builder(uri.parse()?)
            .connect_timeout(Duration::from_millis(100))
            .connect()
            .await?;
        let client = RaftServerRpcClient::new(channel);
        Ok(NodeClient { client })
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn request_vote(
        &mut self,
        vote_request: RequestVoteRequest,
    ) -> Result<Reply, Box<dyn Error + Send + Sync>> {
        let request = tonic::Request::new(vote_request);
        let response = self.client.request_votes(request).await?;
        let response_arguments = response.into_inner();
        info!("vote granted: {}", response_arguments.vote_granted);
        Ok(Reply {
            term: response_arguments.term,
            success_or_granted: response_arguments.vote_granted,
        })
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn append_entry(
        &mut self,
        append_entry_request: AppendEntriesRequest,
    ) -> Result<Reply, Box<dyn Error + Send + Sync>> {
        let request = tonic::Request::new(append_entry_request);
        let response = self.client.append_entries(request).await?;
        let response_arguments = response.into_inner();
        info!("replicated: {}", response_arguments.success);

        Ok(Reply {
            term: response_arguments.term,
            success_or_granted: response_arguments.success,
        })
    }
}

// deprecated
// #[tracing::instrument(ret, level = "debug")]
// pub async fn request_vote(
//     uri: String,
//     vote_request: RequestVoteRequest,
// ) -> Result<Reply, Box<dyn Error + Send + Sync>> {
//     let channel = Channel::builder(uri.parse()?)
//         .connect_timeout(Duration::from_millis(2))
//         .connect()
//         .await?;
//
//     let request = tonic::Request::new(vote_request);
//     //let timeout_channel = Timeout::new(channel, Duration::from_secs(10));
//     let mut client = RaftServerRpcClient::new(channel);
//
//     let response = client.request_votes(request).await?;
//     let response_arguments = response.into_inner();
//     info!("vote granted: {}", response_arguments.vote_granted);
//     Ok(Reply {
//         term: response_arguments.term,
//         success_or_granted: response_arguments.vote_granted,
//     })
// }

// deprecated
// #[tracing::instrument(ret, level = "debug")]
// pub async fn append_entry(
//     uri: String,
//     append_entry_request: AppendEntriesRequest,
// ) -> Result<Reply, Box<dyn Error + Send + Sync>> {
//     let channel = Channel::builder(uri.parse()?)
//         .connect_timeout(Duration::from_millis(2))
//         .connect()
//         .await?;
//
//     let request = tonic::Request::new(append_entry_request);
//     //let timeout_channel = Timeout::new(channel, Duration::from_secs(10));
//     let mut client = RaftServerRpcClient::new(channel);
//
//     let response = client.append_entries(request).await?;
//     let response_arguments = response.into_inner();
//     info!("replicated: {}", response_arguments.success);
//
//     Ok(Reply {
//         term: response_arguments.term,
//         success_or_granted: response_arguments.success,
//     })
// }

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::{Request, Response, Status};

    use crate::raft_server::rpc::utils::test::{
        get_test_port, start_test_request, start_test_server, TestServerFalse, TestServerTrue,
    };
    use crate::raft_server_rpc::{AppendEntriesReply, RequestVoteReply, RequestVoteRequest};

    #[tokio::test]
    async fn request_vote_true_test() {
        let vote_request = RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
        // ports need to be different for each test case since async
        let port = get_test_port().await;
        let ip = format!("[::1]");

        let serve_future = async { start_test_server(port, TestServerTrue {}).await };
        let request_future = async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let client = NodeClient::build(ip, port).await.unwrap();
            client.clone().request_vote(vote_request.clone()).await
        };

        tokio::select! {
                _ = serve_future => panic!("server returned first"),
                r = request_future => {
                    match r {
                        Ok(r) => {
                                    assert!(r.success_or_granted);
                                    assert_eq!(r.term,1)
                    },
                        _ => panic!("must not return Error")
                    }
            }
        }
    }

    #[tokio::test]
    async fn request_vote_false_test() {
        let vote_request = RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
        // ports need to be different for each test case since async
        let port = get_test_port().await;
        let ip = format!("[::1]");

        let serve_future = async { start_test_server(port, TestServerFalse {}).await };
        let request_future = async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let client = NodeClient::build(ip, port).await.unwrap();
            client.clone().request_vote(vote_request.clone()).await
        };

        tokio::select! {
                _ = serve_future => panic!("server returned first"),
                r = request_future => {
                    match r {
                        Ok(r) => {
                                    assert!(!r.success_or_granted);
                                    assert_eq!(r.term,1)
                    },
                        _ => panic!("must not return Error")
                    }
            }
        }
    }

    #[tokio::test]
    async fn request_vote_error_test() {
        let vote_request = RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
        // ports need to be different for each test case since async
        let port = get_test_port().await;
        let ip = format!("wrong");

        let serve_future = async { start_test_server(port, TestServerFalse {}).await };
        let request_future = async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            NodeClient::build(ip, port).await
        };

        tokio::select! {
                _ = serve_future => panic!("server returned first"),
                r = request_future => {
                    if let Ok(_r) = r {panic!("must return Error")
                    }
            }
        }
    }

    #[tokio::test]
    async fn append_entries_true_test() {
        let append_entries_request = AppendEntriesRequest {
            term: 0,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        // ports need to be different for each test case since async
        let port = get_test_port().await;
        let ip = format!("[::1]");

        let serve_future = async { start_test_server(port, TestServerTrue {}).await };
        let request_future = async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let client = NodeClient::build(ip, port).await.unwrap();
            client
                .clone()
                .append_entry(append_entries_request.clone())
                .await
        };

        tokio::select! {
                _ = serve_future => panic!("server returned first"),
                r = request_future => {
                    match r {
                        Ok(r) => {
                                    assert!(r.success_or_granted);
                                    assert_eq!(r.term,1)
                    },
                        _ => panic!("must not return Error")
                    }
            }
        }
    }

    #[tokio::test]
    async fn append_entries_false_test() {
        let append_entries_request = AppendEntriesRequest {
            term: 0,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        // ports need to be different for each test case since async
        let port = get_test_port().await;
        let ip = format!("[::1]");

        let serve_future = async { start_test_server(port, TestServerFalse {}).await };
        let request_future = async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            let client = NodeClient::build(ip, port).await.unwrap();
            client
                .clone()
                .append_entry(append_entries_request.clone())
                .await
        };

        tokio::select! {
                _ = serve_future => panic!("server returned first"),
                r = request_future => {
                    match r {
                        Ok(r) => {
                                    assert!(!r.success_or_granted);
                                    assert_eq!(r.term,1)
                    },
                        _ => panic!("must not return Error")
                    }
            }
        }
    }

    #[tokio::test]
    async fn append_entries_error_test() {
        let append_entries_request = AppendEntriesRequest {
            term: 0,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![],
            leader_commit: 0,
        };
        // ports need to be different for each test case since async
        let port = get_test_port().await;
        let ip = format!("wrong");

        let serve_future = async { start_test_server(port, TestServerFalse {}).await };
        let request_future = async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            NodeClient::build(ip, port).await
        };

        tokio::select! {
                _ = serve_future => panic!("server returned first"),
                r = request_future => {
                    if let Ok(_r) = r {panic!("must return Error")
                    }
            }
        }
    }
}
