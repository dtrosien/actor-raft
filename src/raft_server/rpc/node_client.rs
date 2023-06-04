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

#[tracing::instrument(ret, level = "debug")]
pub async fn request_vote(
    uri: String,
    vote_request: RequestVoteRequest,
) -> Result<Reply, Box<dyn Error + Send + Sync>> {
    let channel = Channel::builder(uri.parse()?)
        .connect_timeout(Duration::from_millis(2)) // todo  [feature] not needed?
        .connect()
        .await?;

    let request = tonic::Request::new(vote_request);
    //let timeout_channel = Timeout::new(channel, Duration::from_secs(10)); //todo [feature] define timeout for answer?
    let mut client = RaftServerRpcClient::new(channel); // todo [performance] keep client in memory and dont build it each time?

    let response = client.request_votes(request).await?;
    let response_arguments = response.into_inner();
    info!("vote granted: {}", response_arguments.vote_granted);
    Ok(Reply {
        term: response_arguments.term,
        success_or_granted: response_arguments.vote_granted,
    })
}

#[tracing::instrument(ret, level = "debug")]
pub async fn append_entry(
    uri: String,
    append_entry_request: AppendEntriesRequest,
) -> Result<Reply, Box<dyn Error + Send + Sync>> {
    let channel = Channel::builder(uri.parse()?)
        .connect_timeout(Duration::from_millis(2)) // todo [feature] not needed?
        .connect()
        .await?;

    let request = tonic::Request::new(append_entry_request);
    //let timeout_channel = Timeout::new(channel, Duration::from_secs(10)); //todo [feature] define timeout for answer?
    let mut client = RaftServerRpcClient::new(channel);

    let response = client.append_entries(request).await?;
    let response_arguments = response.into_inner();
    info!("replicated: {}", response_arguments.success);

    Ok(Reply {
        term: response_arguments.term,
        success_or_granted: response_arguments.success,
    })
}

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
        let uri = format!("https://[::1]:{port}");

        let serve_future = async { start_test_server(port, TestServerTrue {}).await };
        let request_future =
            async { start_test_request(|| request_vote(uri.clone(), vote_request.clone())).await };

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
        let uri = format!("https://[::1]:{port}");

        let serve_future = async { start_test_server(port, TestServerFalse {}).await };
        let request_future =
            async { start_test_request(|| request_vote(uri.clone(), vote_request.clone())).await };

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
        let uri = "caseMustPanic".to_string();

        let serve_future = async { start_test_server(port, TestServerFalse {}).await };
        let request_future =
            async { start_test_request(|| request_vote(uri.clone(), vote_request.clone())).await };

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
        let uri = format!("https://[::1]:{port}");

        let serve_future = async { start_test_server(port, TestServerTrue {}).await };
        let request_future = async {
            start_test_request(|| append_entry(uri.clone(), append_entries_request.clone())).await
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
        let uri = format!("https://[::1]:{port}");

        let serve_future = async { start_test_server(port, TestServerFalse {}).await };
        let request_future = async {
            start_test_request(|| append_entry(uri.clone(), append_entries_request.clone())).await
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
        let uri = "caseMustPanic".to_string();

        let serve_future = async { start_test_server(port, TestServerFalse {}).await };
        let request_future = async {
            start_test_request(|| append_entry(uri.clone(), append_entries_request.clone())).await
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
