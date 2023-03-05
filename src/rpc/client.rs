use crate::raft_rpc::raft_rpc_client::RaftRpcClient;
use crate::raft_rpc::{AppendEntriesRequest, RequestVoteRequest};
use std::error::Error;
use std::time::Duration;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct Reply {
    pub term: u64,
    pub success_or_granted: bool,
}

pub async fn request_vote(
    uri: String,
    vote_request: RequestVoteRequest,
) -> Result<Reply, Box<dyn Error + Send + Sync>> {
    let channel = Channel::builder(uri.parse()?)
        .connect_timeout(Duration::from_millis(2)) // todo not needed?
        .connect()
        .await?;

    let request = tonic::Request::new(vote_request);
    //let timeout_channel = Timeout::new(channel, Duration::from_secs(10)); //todo define timeout for answer?
    let mut client = RaftRpcClient::new(channel);

    let response = client.request_votes(request).await?;
    let response_arguments = response.into_inner();
    println!("vote granted:{}", response_arguments.vote_granted);
    Ok(Reply {
        term: response_arguments.term,
        success_or_granted: response_arguments.vote_granted,
    })
}

async fn append_entry(
    uri: String,
    append_entry_request: AppendEntriesRequest,
) -> Result<Reply, Box<dyn Error>> {
    //todo implement append_entry client

    Ok(Reply {
        term: 0,
        success_or_granted: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::raft_rpc::RequestVoteRequest;
    use crate::rpc::test_tools::{
        start_test_request, start_test_server, TestServerFalse, TestServerTrue,
    };

    #[tokio::test]
    async fn request_vote_true_test() {
        let vote_request = RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
        // ports need to be different for each test case since async
        let port = 50065;
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
        let port = 50066;
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
        let port = 50067;
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
}
