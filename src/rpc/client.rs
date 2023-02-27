use crate::raft_rpc::raft_rpc_client::RaftRpcClient;
use crate::raft_rpc::{AppendEntriesRequest, RequestVoteRequest};
use std::error::Error;
use std::time::Duration;
use tonic::transport::Channel;

#[derive(Clone)]
struct Reply {
    term: u64,
    success_or_granted: bool,
}

async fn request_vote(
    uri: String,
    vote_request: RequestVoteRequest,
) -> Result<Reply, Box<dyn Error>> {
    let channel = Channel::builder(uri.parse().unwrap())
        .connect_timeout(Duration::from_secs(10))
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
    //todo implement

    Ok(Reply {
        term: 0,
        success_or_granted: true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft_rpc::raft_rpc_server::{RaftRpc, RaftRpcServer};
    use crate::raft_rpc::{
        AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
    };
    use std::future::Future;
    use std::time::Duration;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    #[tokio::test]
    async fn request_vote_true_test() {
        let vote_request = RequestVoteRequest {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        };
        // ports need to be different for each test case since async
        let port = 50060;
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
        let port = 50061;
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
        let port = 50062;
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

    // Test Utilities //////////////////////////////////////////////////////////////////////////////

    // wrapper for rpc requests, delays request to give tonic server time to start
    // takes a async function as input
    // function needs to be inserted as a closure

    async fn start_test_request<F, Fut>(f: F) -> Result<Reply, Box<dyn Error>>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Reply, Box<dyn Error>>>,
    {
        // give server time to start
        tokio::time::sleep(Duration::from_millis(1)).await;
        // actual request
        f().await
    }

    // starts tonic test server for unit tests
    async fn start_test_server<T: RaftRpc>(port: usize, rpc_test_case: T) {
        let raft_service = RaftRpcServer::new(rpc_test_case);
        let addr = format!("[::1]:{port}").parse().unwrap();
        let result = Server::builder()
            .add_service(raft_service)
            .serve(addr)
            .await;
        // check if server is running
        assert!(result.is_ok());
    }

    // used for test cases which require true as an answer
    struct TestServerTrue {}
    #[tonic::async_trait]
    impl RaftRpc for TestServerTrue {
        async fn append_entries(
            &self,
            request: Request<AppendEntriesRequest>,
        ) -> Result<Response<AppendEntriesReply>, Status> {
            let reply = AppendEntriesReply {
                term: 1,
                success: true,
            };
            Ok(Response::new(reply))
        }

        async fn request_votes(
            &self,
            request: Request<RequestVoteRequest>,
        ) -> Result<Response<RequestVoteReply>, Status> {
            let reply = RequestVoteReply {
                term: 1,
                vote_granted: true,
            };
            Ok(Response::new(reply))
        }
    }

    // used for test cases which require false as an answer
    struct TestServerFalse {}
    #[tonic::async_trait]
    impl RaftRpc for TestServerFalse {
        async fn append_entries(
            &self,
            request: Request<AppendEntriesRequest>,
        ) -> Result<Response<AppendEntriesReply>, Status> {
            let reply = AppendEntriesReply {
                term: 1,
                success: false,
            };
            Ok(Response::new(reply))
        }

        async fn request_votes(
            &self,
            request: Request<RequestVoteRequest>,
        ) -> Result<Response<RequestVoteReply>, Status> {
            let reply = RequestVoteReply {
                term: 1,
                vote_granted: false,
            };
            Ok(Response::new(reply))
        }
    }
}
