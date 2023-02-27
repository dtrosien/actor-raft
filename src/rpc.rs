use crate::raft_rpc::raft_rpc_client::RaftRpcClient;
use crate::raft_rpc::RequestVoteRequest;
use std::time::Duration;
use tonic::transport::Channel;

struct Reply {
    term: u64,
    success_or_granted: bool,
}

async fn request_vote() -> Reply {
    let uri = "https://[::1]:50060".to_string();

    let channel = Channel::builder(uri.parse().unwrap())
        .connect_timeout(Duration::from_secs(10))
        .connect()
        .await;

    let vote_request = RequestVoteRequest {
        term: 0,
        candidate_id: 0,
        last_log_index: 0,
        last_log_term: 0,
    };

    println!("request");
    //todo rework

    match channel {
        Ok(channel) => {
            let request = tonic::Request::new(vote_request);
            //let timeout_channel = Timeout::new(channel, Duration::from_secs(10)); //todo define timeout for answer?
            let mut client = RaftRpcClient::new(channel);

            let response = client.request_votes(request).await;
            match response {
                Ok(response) => {
                    let response_arguments = response.into_inner();
                    let response_term = response_arguments.term as u64;
                    let response_granted = response_arguments.vote_granted as bool;
                    println!(
                        "Got vote response: term: {}, granted: {}",
                        &response_term, &response_granted
                    );
                    Reply {
                        term: response_term,
                        success_or_granted: response_granted,
                    }
                }
                Err(_) => Reply {
                    term: 0,
                    success_or_granted: false,
                },
            }
        }
        Err(_) => Reply {
            term: 0,
            success_or_granted: false,
        },
    }
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
    async fn request_vote_test() {
        let serve_future = async { start_test_server(TestServerTrue {}).await };
        let request_future = async { start_test_request(request_vote).await };

        tokio::select! {
            _ = serve_future => panic!("server returned first"),
            r = request_future => {assert!(r.success_or_granted); assert_eq!(r.term,1)}  ,
        }
    }

    // Test Utilities //////////////////////////////////////////////////////////////////////////////

    async fn start_test_request<F, Fut>(f: F) -> Reply
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Reply>,
    {
        // give server time to start
        tokio::time::sleep(Duration::from_millis(1)).await;
        // actual request
        f().await
    }

    async fn start_test_server<T: RaftRpc>(rpc_test_case: T) {
        let port = 50060;
        let addr = format!("[::1]:{port}").parse().unwrap();
        let raft_service = RaftRpcServer::new(rpc_test_case);

        let result = Server::builder()
            .add_service(raft_service)
            .serve(addr)
            .await;
        // check if server is running
        assert!(result.is_ok());
    }

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
