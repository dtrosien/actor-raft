pub mod client;
pub mod server;

#[cfg(test)]
pub mod test_tools {

    // Test Utilities //////////////////////////////////////////////////////////////////////////////

    // wrapper for rpc requests, delays request to give tonic server time to start
    // takes a async function as input
    // function needs to be inserted as a closure

    use crate::raft_rpc::raft_rpc_server::{RaftRpc, RaftRpcServer};
    use crate::raft_rpc::{
        AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
    };
    use crate::rpc::client::Reply;
    use std::error::Error;
    use std::future::Future;
    use std::time::Duration;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    //todo solve problem with parallel calls from and to same ports when testing

    pub async fn start_test_request<F, Fut>(f: F) -> Result<Reply, Box<dyn Error + Send + Sync>>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<Reply, Box<dyn Error + Send + Sync>>>,
    {
        // give server time to start
        tokio::time::sleep(Duration::from_millis(1)).await;
        // actual request
        f().await
    }

    // starts tonic test server for unit tests
    pub async fn start_test_server<T: RaftRpc>(port: usize, rpc_test_case: T) {
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
    pub struct TestServerTrue {}
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
    pub struct TestServerFalse {}
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
