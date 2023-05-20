use crate::raft_client_rpc::raft_client_rpc_server::RaftClientRpcServer;
use crate::raft_server::raft_handles::RaftHandles;
use crate::raft_server::rpc::client_server::RaftClientServer;
use crate::raft_server::rpc::node_server::RaftNodeServer;
use crate::raft_server_rpc::raft_server_rpc_server::RaftServerRpcServer;
use futures_util::FutureExt;
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::sync::broadcast::Receiver;
use tokio::task::JoinHandle;
use tonic::body::BoxBody;
use tonic::codegen::http::{Request, Response};
use tonic::codegen::Service;
use tonic::server::NamedService;
use tonic::transport::{Body, Server};
use tracing::info;

pub async fn init_node_server(
    r_shutdown: Receiver<()>,
    handles: RaftHandles,
    ip: String,
    port: u16,
) -> Option<JoinHandle<()>> {
    let node_rpc = RaftNodeServer::new(handles);
    let node_service = RaftServerRpcServer::new(node_rpc);

    let addr = format!("{}:{}", ip, port).parse().unwrap();

    Some(build_rpc_server(r_shutdown, addr, node_service, "node".to_string()).await)
}

pub async fn init_client_server(
    r_shutdown: Receiver<()>,
    handles: RaftHandles,
    ip: String,
    port: u16,
) -> Option<JoinHandle<()>> {
    let client_rpc = RaftClientServer::new(handles);
    let client_service = RaftClientRpcServer::new(client_rpc);

    let addr = format!("{}:{}", ip, port).parse().unwrap();

    Some(build_rpc_server(r_shutdown, addr, client_service, "client".to_string()).await)
}

async fn build_rpc_server<S>(
    mut r_shutdown: Receiver<()>,
    addr: SocketAddr,
    service: S,
    service_type: String,
) -> JoinHandle<()>
where
    S: Service<Request<Body>, Response = Response<BoxBody>, Error = Infallible>
        + NamedService
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    tokio::spawn(async move {
        info!("starting tonic raft-{} rpc server", service_type);
        Server::builder()
            .add_service(service)
            .serve_with_shutdown(addr, r_shutdown.recv().map(|_| ()))
            .await
            .unwrap();
        info!("tonic raft-{} rpc server was shut down", service_type);
    })
}

#[cfg(test)]
pub mod test {
    use crate::raft_server::rpc::node_client::Reply;
    use crate::raft_server_rpc::raft_server_rpc_server::{RaftServerRpc, RaftServerRpcServer};
    use crate::raft_server_rpc::{
        AppendEntriesReply, AppendEntriesRequest, RequestVoteReply, RequestVoteRequest,
    };
    use once_cell::sync::Lazy;
    use std::error::Error;
    use std::future::Future;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};

    // global var used to offer unique ports for each rpc call in unit tests starting from port number 50060
    static GLOBAL_PORT_COUNTER: Lazy<Mutex<u16>> = Lazy::new(|| Mutex::new(50060));
    // get port from GLOBAL_PORT_COUNTER
    pub async fn get_test_port() -> u16 {
        let mut i = GLOBAL_PORT_COUNTER.lock().await;
        *i += 1;
        *i
    }

    // wrapper for rpc requests, delays request to give tonic server time to start
    // takes a async function as input
    // function needs to be inserted as a closure
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
    pub async fn start_test_server<T: RaftServerRpc>(port: u16, rpc_test_case: T) {
        let raft_service = RaftServerRpcServer::new(rpc_test_case);
        let addr = format!("[::1]:{port}").parse().unwrap();
        let result = Server::builder()
            .add_service(raft_service)
            .serve(addr)
            .await;
        // check if server is running
        assert!(result.is_ok());
    }

    // Test Servers/////////////////////////////////////////////////////////////////////////////////

    // used for test cases which require true as an answer
    pub struct TestServerTrue {}
    #[tonic::async_trait]
    impl RaftServerRpc for TestServerTrue {
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
    impl RaftServerRpc for TestServerFalse {
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
