use crate::raft_server::raft_handles::RaftHandles;

#[derive(Debug)]
pub struct RaftClientServer {
    handles: RaftHandles,
}

impl RaftClientServer {
    pub fn new(handles: RaftHandles) -> Self {
        RaftClientServer { handles }
    }
}
