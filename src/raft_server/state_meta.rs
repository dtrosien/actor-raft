use crate::raft_server::actors::log::log_store::LogStoreHandle;
use crate::raft_server::actors::term_store::TermStoreHandle;

#[derive(Clone, Debug)]
pub struct StateMeta {
    pub last_log_index: u64,
    pub last_log_term: u64,
    pub term: u64,
    pub id: u64,
    pub leader_commit: u64, // todo [test] why couldnt this be set to zero inside actor
}

impl StateMeta {
    pub fn new(term: u64) -> Self {
        StateMeta {
            last_log_index: 0,
            last_log_term: 0,
            term,
            id: 0,
            leader_commit: 0,
        }
    }

    pub async fn build(id: u64, log_store: LogStoreHandle, term_store: TermStoreHandle) -> Self {
        let last_log_index = log_store.get_last_log_index().await;
        let last_log_term = log_store.get_last_log_term().await;
        let term = term_store.get_term().await;
        StateMeta {
            last_log_index,
            last_log_term,
            term,
            id,
            leader_commit: 0,
        }
    }
}
