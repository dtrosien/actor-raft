use crate::actors::log::log_store::LogStoreHandle;
use crate::actors::term_store::TermStoreHandle;

#[derive(Clone, Debug)]
pub struct StateMeta {
    pub previous_log_index: u64, // todo [test] better name it last index or latest replicated index? or need to track them differently?????
    pub previous_log_term: u64,
    pub term: u64,
    pub id: u64,
    pub leader_commit: u64, // todo [test] why couldnt this be set to zero inside actor
}

impl StateMeta {
    pub fn new(term: u64) -> Self {
        StateMeta {
            previous_log_index: 0,
            previous_log_term: 0,
            term,
            id: 0,
            leader_commit: 0,
        }
    }

    pub async fn build(id: u64, log_store: LogStoreHandle, term_store: TermStoreHandle) -> Self {
        let previous_log_term = log_store.get_last_log_term().await;
        let previous_log_index = log_store.get_last_log_index().await;
        let term = term_store.get_term().await;
        StateMeta {
            previous_log_index,
            previous_log_term,
            term,
            id,
            leader_commit: 0,
        }
    }
}
