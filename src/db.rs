use crate::raft_rpc::append_entries_request::Entry;
use sled::Db;
use std::error::Error;

pub struct RaftDb {
    db: Db,
}

impl RaftDb {
    pub fn new(db_path: String) -> Self {
        RaftDb {
            db: sled::open(db_path).expect("could not open log-db"),
        }
    }

    pub async fn store_current_term(&self, current_term: u64) -> Result<(), Box<dyn Error>> {
        self.db
            .insert(b"current_term", &current_term.to_ne_bytes())?;
        self.db.flush_async().await?;
        Ok(())
    }

    pub fn read_current_term(&self) -> Result<Option<u64>, Box<dyn Error>> {
        Ok(match self.db.get(b"current_term")? {
            Some(current_term) => {
                let current_term: u64 = bincode::deserialize(&current_term)?;
                Some(current_term)
            }
            None => None,
        })
    }

    pub async fn store_voted_for(&self, voted_for: u64) -> Result<(), Box<dyn Error>> {
        self.db.insert(b"voted_for", &voted_for.to_ne_bytes())?;
        self.db.flush_async().await?;
        Ok(())
    }

    pub fn read_voted_for(&self) -> Result<Option<u64>, Box<dyn Error>> {
        Ok(match self.db.get(b"voted_for")? {
            Some(voted_for) => {
                let voted_for: u64 = bincode::deserialize(&voted_for)?;
                Some(voted_for)
            }
            None => None,
        })
    }

    pub async fn store_entry(&self, entry: Entry) -> Result<(), Box<dyn Error>> {
        let bytes = bincode::serialize(&entry)?;
        self.db.insert(&entry.index.to_ne_bytes(), bytes)?;
        self.db.flush_async().await?;
        Ok(())
    }

    pub async fn store_entries(&self, entry: Vec<Entry>) -> Result<(), Box<dyn Error>> {
        //todo implement

        self.db.flush_async().await?;
        Ok(())
    }

    pub fn read_entry(&self, index: u64) -> Result<Option<Entry>, Box<dyn Error>> {
        Ok(match self.db.get(&index.to_ne_bytes())? {
            Some(bytes) => {
                let entry: Entry = bincode::deserialize(&bytes)?;
                Some(entry)
            }
            None => None,
        })
    }
    pub fn read_last_entry(&self) -> Result<Option<Entry>, Box<dyn Error>> {
        Ok(match self.db.last()? {
            Some(bytes) => {
                let entry: Entry = bincode::deserialize(&bytes.1)?;
                Some(entry)
            }
            None => None,
        })
    }

    pub fn clear_db(&mut self) -> Result<(), Box<dyn Error>> {
        self.db.clear()?;
        Ok(())
    }

    //todo rename? split? move to log store?
    pub fn get_last_log_index_and_term(&self) -> (u64, u64) {
        match self.read_last_entry() {
            Ok(result) => match result {
                None => (0, 0),
                Some(entry) => (entry.index, entry.term),
            },
            Err(_) => (0, 0),
        }
    }

    // insert and get, similar to std's BTreeMap
    // tree.insert("key", "value")?;
    //
    // assert_eq!(tree.get(&"key")?, Some(sled::IVec::from("value")),);
    //
    // // range queries
    // for kv_result in tree.range("key_1".."key_9") {}
    //
    // // deletion
    // let old_value = tree.remove(&"key")?;
    //
    // // atomic compare and swap
    // tree.compare_and_swap("key", Some("current_value"), Some("new_value"))?;
    //
    // // block until all operations are stable on disk
    // // (flush_async also available to get a Future)
    // tree.flush_async().await?;
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use tokio::sync::Mutex;

    static TEST_DB: Lazy<Mutex<RaftDb>> =
        Lazy::new(|| Mutex::new(RaftDb::new("test-db".to_string())));

    #[tokio::test]
    async fn term_test() {
        let current_term = 1_u64;
        let mut db = TEST_DB.lock().await;
        db.clear_db().unwrap();

        db.store_current_term(current_term).await.unwrap();
        assert_eq!(current_term, db.read_current_term().unwrap().unwrap());
    }

    #[tokio::test]
    async fn voted_for_test() {
        let voted_for = 29978769_u64;
        let mut db = TEST_DB.lock().await;
        db.clear_db().unwrap();

        db.store_voted_for(voted_for).await.unwrap();
        assert_eq!(voted_for, db.read_voted_for().unwrap().unwrap());
    }

    #[tokio::test]
    async fn entry_test() {
        let entry1 = Entry {
            index: 1,
            term: 21313131,
            payload: "some payload".to_string(),
        };
        let entry2 = Entry {
            index: 2,
            term: 21313131,
            payload: "some payload".to_string(),
        };
        let entry3 = Entry {
            index: 3,
            term: 21313131,
            payload: "some payload".to_string(),
        };
        let mut db = TEST_DB.lock().await;
        db.clear_db().unwrap();

        db.store_entry(entry3.clone()).await.unwrap();
        db.store_entry(entry1.clone()).await.unwrap();
        db.store_entry(entry2.clone()).await.unwrap();
        assert_eq!(entry3.clone(), db.read_last_entry().unwrap().unwrap());
        assert_eq!(entry1.clone(), db.read_entry(1).unwrap().unwrap());
        assert_eq!(entry2.clone(), db.read_entry(2).unwrap().unwrap());
    }

    #[tokio::test]
    async fn entries_test() {
        //todo implement
    }

    #[tokio::test]
    async fn last_log_index_and_term_test() {
        let entry1 = Entry {
            index: 1,
            term: 21313131,
            payload: "some payload".to_string(),
        };
        let entry2 = Entry {
            index: 2,
            term: 21313131,
            payload: "some payload".to_string(),
        };
        let entry3 = Entry {
            index: 3,
            term: 21313131,
            payload: "some payload".to_string(),
        };
        let mut db = TEST_DB.lock().await;
        db.clear_db().unwrap();

        db.store_entry(entry3.clone()).await.unwrap();
        db.store_entry(entry1.clone()).await.unwrap();
        db.store_entry(entry2.clone()).await.unwrap();

        assert_eq!(
            (entry3.index, entry3.term),
            db.get_last_log_index_and_term()
        );
    }
}
