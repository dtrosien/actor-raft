use crate::raft_server_rpc::append_entries_request::Entry;
use sled::Db;
use std::error::Error;

#[derive(Debug)]
pub struct RaftDb {
    db: Db,
}

impl RaftDb {
    #[tracing::instrument(ret, level = "debug")]
    pub fn new(db_path: String) -> Self {
        RaftDb {
            db: sled::open(db_path).expect("could not open log-db"),
        }
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn store_current_term(
        &self,
        current_term: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.db
            .insert(b"current_term", &current_term.to_ne_bytes())?;
        self.db.flush_async().await?;
        Ok(())
    }

    #[tracing::instrument(ret, level = "debug")]
    pub fn read_current_term(&self) -> Result<Option<u64>, Box<dyn Error + Send + Sync>> {
        Ok(match self.db.get(b"current_term")? {
            Some(current_term) => {
                let current_term: u64 = bincode::deserialize(&current_term)?;
                Some(current_term)
            }
            None => None,
        })
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn store_voted_for(
        &self,
        voted_for: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.db.insert(b"voted_for", &voted_for.to_ne_bytes())?;
        self.db.flush_async().await?;
        Ok(())
    }

    #[tracing::instrument(ret, level = "debug")]
    pub fn read_voted_for(&self) -> Result<Option<u64>, Box<dyn Error + Send + Sync>> {
        Ok(match self.db.get(b"voted_for")? {
            Some(voted_for) => {
                let voted_for: u64 = bincode::deserialize(&voted_for)?;
                Some(voted_for)
            }
            None => None,
        })
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn store_entry_and_flush(
        &self,
        entry: Entry,
    ) -> Result<Option<Entry>, Box<dyn Error + Send + Sync>> {
        let b_entry = bincode::serialize(&entry)?;
        let b_old_entry = self.db.insert(entry.index.to_ne_bytes(), b_entry)?;
        self.db.flush_async().await?;

        let old_entry = match b_old_entry {
            None => None,
            Some(b_old_entry) => Some(bincode::deserialize(&b_old_entry)?),
        };
        Ok(old_entry)
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn store_entry(
        &self,
        entry: Entry,
    ) -> Result<Option<Entry>, Box<dyn Error + Send + Sync>> {
        let b_entry = bincode::serialize(&entry)?;
        let b_old_entry = self.db.insert(entry.index.to_ne_bytes(), b_entry)?;

        let old_entry = match b_old_entry {
            None => None,
            Some(b_old_entry) => Some(bincode::deserialize(&b_old_entry)?),
        };
        Ok(old_entry)
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn store_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut batch = sled::Batch::default();
        for entry in entries {
            let b_entry = bincode::serialize(&entry)?;
            batch.insert(&entry.index.to_ne_bytes(), b_entry);
        }
        self.db.apply_batch(batch)?;
        self.db.flush_async().await?;
        Ok(())
    }

    #[tracing::instrument(ret, level = "debug")]
    pub fn read_entry(&self, index: u64) -> Result<Option<Entry>, Box<dyn Error + Send + Sync>> {
        Ok(match self.db.get(index.to_ne_bytes())? {
            Some(b_entry) => {
                let entry: Entry = bincode::deserialize(&b_entry)?;
                Some(entry)
            }
            None => None,
        })
    }

    #[tracing::instrument(ret, level = "debug")]
    pub fn read_last_entry(&self) -> Result<Option<Entry>, Box<dyn Error + Send + Sync>> {
        Ok(match self.db.last()? {
            Some(b_entry) => {
                let entry: Entry = bincode::deserialize(&b_entry.1)?;
                Some(entry)
            }
            None => None,
        })
    }

    #[tracing::instrument(ret, level = "debug")]
    pub fn read_previous_entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Box<dyn Error + Send + Sync>> {
        Ok(match self.db.get_lt(index.to_ne_bytes())? {
            Some(b_entry) => {
                let entry: Entry = bincode::deserialize(&b_entry.1)?;
                Some(entry)
            }
            None => None,
        })
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn delete_entry(&self, index: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.db.remove(index.to_ne_bytes())?;
        self.db.flush_async().await?;
        Ok(())
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn delete_entries(
        &self,
        first_index: u64,
        last_index: u64,
    ) -> Result<(), Box<dyn Error>> {
        let mut batch = sled::Batch::default();

        for index in first_index..=last_index {
            batch.remove(&index.to_ne_bytes());
        }

        self.db.apply_batch(batch)?;
        self.db.flush_async().await?;
        Ok(())
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn clear_db(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.db.clear()?;
        self.db.flush_async().await?;
        Ok(())
    }

    #[tracing::instrument(ret, level = "debug")]
    pub async fn flush_entries(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.db.flush_async().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use once_cell::sync::Lazy;
    use tokio::sync::Mutex;

    static TEST_DB: Lazy<Mutex<RaftDb>> =
        Lazy::new(|| Mutex::new(RaftDb::new("databases/test-db".to_string())));

    #[tokio::test]
    async fn term_test() {
        let current_term = 1_u64;
        let mut db = TEST_DB.lock().await;
        db.clear_db().await.unwrap();

        db.store_current_term(current_term).await.unwrap();
        assert_eq!(current_term, db.read_current_term().unwrap().unwrap());
    }

    #[tokio::test]
    async fn voted_for_test() {
        let voted_for = 29978769_u64;
        let mut db = TEST_DB.lock().await;
        db.clear_db().await.unwrap();

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
        let entry4 = Entry {
            index: 3,
            term: 1111111,
            payload: "some payload".to_string(),
        };
        let mut db = TEST_DB.lock().await;
        db.clear_db().await.unwrap();

        db.store_entry_and_flush(entry3.clone()).await.unwrap();
        db.store_entry_and_flush(entry1.clone()).await.unwrap();
        db.store_entry_and_flush(entry2.clone()).await.unwrap();
        assert_eq!(entry3.clone(), db.read_last_entry().unwrap().unwrap());
        assert_eq!(entry1.clone(), db.read_entry(1).unwrap().unwrap());
        assert_eq!(entry2.clone(), db.read_entry(2).unwrap().unwrap());

        //test overwrite
        let old_entry = db.store_entry_and_flush(entry4.clone()).await.unwrap();
        assert_eq!(entry3.clone(), old_entry.unwrap());
        assert_eq!(entry4.clone(), db.read_last_entry().unwrap().unwrap());
    }

    #[tokio::test]
    async fn manual_flush_entry_test() {
        let entry1 = Entry {
            index: 1,
            term: 21313131,
            payload: "some payload".to_string(),
        };
        let mut db = TEST_DB.lock().await;
        db.clear_db().await.unwrap();
        db.store_entry(entry1.clone()).await.unwrap();
        db.flush_entries().await.unwrap();
        assert_eq!(entry1, db.read_entry(1).unwrap().unwrap());
    }

    #[tokio::test]
    async fn entries_test() {
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
        let entries = vec![entry1.clone(), entry2.clone(), entry3.clone()];
        let mut db = TEST_DB.lock().await;
        db.clear_db().await.unwrap();

        db.store_entries(entries).await.unwrap();

        assert_eq!(entry3.clone(), db.read_last_entry().unwrap().unwrap());
        assert_eq!(entry1.clone(), db.read_entry(1).unwrap().unwrap());
        assert_eq!(entry2.clone(), db.read_entry(2).unwrap().unwrap());
    }

    #[tokio::test]
    async fn previous_entry_test() {
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
        db.clear_db().await.unwrap();

        db.store_entry_and_flush(entry3.clone()).await.unwrap();
        db.store_entry_and_flush(entry1.clone()).await.unwrap();
        db.store_entry_and_flush(entry2.clone()).await.unwrap();

        assert_eq!(entry2, db.read_previous_entry(3).unwrap().unwrap());
        assert_eq!(entry1, db.read_previous_entry(2).unwrap().unwrap());
    }
}
