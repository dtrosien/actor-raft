use futures_util::future::BoxFuture;
use std::error::Error;
use std::fmt::Debug;

pub trait Snapshot: Send + Sync + Debug {
    fn take_snapshot(&self) -> BoxFuture<'_, Result<(), Box<dyn Error + Send + Sync>>>;
}
