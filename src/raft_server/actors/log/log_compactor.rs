// todo impl actor which gets counts from log store which triggers the take_snapshot function of the App trait.
// after successful execution the log_compactor fires a
// delete signal to the log_store to delete all entries from the log

// todo depricated: impl actor which gets counts from log store an where the user code can ask for a
// snapshot trigger. when the onshot channel returns true from user the log_compactor fires a
// delete signal to the log_store to delete all entries from the log
//snapshot_sender: broadcast::Sender<oneshot::Sender<bool>>,
