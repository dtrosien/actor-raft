// todo impl actor which gets counts from log store an where the user code can ask for a 
// snapshot trigger. when the onshot channel returns true from user the log_compactor fires a 
// delete signal to the log_store to delete all entries from the log
//snapshot_sender: broadcast::Sender<oneshot::Sender<bool>>,