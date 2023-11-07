# Actor-Raft

## todos:

- implement and test snapshot capabilities with compactor
- implement install_snapshot rpc, which send snapshot files over to newly added or erased nodes to
- implement membership changes (see executor todos)



- no opt entries should be created by leader after leader change to prevent certain bugs (remember in tests the log
  index will be moved when asserted)
