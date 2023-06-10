# Actor-Raft

## common todos:

- implement + test client registration (client store)
- implement and test snapshot capabilities with log compactor
- implement install_snapshot rpc, which send snapshot files over to newly added or erased nodes to 


- more integration tests
- more unit tests 


- no opt entries should be created by leader after leader change to prevent certain bugs


- remember: for App integration lazy statics can be used 
- it might be an idea to make run mutable in App since lock is only required in executor and this would give users more possibilities
