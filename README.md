# Actor-Raft

## common todos:


- client integration test


- more integration tests
- more unit tests 


- implement client store
- no opt entries should be created by leader after leader change to prevent certain bugs


- remember: for App integration lazy statics can be used 
- it might be an idea to make run mutable in App since lock is only required in executor and this would give users more possibilities
