### Documentation

- Libary
    - [labgob](https://github.com/yjzhai-cs/6.5840/blob/master/docs/doc-lib-labgob.md/)
    - [labrpc](https://github.com/yjzhai-cs/6.5840/blob/master/docs/doc-lib-labrpc.md/)
    - [logger](https://github.com/yjzhai-cs/6.5840/blob/master/docs/doc-lib-logger.md/)
    - [deepcopy](https://github.com/yjzhai-cs/6.5840/blob/master/docs/doc-lib-deepcopy.md/)

- Architecture
    - [mapreduce](https://github.com/yjzhai-cs/6.5840/blob/master/docs/doc-arch-mapreduce.md/)
    - [raft](https://github.com/yjzhai-cs/6.5840/blob/master/docs/doc-arch-raft.md/)

- Application
    - [kvraft](https://github.com/yjzhai-cs/6.5840/blob/master/docs/doc-app-kvraft.md/)

- Others
    - [configs](https://github.com/yjzhai-cs/6.5840/blob/master/docs/doc-configs.md/)


### Algorithms

Project‘s Catalogue:

```shell
.
|-- mr // mapreduce libary
|-- mrapp // application for mapreduce
|-- main // start up a mapreduce
|-- raft // raft libary
|-- kvraft // kv service
|-- labrpc // rpc libary
|-- labgob // gob libary
|-- config // hyper-parameter
|-- utils // logger, deepcopy,..
`-- script // dstest, dslogs
```

#### I MapReduce

*A. Progress*

- worker count test. PASS
- job count test. PASS
- early exit test. PASS
- indexer test. PASS
- map parallelism test. PASS
- reduce parallelism test. PASS
- crash test. NO

*B. Debug*

```go
log.Pirntln("...")
log.Printf("...")
```

#### II Raft

Features: leader election; log replication; log persistence; snapshot; log compression; no-op log entry
Structure of Raft library I'll show
- many threads (Start() thread, thread reading from applych)
- many RPC threads to talk to peers in parallel
- many RPC handler threads (started by RPC package)
- raft state with lock
- one thread writing to applych
    - use condvar to signal it

*A. Progress*

Part A: leader election
- InitialElection. 2A  test. PASS
- ReElection. 2A  test. PASS
- ManyElections. 2A  test. PASS

<div align="center">
    <img src="https://raw.githubusercontent.com/JackFroster/Images/main/image/%E6%88%AA%E5%B1%8F2023-04-11%2022.53.39.png" alt="1" width="550px" />
</div>

<div align="center">
    <img src="https://raw.githubusercontent.com/JackFroster/Images/main/image/%E6%88%AA%E5%B1%8F2023-04-11%2022.56.04.png" alt="1" width="500px" />
</div>


Part B: log replication
- BasicAgree(implement simple log replication). 2B test. PASS
- RPCBytes(based on counting bytes of RPCs, check if each command is sent to each peer just once). 2B test. PASS(However, there are few bad cases)

```python
000054 INFO S0 Follower, created
000055 INFO S1 Follower, created
000056 INFO S2 Follower, created
Test (2B): RPC byte count ...
001612 TRCE S1 Candidate, starting a new election, 1th term
001613 VOTE S1 Candidate, sending vote request to S2, 1th term
001614 VOTE S2 Follower, voting for S1, 1th term.
labgob warning: Decoding into a non-default variable/field Term may not work
001615 VOTE S1 Candidate, receiving reply from S2, 1th term, true
001615 TRCE S1 Leader, elected as leader, 1th term. 2 servers agree this
001615 LEAD S1 Leader, Start agreement, index 1, term 1
001616 INFO S2 Follower, Replicated entry v[{%!,(int=99) %!,(int=1)}] index[1, 1], term 1
001616 TRCE S2 Candidate, starting a new election, 2th term
001616 VOTE S2 Candidate, sending vote request to S1, 2th term
001617 VOTE S1 Leader, voting for S2, 2th term.
001617 VOTE S2 Candidate, receiving reply from S1, 2th term, true
001617 TRCE S2 Leader, elected as leader, 2th term. 2 servers agree this
001617 VOTE S1 Follower, sending vote request to S0, 1th term
001617 VOTE S0 Follower, voting for S1, 1th term.
001618 INFO S2 Leader, Apply entry [{<nil> 0}] ,index [0, 0], term 2
001618 VOTE S2 Leader, sending vote request to S0, 2th term
001618 VOTE S0 Follower, voting for S2, 2th term.
001619 INFO S1 Follower, Apply entry [{<nil> 0}] ,index [0, 0], term 2
001829 INFO S0 Follower, Replicated entry v[{%!,(int=99) %!,(int=1)}] index[1, 1], term 2
001830 INFO S0 Follower, Apply entry [{<nil> 0}] ,index [0, 0], term 2
--- FAIL: TestRPCBytes2B (2.17s)
    config.go:597: one(99) failed to reach agreement
```

- FollowerFailure. 2B test. PASS
- LeaderFailure. 2B test. PASS
- FailAgree(test that a follower participates after disconnect and re-connect). 2B test. PASS
- FailNoAgree(handel a huge number of crashed workers). 2B test. PASS
- ConcurrentStarts(handel some concurrent RPC requests from applicaton layer). 2B test. PASS
- Backup(fast backup). 2B test. PASS(However, there are some bad cases.)
- Rejoin. 2B test. PASS
- Count. 2B test. PASS

Part C: state presistence
- Persist1. 2C test. PASS
- Persist2. 2C test. PASS
- Persist3. 2C test. PASS
- Figure8. 2c test. PASS
- UnreliableAgree. 2C test. PASS
- Figure8Unreliable. 2C test. PASS
- ReliableChurn. 2C test. PASS
- UnreliableChurn. 2C test. PASS

Part D: snapshot
- SnapshotBasic. 2D test. PASS
- SnapshotInstall. 2D test. PASS
- SnapshotInstallUnreliable. 2D test. PASS
- SnapshotInstallCrash. 2D test. PASS
- SnapshotInstallUnCrash. 2D test. PASS
- SnapshotAllCrash. 2D test. PASS
- SnapshotInit. 2D test. PASS

<div align="center">
    <img src="https://raw.githubusercontent.com/JackFroster/Images/main/image/%E6%88%AA%E5%B1%8F2023-04-25%2010.58.48.png" alt="555" width="340px" />
</div>

*B. Debug&Test*

[Debugging by Pretty Printing](https://blog.josejg.com/debugging-pretty/)

start testing by script.

```shell
go test -run 2A -race | dslogs > out.log

python3 ./../script/dstest -n 100 -p 50 InitialElection ReElection ManyElections

python3 ./../script/dstest -n 100 -p 50 BasicAgree RPCBytes FollowerFailure LeaderFailure FailAgree FailNoAgree ConcurrentStarts Backup Rejoin Count

python3 ./../script/dstest -n 100 -p 50 Persist1 Persist2 Persist3 Figure8 UnreliableAgree Figure8Unreliable ReliableChurn UnreliableChurn

python3 ./../script/dstest -n 100 -p 50 2A 2B 2C 2D
```

*C. Dir*

```shell
.
|-- raft.go
|-- election.go
|-- election_parallel.go
|-- election_sync.go
|-- replication.go
|-- persist.go
|-- persister.go
|-- snapshot.go
|-- channel.go
|-- logger.go
|-- test-test.go
|-- config.go
`-- util.go
```

#### III KV Raft

Features: 
- select server as 'leader' randomly(clerk remembers **which server turned out to be the leader for the last RPC**, and send the next RPC to that server first);
- mechanism to handle get/put timeout(**it handle this situation** where a leader has called Start() for a Clerk's RPC, but loses its leadership before the request is committed to the log);
- duplicate RPC detection(cope with duplicate Clerk requests);
    - k/v service maintains a **"duplicate table"** indexed by ID
    -  **duplicate detection should free server memory quickly**, for example by having each RPC imply that the client has seen the reply for its previous RPC

Part A:You will implement a key/value service using your Raft implementation, but **without using snapshots**.
- TestBasic3A. PASS.
- TestSpeed3A. PASS.
- TestConcurrent3A. PASS.
- TestUnreliable3A. PASS.
- TestUnreliableOneKey3A
- TestOnePartition3A. PASS
- TestManyPartitionsOneClient3A. PASS
- TestManyPartitionsManyClients3A (partitions, many clients). PASS
- TestPersistOneClient3A (restarts, one client). PASS
- TestPersistConcurrent3A (restarts, many clients). PASS
- TestPersistConcurrentUnreliable3A (unreliable net, restarts, many clients). PASS
- TestPersistPartition3A (restarts, partitions, many clients). PASS
- TestPersistPartitionUnreliable3A (unreliable net, restarts, partitions, many clients)。 PASS
- TestPersistPartitionUnreliableLinearizable3A (unreliable net, restarts, partitions, random keys, many clients). PASS

Part B: You will use your snapshot implementation from Lab 2D, which will allow Raft to **discard old log entries**.


```shell
python3 ./../script/dstest -n 20 -p 20 TestBasic3A TestSpeed3A TestConcurrent3A TestUnreliable3A TestUnreliableOneKey3A TestOnePartition3A TestManyPartitionsOneClient3A TestManyPartitionsManyClients3A TestPersistOneClient3A TestPersistConcurrent3A TestPersistConcurrentUnreliable3A TestPersistPartition3A TestPersistPartitionUnreliable3A TestPersistPartitionUnreliableLinearizable3A

python3 ./../script/dstest -n 20 -p 20 TestSnapshotRPC3B TestSnapshotSize3B TestSpeed3B TestSnapshotRecover3B TestSnapshotRecoverManyClients3B TestSnapshotUnreliable3B TestSnapshotUnreliableRecover3B TestSnapshotUnreliableRecoverConcurrentPartition3B TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B

```

### Reference

- [The Raft Consensus Algorithm](https://raft.github.io/)
- [Raft](http://thesecretlivesofdata.com/raft/#replication)
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)
- [Raft Q&A](https://thesquareplanet.com/blog/raft-qa/)

### Links

- [MIT 6.5840 Distributed System](https://pdos.csail.mit.edu/6.824/schedule.html)
- [GO](https://tour.go-zh.org/welcome/1)