# KVRaft Documentation

## Introduction

KVRaft is an application on top of Raft. In this libary, we build **a fault-tolerant key/value storage service** using our Raft library from `src/raft`. The following picture describes the structure of KVRaft.

<div align="center">
<img src="https://raw.githubusercontent.com/JackFroster/Images/main/image/%E6%88%AA%E5%B1%8F2023-05-09%2019.02.24.png" alt="w" width=1000px />
</div>

The key/value service will be **a replicated state machine**, consisting of several key/value servers that use Raft for replication. **Our key/value service can continue to process client requests as long as a majority of the servers are alive and can communicate, in spite of other failures or network partitions.** In this kvraft libary, we have implemented all parts(Clerk, Service, and Raft) shown in the diagram of Raft interactions as above.

When building a service on top of Raft(such as the key/value store in the second 6.824 Raft lab), the interaction between the service and the Raft log can be tricky to get right. This section details some aspects of the development process that we may find useful when building our application.

Directory is following:
```shell
.
|-- client.go
|-- server.go
|-- common.go
|-- util.go
|-- config.go
|-- test_test.go
`-- util.go
```

## Linearizability

Our service arrange that application calls to Clerk `Get/Put/Append` methods **be linearizable**. 

If called one at a time, the `Get/Put/Append` methods should **act as if the system had only one copy of its state**, and each call should observe the modifications to the state implied by the preceding sequence of calls. 

**For concurrent calls**, the return values and final state must be the same as if the operations had executed one at a time in some order. Calls are concurrent if they overlap in time: for example, if client X calls Clerk.Put() , and client Y calls Clerk.Append() , and then client X's call returns. **A call must observe the effects of all calls that have completed before the call starts**.

**Linearizability is convenient** for applications because it's the behavior you'd see **from a single server** that processes requests one at a time. For example, if one client gets a successful response from the service for an update request, subsequently launched reads from other clients are guaranteed to see the effects of that update. Providing linearizability is relatively easy for a single server. **However, it is harder if the service is replicated**, since all servers must choose the same execution order for concurrent requests, **must avoid replying to clients using state that isn't up to date, and must recover their state after a failure in a way that preserves all acknowledged client updates**.

## Interface between Clerl Machine and Service/State Machine Leader

Clients/Clerk can send three different RPCs to the key/value service: `Put(key, value)` , `Append(key, arg)` , and `Get(key)`. 

```go
type Clerk struct {
	mu sync.Mutex
	servers []*labrpc.ClientEnd
	randomizer *random.Rand
	
	cid int64 // a unique client ID, perhaps a 64-bit random number
	rid int64 // a sequence number of request.
}
```

The service maintains a **simple database of key/value pairs**. Keys and values are strings. 

```go
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister  // Object to hold this peer's persisted state

	database map[string]string // kv table
	duplicate map[int64]Result // (cid, operation result), k/v service maintains a "duplicate table" indexed by ID
	
	cond *sync.Cond
}
```

`Put(key, value)` **replaces** the value for a particular key in the database

`Append(key, arg)` **appends** arg to key's value

`Get(key)` **fetches** the current value for the key. 

```go
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	RID int64 // request id
	CID int64 // client id
}

type PutAppendReply struct {
	Err Err
	LeaderId int
}

type GetArgs struct {
	Key string
	Op    string
	RID int64 // request id
	CID int64 // client id
}

type GetReply struct {
	Err   Err
	Value string
	LeaderId int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    // ...
}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    // ...
}
```
A Get for a **non-existent key** should return an empty string. An Append to a non-existent key should act like Put. Each client talks to the service through a Clerk with Put/Append/Get methods. A Clerk manages RPC interactions with the servers

## Leader Selection

A Clerk sometimes doesn't know **which kvserver is the Raft leader**. 

**Workflow:**
1. Clients of Raft send all of their requests to the leader. 
2. When a client first starts up, it connects to a **randomly chosen** server. 
3. If the client’s first choice is not the leader, that server will reject the client’s request and **supply information about the most recent leader** it has heard from (`AppendEntries` requests include the network address of the leader). 
4. If the leader crashes, **client requests will time out**; clients then try again with randomly-chosen servers.

Clerk remembers which server turned out to be the leader for the last RPC, and send the next RPC to that server first. **This will avoid wasting time searching for the leader on every RPC**, which may help you pass some of the tests quickly enough

Details: 
- If the Clerk sends an RPC to the **wrong kvserver**, or if it **cannot reach** the kvserver, the Clerk should **re-try by sending to a different kvserver**. 
- If the key/value service commits the operation to its Raft log (and hence applies the operation to the key/value state machine), the leader reports the result to the Clerk by responding to its RPC. 
- If the **operation failed to commit** (for example, if the leader was replaced), the server reports an error, and the Clerk retries with a different server.

## Key/value service without snapshots

**Workflow**：
1. Each of your key/value servers ("kvservers") will have an associated Raft peer. 
2. Clerks send `Put()`, `Append()`, and `Get()`RPCs to the kvserver whose associated Raft is the leader. 
3. The kvserver code submits the `Put/Append/Get` operation to Raft, so that the Raft log holds a sequence of `Put/Append/Get` operations. 
4. All of the kvservers execute operations from the Raft log in order, applying the operations to their key/value databases; the intent is for the servers to maintain identical replicas of the key/value database.

Details:
- After calling `Start()`, our kvservers will need to **wait for Raft to complete agreement**. 
- Commands that have been agreed upon arrive on the `applyCh`. 
- Our code will need to **keep reading** `applyCh` while `PutAppend()` and `Get()`handlers submit commands to the Raft log using `Start()`
- A kvserver doesn't complete a `Get()` RPC if it is not part of a majority (so that it does not serve stale data). Our solution is to enter every `Get()` (as well as each `Put()` and `Append()` ) in the Raft log.

## Applying client operations

The service can be constructed as a state machine where client operations transition the machine from one state to another. 

We **have a loop** somewhere that takes one client operation at the time (in the same order on all servers – this is where Raft comes in), and applies each one to the state machine in order. This loop should be the only part of our code that touches the application state (the key/value mapping in 6.824).

Workflow:
1. **This means that our client-facing RPC methods should simply submit the client’s operation to Raft, and then wait for that operation to be applied by this “applier loop”**. 
2. Only when the client’s command comes up should it be executed, and any return values read out. 
3. Note that this includes read requests!

```go
func (kv *KVServer) applier() {
	for !kv.killed() {
		m := <- kv.applyCh

		if m.CommandValid {
			// ...
		} else if m.SnapshotValid { 
			// ...
		}
	}
}
```
Details:

This brings up another question: **how do you know when a client operation has completed?** 
- **In the case of no failures**, this is simple – you just wait for the thing you put into the log to come back out (i.e., be passed to ). When that happens, you return the result to the client.
- However, **what happens if there are failures?** For example, you may have been the leader when the client initially contacted you, but someone else has since been elected, and the client request you put in the log has been discarded. **Clearly you need to have the client try again, but how do you know when to tell them about the error?**

**One simple way to solve this problem is** 
1. To record where in the Raft log the client’s operation appears when you insert it. 
2. Once the operation at that index is sent to `apply()`, you can tell whether or not the client’s operation succeeded based on whether the operation that came up for that index is in fact the one you put there. 
3. If it isn’t, a failure has happened and an error can be returned to the client.

**In reailty, if kvserver failed, Clerk will wail until timeout.**
```go
func (ck *Clerk) Get(key string) string {
	// ..
	for {
		// ...
		go func(ck *Clerk, args *GetArgs, reply *GetReply, leaderId int, ch chan bool) {
			ok := ck.servers[leaderId].Call("KVServer.Get", args, reply) // sync call
			ch <- ok
		}(ck, args, reply, leaderId, ch)

		ok := false
		select // timer
		{
		case ok = <- ch: // ok whose value is false means that request or reply is discarded
		case <-time.After(timeout(configs.KVRAFT_REQUEST_TIMEOUT_START, configs.KVRAFT_REQUEST_TIMEOUT_END)): // dist server disconnect or long delay
			ok = false
			go func(ch chan bool) { // get the value from ch so that this goroutine(sync-call-goroutine) can be destroied
				<- ch
			}(ch)
		}
		// ...			
	}
	// ...
}
```

## Solution to the face of network and server failures

Situations may be:

1. If **a leader fails just after committing** an entry to the Raft log, the Clerk may not receive a reply, and thus may **re-send the request to another leader**. 
2. A leader that has called Start() for a Clerk's RPC, but **loses its leadership before the request is committed** to the log
3. If **the ex-leader is partitioned by itself**, it won't know about new leaders; but any client in the same partition won't be able to talk to a new leader either, so it's OK in this case for the server and client to wait indefinitely until the partition heals.

In these cases, we arrange for the Clerk **to re-send the request to other servers until it finds the new leader.**


## Duplicate RPC detection

One problem we'll face is that a Clerk may have to send an RPC **multiple times until it finds a kvserver that replies positively.**

Each call to `Clerk.Put()` or `Clerk.Append()` should result in **just a single execution**, so we will have to ensure that the **re-send doesn't result in the servers executing the request twice**.

**What should a client do if a Put or Get RPC times out?**

i.e. `Call()` returns false
- if server is dead, or request was dropped: re-send
- if server executed, but reply was lost: **re-send is dangerous**

**Problem:**
- these two cases look the same to the client (no reply)
- if already executed, client still needs the result

**Idea: duplicate RPC detection** 
- let's have the k/v service detect duplicate client requests
- client picks a unique ID(`rid`) for each request, sends in RPC
    - **same ID in re-sends of same RPC**
- k/v service maintains a "**duplicate table**" indexed by ID
- makes a table entry for each RPC
    - after executing, record reply content in duplicate table
- if 2nd RPC arrives with the same ID, it's a duplicate
    - generate reply from the value in the table

**How does a new leader get the duplicate table?**
- put ID in logged operations handed to Raft
- **all replicas** should update their duplicate tables as they execute
- so the information is already there if they become leader

**If server crashes how does it restore its table?**
- if no snapshots, replay of log will populate the table
- if snapshots, snapshot must contain a copy of the table

**What if a duplicate request arrives before the original executes?**
- could just call Start() (again)
- it will probably appear twice in the log (same client ID, same seq #)
- when cmd appears on `applyCh`, don't execute if table says already seen

**Idea to keep the duplicate table small**
- **one table entry per client, rather than one per RPC**
- each client has only one RPC **outstanding** at a time
- each client numbers RPCs sequentially
- when server receives client RPC #10,
    - it can forget about client's lower entries
    - since this means client won't ever re-send older RPCs

```go
duplicate map[int64]Result 
```

**Some details:**
- each client needs a unique client ID(`cid`) -- perhaps a 64-bit random number
- client sends client ID and seq # in every RPC
    - repeats seq # if it re-sends
- duplicate table in k/v service indexed by client ID
    - contains just seq #, and value if already executed
- RPC handler first checks table, only Start()s if seq # > table entry
- each log entry must include client ID, seq #
- when operation appears on applyCh
    - update the seq # and value in the client's table entry
    - wake up the waiting RPC handler (if any)

```go
type Clerk struct {
	// ...
	
	cid int64 // a unique client ID, perhaps a 64-bit random number
	rid int64 // a sequence number of request.
}

func (ck *Clerk) initRid() {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.rid = 0
}

func (ck *Clerk) getRid() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.rid += 1
	return ck.rid
}
```

**But wait!**
- the k/v server is now returning old values from the duplicate table
- what if the reply value in the table is no longer up to date?
- is that OK?

Example:
```go
  C1           C2
  --           --
  put(x,10)
               first send of get(x), reply(10) dropped
  put(x,20)
               re-sends get(x), server gets 10 from table, not 20
```
`get(x)` and `put(x,20)` run concurrently, so could run before or after;
so, **returning the remembered value 10 is correct**

## Key/value service with snapshots

As things stand now, our key/value server doesn't call our Raft library's `Snapshot()` method, so a rebooting server has to replay the complete persisted Raft log in order to restore its state. 

Now we'll modify kvserver to cooperate with Raft to save log space, and **reduce restart time**, using Raft's Snapshot() from `src/raft`. **When a kvserver server restarts, it should read the snapshot from persister and restore its state from the snapshot.**

Workflow:
1. The tester passes `maxraftstate` to our `StartKVServer()`. 
2. `maxraftstate` indicates the maximum allowed size of our persistent Raft state in bytes (including the log, but not including snapshots). 
3. We compare maxraftstate to `persister.RaftStateSize()`. 
4. **Whenever our key/value server detects that the Raft state size is approaching this threshold, it should save a snapshot by calling Raft's `Snapshot()`**. If maxraftstate is -1, you do not have to snapshot. 
5. `maxraftstate` applies to the GOB-encoded bytes your Raft passes as the first argument to to `persister.Save()`.

Details:

When should a kvserver snapshot its state and what should be included in the snapshot？

if `kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate `, a kvserver snapshot its state as following.
```go
func (kv *KVServer) applier() {
	for !kv.killed() {
		m := <- kv.applyCh

		if m.CommandValid {
			// ...
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate { // snapshot.
				kv.snapshot(m.CommandIndex)
			}

		} else if m.SnapshotValid { 
			kv.ingestSnap(m.Snapshot, m.SnapshotIndex)
			// ...
		}
	}
}
```

A kvserver must be able to detect duplicated operations in the log across checkpoints, so any state we are using to detect them must be included in the snapshots such as `dupicate`. When a kvserver server restarts, it should read the snapshot from persister and restore its state from the snapshot. Therefore, `database` should be included in the snapshot.

```go
func (kv *KVServer) snapshot(CommandIndex int) { 
	var serviceLayerState []interface{} // serviceLayerState
	kv.mu.Lock()
	serviceLayerState = append(serviceLayerState, kv.database)
	serviceLayerState = append(serviceLayerState, kv.duplicate)
	kv.mu.Unlock()

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(CommandIndex)
	encoder.Encode(serviceLayerState)

	kv.rf.Snapshot(CommandIndex, buffer.Bytes())
}
```

When a kvserver server restarts, it should read the snapshot from persister.
```go
func (kv *KVServer) loadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { return }
	// ...
	kv.mu.Lock()
	kv.database = serviceLayerState[0].(map[string]string)
	kv.duplicate = serviceLayerState[1].(map[int64]Result)
	kv.mu.Unlock()
}
```
Note that capitalize all fields of structures stored in the snapshot.

## API

Struct
- `type Op struct {}`. Define the operation of Clent.
- `type Result struct {}`. Define the result format of PRC respone
- `type PutAppendArgs struct {}`. It is used as argument of `PutAppend` PRC.
- `type PutAppendReply struct {}`. It is used as argument of `PutAppend` PRC.
- `type GetArgs struct {}`. It is used as argument of `Get` PRC.
- `type GetReply struct {}`. It is used as argument of `Get` PRC.
- `type Clerk struct {}`. This struce defines specifical information of Client.
- `type KVServer struct {}`. It decribes detailed information of Server.

Methods for Clerk
- `func (ck *Clerk) initRid() {}`. Init a sequential number for each PRC.
- `func (ck *Clerk) getRid() int64 {}`. Increment the sequential number and return it.
- `func (ck *Clerk) Get(key string) string {}`
- `func (ck *Clerk) PutAppend(key string, value string, op string) {}`
- `func (ck *Clerk) Put(key string, value string) {}`
- `func (ck *Clerk) Append(key string, value string) {}`
- `func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {}`

Methods for Server
- `func (kv *KVServer) get(key string) (string, bool) {}`. Fetche the current value for the key from the database.
- `func (kv *KVServer) put(key string, value string) {}`. Replace the value for a particular key in the database.
- `func (kv *KVServer) append(key string, value string) {}`. Append arg to key's value.
- `func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {}`. A handler which handles the Get request.
- `func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {}`. A handler which deals with the PutAppend request.
- `func (kv *KVServer) applier() {}`. A loop somewhere that takes one client operation at the time and applies each one to the state machine in order.
- `func (kv *KVServer) snapshot(CommandIndex int) {}`. Invoke `kv.rf.Snapshot()` and hands a snapshot to Raft.
- `func (kv *KVServer) ingestSnap(snapshot []byte, index int) {}`
- `func (kv *KVServer) loadSnapshot(snapshot []byte) {}`.  Read the snapshot from persister and restore its state from the snapshot.
- `func (kv *KVServer) Kill() {}`
- `func (kv *KVServer) killed() bool {}`
- `func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {}`


## Reference
- [Lab3 KVRaft](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html)
- [l-raft-QA](https://pdos.csail.mit.edu/6.824/notes/l-raft-QA.txt)
- [In Search of an Understandable Consensus Algorithm(Raft Extended)](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)
- [Students' Guide to Raft](https://thesquareplanet.com/blog/students-guide-to-raft/)