package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/utils"
	"sync"
	"sync/atomic"
	"bytes"
)

// The kvserver code submits the Put/Append/Get operation to Raft, so that the Raft log holds a sequence of Put/Append/Get operations. 
// All of the kvservers execute operations from the Raft log in order, applying the operations to their key/value databases; 
// the intent is for the servers to maintain identical replicas of the key/value database.
// idea: duplicate RPC detection
//   1. let's have the k/v service detect duplicate client requests
//   2. client picks a unique ID for each request, sends in RPC. same ID in re-sends of same RPC.
//   3. k/v service maintains a "duplicate table" indexed by ID
//          makes a table entry for each RPC
//          after executing, record reply content in duplicate table
//   4. if 2nd RPC arrives with the same ID, it's a duplicate
//          generate reply from the value in the table
// how does a new leader get the duplicate table?
//   put ID in logged operations handed to Raft
//   all replicas should update their duplicate tables as they execute
//   so the information is already there if they become leader
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

func (kv *KVServer) broadcast() {
	if _, ok := kv.rf.GetState(); ok { 
		utils.Debugger(utils.DInfo, "S%d, apply, broadcast.", kv.me)
		kv.cond.Broadcast() 
	}
}

func (kv *KVServer) setDuplicate(cid int64, res Result) { // modify kv.intermediate securely
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.duplicate[cid] = Result{res.RID, res.Err, res.Value}
}

func (kv *KVServer) get(key string) (string, bool) { // get value based on key from kv.database securely
	utils.Debugger(utils.DInfo, "S%d, apply, GET, key '%s'.", kv.me, key)
	kv.mu.Lock()
	value, ok := kv.database[key]
	kv.mu.Unlock()
	return value, ok
}

func (kv *KVServer) put(key string, value string) { // put (key, value) to kv.database securely
	utils.Debugger(utils.DInfo, "S%d, apply, PUT, key '%s', value '%s'.", kv.me, key, value)
	kv.mu.Lock()
	kv.database[key] = value
	kv.mu.Unlock()
}

func (kv *KVServer) append(key string, value string) { // append (key, value) to kv.database securely
	utils.Debugger(utils.DInfo, "S%d, apply, APPEND, key '%s', value '%s'.", kv.me, key, value)
	kv.mu.Lock()
	v, ok := kv.database[key]
	if ok {
		kv.database[key] = v + value
	} else {
		kv.database[key] = value
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()

	if !isLeader { // return directly when current raft's server is not leader
		reply.Err = ErrWrongLeader
		reply.LeaderId = kv.rf.GetCurrentLeaderId()
		return
	}

	kv.mu.Lock()
	res, ok := kv.duplicate[args.CID] // check if the command whose uid is args.UID has executed.
	kv.mu.Unlock()

	if !ok || args.RID > res.RID {
		op := Op{
			Operation:args.Op, 
			Key:args.Key, 
			CID:args.CID,
			RID:args.RID,
		}

		_, _, _ = kv.rf.Start(op)
		utils.Debugger(utils.DLeader, "Leader %d, Get, key '%s'.", kv.me, args.Key)

		kv.mu.Lock() // wait
		res, ok = kv.duplicate[args.CID]
		for !ok || args.RID != res.RID { 
			kv.cond.Wait()
			res, ok = kv.duplicate[args.CID]
		}
		kv.mu.Unlock()
	}

	reply.LeaderId = kv.rf.GetCurrentLeaderId()

	if res.Err { // No Key
		reply.Err = ErrNoKey
		return
	}
	reply.Err = OK // success
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	_, isLeader := kv.rf.GetState()

	if !isLeader { // return directly when current raft's server is not leader
		reply.Err = ErrWrongLeader
		reply.LeaderId = kv.rf.GetCurrentLeaderId()
		return
	}

	kv.mu.Lock()
	res, ok := kv.duplicate[args.CID] // check if the command whose uid is args.UID has executed.
	kv.mu.Unlock()

	if !ok || args.RID > res.RID {
		op := Op{ 
			Operation:args.Op, 
			Key:args.Key, 
			Value:args.Value, 
			CID:args.CID,
			RID:args.RID,
		}
		_, _, _ = kv.rf.Start(op)
		utils.Debugger(utils.DLeader, "S%d Leader, Put/Append, key '%s', value '%s'.", kv.me, args.Key, args.Value)

		kv.mu.Lock() // wait
		res, ok = kv.duplicate[args.CID]
		for !ok || args.RID != res.RID { 
			kv.cond.Wait() 
			res, ok = kv.duplicate[args.CID]
		}
		kv.mu.Unlock()
	}

	reply.Err = OK
	reply.LeaderId = kv.rf.GetCurrentLeaderId()
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		m := <- kv.applyCh

		if m.CommandValid {
			if m.Command == nil { continue }
			op := m.Command.(Op)
			
			kv.mu.Lock()
			res, ok := kv.duplicate[op.CID] // check if the command whose uid is args.UID has executed.
			kv.mu.Unlock()
			if ok && res.RID == op.RID {
				kv.cond.Broadcast() 
				continue
			}

			if op.Operation == GET {
				value, ok := kv.get(op.Key)
				kv.setDuplicate(op.CID, Result{RID:op.RID , Err:!ok, Value:value})
				kv.broadcast() // wake up
			} else if op.Operation == PUT {
				kv.put(op.Key, op.Value)
				kv.setDuplicate(op.CID, Result{RID:op.RID , Err:false, Value:""})
				kv.broadcast() // wake up
			} else if op.Operation == APPEND {
				kv.append(op.Key, op.Value)
				kv.setDuplicate(op.CID, Result{RID:op.RID, Err:false, Value:""})
				kv.broadcast() // wake up
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate { // snapshot. If maxraftstate is -1, we do not have to snapshot
				// fmt.Printf("Beofore Snapshot, kv.persister.RaftStateSize()=%d\n", kv.persister.RaftStateSize())
				kv.snapshot(m.CommandIndex)
			}

		} else if m.SnapshotValid { 
			kv.ingestSnap(m.Snapshot, m.SnapshotIndex)
			utils.Debugger(utils.DInfo, "ingestSnap")
		}
	}
}

func (kv *KVServer) snapshot(CommandIndex int) { // monitor if persister.RaftStateSize() is greater than maxraftstate
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
	// fmt.Printf("After Snapshot, kv.persister.RaftStateSize()=%d\n", kv.persister.RaftStateSize())
}

func (kv *KVServer) ingestSnap(snapshot []byte, index int) {
	if snapshot == nil {
		utils.Debugger(utils.DError, "nil snapshot in src/kvraft/server.go")
		return
	}
	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var lastIncludedIndex int
	var serviceLayerState []interface{}

	if decoder.Decode(&lastIncludedIndex) != nil ||
		decoder.Decode(&serviceLayerState) != nil {
		utils.Debugger(utils.DError,"snapshot decode error in src/kvraft/server.go")
		return
	}
	if index != -1 && index != lastIncludedIndex {
		utils.Debugger(utils.DWarn, "server snapshot doesn't match m.SnapshotIndex in src/kvraft/server.go")
		return
	}
	
	kv.mu.Lock()
	kv.database = serviceLayerState[0].(map[string]string)
	kv.duplicate = serviceLayerState[1].(map[int64]Result)
	kv.mu.Unlock()
}

func (kv *KVServer) loadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { return }

	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var lastIncludedIndex int
	var serviceLayerState []interface{}
	var lastIncludedTerm int
	if decoder.Decode(&lastIncludedIndex) != nil  || decoder.Decode(&serviceLayerState) != nil || 
		decoder.Decode(&lastIncludedTerm) != nil {
		utils.Debugger(utils.DError, "Can't decode snapshot!")
		return
	}
	
	kv.mu.Lock()
	kv.database = serviceLayerState[0].(map[string]string)
	kv.duplicate = serviceLayerState[1].(map[int64]Result)
	kv.mu.Unlock()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(make(map[string]string)) // register map, reference https://www.jb51.net/article/211523.htm
	labgob.Register(make(map[int64]Result))

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister

	// You may need initialization code here.
	kv.database = make(map[string]string)
	kv.duplicate = make(map[int64]Result)
	kv.cond = sync.NewCond(&kv.mu)

	kv.loadSnapshot(persister.ReadSnapshot()) // ReadSnapshot

	go kv.applier()

	return kv
}
