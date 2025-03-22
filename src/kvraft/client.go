package kvraft

import "6.5840/labrpc"
import "6.5840/utils"
import random "math/rand"
import "time"
import "6.5840/configs"
import "sync"

// Reference: Raft's paper, Section 8
// Each client talks to the service through a Clerk with Put/Append/Get methods. A Clerk manages RPC interactions with the servers.
// Each of your key/value servers ("kvservers") will have an associated Raft peer. 
// Clerks send Put(), Append(), and Get() RPCs to the kvserver whose associated Raft is the leader. 
type Clerk struct {
	mu sync.Mutex
	servers []*labrpc.ClientEnd
	randomizer *random.Rand
	
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

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)

	ck.servers = servers
	ck.randomizer = random.New(random.NewSource(time.Now().UnixNano())) // 使用当前时间戳 生成真正的随机数字 
	
	ck.cid = nrand()
	ck.initRid()
	// ck.lastUIDs = make(map[int64]bool)

	return ck
}

// What should a client do if a Put or Get RPC times out?
//   i.e. Call() returns false
//   if server is dead, or request was dropped: re-send
//   if server executed, but reply was lost: re-send is dangerous
// A Clerk sometimes doesn't know which kvserver is the Raft leader. 
// If the Clerk sends an RPC to the wrong kvserver, or if it cannot reach the kvserver, 
// the Clerk should re-try by sending to a different kvserver.

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	leaderId := ck.randomizer.Intn(len(ck.servers))

	args := &GetArgs{ 
		Op: GET,
		Key:key, 
		RID:ck.getRid(), // same ID in re-sends of same RPC
		CID:ck.cid,
	}

	for {
		reply := &GetReply{}
		utils.Debugger(utils.DInfo, "S, Leader may be %d.", leaderId)

		ch := make(chan bool)
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
		
		if ok && reply.Err == OK { // success
			utils.Debugger(utils.DInfo, "GET Reponse, sucess, key '%s', value '%s'.", key, reply.Value)
			return reply.Value 
		}

		if ok && reply.Err == ErrNoKey { // no key
			utils.Debugger(utils.DInfo, "GET Reponse, no key.")
			return "" 
		}

		if !ok { // failed rpc, cann't reach kvserver, drop reply
			leaderId = ck.randomizer.Intn(len(ck.servers)) // select a server randomly
		} else if reply.Err == ErrWrongLeader { // wrong leader
			leaderId = reply.LeaderId
			utils.Debugger(utils.DInfo, "GET Reponse, Wrong Leader, Leader may be %d.", leaderId)
		}
	}
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	leaderId := ck.randomizer.Intn(len(ck.servers))

	args := &PutAppendArgs{
		Key:key, 
		Value:value, 
		Op:op, 
		RID:ck.getRid(),
		CID:ck.cid,
	}
	
	for {
		reply := &PutAppendReply{}
		utils.Debugger(utils.DInfo, "S, Leader may be %d", leaderId)
		
		ch := make(chan bool)
		go func(ck *Clerk, args *PutAppendArgs, reply *PutAppendReply, leaderId int, ch chan bool) {
			ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, reply) // sync call
			ch <- ok
		}(ck, args, reply, leaderId, ch)

		ok := false
		select // timer
		{
		case ok = <- ch: // ok whose value is false means that request or reply is discarded
		case <-time.After(timeout(configs.KVRAFT_REQUEST_TIMEOUT_START, configs.KVRAFT_REQUEST_TIMEOUT_END)): // dist server disconnect or long delay
			ok = false
			go func(ch chan bool) {
				<-ch
			}(ch)
		}
		
		if ok && reply.Err == OK { // success
			return 
		}

		if !ok { // failed rpc, cann't reach kvserver, drop reply
			leaderId = ck.randomizer.Intn(len(ck.servers)) // select a server randomly
		} else if reply.Err == ErrWrongLeader { // wrong leader
			leaderId = reply.LeaderId
			utils.Debugger(utils.DInfo, "%s Reponse, wrong leader, leader may be S%d.",  op, leaderId)
		}
	}
	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PUT)
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, APPEND)
}
