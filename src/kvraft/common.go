package kvraft

import "log"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	GET = "GET"
	PUT = "PUT"
	APPEND = "APPEND"
)

type Err string

// Field names must start with capital letters,
// otherwise RPC will break.
type Op struct {
	Operation string // "GET" , "Put" or "Append"
	Key string
	Value string
	RID int64 // request id
	CID int64 // client id
}

type Result struct {
	RID int64
	Err bool
	Value string 
}

// Put or Append
// Field names must start with capital letters,
// otherwise RPC will break.
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

const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}