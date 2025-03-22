package raft

import (
	"bytes"
	"6.5840/labgob"
	"6.5840/utils"
)

// require lock when it's invoked
func (rf *Raft) persist() {
	buffer := new(bytes.Buffer) // create empty buffer
	encoder := labgob.NewEncoder(buffer) // create encoder

	encoder.Encode(rf.log)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)

	raftstate := buffer.Bytes() // []byte
	rf.persister.Save(raftstate, rf.snapshot) // save
	utils.Debugger(utils.DPersist, "S%d, Persists the raft state, log:%v, term:%v, vote:%v", rf.me, rf.log, rf.currentTerm, rf.votedFor)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { return } // bootstrap without any state?

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)
	var log []LogEntry
	var term int
	var votedFor int
	if decoder.Decode(&log) != nil || decoder.Decode(&term) != nil || decoder.Decode(&votedFor) != nil {
		utils.Debugger(utils.DError, "S%d, Can't decode raft state!", rf.me)
		return
	}

	rf.mu.Lock()
	rf.log = log
	rf.currentTerm = term
	rf.votedFor = votedFor
	rf.mu.Unlock()
	utils.Debugger(utils.DPersist, "S%d, Reads raft state form persistent storage, log:%v, term:%v, vote:%v", rf.me, rf.log, rf.currentTerm, rf.votedFor)
}