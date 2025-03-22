package raft

import "6.5840/labgob"
import "6.5840/utils"
import "6.5840/configs"
import "bytes"
import "time"

type RequestSnapshotArgs struct {
	Term int // leader's term
	LeaderId int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm int // term of lastIncludedIndex
	Offset int // byte offset where chunk is positioned in the snapshot file
	Data []byte // raw bytes of the snapshot chunk, starting at offset
	Done bool // true if this is the last chunk
}

type RequestSnapshotReply struct {
	Term int // urrentTerm, for leader to update itself
}

func (rf *Raft) RequestSnapshot(args *RequestSnapshotArgs, reply *RequestSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	utils.Debugger(utils.DSnap, "S%d %s, Receive snapot, args.LastIncludedIndex %d, %dth term", rf.me, rf.state, args.LastIncludedIndex, rf.currentTerm)
	
	if  rf.currentTerm > args.Term { // stale leader
		reply.Term = rf.currentTerm
		return
	}

	rf.timer.Reset(timeout(configs.ELECTION_TIME_START, configs.ELECTION_TIME_END)) // reset timer

	if rf.state == CANDIDATE { utils.Debugger(utils.DInfo, "S%d %s, Give up election, %dth term", rf.me, rf.state, rf.currentTerm) } // candidate receives a heartbeat from leader

	rf.currentTerm, reply.Term  = args.Term, args.Term
	rf.state = FOLLOWER

	if args.LastIncludedIndex > rf.sliceIndex2logIndex(len(rf.log) - 1) {
		rf.log = make([]LogEntry, 0) // delete all log
	} else if args.LastIncludedIndex > rf.lastIncludedIndex {
		rf.log = utils.Copy(rf.log[rf.logIndex2sliceIndex(args.LastIncludedIndex) + 1:]).([]LogEntry) // delete log entries that are covered by snapshot
	}

	if args.LastIncludedIndex > rf.sliceIndex2logIndex(len(rf.log) - 1) ||  args.LastIncludedIndex > rf.lastIncludedIndex { // lagged
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.snapshot = clone(args.Data)

		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex

		go func(rf *Raft, data []byte , index int, term int) { // re-bulid state of machine state by snapshot
			utils.Debugger(utils.DSnap, "S%d %s, Apply snapshot, %dth term", rf.me, rf.state, rf.currentTerm)
			rf.applyCh <- ApplyMsg{SnapshotValid:true, Snapshot: data, SnapshotIndex:index, SnapshotTerm:term}
		}(rf, args.Data, args.LastIncludedIndex, args.LastIncludedTerm)

		rf.persist() // persist
	}
}

func (rf *Raft) sendRequestSnapshot(server int, args *RequestSnapshotArgs, reply *RequestSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.RequestSnapshot", args, reply)
	return ok
}

func (rf *Raft) syncer() {
	for i := 0; i < len(rf.peers); i ++ {
		if rf.me == i { continue } // skip me

		go func(rf *Raft, server int) {
			for rf.killed() == false {
				rf.mu.Lock()
				rf.snapshotCond[server].Wait() // wait

				snapArgs := &RequestSnapshotArgs {
					Term: rf.currentTerm,
					LeaderId: rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm: rf.lastIncludedTerm,
					Data: rf.snapshot,
				}
				snapReply := &RequestSnapshotReply {}
				rf.mu.Unlock()

				snapCh := make(chan bool)

				if !rf.isState(LEADER) { continue }

				go func(rf *Raft, server int, snapCh chan bool, snapArgs *RequestSnapshotArgs, snapReply *RequestSnapshotReply) {
					snapCh <- rf.sendRequestSnapshot(server, snapArgs, snapReply)
				}(rf, server, snapCh, snapArgs, snapReply)

				select
				{
				case ok :=  <- snapCh:
					if(!ok) { continue } // ok==false, request/reply is discarded
				case <-time.After(timeout(configs.REQUEST_TIMEOUT_START, configs.REQUEST_TIMEOUT_END)): // dist server disconnect
					go func(snapCh chan bool) { // get the value from ch so that this goroutine(sync-call-goroutine) can be destroied
						<- snapCh
					}(snapCh)
					continue
				}

				if rf.isStaleServer(snapReply.Term) { continue } // check term, stale server
				
				rf.mu.Lock()
				rf.nextIndex[server] = snapArgs.LastIncludedIndex + 1
				rf.matchIndex[server] = snapArgs.LastIncludedIndex
				rf.mu.Unlock()
			}
		}(rf, i)
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// decode snapshot
	decodeBuffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(decodeBuffer)
	var lastIncludedIndex int
	var serviceLayerState []interface{}
	if decoder.Decode(&lastIncludedIndex) != nil || 
		decoder.Decode(&serviceLayerState) != nil {
		utils.Debugger(utils.DError, "S%d, Can't decode snapshot!, Snapshot", rf.me)
		return
	}
	if index != -1 && index != lastIncludedIndex {
		utils.Debugger(utils.DError, "S%d, Snapshot doesn't match m.SnapshotIndex", rf.me)
		return
	}

	// re-encode snapshot
	rf.mu.Lock()
	if rf.logIndex2sliceIndex(index) < 0 || rf.logIndex2sliceIndex(index) >= len(rf.log){
		rf.mu.Unlock()
		return
	}
	utils.Debugger(utils.DSnap, "S%d %s, Start snapshot, lastIncludedIndex %d, index %d, logIndex2sliceIndex(index) %d, %dth term", rf.me, rf.state, rf.lastIncludedIndex, index, rf.logIndex2sliceIndex(index), rf.currentTerm)
	
	rf.lastIncludedTerm = rf.log[rf.logIndex2sliceIndex(index)].Term
	rf.log = utils.Copy(rf.log[rf.logIndex2sliceIndex(index) + 1:]).([]LogEntry) // discard old log entries
	rf.serviceLayerState = serviceLayerState

	rf.lastIncludedIndex = index
	
	// meta data
	encodeBuffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(encodeBuffer)
	encoder.Encode(rf.lastIncludedIndex)
	encoder.Encode(rf.serviceLayerState)
	encoder.Encode(rf.lastIncludedTerm)
	rf.snapshot = clone(encodeBuffer.Bytes())

	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1{ return }

	rf.mu.Lock()
	defer rf.mu.Unlock()

	buffer := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(buffer)
	var lastIncludedIndex int
	var serviceLayerState []interface{}
	var lastIncludedTerm int
	if decoder.Decode(&lastIncludedIndex) != nil  || decoder.Decode(&serviceLayerState) != nil || 
		decoder.Decode(&lastIncludedTerm) != nil {
		utils.Debugger(utils.DError, "S%d, Can't decode snapshot!, readSnapshot", rf.me)
		return
	}
	
	rf.snapshot = clone(snapshot)
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.serviceLayerState = serviceLayerState

	rf.commitIndex = lastIncludedIndex
	rf.lastApplied = lastIncludedIndex

	utils.Debugger(utils.DSnap, "S%d %s, Read snapshot, lastIncludedIndex %d, lastIncludedTerm %d, %dth term", rf.me, rf.state, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.currentTerm)
}