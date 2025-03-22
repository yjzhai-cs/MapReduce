package raft

import (
	"time"
	"math"
	"6.5840/utils"
	"6.5840/configs"
)

type RequestEntryArgs struct {
	Term int // leader's term
	LeaderId int // follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm int // term of prevLogIndex entry
	Entries []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader’s commitIndex
}

type RequestEntryReply struct {
	Term int // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ServerId int // replier id
	Live bool // 
	// Fast Backup
	XTerm int
	XIndex int
	XLen int
}

// Students' Guide to Raft(The importance of details):https://thesquareplanet.com/blog/students-guide-to-raft/
func (rf *Raft) RequestEntry(args *RequestEntryArgs, reply *RequestEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if  rf.currentTerm > args.Term { // stale leader
		reply.Term = rf.currentTerm
		return
	}

	rf.currentLeaderId = args.LeaderId

	persistent := false
	if rf.currentTerm != args.Term { 
		persistent = true
		defer rf.persist() // update term
	}

	rf.currentTerm, reply.Term, reply.Success = args.Term, args.Term, false
	rf.timer.Reset(timeout(configs.ELECTION_TIME_START, configs.ELECTION_TIME_END)) // reset timer

	if args.Entries == nil { utils.Debugger(utils.DDrop, "S%d %s, Receive heartbeat from S%d", rf.me, rf.state, args.LeaderId) } // heartbeats
	if rf.state == CANDIDATE { utils.Debugger(utils.DInfo, "S%d %s, Give up election", rf.me, rf.state) } // candidate receives a heartbeat from leader
	if rf.state == LEADER { utils.Debugger(utils.DInfo, "S%d %s, Stale leader", rf.me, rf.state) } // leader receives a heartbeat from another leader
	if args.Entries != nil { utils.Debugger(utils.DDrop, "S%d %s, Receive entries from S%d", rf.me, rf.state, args.LeaderId) } // entries

	rf.state = FOLLOWER

	if rf.sliceIndex2logIndex(len(rf.log) - 1) < args.PrevLogIndex { // check inconsistency or consistency, sliceIndex2logIndex
		if configs.FASTBACKUP { reply.XTerm = -1; reply.XLen = args.PrevLogIndex - rf.sliceIndex2logIndex(len(rf.log) - 1) } // reply.XLen = args.PrevLogIndex - len(rf.log) + 1
		return 
	}
	if (args.PrevLogIndex == rf.lastIncludedIndex && rf.lastIncludedTerm != args.PrevLogTerm) || // args.PrevLogIndex == rf.lastIncludedIndex
		( rf.logIndex2sliceIndex(args.PrevLogIndex) >= 0 && rf.log[rf.logIndex2sliceIndex(args.PrevLogIndex)].Term != args.PrevLogTerm) { // args.PrevLogIndex > rf.lastIncludedIndex, check inconsistency or consistency,logIndex2sliceIndex
		if configs.FASTBACKUP { 
			reply.XTerm = rf.log[rf.logIndex2sliceIndex(args.PrevLogIndex)].Term; // logIndex2sliceIndex
			index, _, _ := rf.bSearchLeftBound(0, rf.logIndex2sliceIndex(args.PrevLogIndex), 
			                    utils.Copy(rf.log).([]LogEntry), rf.log[rf.logIndex2sliceIndex(args.PrevLogIndex)].Term)
			reply.XIndex = rf.sliceIndex2logIndex(index)
		}
		return
	}
	if (args.PrevLogIndex < rf.lastIncludedIndex) { // args.PrevLogIndex < rf.lastIncludedIndex, check if AppendEntries RPC is outdate
		utils.Debugger(utils.DWarn, "S%d %s, Outdate AppendEntries RPC, The args.Entries[0] has been applied.", rf.me, rf.state)
		return
	}

	if args.Entries != nil { // && len(args.Entries) != 0
		// args.PrevLogIndex >= rf.lastIncludedIndex
		if len(args.Entries) <= (len(rf.log) - (rf.logIndex2sliceIndex(args.PrevLogIndex) + 1)) { // check if AppendEntries RPC is outdate
			var consistent = true 
			for i := 0; i < len(args.Entries); i ++ {
				if (args.Entries[i].Term != rf.log[rf.logIndex2sliceIndex(args.PrevLogIndex + 1) + i].Term) {
					consistent = false
					break
				}
			}
			if consistent { // outdate AppendEntries RPC
				utils.Debugger(utils.DWarn, "S%d %s, Outdate AppendEntries RPC, Follow has all the args.Entries.", rf.me, rf.state)
				reply.Success = true
				return
			}
		}
		// truncate
		rf.log = utils.Copy(rf.log[:rf.logIndex2sliceIndex(args.PrevLogIndex) + 1]).([]LogEntry) // delete
		rf.log = append(rf.log, utils.Copy(args.Entries).([]LogEntry)...) // append
		utils.Debugger(utils.DInfo, "S%d %s, Follower, Replicated entry, index[%d, %d], term %d", rf.me, rf.state, args.PrevLogIndex + 1, 
							rf.sliceIndex2logIndex(len(rf.log) - 1), rf.currentTerm)
		if !persistent { rf.persist() } // update log entries
	}
	// leaderCommit > len(rf.log) - 1 means all the follower's log entrites should have been commited.
	if args.LeaderCommit > rf.commitIndex { 
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64( rf.sliceIndex2logIndex(len(rf.log) - 1) )))
		if (rf.commitIndex > rf.lastApplied) {  rf.applyCond.Broadcast() } // apply commands to state machine
	}
	reply.Success = true
}

func (rf *Raft) sendRequestEntry(server int, args *RequestEntryArgs, reply *RequestEntryReply) bool {
	ok := rf.peers[server].Call("Raft.RequestEntry", args, reply)
	return ok
}

func (rf *Raft) heartbeat() {
	ticker := time.NewTicker(timeout(configs.HEARTBEAT_TIME_START, configs.HEARTBEAT_TIME_END))
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.state != LEADER { // this server isn't leader. go wating.
			rf.leaderCond.Wait() 
			utils.Debugger(utils.DDrop, "S%d %s, Heartbeat, Go live", rf.me, rf.state)
		} 
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i ++ { 
			rf.triggerCond[i].Broadcast() 
		}
		<-ticker.C
	}
}

// Reference: Students' Guide to Raft(https://thesquareplanet.com/blog/students-guide-to-raft/)
// A related, but not identical problem is that of assuming that your state has not changed between when you sent the RPC, 
// and when you received the reply. A good example of this is setting matchIndex = nextIndex - 1, 
// or matchIndex = len(log) when you receive a response to an RPC. This is not safe, 
// because both of those values could have been updated since when you sent the RPC. 
// Instead, the correct thing to do is update matchIndex to be prevLogIndex + len(entries[]) from the arguments you sent in the RPC originally.
func (rf *Raft) trigger() {
	for i := 0; i < len(rf.peers); i ++ {
		if rf.me == i { continue }
		
		go func(rf *Raft, server int) {
			for rf.killed() == false {
				rf.mu.Lock()
				rf.triggerCond[server].Wait() 

				next := rf.nextIndex[server]
				args := &RequestEntryArgs{
					Term:rf.currentTerm, 
					LeaderId:rf.me, 
					LeaderCommit:rf.commitIndex,
				}
				reply := &RequestEntryReply{}
				args.PrevLogIndex = next - 1

				if rf.logIndex2sliceIndex(next - 1) == -1 { // args.PrevLogIndex == rf.lastIncludeIndex
					args.PrevLogTerm = rf.lastIncludedTerm
				} else if rf.logIndex2sliceIndex(next - 1) < -1 { // follow-server's log lagged leader, args.PrevLogIndex < rf.lastIncludeIndex
					utils.Debugger(utils.DInfo, "S%d %s, Send Snapshot to S%d, rf.logIndex2sliceIndex(next - 1) %d, %dth term", rf.me, rf.state, server, rf.logIndex2sliceIndex(next - 1), rf.currentTerm)
					rf.snapshotCond[server].Broadcast()
					rf.mu.Unlock()
					continue
				} else /*if rf.logIndex2sliceIndex(next - 1) < len(rf.log)*/ { // args.PrevLogIndex > rf.lastIncludeIndex, rf.logIndex2sliceIndex(next - 1) >= 0
					args.PrevLogTerm = rf.log[rf.logIndex2sliceIndex(next - 1)].Term
				// } else {
					// rf.logIndex2sliceIndex(next - 1) >= len(rf.log) // nextIndex > len(rf.log)
				}
				
				if rf.logIndex2sliceIndex(next) < len(rf.log) { 
					args.Entries = rf.log[rf.logIndex2sliceIndex(next):] // non-heartbeat
				}
				rf.mu.Unlock()

				ch := make(chan bool)

				if !rf.isState(LEADER) { continue }

				go func(rf *Raft, server int, ch chan bool, args *RequestEntryArgs, reply *RequestEntryReply) {
					ch <- rf.sendRequestEntry(server, args, reply)
				}(rf, server, ch, args, reply)

				select
				{
				case ok :=  <- ch:
					if(!ok) { continue } // ok==false, request/reply is discarded
				case <-time.After(timeout(configs.REQUEST_TIMEOUT_START, configs.REQUEST_TIMEOUT_END)): // dist server disconnect
					go func(ch chan bool) { // get the value from ch so that this goroutine(sync-call-goroutine) can be destroied
						<- ch
					}(ch)
					continue
				}

				if rf.isStaleServer(reply.Term) { continue } // check term, stale server
				
				if !reply.Success { // reply failure
					// utils.Debugger(utils.DInfo, "S%d %s, S%d need backup, rf.logIndex2sliceIndex(next - 1) %d, %dth term", rf.me, rf.getState(), server, rf.logIndex2sliceIndex(next - 1), rf.currentTerm)
					rf.backup(server, reply.XTerm, reply.XIndex, reply.XLen)
					continue
				}

				rf.mu.Lock() // reply success
				rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)  // rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) // rf.matchIndex[server] = rf.nextIndex[server] - 1
				rf.mu.Unlock()
			}
		}(rf, i)
	}
}

// It is useful for bringing stale followers up to date quickl
func (rf *Raft) backup(server int, xterm int, xindex int, xlen int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if configs.FASTBACKUP { // fast backup
		if xterm == -1 { rf.nextIndex[server] -= xlen 
		} else {
			l, _, ok := rf.bSearchRightBound(0, len(rf.log) - 1, utils.Copy(rf.log).([]LogEntry), xterm)
			if ok { 
				rf.nextIndex[server] = l + 1 // fix bugs:rf.nextIndex[server] = int(math.Max(float64(l - 1), 1))
			} else { 
				rf.nextIndex[server] = xindex
			}
		}
	} else { rf.nextIndex[server] -= 1 } // decrement nextIndex
}

func (rf *Raft) checker() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.state != LEADER { rf.leaderCond.Wait() } // this server isn't leader. go wating.
		
		for n := len(rf.log) - 1; n > rf.logIndex2sliceIndex(rf.commitIndex) && n >= 0 ; n -- {  // logIndex2sliceIndex
			if rf.log[n].Term != rf.currentTerm { continue } // A leader is not allowed to update commitIndex to somewhere in a previous term
			
			count := 1 // leader
			for i := 0; i < len(rf.matchIndex); i ++ {
				if rf.me != i && rf.matchIndex[i] >= rf.sliceIndex2logIndex(n) { count ++ } // sliceIndex2logIndex
			}
			
			if 2 * count > len(rf.peers) {
				if rf.sliceIndex2logIndex(n) > rf.lastApplied { // sliceIndex2logIndex
					rf.commitIndex = rf.sliceIndex2logIndex(n) // sliceIndex2logIndex
					utils.Debugger(utils.DInfo, "S%d Leader, Commit, index %d, term %d", rf.me, rf.commitIndex, rf.currentTerm)
					rf.applyCond.Broadcast() 
				}
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(heartime(30))
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		rf.applyCond.Wait()
	
		start := rf.lastApplied + 1
		end := rf.commitIndex
		log := utils.Copy(rf.log[rf.logIndex2sliceIndex(start): rf.logIndex2sliceIndex(end + 1)]).([]LogEntry) // logIndex2sliceIndex
		utils.Debugger(utils.DInfo, "S%d %s, Apply entry, index [%d, %d], term %d", rf.me, rf.state, start, end, rf.currentTerm)
		rf.lastApplied = rf.commitIndex
		rf.mu.Unlock()

		go func(rf *Raft, start int, log[]LogEntry) {
			for i := 0; i < len(log); i ++ {
					rf.applyCh <- ApplyMsg{CommandValid:true, Command:log[i].Command, CommandIndex:start + i} // CommandIndex:logIndex
			}
		}(rf, start, log)
	}
}