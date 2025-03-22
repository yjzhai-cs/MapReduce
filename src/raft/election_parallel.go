package raft

import (
	"time"
	"sync/atomic"
	"6.5840/utils"
	"6.5840/configs"
)

// version 2 for leader election

func (rf *Raft) addTicket() {
	atomic.AddInt32(&rf.ticket, 1)
}

func (rf *Raft) resetTicket() {
	atomic.StoreInt32(&rf.ticket, 1)
}

func (rf *Raft) isWin() bool {
	votes := atomic.LoadInt32(&rf.ticket)
	return int(2 * votes) > len(rf.peers)
}

func (rf *Raft) elect()  {
	rf.wgForTicket.Add(len(rf.peers) - 1)
	rf.resetTicket() // reset
	
	rf.mu.Lock()
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	utils.Debugger(utils.DTrace, "S%d %s, Start a new election, %dth term", rf.me, rf.state, rf.currentTerm)

	rf.persist() // update rf.votedFor

	for i := 0; i < len(rf.peers); i ++ { // reinitialized nextIndex
		rf.nextIndex[i] = rf.sliceIndex2logIndex(len(rf.log)) // sliceIndex2logIndex
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()

	rf.candidateCond.Broadcast() // rf.ticketer, go live
	for i := 0; i < len(rf.peers); i ++ { rf.electCond[i].Broadcast() } // vote request goroutine, go live
}

func (rf *Raft) committee() {
	for i := 0; i < len(rf.peers); i ++ {
		if rf.me == i { continue }
		
		go func(rf *Raft, server int) {
			for rf.killed() == false {
				rf.mu.Lock()
				rf.electCond[server].Wait() // block

				var lastLogIndex int
				var lastLogTerm int
				if len(rf.log) == 0 {
					lastLogIndex, lastLogTerm = rf.lastIncludedIndex, rf.lastIncludedTerm
				} else {  
					lastLogIndex, lastLogTerm = rf.sliceIndex2logIndex(len(rf.log) - 1), rf.log[len(rf.log) - 1].Term
				}

				args := &RequestVoteArgs{ 
					Term:rf.currentTerm, 
					CandidateId:rf.me, 
					LastLogIndex:lastLogIndex, 
					LastLogTerm:lastLogTerm,
				}
				reply := &RequestVoteReply{} 
				rf.mu.Unlock()

				ch := make(chan bool)

				if !rf.isState(CANDIDATE) { 
					rf.wgForTicket.Done()
					continue 
				}

				go func(rf *Raft, server int, ch chan bool, args *RequestVoteArgs, reply *RequestVoteReply) { // go request
					ch <- rf.sendRequestVote(server, args, reply)
				}(rf, server, ch, args, reply)

				select
				{
				case ok :=  <- ch:
					if(!ok) {
						rf.wgForTicket.Done()
						continue
					}
				case <-time.After(timeout(configs.REQUEST_TIMEOUT_START, configs.REQUEST_TIMEOUT_END)): // long delay, timeout, timer
					rf.wgForTicket.Done()
					go func(ch chan bool) { // get the value from ch so that this goroutine(sync-call-goroutine) can be destroied
						<- ch
					}(ch)
					continue
				}

				if rf.isStaleServer(reply.Term) { // check term, stale server
					rf.wgForTicket.Done()
					continue 
				} 

				utils.Debugger(utils.DVote, "S%d Candidate, Receive reply from S%d, %dth term, %v", rf.me, server, rf.getCurrentTerm(), reply.VoteGranted)
				if reply.VoteGranted { rf.addTicket() }
				rf.wgForTicket.Done()
			}
		}(rf, i)
	}
}

func (rf *Raft) ticketer() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.state != CANDIDATE { rf.candidateCond.Wait() } // this server isn't candidate. go wating.
		rf.mu.Unlock()

		rf.wgForTicket.Wait()

		if rf.isWin() {
			rf.mu.Lock()
			rf.state = LEADER

			rf.currentLeaderId = rf.me

			// rf.Start(nil) // no-op log entry
			rf.mu.Unlock()

			utils.Debugger(utils.DTrace, "S%d %s, elected as leader, %dth term. %d servers agree this", rf.me, rf.state,rf.currentTerm, atomic.LoadInt32(&rf.ticket))
			rf.leaderCond.Broadcast() // go live
		} else { 
			rf.mu.Lock()
			rf.state = FOLLOWER 
			rf.mu.Unlock()
		}
	}
}