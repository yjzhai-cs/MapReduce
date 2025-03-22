package raft

import (
	"reflect"
	"6.5840/utils"
)

func (rf *Raft) spreadRequestVote(channel *Channel, term int, lastLogIndex int, lastLogTerm int) {
	for i := 0; i < len(rf.peers) && rf.isState(CANDIDATE) && rf.killed() == false; i ++ {
		if i == rf.me { continue } // skip me
		go func(rf *Raft, channel *Channel, server int, term int, lastLogIndex int, lastLogTerm int) {
			reply := &RequestVoteReply{Term:term, VoteGranted:false} // init
			rf.sendRequestVote(server, &RequestVoteArgs{ 
				Term:term, CandidateId:rf.me, LastLogIndex:lastLogIndex, LastLogTerm:lastLogTerm,
				}, reply)
			if !channel.isClose() { 
				utils.Debugger(utils.DVote, "S%d Candidate, receiving reply from S%d, %dth term, %v", rf.me, server, term, reply.VoteGranted)
				channel.ch <- RequestVoteReply{reply.Term, reply.VoteGranted} 
			}
		}(rf, channel, i, term, lastLogIndex, lastLogTerm)
	}
}

func (rf *Raft) initElection() (int, string, int, int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.state = CANDIDATE
	rf.votedFor = rf.me

	rf.persist() // update rf.votedFor

	for i := 0; i < len(rf.peers); i ++ { // reinitialized nextIndex
		rf.nextIndex[i] = rf.sliceIndex2logIndex(len(rf.log))
		rf.matchIndex[i] = 0
	}
	if len(rf.log) == 0 {
		return rf.currentTerm, rf.state, rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	return rf.currentTerm, rf.state, rf.sliceIndex2logIndex(len(rf.log) - 1), rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) startElection() {
	term, state, lastLogIndex, lastLogTerm := rf.initElection() // init election
	utils.Debugger(utils.DTrace, "S%d %s, starting a new election, %dth term", rf.me, state, term)
	channel := &Channel{ch:make(chan interface{}), closeFlag:false}
	go rf.spreadRequestVote(channel, term, lastLogIndex, lastLogTerm) // request vote
	
	defer channel.close()
	votes, count := 1, 0 // process reply, vote=1 measns that it includes the candidate
	for count < len(rf.peers) - 1 && rf.isState(CANDIDATE) && rf.killed() == false { // receive reply
		element := <-channel.ch
		reply := reflect.ValueOf(element) // reflect
		if rf.isStaleServer(int(reply.FieldByName("Term").Int())) { return }
		if reply.FieldByName("VoteGranted").Bool() { votes += 1 }
		count += 1
		if 2 * votes > len(rf.peers) { break }
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm { return } // check term
	if 2 * votes > len(rf.peers) {
		rf.state = LEADER
		utils.Debugger(utils.DTrace, "S%d %s, elected as leader, %dth term. %d servers agree this", rf.me, rf.state,rf.currentTerm, votes)
		// rf.mu.Unlock()
		// rf.Start(nil) // no-op log entry
		// rf.mu.Lock()
		rf.leaderCond.Broadcast() // go live
	} else { rf.state = FOLLOWER }
}