package raft

// import "time"
import "6.5840/utils"
import "6.5840/configs"

type RequestVoteArgs struct {
	Term int // candidate's term
	CandidateId int // candidate requesting vote
	// election restriction
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm int // term of candidate’s last log entry
}

type RequestVoteReply struct {
	Term int // current term, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
// if you have already voted in the current term, and an incoming RequestVote RPC has a higher term that you, 
// you should first step down and adopt their term (thereby resetting votedFor), 
// and then handle the RPC, which will result in you granting the vote!
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// time.Sleep(timeout(0,5))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist() // update rf.currentTerm and rf.votedFor

	if (rf.currentTerm > args.Term || (rf.votedFor != -1 && 
		rf.votedFor != args.CandidateId && rf.currentTerm == args.Term)) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false // stale candidate or has voted in the term
	} else if rf.votedFor == args.CandidateId && rf.currentTerm == args.Term { // lose reply, client sends request again
		reply.Term, reply.VoteGranted = rf.currentTerm, true
	} else if ( (len(rf.log) == 0 && rf.lastIncludedTerm > args.LastLogTerm) || 
			( len(rf.log) == 0 && rf.lastIncludedTerm == args.LastLogTerm && rf.lastIncludedIndex > args.LastLogIndex ) || 
			( len(rf.log) - 1 >= 0 && rf.log[len(rf.log) - 1].Term > args.LastLogTerm) || // election restriction
		    ( len(rf.log) - 1 >= 0 && rf.log[len(rf.log) - 1].Term == args.LastLogTerm && rf.sliceIndex2logIndex(len(rf.log) - 1) > args.LastLogIndex)) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		utils.Debugger(utils.DVote, "S%d %s, election restriction, refuse to vote for S%d, %dth term.", rf.me, rf.state, args.CandidateId, args.Term) 
	} else { //new term, vote
		utils.Debugger(utils.DVote, "S%d %s, voting for S%d, %dth term.", rf.me, rf.state, args.CandidateId, args.Term)
		rf.timer.Reset(timeout(configs.ELECTION_TIME_START, configs.ELECTION_TIME_END))
		rf.currentTerm, reply.Term = args.Term, args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.state = FOLLOWER // step down
	}
}

// version 1 for leader election
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok, count := false, 0
	for !ok  { // ok = false, lose request or reply
		if count >= configs.MAXREQUESTCOUNT { break }
		utils.Debugger(utils.DVote, "S%d %s, sending vote request to S%d, %dth term", rf.me, rf.getState(), server, args.Term)
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply) // sync call
		count ++ 
	}
	if !ok { 
		utils.Debugger(utils.DVote, "S%d %s, failed to vote request S%d, %dth term", rf.me, rf.getState() ,server, args.Term)
	}
	return ok
}