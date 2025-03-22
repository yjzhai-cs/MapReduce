package raft

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labrpc"
	"6.5840/utils"
	"6.5840/configs"
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	Term int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentLeaderId int

	// state a Raft server must maintain.
	log []LogEntry // sliceIndex, log entries
	timer *time.Timer // election timeout timer
	applyCh chan ApplyMsg // send and apply log entries to state machine by using it
	
	// Persistent state on all servers
	state string // follower, candidate or leader
	currentTerm int // greater than zero
	votedFor int
    
	// Volatile state on all servers
	commitIndex int // logIndex, index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // logIndex, index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	
	// Volatile state on leaders
	// Reinitialized after election
	nextIndex []int // logIndex, for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // logIndex, for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// conditional variable
	applyCond *sync.Cond // trigger applier
	leaderCond *sync.Cond // trigger heartbeat or checker
	candidateCond *sync.Cond
	triggerCond []*sync.Cond // trigger request of heartbeat or request of replication
	electCond []*sync.Cond

	// parallel election
	wgForTicket sync.WaitGroup
	ticket int32

	// snapshot
	snapshot []byte
	lastIncludedIndex int // the snapshot replaces all entries up through and including this index
	lastIncludedTerm int // term of lastIncludedIndex
	serviceLayerState []interface{}

	snapshotCond []*sync.Cond
}

func (rf *Raft) GetCurrentLeaderId() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentLeaderId
}

// logIndex to sliceIndex
// require lock when it's invoked
func (rf *Raft) logIndex2sliceIndex(logIndex int) int {
	var discardLen int = 0 
	if rf.lastIncludedIndex != -1 {
		discardLen = rf.lastIncludedIndex + 1
	}
	return logIndex - discardLen
}

// sliceIndex to logIndex
// require lock when it's invoked
func (rf *Raft) sliceIndex2logIndex(sliceIndex int) int {
	var discardLen int = 0 
	if rf.lastIncludedIndex != -1 {
		discardLen = rf.lastIncludedIndex + 1
	}
	return sliceIndex + discardLen
}

func (rf *Raft) isState(state string) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state == state
} 

// if rf.currentTerm < term, leader/candidate -> follower and update term
func (rf *Raft) isStaleServer(term int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm < term {
		utils.Debugger(utils.DWarn, "S%d %s, A stale server, current term: %d, overdue term: %d.", rf.me, rf.state, term, rf.currentTerm)
		rf.state = FOLLOWER
		rf.currentTerm = term
		rf.timer.Reset(timeout(configs.ELECTION_TIME_START, configs.ELECTION_TIME_END))

		rf.persist() // update rf.currentTerm
		return true
	}
	return false
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) getState() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) ReadState() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) applied() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		return true
	}
	return false
}

// return true means it find this term
func (rf *Raft) bSearchLeftBound(l int, r int, log []LogEntry, term int) (int, int, bool) {
	if len(log) == 0 { return -1, -1, false }
	for l < r {
		mid := (l + r) / 2
		if log[mid].Term < term { l = mid + 1 } else { r = mid }
	}
	return l, r, log[l].Term == term
}

// return true means it find this term
func (rf *Raft) bSearchRightBound(l int, r int, log []LogEntry, term int) (int, int, bool) {
	if len(log) == 0 { return -1, -1, false }
	for l < r {
		mid := (l + r + 1) / 2
		if log[mid].Term <= term { l = mid } else { r = mid - 1 }
	}
	return l, r, log[l].Term == term
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func timeout(start int64, end int64) time.Duration {
	ms := start + (rand.Int63() % (end - start)) // ms [start, end]
	return time.Duration(ms) * time.Millisecond
}

func heartime(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.timer.Reset(timeout(configs.ELECTION_TIME_START, configs.ELECTION_TIME_END))
		<- rf.timer.C
		if !rf.isState(LEADER) && rf.killed() == false { // followers or candidates timeout and start election
			if configs.HIGH_CONCURRENCY_ELECTION {
				go rf.elect()
			} else {
				go rf.startElection() 
			}
		} 
	}
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index, term, isLeader := -1, rf.currentTerm, rf.state == LEADER
	if !isLeader { return -1, -1, false} // this server isn't the leader
	rf.log = append(rf.log, LogEntry{command, term}) // append log entry
	index = rf.sliceIndex2logIndex(len(rf.log) - 1) // sliceIndex2logIndex

	rf.persist()

	utils.Debugger(utils.DLeader, "S%d Leader, Start agreement, index %d, term %d", rf.me, index, term)
	for i := 0; i < len(rf.peers); i ++ { rf.triggerCond[i].Broadcast() }
	return index, term, isLeader
}

func Make(peers []*labrpc.ClientEnd, me int,
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:peers, // raft's leader election requires that len(rf.peers) is a odd. Then, it's effective to avoid the 'brain split'.
		persister:persister, 
		me:me,
		state:FOLLOWER, 
		currentTerm:0, // 0
		votedFor:-1, // -1
		timer:time.NewTimer(timeout(configs.ELECTION_TIME_START, configs.ELECTION_TIME_END)),
		nextIndex:make([]int, len(peers)),
		matchIndex:make([]int, len(peers)),
		log:[]LogEntry{LogEntry{nil,0}},
		applyCh:applyCh,
		lastApplied:-1, // -1
		triggerCond:make([]*sync.Cond, len(peers)),
		electCond:make([]*sync.Cond, len(peers)),
		snapshotCond:make([]*sync.Cond, len(peers)),
		ticket:1, // 1
		lastIncludedIndex:-1, // -1
		snapshot:nil, // nil
		currentLeaderId:rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(peers)), // random
	}

	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.candidateCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	
	for i := 0 ; i < len(rf.peers) ; i ++ {
		rf.triggerCond[i] = sync.NewCond(&rf.mu)
		rf.electCond[i] = sync.NewCond(&rf.mu)
		rf.snapshotCond[i] = sync.NewCond(&rf.mu)
	}

	// raft reads persistent state and application reads snapshot
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	utils.Debugger(utils.DInfo, "S%d %s, Created", rf.me, rf.getState())

	// start ticker goroutine to start elections
	// daemon goroutine
	// election
	go rf.ticker()
	if configs.HIGH_CONCURRENCY_ELECTION {
		go rf.committee() 
		go rf.ticketer()
	}

	// replication
	go rf.trigger()
	go rf.heartbeat()
	go rf.checker()
	go rf.applier()
	// snapshot
	go rf.syncer()

	return rf
}