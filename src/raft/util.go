package raft

import "log"

const (
	FOLLOWER = "Follower"
	CANDIDATE = "Candidate"
	LEADER = "Leader"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
