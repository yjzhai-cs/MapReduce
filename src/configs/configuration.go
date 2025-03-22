package configs

// utils package
const KVRAFT_DEBUG = false
const RAFT_DEBUG = false

// raft package
const MAXREQUESTCOUNT = 1
const FASTBACKUP = true
const HIGH_CONCURRENCY_ELECTION = true

const ELECTION_TIME_START = 105
const ELECTION_TIME_END = 335

const HEARTBEAT_TIME_START = 45
const HEARTBEAT_TIME_END = 46

const REQUEST_TIMEOUT_START = 33
const REQUEST_TIMEOUT_END = 34

// kvraft package
const KVRAFT_REQUEST_TIMEOUT_START = 160
const KVRAFT_REQUEST_TIMEOUT_END = 161