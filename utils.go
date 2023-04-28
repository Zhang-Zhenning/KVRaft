package main

import (
	"math/rand"
	"sync"
	"time"
)

// state status
const Follower int = 1
const Leader int = 2
const Candidate int = 3

// heartbeat interval
const HeartbeatInterval = time.Duration(time.Millisecond * 600)
const VoteInterval = HeartbeatInterval * 2

const enableDebug bool = false

const RPCServerPath string = "."

// apply structure
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// log structure
type LogEntry struct {
	Term  int
	Index int
	Log   interface{}
}

// log snapshot structure
type LogSnapshot struct {
	Term  int
	Index int
	Datas []byte
}

// request vote structure
type RequestVoteArgs struct {
	LeaderId int
	Term     int
	LogIndex int
	LogTerm  int
}

// request vote reply structure
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// request append entries structure
type AppendEntriesArgs struct {
	LeaderId     int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	Snapshot     LogSnapshot
}

// request append entries reply structure
type AppendEntriesReply struct {
	Term        int
	Success     bool
	LastApplied int
}

// RAFT structure
type Raft struct {
	mu              sync.Mutex
	peers           []string
	me              int
	logs            []LogEntry
	Snapshot        LogSnapshot
	CommitIndex     int
	LastApplied     int
	Status          int
	Term            int
	HeartbeatTimers []*time.Timer
	ElectionTimer   *time.Timer
	ElectionTime    *rand.Rand

	FollowerNextLogIndex []int
	FollowerMatchIndex   []int

	VoteCount int
	VoteFor   int

	ApplyCh  chan ApplyMsg
	IsKilled bool

	// last appendrequest from leader
	LastReqFromLeader AppendEntriesArgs
}
