package main

import (
	"fmt"
	"math/rand"
	"net"
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

const ElectionWinning int = -1000
const LeaderMaximumTime = time.Duration((time.Second * 20))

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

type CommandArgs struct {
	Command interface{}
}

type CommandReply struct {
	Success  bool
	IsLeader bool
}

// RAFT structure
type Raft struct {
	mu              sync.Mutex
	peers           []string
	me              int
	me_name         string
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

	listener      net.Listener
	shutdown_chan chan bool

	CustomerCommandIndex int
	CustomerCommandDone  chan bool

	// last appendrequest from leader
	LastReqFromLeader AppendEntriesArgs
}

// apply msg from the msg channels
func handleMsg(chans []chan ApplyMsg, raftnodes []string) {

	for i := 0; i < len(chans); i++ {
		go func(i int) {
			for {
				msg := <-chans[i]
				fmt.Printf("Node %s: implemented command %s\n", raftnodes[i], msg.Command)
			}
		}(i)

	}

}

// create command and send to the raft node
func sendCommands(raftnodes []string, cmds []string) {

	for i := 0; i < len(cmds); i++ {
		time.Sleep(1 * time.Second)
		ret := false

		for ret == false {
			for j := 0; j < len(raftnodes); j++ {
				retj := SendCommandToLeader(raftnodes[j], cmds[i])
				if retj == true {
					ret = true
					break
				}
			}
		}
	}
}

// kill all nodes
func killAllNodes(rafts []string) {
	for i := 0; i < len(rafts); i++ {
		SendKillRequest(rafts[i])
	}
}
