package main

import (
	"raft"
	"sync"
)

type Err string

// operation struct for key-value system
// will be served as command field in log struct
type Operation struct {
	Req interface{}
	Ch  chan interface{} // finish channel, will only be valid for leader (in other nodes it will be nil as channel can't be transfered by rpc)
}

// RaftKV node struct
type KVNode struct {
	mu              sync.Mutex
	me              int
	me_name         string
	RaftNode        *raft.Raft
	RaftApplyChan   chan raft.ApplyMsg
	KeyValueStorage map[string]string
	OperationRecord map[int64]int64
	KillChan        chan bool
	AppliedLogIndex int
}

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	Me    int64
	MsgId int64
}

type PutAppendReply struct {
	Success bool
	Err     Err
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Success bool
	Err     Err
	Value   string
}
