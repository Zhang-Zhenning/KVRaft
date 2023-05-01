package main

import (
	"raft"
	"sync"
)

type Err string

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

func ShutdownKV(kvs []*KVNode) {
	for _, kv := range kvs {
		kv.StopKVNode()
	}
}
