package main

import (
	"raft"
	"sync"
)

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
	OpIDChanDict    *map[int64]chan interface{}
}

func ShutdownKV(kvs []*KVNode) {
	for _, kv := range kvs {
		kv.StopKVNode()
	}
}
