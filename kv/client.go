package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"raft"
	"sync/atomic"
	"time"
)

type KVClient struct {
	servers []*KVNode
	me      int64
	msgId   int64
}

func GenerateOpId() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func CreateClient(servers []*KVNode) *KVClient {
	ck := new(KVClient)
	ck.servers = servers
	ck.me = GenerateClientId()
	ck.msgId = 0
	return ck
}

// client get interface
// if timeout, return "nil"
func (ck *KVClient) ClientGet(key string) string {
	req := raft.GetArgs{
		Key: key,
	}

	for i := 0; i < 5000; i++ {
		resp := raft.GetReply{}
		ck.servers[i%len(ck.servers)].Get(&req, &resp)
		if resp.Success {
			fmt.Printf("Client %d: Get %s--%s\n", ck.me, key, resp.Value)
			return resp.Value
		}
		time.Sleep(time.Millisecond * 5)
	}

	fmt.Printf("Client %d: Get %s timeout\n", ck.me, key)
	return "nil"

}

// client put/append interface
func (ck *KVClient) ClientPutAppend(key string, value string, op string) {
	req := raft.PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Me:    ck.me,
		MsgId: atomic.AddInt64(&ck.msgId, 1),
	}

	for i := 0; i < 5000; i++ {
		resp := raft.PutAppendReply{}
		ck.servers[i%len(ck.servers)].PutAppend(&req, &resp)
		if resp.Success {
			fmt.Printf("Client %d: %s %s--%s\n", ck.me, op, key, value)
			return
		}
		time.Sleep(time.Millisecond * 5)
	}

	fmt.Printf("Client %d: %s %s--%s timeout\n", op, ck.me, key, value)
}

func GenerateClientId() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
