package main

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync/atomic"
	"time"
)

type Clerk struct {
	servers []*KVNode
	me      int64
	msgId   int64
}

func GenerateClerkId() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func CreateClerk(servers []*KVNode) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.me = GenerateClerkId()
	ck.msgId = 0
	return ck
}

// client get interface
// if timeout, return "nil"
func (ck *Clerk) Get(key string) string {
	req := GetArgs{
		Key: key,
	}

	for i := 0; i < 8000; i++ {
		resp := GetReply{}
		ck.servers[i%len(ck.servers)].Get(&req, &resp)
		if resp.Success {
			return resp.Value
		}
		time.Sleep(time.Millisecond * 5)
	}

	fmt.Printf("Client %d: Get %s timeout\n", ck.me, key)
	return "nil"

}

// client put/append interface
func (ck *Clerk) PutAppend(key string, value string, op string) {
	req := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Me:    ck.me,
		MsgId: atomic.AddInt64(&ck.msgId, 1),
	}

	for i := 0; i < 8000; i++ {
		resp := PutAppendReply{}
		ck.servers[i%len(ck.servers)].PutAppend(&req, &resp)
		if resp.Success {
			return
		}
		time.Sleep(time.Millisecond * 5)
	}

	fmt.Printf("Client %d: %s %s--%s timeout\n", op, ck.me, key, value)
}
