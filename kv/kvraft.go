package main

import (
	"fmt"
	"raft"
	"time"
)

// start a kvraft fleet
func main() {

	// start Raft fleet
	s := raft.SetupUnixSocketFolder()
	defer raft.CleanupUnixSocketFolder(s)

	nodes := []string{raft.Get_socket_name("node1"), raft.Get_socket_name("node2"), raft.Get_socket_name("node3"), raft.Get_socket_name("node4"), raft.Get_socket_name("node5")}
	applyChans := []chan raft.ApplyMsg{}
	// create all applychannels
	for i := 0; i < len(nodes); i++ {
		applyChans = append(applyChans, make(chan raft.ApplyMsg))
	}
	// create all raft nodes
	rafts := []*raft.Raft{}
	for i := 0; i < len(nodes); i++ {
		rafts = append(rafts, raft.CreateNode(nodes, nodes[i], i, applyChans[i]))
	}

	// start running
	raft.StartRaft(rafts, nodes, applyChans)

	time.Sleep(1 * time.Second)

	// start KV fleet
	KVservers := []*KVNode{}
	for i := 0; i < len(nodes); i++ {
		KVservers = append(KVservers, CreateKVNode(rafts[i], nodes[i], i, applyChans[i]))
	}

	time.Sleep(1 * time.Second)

	// create 2 clients
	client1 := CreateClient(KVservers)
	client2 := CreateClient(KVservers)

	// client1 put some data into the KV fleet
	client1.ClientPutAppend("a", "1", "Put")
	client1.ClientPutAppend("b", "2", "Put")
	client1.ClientPutAppend("c", "3", "Put")
	client1.ClientPutAppend("d", "4", "Put")
	client1.ClientPutAppend("e", "5", "Put")
	client1.ClientPutAppend("f", "6", "Put")
	client1.ClientPutAppend("g", "7", "Put")

	time.Sleep(1 * time.Second)

	// client2 get some data from the KV fleet
	fmt.Println("a:", client2.ClientGet("a"))
	fmt.Println("b:", client2.ClientGet("b"))
	fmt.Println("c:", client2.ClientGet("c"))
	fmt.Println("d:", client2.ClientGet("d"))
	fmt.Println("e:", client2.ClientGet("e"))
	fmt.Println("f:", client2.ClientGet("f"))
	fmt.Println("g:", client2.ClientGet("g"))

	// shutdown raft fleet
	raft.ShutdownRaft(rafts)

	// shutdown KV fleet
	ShutdownKV(KVservers)
}
