package main

import (
	"fmt"
	"sync"
	"time"
)

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
		time.Sleep(4 * time.Second)
		for j := 0; j < len(raftnodes); j++ {
			SendCommandToLeader(raftnodes[j], cmds[i])
		}
	}
}

func main() {

	// setup/cleanup the socket files
	s := SetupUnixSocketFolder()
	defer CleanupUnixSocketFolder(s)

	// get all commands
	commands := []string{"a = 1", "a = 2", "a = 3", "a = 4", "a = 5", "a = 6", "a = 7", "a = 8", "a = 9", "a = 10"}

	// get all nodes names
	nodes := []string{get_socket_name("node1"), get_socket_name("node2"), get_socket_name("node3"), get_socket_name("node4"), get_socket_name("node5")}
	applyChans := []chan ApplyMsg{}
	// create all applychannels
	for i := 0; i < len(nodes); i++ {
		applyChans = append(applyChans, make(chan ApplyMsg))
	}
	// create all raft nodes
	rafts := []*Raft{}
	for i := 0; i < len(nodes); i++ {
		rafts = append(rafts, CreateNode(nodes, nodes[i], i, applyChans[i]))
	}

	// start running
	fmt.Println("Hello, playground")

	var wg sync.WaitGroup
	wg.Add(len(nodes))

	// start all nodes'servers
	for i := 0; i < len(nodes); i++ {
		curIdx := i
		go func() { rafts[curIdx].StartServer(&wg) }()
	}

	wg.Wait()

	// handle all msg
	handleMsg(applyChans, nodes)

	// deploy all nodes
	for i := 0; i < len(nodes); i++ {
		go rafts[i].Deploy()
	}

	time.Sleep(1 * time.Second)

	go sendCommands(nodes, commands)

	for {
		time.Sleep(1 * time.Second)
	}

}
