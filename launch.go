package main

import (
	"fmt"
)

func main() {

	// setup/cleanup the socket files
	s := SetupUnixSocketFolder()
	defer CleanupUnixSocketFolder(s)

	// get all commands
	commands := []string{}
	for i := 0; i < 500; i++ {
		commands = append(commands, fmt.Sprintf("a = %d", i))
	}
	// get all nodes names
	nodes := []string{get_socket_name("node1"), get_socket_name("node2"), get_socket_name("node3"), get_socket_name("node4"), get_socket_name("node5"), get_socket_name("node6"), get_socket_name("node7"), get_socket_name("node8"), get_socket_name("node9"), get_socket_name("node10")}
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
	startRaft(rafts, nodes, applyChans)

	// send all commands, and wait for Raft fleet finishing all tasks
	writeCommands(nodes, commands)

	// shutdown all nodes
	shutdownRaft(rafts)
}
