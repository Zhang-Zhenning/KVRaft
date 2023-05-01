package raft

import (
	"encoding/gob"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// create folder for unix socket files
func SetupUnixSocketFolder() string {
	s := RPCServerPath + "/uid-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.RemoveAll(s)
	os.Mkdir(s, 0777)

	// register Operation struct to rpc
	gob.Register(Operation{})
	gob.Register(PutAppendArgs{})
	gob.Register(PutAppendReply{})
	gob.Register(GetArgs{})
	gob.Register(GetReply{})

	return s
}

// clean up the unix socket folder
func CleanupUnixSocketFolder(ss string) {
	os.RemoveAll(ss)
}

// Cook up a unique-ish UNIX-domain socket Worker_name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func Get_socket_name(suffix string) string {
	s := RPCServerPath + "/uid-"
	s += strconv.Itoa(os.Getuid()) + "/"
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

// rpc Call function
func Call(srv_name string, func_name string, args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv_name)
	if errx != nil {
		log.Fatal("dialing:", errx)
		return false
	}
	defer c.Close()

	err := c.Call(func_name, args, reply)
	if err != nil {
		log.Fatal("Call:", err)
		return false
	}

	return true
}
