package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"sort"
	"sync"
	"time"
)

// print debug info
func (rf *Raft) DebugPrint(args ...interface{}) {
	if enableDebug {
		log.Println(args...)
	}
}

// reset election timer
func (rf *Raft) ResetElectionTimer() {
	randCnt := rf.ElectionTime.Intn(250)
	duration := time.Duration(randCnt)*time.Millisecond + VoteInterval
	rf.ElectionTimer.Reset(duration)
}

// set status of current node
func (rf *Raft) SetStatus(status int) {

	if (rf.Status != Follower) && (status == Follower) {
		rf.ResetElectionTimer()
	}

	if (rf.Status != Leader) && (status == Leader) {
		index := len(rf.logs)
		for i := 0; i < len(rf.peers); i++ {
			rf.FollowerNextLogIndex[i] = index + 1 + rf.Snapshot.Index
			rf.FollowerMatchIndex[i] = 0
		}
	}

	rf.Status = status
}

// get status of current node
func (rf *Raft) GetStatus() int {

	return rf.Status
}

// get current term and leader status
func (rf *Raft) GetTerm() (int, bool) {

	return rf.Term, rf.Status == Leader
}

// set current term
func (rf *Raft) SetTerm(term int) {

	rf.Term = term
}

// get commit index
func (rf *Raft) GetCommitIndex() int {

	return rf.CommitIndex
}

// set commit index
func (rf *Raft) SetCommitIndex(index int) {

	rf.CommitIndex = index
}

// get latest log index and term
func (rf *Raft) GetLatestLogTermAndIndex() (int, int) {

	if len(rf.logs) == 0 {
		return rf.Snapshot.Term, rf.Snapshot.Index
	} else {
		return rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index
	}

}

// get the term of log at index
func (rf *Raft) GetLogTerm(index int) (bool, int) {

	if index-rf.Snapshot.Index-1 < 0 {
		if index == rf.Snapshot.Index {
			return true, rf.Snapshot.Term
		}
		return false, rf.Snapshot.Term
	} else {
		return true, rf.logs[index-rf.Snapshot.Index-1].Term
	}
}

// memorize the latest append entry request from leader
func (rf *Raft) RecordRequest(req *AppendEntriesArgs) {

	rf.LastReqFromLeader = *req
}

// get the log entries newer than or equal to a given index
func (rf *Raft) GetNewLogEntries(index int, snapshot *LogSnapshot, logentries *[]LogEntry) (preterm int, preindex int) {
	// firstly check whether the snapshot needs to be sent
	arraystart := index - rf.Snapshot.Index - 1
	if index <= rf.Snapshot.Index {
		*snapshot = rf.Snapshot
		arraystart = -1
	}

	// check the log entries in log array
	if arraystart < 0 {
		preterm = 0
		preindex = 0
	} else if arraystart == 0 {
		preterm = rf.Snapshot.Term
		preindex = rf.Snapshot.Index
	} else {
		preterm = rf.logs[arraystart-1].Term
		preindex = rf.logs[arraystart-1].Index
	}

	if arraystart < 0 {
		arraystart = 0
	}

	// copy log entries
	for i := arraystart; i < len(rf.logs); i++ {
		*logentries = append(*logentries, rf.logs[i])
	}

	return
}

// get the log entries we need to send to a given peer, only when the peer is a follower and we are leader
func (rf *Raft) GetLogEntriesFor(peer int) AppendEntriesArgs {

	var args AppendEntriesArgs
	args.Term = rf.Term
	args.LeaderId = rf.me
	args.LeaderCommit = rf.CommitIndex
	args.Snapshot = LogSnapshot{Index: 0}

	// the peer's next log index
	nextIndex := rf.FollowerNextLogIndex[peer]
	args.PrevLogTerm, args.PrevLogIndex = rf.GetNewLogEntries(nextIndex, &args.Snapshot, &args.Entries)

	return args
}

// set follower's next log index
func (rf *Raft) SetFollowerNextLogIndex(peer int, index int) {

	rf.FollowerNextLogIndex[peer] = index
}

// set follower's match index
func (rf *Raft) SetFollowerMatchIndex(peer int, index int) {

	rf.FollowerNextLogIndex[peer] = index + 1
	rf.FollowerMatchIndex[peer] = index
}

// insert a new log entry, only when we are leader
func (rf *Raft) InsertLogEntry(command interface{}) int {

	newl := LogEntry{
		Term:  rf.Term,
		Index: 1,
		Log:   command,
	}

	if len(rf.logs) > 0 {
		newl.Index = rf.logs[len(rf.logs)-1].Index + 1
	} else {
		newl.Index = rf.Snapshot.Index + 1
	}

	rf.logs = append(rf.logs, newl)
	return newl.Index
}

// leader update commit index
func (rf *Raft) UpdateCommitIndex() bool {
	rst := false
	var indexs []int
	if len(rf.logs) > 0 {
		rf.FollowerMatchIndex[rf.me] = rf.logs[len(rf.logs)-1].Index
	} else {
		rf.FollowerMatchIndex[rf.me] = rf.Snapshot.Index
	}
	for i := 0; i < len(rf.FollowerMatchIndex); i++ {
		indexs = append(indexs, rf.FollowerMatchIndex[i])
	}
	sort.Ints(indexs)
	index := len(indexs) / 2
	commit := indexs[index]
	if commit > rf.CommitIndex {
		rst = true
		rf.CommitIndex = commit
	}
	return rst
}

// check whether the args is the old one
// it is important to mark the heartbeat appendentry request (empty log entries but the latest term) as old
// which means if the current request's max index (previndex + snapshot.index + len(entries))
// is equal to the last request, we need to accept it
// then the vote counter will be reset (avoid the followers starting a new election)
func (rf *Raft) isOldRequest(req *AppendEntriesArgs) bool {

	if req.Term == rf.LastReqFromLeader.Term && req.LeaderId == rf.LastReqFromLeader.LeaderId {
		lastIndex := rf.LastReqFromLeader.PrevLogIndex + rf.LastReqFromLeader.Snapshot.Index + len(rf.LastReqFromLeader.Entries)
		reqLastIndex := req.PrevLogIndex + req.Snapshot.Index + len(req.Entries)
		return lastIndex > reqLastIndex
	}
	return false
}

// reset the heartbeat timer to 0
// start sync log entries to peers immediately
// only when we are leader
func (rf *Raft) SyncLogNow() {

	for i := 0; i < len(rf.peers); i++ {
		rf.HeartbeatTimers[i].Reset(0)
	}
}

// request vote sent to other peers
// can be sent from every peer not only Leader
func (rf *Raft) SendVoteRequest(server int, args *RequestVoteArgs, repl *RequestVoteReply) error {
	finishChan := make(chan bool)

	// use a new thread to invoke rpc call
	go func() {
		rpcret := call(rf.peers[server], "Raft.HandleRequestVote", args, repl)
		finishChan <- rpcret
	}()

	select {
	case <-finishChan:
	case <-time.After(HeartbeatInterval):
	}

	return nil
}

// send new command to the leader node
func SendCommandToLeader(server string, command interface{}) bool {
	finishChan := make(chan bool)

	// construct the args
	args := &CommandArgs{}
	args.Command = command

	// construct the reply
	reply := &CommandReply{IsLeader: false, Success: false}

	// use a new thread to invoke rpc call
	go func() {
		rpcret := call(server, "Raft.HandleCommand", args, reply)
		finishChan <- rpcret
	}()

	select {
	case <-finishChan:
		if (reply.IsLeader == true) && (reply.Success == true) {
			// send to a leader and get success response
			return true
		} else {
			// not send to a leader or leader fails to accept, return false and resend the command
			return false
		}
	case <-time.After(HeartbeatInterval * 7):
		fmt.Printf("In SendCommandToLeader: send command %s to Node %s timeout\n", command, server)
		return false
	}

}

// handle the command request
// if return then rf is the leader and the command is applied
// else never return!
func (rf *Raft) HandleCommand(args *CommandArgs, reply *CommandReply) error {
	rf.mu.Lock()
	_, isl := rf.GetTerm()
	reply.IsLeader = isl

	if isl == false {
		// not leader, return
		reply.Success = false
		rf.mu.Unlock()
		return nil
	}

	cdChan := make(chan bool)
	rf.ImplementCommand(args.Command, cdChan)
	rf.mu.Unlock()

	// wait for the command to be committed if it is leader
	select {
	case <-cdChan:
		// leader successfully applied the command
		reply.Success = true
	case <-time.After(HeartbeatInterval * 5):
		fmt.Printf("In HandleCommand: Node %s timeout when handle command %s\n", rf.me_name, args.Command)
		reply.Success = false
	}

	return nil
}

// handle the request vote from other peers
// can be invoked from every peer not only Leader
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = true
	reply.Term, _ = rf.GetTerm()

	// check whether is term of candidate is smaller than current term
	if reply.Term > args.Term {
		reply.VoteGranted = false
		return nil
	}

	// if the term of candidate is larger than current term, update current term
	if reply.Term < args.Term {
		rf.SetTerm(args.Term)
		rf.SetStatus(Follower)
	}

	// if we have voted, then refuse
	if rf.VoteCount > 0 && rf.VoteFor != args.LeaderId {
		reply.VoteGranted = false
		return nil
	}

	// check whether the candidate's log is at least as up-to-date as receiver's log
	logt, logi := rf.GetLatestLogTermAndIndex()
	if logt > args.LogTerm {
		reply.VoteGranted = false
	} else if logt == args.LogTerm {
		reply.VoteGranted = logi <= args.LogIndex
	}

	if reply.VoteGranted {
		rf.VoteFor = args.LeaderId
		rf.VoteCount = 1
		rf.ResetElectionTimer()
		//fmt.Printf("Node %s vote for %s\n", rf.peers[rf.me], rf.peers[args.LeaderId])
	}

	return nil
}

// append entries sent to other peers
// can only be sent from Leader
func (rf *Raft) SendAppendEntryRequest(server int, args *AppendEntriesArgs, repl *AppendEntriesReply) error {
	finishChan := make(chan bool)

	// use a new thread to invoke rpc call
	go func() {
		rpcret := call(rf.peers[server], "Raft.HandleAppendEntry", args, repl)
		finishChan <- rpcret
	}()

	select {
	case <-finishChan:
	case <-time.After(time.Millisecond * 400):
	}

	return nil
}

// handle request append entry rpc call
func (rf *Raft) HandleAppendEntry(args *AppendEntriesArgs, repl *AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm, _ := rf.GetTerm()
	repl.Term = currentTerm
	repl.Success = true

	// leader's term is smaller than current node's term
	// refuse sync the log
	if args.Term < currentTerm {
		repl.Success = false
		return nil
	}

	// if the args is the old one
	// refuse sync the log
	if rf.isOldRequest(args) {
		repl.Success = false
		return nil
	}

	// update the term and status
	rf.SetTerm(args.Term)
	rf.SetStatus(Follower)
	rf.ResetElectionTimer()

	// handle the election success entry
	if args.PrevLogIndex == ElectionWinning {
		rf.VoteCount = 0
		rf.VoteFor = -1
		return nil
	}

	// get latest log index and term
	_, latestLogIndex := rf.GetLatestLogTermAndIndex()

	// check whether the log is consistent
	// whether the incoming prevlogindex is smaller than the latest log index
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > latestLogIndex {
			// the log is not consistent, refuse to sync
			repl.Success = false
			repl.LastApplied = rf.LastApplied // tell leader the last applied one so the leader can send the new one
			return nil
		}

		flg, bestterm := rf.GetLogTerm(args.PrevLogIndex)
		if flg == true && bestterm != args.PrevLogTerm {
			// the log is not consistent, refuse to sync
			repl.Success = false
			repl.LastApplied = rf.LastApplied // tell leader the last applied one so the leader can send the new one
			return nil
		}
		if flg == false && bestterm < args.PrevLogTerm {
			// the log is not consistent, refuse to sync
			repl.Success = false
			repl.LastApplied = rf.LastApplied // tell leader the last applied one so the leader can send the new one
			return nil
		}

	}

	// update log entries and snapshot
	rf.RecordRequest(args)
	if len(args.Entries) > 0 || args.Snapshot.Index > 0 {
		start := args.PrevLogIndex
		if args.Snapshot.Index > 0 {
			rf.Snapshot = args.Snapshot
			start = rf.Snapshot.Index
		}

		index := start - rf.Snapshot.Index
		for i := 0; i < len(args.Entries); i++ {
			if index+i < 0 {
				continue
			}
			if index+i < len(rf.logs) {
				rf.logs[index+i] = args.Entries[i]
			} else {
				rf.logs = append(rf.logs, args.Entries[i])
			}
		}

		size := index + len(args.Entries)
		if size < 0 {
			size = 0
		}

		rf.logs = rf.logs[:size]
	}

	// update commit index
	rf.SetCommitIndex(args.LeaderCommit)
	rf.Apply()

	return nil
}

// apply all the committed log entries
func (rf *Raft) Apply() {

	if rf.Status == Leader {
		rf.UpdateCommitIndex()
	}

	lastapplied := rf.LastApplied

	// apply the snapshot
	if rf.Snapshot.Index > rf.LastApplied {
		appmsg := ApplyMsg{
			CommandValid: false,
			Command:      rf.Snapshot,
			CommandIndex: 0,
		}

		rf.ApplyCh <- appmsg
		rf.LastApplied = rf.Snapshot.Index
	}

	last := 0
	if len(rf.logs) > 0 {
		last = rf.logs[len(rf.logs)-1].Index
	}

	// apply the log entries
	for ; rf.LastApplied < rf.CommitIndex && rf.LastApplied < last; rf.LastApplied++ {
		index := rf.LastApplied
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[index-rf.Snapshot.Index].Log,
			CommandIndex: rf.logs[index-rf.Snapshot.Index].Index,
		}
		rf.ApplyCh <- msg
	}

	// if in this round we have applied some log entries
	if rf.LastApplied > lastapplied {
		_, p1 := rf.GetLogTerm(lastapplied + 1)
		_, p2 := rf.GetLogTerm(rf.LastApplied)

		if rf.Status == Leader {
			// anounce the customer the command is done, can send the next one
			if (rf.CustomerCommandIndex > lastapplied) && (rf.CustomerCommandIndex <= rf.LastApplied) && (rf.CustomerCommandDone != nil) {
				// notify customer the command is done, so the customer can send the next command
				rf.CustomerCommandDone <- true
				// reset the customer command index and channel
				rf.CustomerCommandIndex = -1
			}
		}

		fmt.Printf("Node %s pushed logs from index %d (term %d) to index %d (term %d) into implementation channel\n", rf.peers[rf.me], lastapplied+1, p1, rf.LastApplied, p2)
	}
}

// send election winning message to other peers
func (rf *Raft) SendElectionWinning() {

	args := AppendEntriesArgs{
		Term:         rf.Term,
		LeaderId:     rf.me,
		PrevLogIndex: ElectionWinning,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.CommitIndex,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		} else {
			go func(server int) {
				repl := AppendEntriesReply{Term: 0}
				rf.SendAppendEntryRequest(server, &args, &repl)
			}(i)
		}
	}
}

// request vote, try to be a new leader
func (rf *Raft) Election() {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.SetTerm(rf.Term + 1)
	logt, logi := rf.GetLatestLogTermAndIndex()
	electTerm, _ := rf.GetTerm()

	voteArgs := RequestVoteArgs{
		LeaderId: rf.me,
		Term:     electTerm,
		LogTerm:  logt,
		LogIndex: logi,
	}

	var waitPeerFinish sync.WaitGroup
	waitPeerFinish.Add(len(rf.peers))

	numGranted := 0
	term := electTerm

	// start sending request vote rpc call to other peers
	for i := 0; i < len(rf.peers); i++ {
		go func(server int) {
			defer waitPeerFinish.Done()
			resp := RequestVoteReply{-1, false}
			if server == rf.me {
				numGranted += 1
				return
			}
			rst := rf.SendVoteRequest(server, &voteArgs, &resp)
			if rst != nil {
				return
			}
			if resp.VoteGranted == true {
				numGranted += 1
				return
			}
			if resp.Term > term {
				term = resp.Term
			}
		}(i)
	}

	// wait until all the rpc call finish
	waitPeerFinish.Wait()

	if term > electTerm {
		rf.SetTerm(term)
		rf.SetStatus(Follower)
		rf.ResetElectionTimer()
		return
	} else if numGranted*2 > len(rf.peers) {
		fmt.Printf("Node %s win the election in term %d\n", rf.peers[rf.me], rf.Term)
		rf.SetStatus(Leader)
		rf.ResetElectionTimer()
		rf.SendElectionWinning()
		rf.SyncLogNow()
		go rf.KillLeaderAfterTime(LeaderMaximumTime)
		return
	}

}

// election loop, should never terminate unless the node is dead
func (rf *Raft) ElectionLoop() {
	rf.ResetElectionTimer()
	defer rf.ElectionTimer.Stop()

	for !rf.IsKilled {
		<-rf.ElectionTimer.C
		if rf.IsKilled {
			break
		}

		// if the node is candidate, start a new election
		if rf.Status == Candidate {
			rf.ResetElectionTimer()
			rf.Election()
		} else if rf.Status == Follower {
			rf.ResetElectionTimer()
			rf.SetStatus(Candidate)
			rf.Election()
		}
	}
}

// kill the leader after certain time, for testing purpose
func (rf *Raft) KillLeaderAfterTime(d time.Duration) {
	time.Sleep(d)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.SetStatus(Follower)
}

// send request append entries to peer, forcing sync between peer and leader
// return when the sync is successful or the node is no longer leader
// it works also as a heartbeat checker to remind follower that the leader is alive
// after update log to follower, leader will constantly (after each heartbeat time) send
// an appendentry request with no log entries but with the leader term to follower
func (rf *Raft) SyncLog(server int) (sync_success bool) {
	sync_success = false
	if server == rf.me {
		return
	}

	continueLoop := true
	for continueLoop {
		continueLoop = false

		curTerm, isLeader := rf.GetTerm()

		// only leader can sync logs to peers
		if !isLeader || rf.IsKilled {
			break
		}

		arg := rf.GetLogEntriesFor(server)
		rep := AppendEntriesReply{Term: 0}
		//rf.mu.Unlock()

		// send rpc call to peer
		rpcRet := rf.SendAppendEntryRequest(server, &arg, &rep)

		//rf.mu.Lock()
		//curTerm, isLeader = rf.GetTerm()

		// if rpc call failed, retry
		if (rpcRet == nil) && isLeader {
			// if the peer has larger term, convert to follower
			if rep.Term > curTerm {
				rf.SetTerm(rep.Term)
				rf.SetStatus(Follower)
			} else if !rep.Success {
				rf.SetFollowerNextLogIndex(server, rep.LastApplied+1)
				continueLoop = true
			} else {
				// success
				sync_success = true
				if len(arg.Entries) > 0 {
					rf.SetFollowerMatchIndex(server, arg.Entries[len(arg.Entries)-1].Index)
				} else if arg.Snapshot.Index > 0 {
					rf.SetFollowerMatchIndex(server, arg.Snapshot.Index)
				}

			}
		} else {
			continueLoop = true

		}

	}

	return sync_success

}

// sync log loop, should never terminate unless the node is dead or the node is no longer leader
// to one peer
func (rf *Raft) SyncLogLoop(server int) {
	defer func() {
		rf.HeartbeatTimers[server].Stop()
	}()

	for !rf.IsKilled {
		<-rf.HeartbeatTimers[server].C

		if rf.IsKilled {
			break
		}

		rf.mu.Lock()
		rf.HeartbeatTimers[server].Reset(HeartbeatInterval)
		_, isLeader := rf.GetTerm()

		if isLeader {
			sync_success := rf.SyncLog(server)
			if sync_success {
				rf.Apply()
				rf.SyncLogNow()
			}
		}
		rf.mu.Unlock()

	}

}

// an interface to other threads to start a new command (a new log)
func (rf *Raft) ImplementCommand(command interface{}, donec chan bool) (index int, term int, isLeader bool) {

	index = 0
	term, isLeader = rf.GetTerm()
	if !isLeader {
		return
	}

	if isLeader {
		index = rf.InsertLogEntry(command)
		rf.CustomerCommandDone = donec
		rf.CustomerCommandIndex = index
		rf.SyncLogNow()
	}
	return
}

// kill the node
func (rf *Raft) Kill(_ *struct{}, _ *struct{}) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.IsKilled = true
	// shutdown the election loop
	rf.ElectionTimer.Reset(0)
	// shutdown the sync log loop
	rf.SyncLogNow()
	// shutdown the server
	time.Sleep(2 * time.Second)
	rf.shutdown_chan <- true
	rf.listener.Close()

	return nil
}

// customer sends shutdown signal to the node
func SendKillRequest(server string) error {
	finishChan := make(chan bool)

	// use a new thread to invoke rpc call
	go func() {
		rpcret := call(server, "Raft.Kill", new(struct{}), new(struct{}))
		finishChan <- rpcret
	}()

	select {
	case <-finishChan:
	case <-time.After(time.Millisecond * 400):
	}

	return nil
}

// deploy the rf node
func (rf *Raft) Deploy() {
	go rf.ElectionLoop()
	for i := range rf.peers {
		if i != rf.me {
			go rf.SyncLogLoop(i)
		}
	}
}

// create a Raft node
func CreateNode(peers []string, nam string, myId int, applyChan chan ApplyMsg) *Raft {
	rf := new(Raft)
	rf.shutdown_chan = make(chan bool, 5)
	rf.peers = peers
	rf.me = myId
	rf.me_name = nam
	rf.Term = 0
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.ApplyCh = applyChan
	rf.ElectionTime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
	rf.CommitIndex = -1
	rf.CustomerCommandDone = nil
	rf.IsKilled = false
	rf.HeartbeatTimers = make([]*time.Timer, len(rf.peers))
	rf.ElectionTimer = time.NewTimer(VoteInterval)
	rf.FollowerNextLogIndex = make([]int, len(rf.peers))
	rf.FollowerMatchIndex = make([]int, len(rf.peers))
	rf.SetStatus(Follower)
	rf.LastReqFromLeader = AppendEntriesArgs{
		Term:     -1,
		LeaderId: -1,
	}
	rf.Snapshot = LogSnapshot{
		Index: 0,
		Term:  0,
	}

	// start the log sync loop
	for i := 0; i < len(rf.peers); i++ {
		rf.HeartbeatTimers[i] = time.NewTimer(HeartbeatInterval)
	}

	// start the election loop

	return rf
}

// start server
func (rf *Raft) StartServer(serverWg *sync.WaitGroup) {

	// register rpc
	rpcs := rpc.NewServer()
	err := rpcs.Register(rf)
	//os.Remove(rf.me_name)
	if err != nil {
		fmt.Printf("Node %s: Error in registering rpc: %s\n", rf.me_name, err)
		return
	}

	// listen to incoming rpc calls
	lis, er := net.Listen("unix", rf.me_name)
	if er != nil {
		fmt.Printf("Node %s: Error in listening: %s\n", rf.me_name, er)
		return
	}

	rf.listener = lis
	fmt.Printf("Node %s: start server\n", rf.me_name)

	// announce the server is ready
	serverWg.Done()

	// start the loop
	for {
		select {
		case <-rf.shutdown_chan:
			break
		default:
		}

		conc, er := rf.listener.Accept()
		if er != nil {
			//fmt.Printf("Node %s: Error in accepting: %s\n", rf.me_name, er)
			break
		} else {
			go func() {
				rpcs.ServeConn(conc)
				conc.Close()
			}()
		}

	}
	fmt.Printf("Node %s: shutdown server\n", rf.me_name)

}

// an interface for write operation to Raft layer
func Write(raftnodes []string, cmd string) {
	ret := false
	for ret == false {
		for j := 0; j < len(raftnodes); j++ {
			retj := SendCommandToLeader(raftnodes[j], cmd)
			if retj == true {
				ret = true
				break
			}
		}
	}

}
