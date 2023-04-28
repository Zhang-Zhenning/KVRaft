package main

import (
	"log"
	"math/rand"
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
func (rf *Raft) SendVoteRequest(server int, args *RequestVoteArgs, repl *RequestVoteReply) bool {
	finishChan := make(chan bool)
	var ret bool = false

	// use a new thread to invoke rpc call
	go func() {
		rpcret := call(rf.peers[server], "Raft.HandleRequestVote", args, repl)
		finishChan <- rpcret
	}()

	select {
	case ret = <-finishChan:
	case <-time.After(HeartbeatInterval):
	}

	return ret
}

// handle the request vote from other peers
// can be invoked from every peer not only Leader
func (rf *Raft) HandleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = true
	reply.Term, _ = rf.GetTerm()

	// check whether is term of candidate is smaller than current term
	if reply.Term > args.Term {
		reply.VoteGranted = false
		return
	}

	// if the term of candidate is larger than current term, update current term
	if reply.Term < args.Term {
		rf.SetTerm(args.Term)
		rf.SetStatus(Follower)
	}

	// if we have voted, then refuse
	if rf.VoteCount > 0 && rf.VoteFor != args.LeaderId {
		reply.VoteGranted = false
		return
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
	}
}

// append entries sent to other peers
// can only be sent from Leader
func (rf *Raft) SendAppendEntryRequest(server int, args *AppendEntriesArgs, repl *AppendEntriesReply) bool {
	finishChan := make(chan bool)
	var ret bool = false

	// use a new thread to invoke rpc call
	go func() {
		rpcret := call(rf.peers[server], "Raft.HandleAppendEntry", args, repl)
		finishChan <- rpcret
	}()

	select {
	case ret = <-finishChan:
	case <-time.After(time.Millisecond * 400):
	}

	return ret
}

// handle request append entry rpc call
func (rf *Raft) HandleAppendEntry(args *AppendEntriesArgs, repl *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	currentTerm, _ := rf.GetTerm()
	repl.Term = currentTerm
	repl.Success = true

	// leader's term is smaller than current node's term
	// refuse sync the log
	if args.Term < currentTerm {
		repl.Success = false
		return
	}

	// if the args is the old one
	// refuse sync the log
	if rf.isOldRequest(args) {
		repl.Success = false
		return
	}

	// update the term and status
	rf.SetTerm(args.Term)
	rf.SetStatus(Follower)
	rf.ResetElectionTimer()

	// get latest log index and term
	_, latestLogIndex := rf.GetLatestLogTermAndIndex()

	// check whether the log is consistent
	if args.PrevLogIndex > 0 {
		if args.PrevLogIndex > latestLogIndex {
			// the log is not consistent, refuse to sync
			repl.Success = false
			repl.LastApplied = rf.LastApplied // tell leader the last applied one so the leader can send the new one
			return
		}

		flg, bestterm := rf.GetLogTerm(args.PrevLogIndex)
		if flg == true && bestterm != args.PrevLogTerm {
			// the log is not consistent, refuse to sync
			repl.Success = false
			repl.LastApplied = rf.LastApplied // tell leader the last applied one so the leader can send the new one
			return
		}
		if flg == false && bestterm < args.PrevLogTerm {
			// the log is not consistent, refuse to sync
			repl.Success = false
			repl.LastApplied = rf.LastApplied // tell leader the last applied one so the leader can send the new one
			return
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

	return
}

// apply all the committed log entries
func (rf *Raft) Apply() {

	if rf.Status == Leader {
		rf.UpdateCommitIndex()
	}

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
			if !rst {
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
		rf.SetStatus(Leader)
		rf.ResetElectionTimer()
		rf.SyncLogNow()
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

// send request append entries to peer, forcing sync between peer and leader
// return when the sync is successful or the node is no longer leader
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
		if rpcRet && isLeader {
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
func (rf *Raft) ImplementCommand(command interface{}) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = 0
	term, isLeader = rf.GetTerm()
	if !isLeader {
		return
	}

	if isLeader {
		index = rf.InsertLogEntry(command)
		rf.SyncLogNow()
	}
	return
}

// kill the node
func (rf *Raft) Kill() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.IsKilled = true
	// shutdown the election loop
	rf.ElectionTimer.Reset(0)
	// shutdown the sync log loop
	rf.SyncLogNow()
}

// create a Raft node
func CreateNode(peers []string, myId int, applyChan chan ApplyMsg) *Raft {
	rf := new(Raft)
	rf.peers = peers
	rf.me = myId
	rf.Term = 0
	rf.CommitIndex = 0
	rf.LastApplied = 0
	rf.ApplyCh = applyChan
	rf.ElectionTime = rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)))
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
		go rf.SyncLogLoop(i)
	}

	// start the election loop
	go rf.ElectionLoop()

	return rf
}
