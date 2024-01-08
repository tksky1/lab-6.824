package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"log"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	isleader = rf.state == 2

	return term, isleader
}

// 恢复持久化的快照
func (rf *Raft) readPersistSnapshot() {
	data := rf.persister.ReadSnapshot()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var SnapshotData []byte
	if d.Decode(&SnapshotData) != nil {
		log.Println("读持久化错误！")
	} else {
		rf.SnapshotData = SnapshotData
	}
}

// the service says it has created a Snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.setSnapshot(index, snapshot)
	println(rf.me, "snapshot saved, now lenLog:", len(rf.Log))
	println("now baseIndex:", rf.BaseIndex)

}

// <--------- Request Vote --------->

func (rf *Raft) sendRequestVote(server *labrpc.ClientEnd, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := server.Call("Raft.RequestVoteHandler", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.CurrentTerm < reply.Term {
		rf.setFollower()
		rf.CurrentTerm = reply.Term
		rf.persist()
		return
	}

	if rf.CurrentTerm == args.Term && rf.state == 1 && reply.VoteGranted {
		rf.votesGot++
		if rf.votesGot > len(rf.peers)/2 && rf.state == 1 {
			rf.becomeLeader()
			rf.votesGot = 0
		}
	}

}

func (rf *Raft) becomeLeader() {
	rf.state = 2
	rf.nextIndex = make([]int, len(rf.peers))
	//log.Println(rf.me, "成为了Leader！")
	lenLog := len(rf.Log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lenLog + rf.BaseIndex
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = rf.BaseIndex // 不安全？
	}
	go rf.doHeartBeat()
}

func (rf *Raft) startElection() {
	rf.state = 1
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	rf.votesGot = 1
	rf.nextExpireTime = generateNextExpireTime()
	go rf.syncSendRequestVote(rf.CurrentTerm)
}

// 添加一个参数，以检查解锁期间term变动
func (rf *Raft) syncSendRequestVote(termBeforeUnlock int) {
	rf.mu.Lock()
	if termBeforeUnlock != rf.CurrentTerm {
		rf.mu.Unlock()
		return
	}
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		arg := RequestVoteArgs{
			rf.CurrentTerm, rf.me,
			rf.Log[len(rf.Log)-1].Index, rf.Log[len(rf.Log)-1].Term,
		}
		thePeer := *peer
		go rf.sendRequestVote(&thePeer, &arg)
	}
	rf.mu.Unlock()
}

// follower判断是否该造反
func (rf *Raft) ticker() {
	var msg []ApplyMsg
	var channel chan ApplyMsg
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		channel = rf.applyCh

		if rf.lastApplied < rf.BaseIndex {
			rf.lastApplied = rf.BaseIndex
		}

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msg = append(msg, ApplyMsg{
				true, rf.Log[rf.lastApplied-rf.BaseIndex].Command,
				rf.Log[rf.lastApplied-rf.BaseIndex].Index,
				false, nil, 0, 0,
			})
		}

		nowTime := time.Now()
		if nowTime.After(rf.nextExpireTime) && rf.state != 2 {
			rf.startElection()
		}
		rf.mu.Unlock()
		// chan发送独立出来，防止持有锁的情况下channel阻塞导致死锁（上层调snapshot()以后会阻塞，而snapshot需要锁）
		if msg != nil {
			for _, message := range msg {
				channel <- message
			}
			msg = nil
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// <--------- AppendEntries --------->

func (rf *Raft) innerHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != 2 || rf.killed() {
		return
	}

	lenLog := len(rf.Log)

	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		var log2send []LogEntry
		if lenLog >= rf.nextIndex[i]-rf.BaseIndex {
			if rf.nextIndex[i] <= rf.BaseIndex {
				// 如果所需信息已经被截掉
				args := InstallSnapshotArgs{
					Term:              rf.CurrentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.BaseIndex,
					LastIncludedTerm:  rf.BaseTerm,
					Data:              rf.SnapshotData,
				}
				go rf.sendInstallSnapshot(rf.peers[i], &args, i)
				continue
			}
			log2send = rf.Log[(rf.nextIndex[i] - rf.BaseIndex):]
		}
		arg := AppendEntriesArgs{}
		if rf.nextIndex[i]-1-rf.BaseIndex == 0 {
			// prevLogIndex 已经不在log中了，要从baseIndex读
			if rf.BaseIndex == 0 {
				// 之前没有snapshot
				arg = AppendEntriesArgs{
					rf.CurrentTerm, rf.me, 0,
					-1, log2send, rf.commitIndex,
				}
			} else {
				// 有snapshot
				arg = AppendEntriesArgs{
					rf.CurrentTerm, rf.me, rf.BaseIndex,
					rf.BaseTerm, log2send, rf.commitIndex,
				}
			}

		} else {
			arg = AppendEntriesArgs{
				rf.CurrentTerm, rf.me, rf.nextIndex[i] - 1,
				rf.Log[rf.nextIndex[i]-1-rf.BaseIndex].Term, log2send, rf.commitIndex,
			}
		}

		thePeer := *peer
		go rf.sendAppendEntries(&thePeer, &arg, i)
	}
}

func (rf *Raft) doHeartBeat() {
	for {
		rf.innerHeartBeat()
		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) sendAppendEntries(server *labrpc.ClientEnd, args *AppendEntriesArgs, serverID int) {
	reply := AppendEntriesReply{}
	ok := server.Call("Raft.AppendEntriesHandler", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm != args.Term || rf.state != 2 {

		return
	}
	if rf.CurrentTerm < reply.Term {
		rf.setFollower()
		rf.CurrentTerm = reply.Term
		rf.persist()
		return
	}

	if reply.Success {
		rf.matchIndex[serverID] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverID] = rf.matchIndex[serverID] + 1
		// 大多数match后即可提交
		for i := len(rf.Log) - 1 + rf.BaseIndex; i >= rf.matchIndex[serverID] && i > rf.commitIndex; i-- {
			count := 0
			for j := range rf.peers {
				if rf.matchIndex[j] >= i {
					count++
				}
				if count > (len(rf.peers)/2) && rf.Log[i-rf.BaseIndex].Term == rf.CurrentTerm {
					rf.commitIndex = i
					println("leader", rf.me, "committed:", rf.commitIndex)
					break
				}
			}
		}

	} else {
		// 根据实验页面指引做的优化
		//  Case 1: leader doesn't have XTerm:
		//    nextIndex = XIndex
		//  Case 2: leader has XTerm:
		//    nextIndex = leader's last entry for XTerm
		//  Case 3: follower's log is too short:
		//    nextIndex = XLen
		// 正确性：log的term一定是递增且连续的; term高的才可能成为leader;
		// peer缺少XTerm的log：回退到XTerm的初始位置(XIndex)以补完;
		// peer在XTerm的log比leader多：回退到leader在XTerm的log的末尾位置以覆盖多余log.
		if (reply.XLen - 1) < args.PrevLogIndex {
			if reply.XLen != -1 {
				rf.nextIndex[serverID] = reply.XLen
			}
		} else {
			lastXTerm := -1
			for i := len(rf.Log) - 1; i >= 0; i-- {
				if rf.Log[i].Term == reply.XTerm {
					lastXTerm = i + rf.BaseIndex
					break
				}
			}
			if lastXTerm == -1 {
				rf.nextIndex[serverID] = reply.XIndex
			} else {
				rf.nextIndex[serverID] = lastXTerm
			}
		}
	}
}

func (rf *Raft) sendInstallSnapshot(server *labrpc.ClientEnd, args *InstallSnapshotArgs, serverID int) {
	println("leader", rf.me, "正在发送InstallSnapshot 到", serverID, ",截止index为", args.LastIncludedIndex)
	reply := InstallSnapshotReply{}
	ok := server.Call("Raft.InstallSnapshotHandler", args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.CurrentTerm != args.Term || rf.state != 2 {
		return
	}
	if rf.CurrentTerm < reply.Term {
		rf.setFollower()
		rf.CurrentTerm = reply.Term
		rf.persist()
		return
	}

	if rf.matchIndex[serverID] < args.LastIncludedIndex {
		rf.matchIndex[serverID] = args.LastIncludedIndex
		rf.nextIndex[serverID] = rf.matchIndex[serverID] + 1
		println("置", serverID, "的matchIndex为", args.LastIncludedIndex)
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()

	rf.readPersist(rf.persister.ReadRaftState())

	index = len(rf.Log) + rf.BaseIndex
	term = rf.CurrentTerm
	isLeader = rf.state == 2
	if isLeader {
		newLog := LogEntry{command, term, index}
		rf.Log = append(rf.Log, newLog)
		rf.persist()
		rf.matchIndex[rf.me] = index
		rf.mu.Unlock()
		rf.innerHeartBeat()
	} else {
		rf.mu.Unlock()
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = []LogEntry{}
	rf.Log = append(rf.Log, LogEntry{nil, -1, 0})
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesGot = 0
	rf.state = 0
	rf.nextExpireTime = generateNextExpireTime()
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readPersistSnapshot()
	rf.lastApplied = rf.BaseIndex
	rf.commitIndex = rf.BaseIndex

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
