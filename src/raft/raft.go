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
	"6.824/labgob"
	"bytes"
	"math/rand"
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	CurrentTerm int
	VotedFor    int `default:"-1"`
	Log         []LogEntry
	commitIndex int
	lastApplied int

	State          int           //状态，0 follower 1: candidate 2: leader
	nextActiveTime time.Time     //下次造反时间
	applyCh        chan ApplyMsg //make提供的与test交流的channel，发applyMsg用

	//candidate使用
	votesGot int //收到的票数

	//leader使用
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// return CurrentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.CurrentTerm
	isleader = rf.State == 2
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.CurrentTerm)
	if err != nil {
		println(err.Error())
	}
	err = e.Encode(rf.VotedFor)
	if err != nil {
		return
	}
	err = e.Encode(rf.Log)
	if err != nil {
		println(err.Error())
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 || len(rf.Log) > 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	if d.Decode(&CurrentTerm) != nil ||
		d.Decode(&VotedFor) != nil ||
		d.Decode(&Log) != nil {
		println("读取错误！")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArg struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int // 快速回退用
	ConflictIndex int // 快速回退时用
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//注意这个是handler
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}
	rf.readPersist(rf.persister.ReadRaftState())

	if args.Term < rf.CurrentTerm || (rf.VotedFor != -1 && rf.VotedFor != args.CandidateId) || rf.State != 0 {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		//println(rf.me, "同意了", args.CandidateId, "的投票请求")
		randomTerm := 150 + rand.Intn(500)
		rf.nextActiveTime = time.Now().Add(time.Duration(randomTerm) * time.Millisecond)
	}
	if args.Term > rf.CurrentTerm {
		rf.State = 0
		rf.CurrentTerm = args.Term
		rf.persist()
		reply.VoteGranted = true
		randomTerm := 150 + rand.Intn(500)
		rf.nextActiveTime = time.Now().Add(time.Duration(randomTerm) * time.Millisecond)
		//println(rf.me, "转follower并为", args.CandidateId, "投票")
	}

	//测试arg up-to-date
	lenLog := len(rf.Log)
	if rf.Log[lenLog-1].Term > args.LastLogTerm {
		reply.VoteGranted = false
	}
	if rf.Log[lenLog-1].Term == args.LastLogTerm && args.LastLogIndex < lenLog-1 {
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		rf.VotedFor = args.CandidateId
	}
	rf.persist()
	reply.Term = rf.CurrentTerm

}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArg, reply *AppendEntriesReply) {
	//注意这个是handler
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || args.LeaderId == rf.me {
		return
	}
	rf.readPersist(rf.persister.ReadRaftState())

	reply.Term = rf.CurrentTerm
	success := true

	// 收到来自有效leader的心跳
	if args.Term >= rf.CurrentTerm {
		rf.VotedFor = -1
		randomTerm := 150 + rand.Intn(500)
		rf.nextActiveTime = time.Now().Add(time.Duration(randomTerm) * time.Millisecond)
		if rf.State != 0 {
			rf.State = 0
			rf.CurrentTerm = args.Term
			reply.Term = rf.CurrentTerm
			//println("因为收到心跳", rf.me, "从", rf.State, "转为了follower")
		}
		rf.persist()
	}

	if args.Term < rf.CurrentTerm {
		success = false
		reply.Success = success
		return
	} else {
		rf.CurrentTerm = args.Term
		rf.persist()
		reply.Term = rf.CurrentTerm
	}

	//Log冲突检查
	var lastNewEntry LogEntry
	if args.PrevLogIndex > len(rf.Log)-1 {
		success = false
		reply.ConflictIndex = len(rf.Log)
		reply.ConflictTerm = -1
	} else if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
		success = false
		reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
		for i := 1; i < len(rf.Log); i++ {
			if rf.Log[i].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
	} else {
		for _, entry := range args.Entry {
			if entry.Index > len(rf.Log)-1 {
				rf.Log = append(rf.Log, entry)
			} else {
				if rf.Log[entry.Index].Term != entry.Term {
					rf.Log = rf.Log[:entry.Index+1]
				}
				rf.Log[entry.Index] = entry
			}
			lastNewEntry = entry
			rf.persist()
		}
	}

	/*
		var lastNewEntry LogEntry
		if args.PrevLogIndex <= len(rf.Log)-1 && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			success = false
			// lecture里的方法，针对log冲突快速回退
			reply.ConflictTerm = rf.Log[args.PrevLogIndex].Term
			reply.ConflictIndex = -1
			for i := 1; i < len(rf.Log); i++ {
				if rf.Log[i].Term == reply.ConflictTerm {
					reply.ConflictIndex = i
					break
				}
			}

			//rf.Log = rf.Log[0:args.PrevLogIndex]
			//rf.persist()
			//println("检测到log冲突：prevLogIndex:", args.PrevLogIndex, " prevLogTerm: ", args.PrevLogTerm, "实际term:", rf.Log[args.PrevLogIndex].Term)
		} else {

			if args.PrevLogIndex > len(rf.Log)-1 {
				reply.ConflictTerm = -1
				reply.ConflictIndex = len(rf.Log)
				success = false
			} else {
				if args.Entry != nil {
					for _, entry := range args.Entry {
						if entry.Index > len(rf.Log)-1 {
							rf.Log = append(rf.Log, entry)
							rf.persist()
							lastNewEntry = entry
						} else {
							if entry.Term != rf.Log[entry.Index].Term {
								rf.Log = rf.Log[0:entry.Index]
								rf.Log = append(rf.Log, entry)
								rf.persist()
								lastNewEntry = entry
							}
						}
					}

					//println(rf.me, "添加log成功，现在长度为", len(rf.Log))
				}
			}
		}
	*/

	if success && args.LeaderCommit > rf.commitIndex && (args.Entry == nil || lastNewEntry.Index == 0) {
		if args.LeaderCommit <= rf.Log[len(rf.Log)-1].Index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.Log[len(rf.Log)-1].Index
		}
	}

	if success && args.LeaderCommit > rf.commitIndex && args.Entry != nil && lastNewEntry.Index > 0 {
		if args.LeaderCommit <= lastNewEntry.Index {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntry.Index
		}
	}
	rf.persist()
	reply.Success = success
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func sendRequestVote(server *labrpc.ClientEnd, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := server.Call("Raft.RequestVote", args, reply)
	return ok
}

func sendAppendEntries(server *labrpc.ClientEnd, args *AppendEntriesArg) (bool, *AppendEntriesReply) {
	reply := AppendEntriesReply{}
	ok := server.Call("Raft.AppendEntriesHandler", args, &reply)
	return ok, &reply
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
	defer rf.mu.Unlock()
	rf.readPersist(rf.persister.ReadRaftState())
	index = len(rf.Log)
	term = rf.CurrentTerm
	isLeader = rf.State == 2
	if isLeader {
		rf.Log = append(rf.Log, LogEntry{index, term, command})
		//println(rf.me, "收到指令", command, "，目前Log长度为", len(rf.Log), "commitIndex:", rf.commitIndex)
		rf.matchIndex[rf.me] = len(rf.Log) - 1
		rf.persist()
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.State = 0
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) executeElection(server *labrpc.ClientEnd, term int, arg RequestVoteArgs) {

	reply := RequestVoteReply{}
	sendRequestVote(server, &arg, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.State != 1 || term != rf.CurrentTerm {
		return
	}

	if rf.CurrentTerm < reply.Term {
		rf.State = 0
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		//println(rf.me, "在candidate发送投票请求时，收到来自更高term的消息，转follower")
	}

	if reply.VoteGranted && rf.State == 1 {
		rf.votesGot++
		if rf.votesGot > len(rf.peers)/2 {
			//println(rf.me, "成为了leader!", rf.CurrentTerm)
			rf.State = 2

			rf.matchIndex = make([]int, len(rf.peers))
			rf.matchIndex[rf.me] = len(rf.Log) - 1
			rf.nextIndex = make([]int, len(rf.peers))
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = rf.Log[len(rf.Log)-1].Index + 1
			}

			go rf.doHeartBeat()
		}
	}

}

func (rf *Raft) launchElection() {

	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.votesGot = 1
	rf.persist()

	lenLog := len(rf.Log)
	arg := RequestVoteArgs{rf.CurrentTerm, rf.me, rf.Log[lenLog-1].Index, rf.Log[lenLog-1].Term}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.executeElection(rf.peers[i], rf.CurrentTerm, arg)
		}
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.State != 1 {
			rf.votesGot = 0
		}
		if rf.State != 2 {
			if time.Now().After(rf.nextActiveTime) {
				// 随机周期内没有收到心跳，转变为candidate
				randomTerm := 150 + rand.Intn(500)
				rf.nextActiveTime = time.Now().Add(time.Duration(randomTerm) * time.Millisecond)
				rf.State = 1
				//println(rf.me, "号没有收到心跳，发起了投票")
				rf.launchElection()
			}
		}

		for rf.lastApplied < rf.commitIndex {
			if rf.commitIndex > len(rf.Log)-1 {
				rf.commitIndex = len(rf.Log) - 1
				break
			}
			rf.lastApplied++
			//println(rf.me, "commit了log", rf.lastApplied)
			rf.applyCh <- ApplyMsg{
				true, rf.Log[rf.lastApplied].Command, rf.lastApplied,
				false, nil, 0, 0}
		}

		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 5)
	}
}

func (rf *Raft) executeHeartBeat(arg AppendEntriesArg, server *labrpc.ClientEnd, serverID int) {
	var reply *AppendEntriesReply
	timeStart := time.Now()
	ok, reply := sendAppendEntries(server, &arg)
	for !ok {
		if rf.killed() {
			return
		}
		if time.Now().Sub(timeStart) > time.Second*1 {
			return
		}
		ok, reply = sendAppendEntries(server, &arg)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return
	}
	if rf.CurrentTerm != arg.Term || rf.State != 2 {
		//println("对", server, "的rpc失败")
		return
	}

	if reply.Term > rf.CurrentTerm {
		rf.State = 0
		rf.CurrentTerm = reply.Term
		rf.VotedFor = -1
		rf.persist()
		//println(rf.me, "发心跳时，收到来自更高term的消息，转follower")
		return
	}

	if reply.Term < rf.CurrentTerm {
		//println("收到之前term的rpc返回，丢弃..")
		return
	}

	if reply.Success {
		rf.matchIndex[serverID] = arg.PrevLogIndex + len(arg.Entry)
		rf.nextIndex[serverID] = rf.matchIndex[serverID] + 1
	}

	if reply.Success {
		lenPeers := len(rf.peers)

		for i := len(rf.Log) - 1; i > rf.commitIndex; i-- {
			n := 0
			for j := 0; j < lenPeers; j++ {
				if rf.matchIndex[j] >= i {
					n++
				}
			}
			if n > lenPeers/2 && rf.Log[i].Term == rf.CurrentTerm {
				rf.commitIndex = i
				break
				//println("leader已commit一份log，现在commitIndex为", rf.commitIndex)
			}

		}
	}

	if !reply.Success {

		// lecture上的快速回退
		hasConflictTerm := false
		lastIndexOfConflictTerm := -1
		for i := 1; i < len(rf.Log); i++ {
			if rf.Log[i].Term == reply.ConflictTerm {
				hasConflictTerm = true
				lastIndexOfConflictTerm = i
			}
		}
		if hasConflictTerm {
			rf.nextIndex[serverID] = lastIndexOfConflictTerm + 1
		} else {
			rf.nextIndex[serverID] = reply.ConflictIndex
		}

		//rf.nextIndex[server]--
		//println("对", server, "的一致性检查失败，回退nextIndex到", rf.nextIndex[server])
	}

}

func (rf *Raft) doHeartBeat() {
	rf.mu.Lock()

	for rf.State == 2 && rf.killed() == false {
		lenPeers := len(rf.peers)
		args := make([]AppendEntriesArg, lenPeers)
		for i := 0; i < lenPeers; i++ {
			if i != rf.me {
				if rf.Log[len(rf.Log)-1].Index >= rf.nextIndex[i] { //按论文检测是否发entry
					if rf.nextIndex[i] == 0 {
						rf.nextIndex[i] = 1
					}
					args[i] = AppendEntriesArg{
						Term: rf.CurrentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm: rf.Log[rf.nextIndex[i]-1].Term, Entry: rf.Log[rf.nextIndex[i]:],
						LeaderCommit: rf.commitIndex}
				} else {
					// 只是普通心跳，不需要发Entry
					args[i] = AppendEntriesArg{
						Term: rf.CurrentTerm, LeaderId: rf.me, PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm: rf.Log[rf.nextIndex[i]-1].Term, Entry: nil,
						LeaderCommit: rf.commitIndex}
				}
			}
		}

		rf.mu.Unlock()
		for i := 0; i < lenPeers; i++ {
			if i != rf.me {
				theArg := args[i]
				go rf.executeHeartBeat(theArg, rf.peers[i], i)
			}
		}
		time.Sleep(time.Millisecond * 100)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
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
	rf.VotedFor = -1
	randomTerm := 150 + rand.Intn(500)
	rf.nextActiveTime = time.Now().Add(time.Duration(randomTerm) * time.Millisecond)
	rf.applyCh = applyCh
	rf.Log = []LogEntry{{Command: nil, Index: 0, Term: 1}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
