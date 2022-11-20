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
	currentTerm int
	votedFor    int `default:"-1"`
	Log         []LogEntry
	commitIndex int
	lastApplied int

	State         int //状态，0 follower 1: candidate 2: leader
	lastHeartBeat time.Time
	hasKilled     bool
	muSend        sync.Mutex //用于发rpc时避免race

	//candidate使用
	votesGot int

	//leader使用
	nextIndex  []int
	matchIndex []int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.State == 2
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//注意这个是handler
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || rf.votedFor != -1 || rf.State != 0 {
		println("refuse vote,votefor==", rf.votedFor, "state==", rf.State)
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		lenLog := len(rf.Log)
		if lenLog > 0 && args.Term == rf.currentTerm && args.LastLogIndex < rf.Log[lenLog-1].Index {
			reply.VoteGranted = false
		}
	}
	println(rf.me, "号收到了投票请求，返回结果是", reply.VoteGranted)

	rf.mu.Unlock()
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArg, reply *AppendEntriesReply) {
	//注意这个是handler
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	success := true

	rf.votedFor = -1
	rf.lastHeartBeat = time.Now()
	if rf.State != 0 && args.Term > rf.currentTerm {
		rf.State = 0
		rf.votesGot = 1
		println("收到heartbeat,", rf.me, "号转为follower")
		rf.currentTerm = args.Term
	}
	if args.Term < rf.currentTerm {
		success = false
	} else {
		rf.currentTerm = args.Term
	}
	if len(rf.Log) > 0 {
		if len(rf.Log) > args.PrevLogIndex && rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {
			//success = false
			rf.Log = rf.Log[0:args.PrevLogIndex]
		}
		if rf.Log[len(rf.Log)-1].Index < args.Entries[0].Index {
			rf.Log = append(rf.Log, args.Entries...)
		}
	} else {
		rf.Log = append(rf.Log, args.Entries...)
	}
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > (len(rf.Log) - 1) {
			rf.commitIndex = len(rf.Log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}
	reply.Success = success
	println(rf.me, "号收到心跳，来自：", args.LeaderId, "success=", success)
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server *labrpc.ClientEnd, args *AppendEntriesArg, reply *AppendEntriesReply) {
	rf.muSend.Lock()
	server.Call("Raft.AppendEntriesHandler", args, reply)
	rf.muSend.Unlock()
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
	rf.hasKilled = true
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) launchElection() {
	rf.muSend.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.votesGot = 1
	lenLog := len(rf.Log)

	var arg RequestVoteArgs
	if lenLog == 0 {
		arg = RequestVoteArgs{rf.currentTerm, rf.me, 0, 0}
	} else {
		arg = RequestVoteArgs{rf.currentTerm, rf.me, rf.Log[lenLog-1].Index, rf.Log[lenLog-1].Term}
	}
	reply := RequestVoteReply{}

	rf.muSend.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				println(rf.me, "向", i, "发送了投票请求")
				rf.muSend.Lock()
				rf.sendRequestVote(i, &arg, &reply)
				if rf.State == 2 {
					rf.muSend.Unlock()
					return
				}
				if reply.VoteGranted {
					rf.votesGot++
					println(rf.me, "号收到一个投票")
					if rf.votesGot > len(rf.peers)/2 {
						rf.State = 2
						println(rf.me, "号成为了leader！")
						go rf.doHeartBeat()
					}
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.State = 0
					println(rf.me, "号发现了更大的term，转为follower")
				}
				rf.muSend.Unlock()
			}(i)
		}
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	randomTerm := 150 + rand.Intn(500)
	time.Sleep(time.Millisecond * time.Duration(randomTerm))
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		if rf.State == 0 {
			if (time.Now().Sub(rf.lastHeartBeat)).Milliseconds() > int64(randomTerm) {
				// 随机周期内没有收到心跳，转变为candidate
				rf.State = 1
				randomTerm = 150 + rand.Intn(500)
				println(rf.me, "号没有收到心跳，发起了投票")
				rf.launchElection()
			}
		} else if rf.State == 1 {
			if (time.Now().Sub(rf.lastHeartBeat)).Milliseconds() > int64(randomTerm) {
				rf.muSend.Lock()
				if rf.votesGot > len(rf.peers)/2 {
					rf.State = 2
					println(rf.me, "号成为了leader！")
					go rf.doHeartBeat()
				} else {
					rf.launchElection()
					randomTerm = 150 + rand.Intn(500)
					println(rf.me, "号发起的投票过期，重新发起")
				}
				rf.muSend.Unlock()
			} else {
				rf.votesGot = 1
				rf.State = 0
				println("收到heartbeat,", rf.me, "号转为follower")
			}
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * time.Duration(randomTerm))
	}
}

func (rf *Raft) doHeartBeat() {
	rf.mu.Lock()
	for rf.State == 2 && rf.killed() == false {

		var arg AppendEntriesArg
		if len(rf.Log) == 0 {
			arg = AppendEntriesArg{
				rf.currentTerm, rf.me, 0,
				0, nil, rf.commitIndex}
		} else {
			arg = AppendEntriesArg{
				rf.currentTerm, rf.me, rf.lastApplied,
				rf.Log[rf.lastApplied].Term, nil, rf.commitIndex}
		}
		reply := AppendEntriesReply{}
		peerAmount := len(rf.peers)
		peers := rf.peers
		rf.mu.Unlock()
		for i := 0; i < peerAmount; i++ {
			if i != rf.me {
				go func(entriesArg *AppendEntriesArg, entriesReply *AppendEntriesReply, server *labrpc.ClientEnd) {
					rf.sendAppendEntries(server, entriesArg, entriesReply)
				}(&arg, &reply, peers[i])
			}
		}
		//println("leader", rf.me, "正在发心跳")
		time.Sleep(time.Millisecond * 150)
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
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
