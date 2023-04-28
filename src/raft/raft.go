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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

type LogEntry struct {
	Command interface{}
	Term    int
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

	//<----- 持久化变量 ----->
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	//<----- 临时变量 ----->
	commitIndex    int
	lastApplied    int
	state          int // follower: 0; candidate: 1; leader: 2
	nextExpireTime time.Time

	//<----- leader使用 ---->
	nextIndex  []int
	matchIndex []int

	//<----- candidate使用 ---->
	votesGot int
}

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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
	CandidateID  int
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

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// <--------- Handler --------->

func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.CurrentTerm {
		rf.setFollower()
		rf.CurrentTerm = args.Term
	}

	reply.Term = rf.CurrentTerm

	if rf.VotedFor == -1 && rf.state == 0 {
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
		rf.nextExpireTime = generateNextExpireTime()
	}
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.CurrentTerm {
		rf.setFollower()
		rf.CurrentTerm = args.Term
		rf.nextExpireTime = generateNextExpireTime()

	} else if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
	}

	// TODO: append log
	reply.Success = true
}

// <--------- Request Vote --------->

func (rf *Raft) sendRequestVote(server *labrpc.ClientEnd, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := server.Call("Raft.RequestVoteHandler", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.CurrentTerm == args.Term && rf.state == 1 {
		rf.votesGot++
		if rf.votesGot > len(rf.peers)/2 && rf.state == 1 {
			rf.becomeLeader()
			rf.votesGot = 0
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) becomeLeader() {
	rf.state = 2
	rf.nextIndex = make([]int, len(rf.peers))
	lenLog := len(rf.Log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lenLog
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.nextIndex[i] = 0
	}
	go rf.doHeartBeat()
}

func (rf *Raft) startElection() {
	rf.state = 1
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.votesGot++
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
			rf.CurrentTerm, rf.me, 0, 0,
		}
		thePeer := *peer
		go rf.sendRequestVote(&thePeer, &arg)
	}
	rf.mu.Unlock()
}

// follower判断是否该造反
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		nowTime := time.Now()
		if nowTime.After(rf.nextExpireTime) && rf.state != 2 {
			rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(30 * time.Millisecond)
	}
}

// <--------- AppendEntries --------->

func (rf *Raft) doHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state != 2 || rf.killed() {
			rf.mu.Unlock()
			return
		}

		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			arg := AppendEntriesArgs{
				rf.CurrentTerm, rf.me, 0, 0, nil, 0,
			}
			thePeer := *peer
			go rf.sendAppendEntries(&thePeer, &arg)
		}

		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) sendAppendEntries(server *labrpc.ClientEnd, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := server.Call("Raft.AppendEntriesHandler", args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if rf.CurrentTerm != args.Term || rf.state != 2 {
		rf.mu.Unlock()
		return
	}
	if rf.CurrentTerm < reply.Term {
		rf.setFollower()
		rf.CurrentTerm = reply.Term
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		//TODO: 5.3
	} else {
		//TODO: 5.3
	}
	rf.mu.Unlock()
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votesGot = 0
	rf.state = 0
	rf.nextExpireTime = generateNextExpireTime()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// <------ 辅助函数 ------>

func (rf *Raft) setFollower() {
	rf.state = 0
	rf.votesGot = 0
	rf.VotedFor = -1
}

func generateNextExpireTime() time.Time {
	millisecond := 150 + rand.Intn(150)
	return time.Now().Add(time.Duration(millisecond) * time.Millisecond)
}
