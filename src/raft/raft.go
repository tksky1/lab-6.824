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
	"log"
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
	Index   int
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
	applyCh        chan ApplyMsg

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
	XTerm   int
	XIndex  int
	XLen    int
	//  lab页面上的快速回退做法
	//  XTerm:  term in the conflicting entry (if any)
	//  XIndex: index of first entry with that term (if any)
	//  XLen:   log length
}

// <--------- Handler --------->

func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term > rf.CurrentTerm {
		rf.setFollower()
		rf.CurrentTerm = args.Term
		rf.nextExpireTime = generateNextExpireTime()
	}

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	up2date := false
	if args.LastLogTerm != rf.Log[len(rf.Log)-1].Term {
		if args.LastLogTerm >= rf.Log[len(rf.Log)-1].Term {
			up2date = true
		}
	} else {
		if args.LastLogIndex >= len(rf.Log)-1 {
			up2date = true
		}
	}

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && up2date {
		rf.VotedFor = args.CandidateID
		reply.VoteGranted = true
	}
	if !up2date {
		log.Println(rf.me, "因为log不够新拒绝了投票:", args.LastLogTerm, " ", rf.Log[len(rf.Log)-1].Term)
	}
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	if args.Term == rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.nextExpireTime = generateNextExpireTime()
	} else if args.Term > rf.CurrentTerm {
		rf.setFollower()
		rf.CurrentTerm = args.Term
		rf.nextExpireTime = generateNextExpireTime()
	} else if args.Term < rf.CurrentTerm {
		reply.Success = false
		return
		// 1. Reply false if term < currentTerm
	}

	reply.Success = true
	// <--- 安全检查 --->

	// 2. Reply false if log does not contain an entry at prevLogIndex
	//whose term matches prevLogTerm
	lenLog := len(rf.Log)
	if lenLog-1 < args.PrevLogIndex {
		reply.Success = false
		reply.XTerm = rf.Log[lenLog-1].Term
		reply.XIndex = lenLog - 1
		reply.XLen = lenLog
		return
	} else {
		if rf.Log[args.PrevLogIndex].Term != args.PrevLogTerm {

			reply.XTerm = rf.Log[args.PrevLogIndex].Term
			for i := 0; i <= args.PrevLogIndex; i++ {
				if rf.Log[i].Term == reply.XTerm {
					reply.XIndex = i
					break
				}
			}
			reply.XLen = lenLog
			reply.Success = false
			return
		}
	}

	// 3. If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it
	for i, argEntry := range args.Entries {
		if argEntry.Index < len(rf.Log) && argEntry.Term != rf.Log[argEntry.Index].Term {
			rf.Log = rf.Log[:argEntry.Index]
			log.Println("server", rf.me, "的log冲突，截取到", argEntry.Index-1)
		}
		if argEntry.Index >= len(rf.Log) {
			// 4. Append any new entries not already in the log
			rf.Log = append(rf.Log, args.Entries[i:]...)
			break
		}
	}

	// 4. If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.Log[len(rf.Log)-1].Index)
	}

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
		rf.nextExpireTime = generateNextExpireTime()
		rf.CurrentTerm = reply.Term
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
	log.Println(rf.me, "成为了Leader！")
	lenLog := len(rf.Log)
	for i := range rf.nextIndex {
		rf.nextIndex[i] = lenLog
	}
	rf.matchIndex = make([]int, len(rf.peers))
	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
	go rf.doHeartBeat()
}

func (rf *Raft) startElection() {
	rf.state = 1
	rf.CurrentTerm++
	rf.VotedFor = rf.me
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
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()

		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			rf.applyCh <- ApplyMsg{
				true, rf.Log[rf.lastApplied].Command, rf.Log[rf.lastApplied].Index,
				false, nil, 0, 0,
			}
		}

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

		lenLog := len(rf.Log)

		for i, peer := range rf.peers {
			if i == rf.me {
				continue
			}
			var log2send []LogEntry
			if lenLog >= rf.nextIndex[i] {
				log2send = rf.Log[rf.nextIndex[i]:]
			}
			arg := AppendEntriesArgs{
				rf.CurrentTerm, rf.me, rf.nextIndex[i] - 1,
				rf.Log[rf.nextIndex[i]-1].Term, log2send, rf.commitIndex,
			}
			thePeer := *peer
			go rf.sendAppendEntries(&thePeer, &arg, i)
		}

		rf.mu.Unlock()
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
		rf.matchIndex[serverID] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[serverID] = rf.matchIndex[serverID] + 1

		// 大多数match后即可提交
		for i := len(rf.Log) - 1; i >= rf.matchIndex[serverID] && i > rf.commitIndex; i-- {
			count := 0
			for j := range rf.peers {
				if rf.matchIndex[j] >= i {
					count++
				}
				if count > (len(rf.peers) / 2) {
					rf.commitIndex = i
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
			rf.nextIndex[serverID] = reply.XLen
		} else {
			lastXTerm := -1
			for i := len(rf.Log) - 1; i >= 0; i-- {
				if rf.Log[i].Term == reply.XTerm {
					lastXTerm = i
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = len(rf.Log)
	term = rf.CurrentTerm
	isLeader = rf.state == 2
	if isLeader {
		newLog := LogEntry{command, term, index}
		rf.Log = append(rf.Log, newLog)
		rf.matchIndex[rf.me] = index
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

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}
