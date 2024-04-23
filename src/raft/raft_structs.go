package raft

import (
	"6.5840/labrpc"
	"sync"
	"time"
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
	CurrentTerm  int
	VotedFor     int
	Log          []LogEntry
	SnapshotData []byte
	BaseIndex    int // 基准变量，2D加入，代表现有log的前一个log(备份的最后一个)在Raft的实际Index值
	BaseTerm     int // BaseIndex的Term

	//<----- 临时变量 ----->
	commitIndex    int
	lastApplied    int
	state          int // follower: 0; candidate: 1; leader: 2
	nextExpireTime time.Time
	applyCh        chan ApplyMsg
	syncApplyCh    chan ApplyMsg // commit to applier to sync-apply msg

	//<----- leader使用 ---->
	nextIndex  []int
	matchIndex []int

	//<----- candidate使用 ---->
	votesGot int
}
