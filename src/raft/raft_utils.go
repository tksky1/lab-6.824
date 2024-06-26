package raft

import (
	"6.5840/labgob"
	"bytes"
	"log"
	"math/rand"
	"time"
)

func (rf *Raft) setFollower() {
	rf.state = 0
	rf.votesGot = 0
	rf.VotedFor = -1
	rf.persist()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current Snapshot
// (or nil if there's not yet a Snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	e.Encode(rf.BaseIndex)
	e.Encode(rf.BaseTerm)
	raftState := w.Bytes()

	rf.persister.Save(raftState, rf.SnapshotData)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	var BaseTerm int
	var BaseIndex int
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil ||
		d.Decode(&BaseIndex) != nil ||
		d.Decode(&BaseTerm) != nil {
		log.Println("读持久化错误！")
	} else {
		rf.CurrentTerm = CurrentTerm
		rf.VotedFor = VotedFor
		rf.Log = Log
		rf.BaseIndex = BaseIndex
		rf.BaseTerm = BaseTerm
	}

	rf.SnapshotData = rf.persister.ReadSnapshot()
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
