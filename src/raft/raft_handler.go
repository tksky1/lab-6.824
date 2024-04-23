package raft

func (rf *Raft) InstallSnapshotHandler(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	rf.readPersist(rf.persister.ReadRaftState())

	if args.Term > rf.CurrentTerm {
		rf.setFollower()
		rf.CurrentTerm = args.Term
		rf.persist()
	}
	reply.Term = rf.CurrentTerm

	if args.Term < rf.CurrentTerm || args.LastIncludedIndex <= rf.BaseIndex {
		rf.mu.Unlock()
		return
	}

	rf.nextExpireTime = generateNextExpireTime()
	for i := 1; i <= len(rf.Log)-1; i++ {
		if rf.Log[i].Index == args.LastIncludedIndex && rf.Log[i].Term == args.LastIncludedTerm {
			if i != len(rf.Log)-1 {
				rf.BaseIndex = args.LastIncludedIndex
				rf.BaseTerm = args.LastIncludedTerm
				var newLog []LogEntry
				newLog = append(newLog, LogEntry{nil, args.LastIncludedTerm, args.LastIncludedIndex})
				newLog = append(newLog, rf.Log[i+1:]...)
				rf.Log = newLog
				rf.SnapshotData = args.Data

				if rf.lastApplied < rf.BaseIndex {
					rf.lastApplied = rf.BaseIndex
				}
				if rf.commitIndex < rf.BaseIndex {
					rf.commitIndex = rf.BaseIndex
				}

				rf.persist()
				rf.mu.Unlock()
				rf.syncApplyCh <- ApplyMsg{
					false, nil, 0, true, args.Data,
					args.LastIncludedTerm, args.LastIncludedIndex,
				}
				return
			}
		}
	}

	// 覆盖
	rf.Log = nil
	rf.Log = append(rf.Log, LogEntry{nil, args.LastIncludedTerm, args.LastIncludedIndex})
	rf.SnapshotData = args.Data
	rf.BaseTerm = args.LastIncludedTerm
	rf.BaseIndex = args.LastIncludedIndex
	rf.lastApplied = rf.BaseIndex
	rf.commitIndex = rf.BaseIndex
	rf.persist()
	rf.mu.Unlock()
	rf.syncApplyCh <- ApplyMsg{
		false, nil, 0, true, args.Data,
		args.LastIncludedTerm, args.LastIncludedIndex,
	}
}

func (rf *Raft) RequestVoteHandler(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.readPersist(rf.persister.ReadRaftState())

	if args.Term > rf.CurrentTerm {
		rf.setFollower()
		rf.CurrentTerm = args.Term
		rf.persist()
	}

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	up2date := false
	if args.LastLogTerm != rf.Log[len(rf.Log)-1].Term {
		if args.LastLogTerm >= rf.Log[len(rf.Log)-1].Term {
			up2date = true
		}
	} else {
		if args.LastLogIndex-rf.BaseIndex >= len(rf.Log)-1 {
			up2date = true
		}
	}

	if (rf.VotedFor == -1 || rf.VotedFor == args.CandidateID) && up2date {
		rf.VotedFor = args.CandidateID
		rf.persist()
		reply.VoteGranted = true
		rf.nextExpireTime = generateNextExpireTime()
	}
}

func (rf *Raft) AppendEntriesHandler(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.readPersist(rf.persister.ReadRaftState())

	reply.Term = rf.CurrentTerm
	if args.Term == rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.persist()
		rf.nextExpireTime = generateNextExpireTime()
	} else if args.Term > rf.CurrentTerm {
		rf.setFollower()
		rf.CurrentTerm = args.Term
		rf.persist()
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

	//	XTerm:  term in the conflicting entry (if any)
	//  XIndex: index of first entry with that term (if any)
	//  XLen:   log length
	lenLog := len(rf.Log)
	if lenLog-1 < args.PrevLogIndex-rf.BaseIndex {
		// follower log 长度不够，XLen 正确返回就行
		reply.Success = false
		reply.XTerm = rf.Log[lenLog-1].Term
		reply.XIndex = lenLog - 1 + rf.BaseIndex
		reply.XLen = lenLog + rf.BaseIndex
		return
	} else {
		if args.PrevLogIndex < rf.BaseIndex {
			println("prevLogIndex<rf.BaseIndex:", args.PrevLogIndex, rf.BaseIndex)
			for i, argEntry := range args.Entries {
				if argEntry.Index-rf.BaseIndex >= len(rf.Log) {
					// 4. Append any new entries not already in the log
					rf.Log = append(rf.Log, args.Entries[i:]...)
					rf.persist()
					break
				}
			}
			reply.Success = true
			return
		} else if rf.Log[args.PrevLogIndex-rf.BaseIndex].Term != args.PrevLogTerm {

			reply.XTerm = rf.Log[args.PrevLogIndex-rf.BaseIndex].Term
			for i := 0; i <= args.PrevLogIndex-rf.BaseIndex; i++ {
				if rf.Log[i].Term == reply.XTerm {
					reply.XIndex = rf.Log[i].Index
					break
				}
			}
			reply.XLen = lenLog + rf.BaseIndex
			reply.Success = false
			return
		}
	}

	// 3. If an existing entry conflicts with a new one (same index
	//but different terms), delete the existing entry and all that
	//follow it
	for i, argEntry := range args.Entries {
		if argEntry.Index-rf.BaseIndex < len(rf.Log) && argEntry.Term != rf.Log[argEntry.Index-rf.BaseIndex].Term {
			rf.Log = rf.Log[:(argEntry.Index - rf.BaseIndex)]
			rf.persist()
			//log.Println("server", rf.me, "的log冲突，截取到", argEntry.Index-1)
		}
		if argEntry.Index-rf.BaseIndex >= len(rf.Log) {
			// 4. Append any new entries not already in the log
			rf.Log = append(rf.Log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}

	// 4. If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.Log[len(rf.Log)-1].Index)
	}

}
