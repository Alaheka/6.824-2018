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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
	"sort"
	"bytes"
	"labgob"
	"log"
)

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

type RoleType int
const (
	LEADER    RoleType = iota
	CANDIDATE
	FOLLOWER
)

const HEARTBEAT_INTERVAL = 100

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid 	bool
	Command      	interface{}
	CommandIndex	int
	SnapshotData	[]byte
}

type LogEntry struct {
	Term  	int
	Index 	int
	Command	interface{}
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//TODO:
	currentTerm			int
	votedFor			int
	log					[]LogEntry
	lastSnapshotIndex	int
	lastSnapshotTerm	int

	commitIndex			int
	lastApplied			int

	nextIndex			[]int
	matchIndex			[]int

	applyCh 		chan ApplyMsg
	role 			RoleType
	votesReceived	int
	appendEntriesCh	chan bool
	grantVoteCh		chan bool
	becomeLeaderCh	chan bool
	notifyApplyCh	chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.getPersistData())
}
func (rf *Raft) getPersistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastSnapshotIndex)
	e.Encode(rf.lastSnapshotTerm)
	return w.Bytes()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm = -1
	var votedFor = -1
	var log = []LogEntry{}
	var lastSnapshotIndex = -1
	var lastSnapshotTerm = -1
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastSnapshotIndex) != nil ||
		d.Decode(&lastSnapshotTerm) != nil {
		DPrintf("error decoding persistence")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = votedFor
	  rf.log = log
	  rf.lastSnapshotIndex = lastSnapshotIndex
	  rf.lastSnapshotTerm = lastSnapshotTerm
	}
}

func (rf *Raft) Snapshot(lastIncludedIndex int, data []byte)  {
	//TODO:
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex < rf.lastSnapshotIndex {
		return
	}
	relativeIndex := rf.getRelativeIndex(lastIncludedIndex)
	if relativeIndex > 0 && relativeIndex < len(rf.log) {
		rf.lastSnapshotIndex = lastIncludedIndex
		rf.lastSnapshotTerm = rf.log[relativeIndex].Term
		rf.log = rf.log[relativeIndex : ]
	} else {
		panic("snapshot error")
	}
	log.Printf("%v snapshot %v", rf.me, rf.lastSnapshotIndex)
	rf.persister.SaveStateAndSnapshot(rf.getPersistData(), data)
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLogTerm := rf.getLastLogTerm()
	lastLogIndex := rf.getLastLogIndex()
	rf.checkConvertToFollower(args.Term)
	reply.VoteGranted = false
	if args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			DPrintf("%v grant vote to %v", rf.me, args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			dropAndSet(rf.grantVoteCh)
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) broadcastRequestVote()  {
	//currentTerm := rf.currentTerm
	args := RequestVoteArgs{
		Term:rf.currentTerm,
		CandidateId:rf.me,
		LastLogIndex:rf.getLastLogIndex(),
		LastLogTerm:rf.getLastLogTerm(),
	}
	rf.votesReceived = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(ind int, args RequestVoteArgs){

			reply := RequestVoteReply{}
			rf.sendRequestVote(ind, &args, &reply)
			rf.mu.Lock()
			rf.checkConvertToFollower(reply.Term)
			if rf.role == CANDIDATE && rf.currentTerm == reply.Term && rf.currentTerm == args.Term{
				if reply.VoteGranted {
					rf.votesReceived++
					DPrintf("%v get vote from %v", rf.me, ind)
					if rf.votesReceived > len(rf.peers) / 2 {
						rf.convertToLeader()
					}
				}
			}
			rf.mu.Unlock()
		}(i, args)
	}
}

type InstallSnapshotArgs struct {
	Term				int
	LeaderId     		int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Data				[]byte
}

type InstallSnapshotReply struct {
	Term	int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply)  {
	rf.mu.Lock()
	rf.checkConvertToFollower(args.Term)
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	if args.Term >= currentTerm {
		dropAndSet(rf.appendEntriesCh)
		relativeIndex := rf.getRelativeIndex(args.LastIncludedIndex)
		if relativeIndex > 0 && relativeIndex < len(rf.log) && rf.log[relativeIndex].Term == args.LastIncludedTerm {
			rf.log = rf.log[relativeIndex : ]
		} else {
			rf.log = make([]LogEntry, 0)
			rf.log = append(rf.log, LogEntry{Index:args.LastIncludedIndex, Term:args.LastIncludedTerm})
		}
		rf.lastSnapshotTerm = args.LastIncludedTerm
		rf.lastSnapshotIndex = args.LastIncludedIndex
		rf.persister.SaveStateAndSnapshot(rf.getPersistData(), args.Data)
		if rf.commitIndex < rf.lastSnapshotIndex {
			rf.commitIndex = rf.lastSnapshotIndex
		}
		//log.Printf("get InstallSnapshot %v applyIndex %v snapshotIndex", rf.lastApplied, rf.lastSnapshotIndex)
		dropAndSet(rf.notifyApplyCh)
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    		int
	Success 		bool
	ConflictTerm	int
	ConflictIndex	int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	DPrintf("%v get appendEntries from %v of term %v preLogIndex %v entriesLength %v leaderCommit %v", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, len(args.Entries), args.LeaderCommit)
	rf.checkConvertToFollower(args.Term)
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	if args.Term >= currentTerm {
		dropAndSet(rf.appendEntriesCh)
		//if rf.getLastLogIndex() >= args.PrevLogIndex && ((rf.getRelativeIndex(args.PrevLogIndex) < 0 && rf.lastSnapshotTerm == args.PrevLogTerm) || (rf.getRelativeIndex(args.PrevLogIndex) >= 0 && rf.log[rf.getRelativeIndex(args.PrevLogIndex)].Term == args.PrevLogTerm)) {
		log.Printf("%v %v append entries %v %v %v %v ", rf.me, args.LeaderId, rf.lastSnapshotIndex, args.PrevLogIndex, rf.getRelativeIndex(args.PrevLogIndex), len(rf.log))
		if rf.getRelativeIndex(args.PrevLogIndex) < 0 {
			reply.Success = true
		} else if rf.getLastLogIndex() >= args.PrevLogIndex && rf.log[rf.getRelativeIndex(args.PrevLogIndex)].Term == args.PrevLogTerm {
			reply.Success = true
			indexA := rf.getRelativeIndex(args.PrevLogIndex + 1)
			indexB := 0
			for indexA < len(rf.log) && indexB < len(args.Entries) && rf.log[indexA].Term == args.Entries[indexB].Term {
				indexA++
				indexB++
			}
			if indexA < len(rf.log) && indexB < len(args.Entries) {
				rf.log = rf.log[0 : indexA]
			}
			if indexB < len(args.Entries) {
				rf.log = append(rf.log, args.Entries[indexB : ]...)
			}
			if args.LeaderCommit > rf.commitIndex {

				rf.commitIndex = minInt(args.LeaderCommit, rf.getAbsoluteIndex(indexA - 1))
			}
			dropAndSet(rf.notifyApplyCh)
		} else {
			reply.Success = false
			if rf.getLastLogIndex() < args.PrevLogIndex {
				reply.ConflictIndex = rf.getLastLogIndex()
				reply.ConflictTerm = rf.getLastLogTerm()
			} else {
				reply.ConflictTerm = rf.log[rf.getRelativeIndex(args.PrevLogIndex)].Term
				for i := range rf.log {
					if rf.log[i].Term == reply.ConflictTerm {
						reply.ConflictIndex = rf.getAbsoluteIndex(i)
						break
					}
				}
			}
		}

	}
	DPrintf("%v reply appendEntries with term %v and success %v", rf.me, reply.Term, reply.Success)
	rf.persist()
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries()  {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	DPrintf("%v broadcasting appendEntries.", rf.me)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(ind int, currentTerm int){
			// TODO: may need send installsnapshot instead of appendEntries
			rf.mu.Lock()
			//log.Printf("%v enter append %v lock1", rf.me, ind)
			LOOP: if rf.role == LEADER && rf.currentTerm == currentTerm {

				if rf.getRelativeIndex(rf.nextIndex[ind]) < 1 {
					args := InstallSnapshotArgs{
						Term:currentTerm,
						LeaderId:rf.me,
						LastIncludedTerm:rf.lastSnapshotTerm,
						LastIncludedIndex:rf.lastSnapshotIndex,
						Data:rf.persister.ReadSnapshot(),
					}
					rf.mu.Unlock()
					reply := InstallSnapshotReply{}
					rf.sendInstallSnapshot(ind, &args, &reply)
					rf.mu.Lock()
					rf.checkConvertToFollower(reply.Term)
					if rf.role == LEADER && rf.currentTerm == reply.Term && rf.currentTerm == args.Term {
						rf.matchIndex[ind] = args.LastIncludedIndex
						rf.nextIndex[ind] = args.LastIncludedIndex + 1
						rf.advanceCommitIndex()
					}
				} else {
					entries := make([]LogEntry, 0)
					entries = append(entries, rf.log[rf.getRelativeIndex(rf.nextIndex[ind]):]...)
					prevLogIndex := rf.nextIndex[ind] - 1
					var prevLogTerm int
					//if rf.getRelativeIndex(prevLogIndex) < 0 {
					//	prevLogTerm = rf.lastSnapshotTerm
					//} else {
						prevLogTerm = rf.log[rf.getRelativeIndex(prevLogIndex)].Term
					//}
					args := AppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: prevLogIndex,
						PrevLogTerm:  prevLogTerm,
						Entries:      entries,
						LeaderCommit: rf.commitIndex,
					}
					//log.Printf("%v release append %v lock2", rf.me, ind)
					rf.mu.Unlock()
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(ind, &args, &reply)
					rf.mu.Lock()
					rf.checkConvertToFollower(reply.Term)
					if rf.role == LEADER && rf.currentTerm == reply.Term && rf.currentTerm == args.Term {
						if reply.Success {
							rf.matchIndex[ind] = args.PrevLogIndex + len(args.Entries)
							rf.nextIndex[ind] = args.PrevLogIndex + len(args.Entries) + 1
							rf.advanceCommitIndex()
						} else {
							rf.nextIndex[ind] = reply.ConflictIndex
							for i := rf.nextIndex[ind] - 1; i >= reply.ConflictIndex; i-- {
								if reply.ConflictTerm == rf.log[rf.getRelativeIndex(i)].Term {
									rf.nextIndex[ind] = i
									break
								}
							}
							goto LOOP
						}
					}
				}
			}
			//log.Printf("%v release append %v lock4", rf.me, ind)
			rf.mu.Unlock()
		}(i, currentTerm)
	}
}

func (rf *Raft) advanceCommitIndex()  {
	//log.Printf("%v enter advanceCommitIndex111", rf.me)
	matchIndexCopy := make([]int, 0)
	matchIndexCopy = append(matchIndexCopy, rf.matchIndex...)
	matchIndexCopy[rf.me] = rf.getLastLogIndex()
	sort.Ints(matchIndexCopy)
	N := matchIndexCopy[(len(rf.matchIndex) - 1) / 2]
	log.Printf("%v %v %v", rf.me, rf.getRelativeIndex(N), N)
	if N > rf.commitIndex && rf.log[rf.getRelativeIndex(N)].Term == rf.currentTerm {
		//log.Printf("%v enter advanceCommitIndex222", rf.me)
		rf.commitIndex = N
		dropAndSet(rf.notifyApplyCh)
	}
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) < 1 {
		return rf.lastSnapshotTerm
	}
	return rf.log[len(rf.log) - 1].Term
}
func (rf *Raft) getLastLogIndex() int {
	if len(rf.log) < 1 {
		return rf.lastSnapshotIndex
	}
	return rf.log[len(rf.log) - 1].Index
	//return len(rf.log) - 1
}
func (rf *Raft) getRelativeIndex(i int) int {
	return i - rf.lastSnapshotIndex
}
func (rf *Raft) getAbsoluteIndex(i int) int {
	return i + rf.lastSnapshotIndex
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	// TODO:
	rf.mu.Lock()
	//log.Printf("%v enter start lock", rf.me)
	defer rf.mu.Unlock()
	isLeader = rf.role == LEADER
	if isLeader {
		index = rf.getLastLogIndex() + 1
		term = rf.currentTerm
		newLog := LogEntry{Index:index, Term:term, Command:command}
		rf.log = append(rf.log, newLog)
		DPrintf("%v start log %v term %v index %v.", rf.me, newLog.Command, newLog.Term, newLog.Index)
		rf.persist()
	}

	//log.Printf("%v leave start lock", rf.me)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func dropAndSet(ch chan bool)  {
	select {
	case <- ch:
	default:
	}
	ch <- true
}

func (rf *Raft) checkConvertToFollower(argsTerm int) {
	if argsTerm > rf.currentTerm {
		DPrintf("%v convert to follower", rf.me)
		rf.role = FOLLOWER
		rf.currentTerm = argsTerm
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) convertToCandidate()  {
	rf.mu.Lock()
	if rf.role != LEADER {
		DPrintf("%v convert to candidate", rf.me)
		rf.role = CANDIDATE
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.broadcastRequestVote()
		rf.persist()
	}
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader()  {
	if rf.role == CANDIDATE {
		rf.role = LEADER
		rf.nextIndex = make([]int, len(rf.peers))
		leaderLastIndex := rf.getLastLogIndex()
		for i := range rf.nextIndex {
			rf.nextIndex[i] = leaderLastIndex + 1
		}
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.matchIndex {
			rf.matchIndex[i] = 0
		}
		DPrintf("%v convert to leader", rf.me)
		dropAndSet(rf.becomeLeaderCh)
		rf.persist()
	}
}

func getRandomElectionTimeout() time.Duration {
	// get random timeout between 500 and 600 milliseconds
	return time.Duration(500 + rand.Intn(200)) * time.Millisecond
}

func (rf *Raft) stateDaemon() {
	for {
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		electionTimeout := getRandomElectionTimeout()
		switch role {
		case FOLLOWER:
			select {
			case <- rf.appendEntriesCh:
			case <- rf.grantVoteCh:
			case <- time.After(electionTimeout):
				rf.convertToCandidate()
			}
		case CANDIDATE:
			select {
			case <- rf.appendEntriesCh:
			case <- rf.grantVoteCh:
			case <- rf.becomeLeaderCh:
			case <- time.After(electionTimeout):
				rf.convertToCandidate()
			}
		case LEADER:
			rf.broadcastAppendEntries()
			time.Sleep(HEARTBEAT_INTERVAL*time.Millisecond)
		}
	}
}

func (rf *Raft) applyDaemon() {
	for {
		<- rf.notifyApplyCh
		rf.mu.Lock()
		if rf.lastApplied < rf.lastSnapshotIndex {
			msg := ApplyMsg{
				CommandValid:false,
				SnapshotData:rf.persister.ReadSnapshot(),
			}
			snapshotIndex := rf.lastSnapshotIndex
			rf.mu.Unlock()
			log.Printf("let server catch up")
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = snapshotIndex
		}
		if rf.lastApplied < rf.commitIndex {
			entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
			copy(entries, rf.log[rf.getRelativeIndex(rf.lastApplied + 1) : rf.getRelativeIndex(rf.commitIndex + 1)])
			rf.mu.Unlock()
			for _, v := range entries {
				msg := ApplyMsg{
					Command:v.Command,
					CommandIndex:v.Index,
					CommandValid:true,
				}
				DPrintf("%v apply log index %v and command %v", rf.me, msg.CommandIndex, msg.Command)
				rf.applyCh <- msg
			}
			rf.mu.Lock()
			rf.lastApplied += len(entries)
		}
		rf.mu.Unlock()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	//TODO:
	rf.currentTerm = 0
	rf.votedFor	= -1
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: -1, Index:0})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastSnapshotIndex = 0
	rf.lastSnapshotTerm = 0

	rf.role = FOLLOWER
	rf.applyCh = applyCh

	rf.votesReceived = 0
	rf.appendEntriesCh = make(chan bool, 1)
	rf.grantVoteCh = make(chan bool, 1)
	rf.becomeLeaderCh = make(chan bool, 1)
	rf.notifyApplyCh = make(chan bool, 1)

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	go rf.stateDaemon()
	go rf.applyDaemon()

	return rf
}
