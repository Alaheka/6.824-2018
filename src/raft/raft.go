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
)

type RoleType int
const (
	LEADER    RoleType = iota
	CANDIDATE
	FOLLOWER
)

const HEARTBEAT_INTERVAL = 100

// import "bytes"
// import "labgob"

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
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	currentTerm	int
	votedFor	int
	log			[]LogEntry

	commitIndex	int
	lastApplied	int

	nextIndex	[]int
	matchIndex	[]int

	role 		RoleType
	votesReceived	int
	appendEntriesCh	chan bool
	grantVoteCh		chan bool
	becomeLeaderCh	chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//var term int
	//var isleader bool
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
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
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
	//DPrintf("%v %v %v %v %v %v", rf.me, args.CandidateId, args.Term, rf.currentTerm, rf.votedFor, )
	if args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			DPrintf("%v grant vote to %v", rf.me, args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			dropAndSet(rf.grantVoteCh)
	}
	reply.Term = rf.currentTerm
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
			if rf.role == CANDIDATE && rf.currentTerm == reply.Term{
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

type AppendEntriesArgs struct {
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	//TODO:
	reply.Success = false
	rf.mu.Lock()
	DPrintf("%v get appendEntries from %v of term %v", rf.me, args.LeaderId, args.Term)
	rf.checkConvertToFollower(args.Term)
	currentTerm := rf.currentTerm
	reply.Term = currentTerm
	if args.Term >= currentTerm {
		dropAndSet(rf.appendEntriesCh)
		if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
			reply.Success = true
			
		}

	}
	rf.mu.Unlock()
}

func (rf *Raft) advanceCommitIndex()  {

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) broadcastAppendEntries()  {
	//TODO:
	//currentTerm := rf.currentTerm
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:rf.currentTerm,
		LeaderId:rf.me,
		PrevLogIndex:0,
		PrevLogTerm:0,
		Entries:nil,
		LeaderCommit:rf.commitIndex,
	}
	rf.mu.Unlock()
	DPrintf("%v broadcasting appendEntries.", rf.me)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(ind int, args AppendEntriesArgs){
			reply := AppendEntriesReply{}
			rf.sendAppendEntries(ind, &args, &reply)
			rf.mu.Lock()
			rf.checkConvertToFollower(reply.Term)
			rf.mu.Unlock()
		}(i, args)
	}
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
}
func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log) - 1].Index
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
	defer rf.mu.Unlock()
	isLeader = rf.role == LEADER
	if isLeader {
		index = rf.getLastLogIndex() + 1
		term = rf.currentTerm
		newLog := LogEntry{Index:index, Term:term, Command:command}
		rf.log = append(rf.log, newLog)
	}

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
	}
	rf.mu.Unlock()
}

func (rf *Raft) convertToLeader()  {
	//TODO:
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
	rf.log = append(rf.log, LogEntry{Term: 0, Index:0})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.role = FOLLOWER

	rf.votesReceived = 0
	rf.appendEntriesCh = make(chan bool, 1)
	rf.grantVoteCh = make(chan bool, 1)
	rf.becomeLeaderCh = make(chan bool, 1)

	go rf.stateDaemon()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
