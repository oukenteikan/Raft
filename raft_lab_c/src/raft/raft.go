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
	"bytes"
	"encoding/gob"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

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

type ServerState string

const (
	FOLLOWER  ServerState = "Follower"
	CANDIDATE             = "Candidate"
	LEADER                = "Leader"
)

const (
	DisconnectTimeOutLowerBound int = 500
	DisconnectTimeOutRandom         = 300
	HeartbeatInterval               = 100
	ElectionTimeOutLowerBound       = 200
	ElectionTimeOutRandom           = 300
	ShortBreath                     = 10
)

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// timeout for disconnect and election
	// they will be randomly initialized in Make()
	// and remain constant throughout
	// actually electiontimeout is not used to avoid race
	disconnectTimeOut time.Duration
	electionTimeOut   time.Duration

	// candidateId that received vote in current term (or null if none)
	// but actually I find it useless so far (2020.05.01)
	// I find it useful at some extreme circumstance (2020.05.03)
	voteFor int

	// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	currentTerm int
	// state of the server, Follower, Candidate or Leader
	currentState ServerState

	// if receive an append entry,, set getHeartbeat as true
	getHeartbeat chan bool
	// if voteGranted more than half, set becomeLeader as true
	becomeLeader chan bool

	// I add this channel to try fixing a bug
	getRequestVote chan bool

	// log entries and indexes related to them
	// log entries
	//		each entry contains command
	// 		for state machine, and term when entry
	// 		was received by leader (first index is 1)
	// commitIndex
	//		index of highest log entry known to be
	//		committed (initialized to 0, increases monotonically)
	// lastApplied
	//		index of highest log entry applied to state
	//		machine (initialized to 0, increases monotonically)
	// nextIndex[]
	//		for each server, index of the next log entry
	//		to send to that server (initialized to leader last log index + 1)
	// matchIndex[]
	//		for each server, index of highest log entry
	//		known to be replicated on server (initialized to 0, increases monotonically)
	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
}

//
// return currentTerm and whether this server
// believes it is the leader.
//
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	var term int = rf.currentTerm
	var isleader bool = (rf.currentState == LEADER)
	rf.mu.Unlock()
	return term, isleader
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// if data == nil || len(data) < 1 { // bootstrap without any state?
	// 	return
	// }
	if data != nil && len(data) >= 1 {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.voteFor)
		d.Decode(&rf.logs)
		d.Decode(&rf.commitIndex)
		d.Decode(&rf.lastApplied)
	}

	// this is really important when bootstrapping without any state
	if len(rf.logs) == 0 {
		firstEntry := LogEntry{Term: 0, Command: nil}
		rf.logs = append(rf.logs, firstEntry)
		rf.commitIndex = 0
		rf.lastApplied = 0
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
	// Claim a term
	ClaimTerm int
	// Claim me
	ClaimMe int
	// Last Log
	LastLogTerm  int
	LastLogIndex int
}

//
// example AppendEntry RPC reply structure.
// field names must start with capital letters!
//
type AppendEntryArgs struct {
	// Your data here (2A, 2B).
	//Claim a term
	ClaimTerm int
	//Claim me
	ClaimMe int
	//Prev log
	PrevLogTerm  int
	PrevLogIndex int
	//New log
	Entries []LogEntry
	//Leader’s commitIndex
	LeaderCommit int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	//Claim a term
	ClaimTerm int
	//Claim whether to vote
	ClaimGranted bool
}

//
// example AppendEntry RPC reply structure.
// field names must start with capital letters!
//
type AppendEntryReply struct {
	// Your data here (2A).
	//Claim a term
	ClaimTerm int
	// //Claim me
	// ClaimMe int
	//Claim whether to vote
	ClaimGranted bool
	//Prev log
	PrevLogIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	// comments are already in debug info print
	// INFO("Server %d received request vote from Server %d", rf.me, args.ClaimMe)

	// select {
	// case rf.getHeartbeat <- true:
	// default:
	// }
	// select {
	// case rf.getRequestVote <- true:
	// default:
	// }

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm < args.ClaimTerm {
		rf.currentTerm = args.ClaimTerm
		rf.currentState = FOLLOWER
		rf.voteFor = -1
	}

	if rf.currentTerm > args.ClaimTerm {
		reply.ClaimTerm = rf.currentTerm
		reply.ClaimGranted = false
		// INFO("Server %d rejected request vote from Server %d for current term", rf.me, args.ClaimMe)
		return
	}

	if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
		reply.ClaimTerm = rf.currentTerm
		reply.ClaimGranted = false
		// INFO("Server %d rejected request vote from Server %d for last log term", rf.me, args.ClaimMe)
		return
	}

	if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex {
		reply.ClaimTerm = rf.currentTerm
		reply.ClaimGranted = false
		// INFO("Server %d rejected request vote from Server %d for last log index", rf.me, args.ClaimMe)
		return
	}

	if rf.voteFor != -1 && rf.voteFor != args.ClaimMe {
		reply.ClaimTerm = rf.currentTerm
		reply.ClaimGranted = false
		// INFO("Server %d rejected request vote from Server %d for already voting for another", rf.me, args.ClaimMe)
		return
	}

	if rf.currentTerm <= args.ClaimTerm {
		rf.currentState = FOLLOWER
		rf.voteFor = args.ClaimMe
		rf.currentTerm = args.ClaimTerm
		reply.ClaimTerm = args.ClaimTerm
		reply.ClaimGranted = true
		// INFO("Server %d accepted request vote from Server %d", rf.me, args.ClaimMe)
		return
	}
}

//
// example AppendEntry RPC handler.
//
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B).

	// comments are already in debug info print

	rf.getHeartbeat <- true
	// INFO("Server %d received append entry from Server %d", rf.me, args.ClaimMe)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	// reply.ClaimMe = rf.me
	if rf.currentTerm > args.ClaimTerm {
		reply.ClaimTerm = rf.currentTerm
		reply.ClaimGranted = false
		reply.PrevLogIndex = args.PrevLogIndex
		// INFO("Server %d rejected append entry from Server %d for current term", rf.me, args.ClaimMe)
		return
	}
	if rf.currentState == CANDIDATE {
		rf.currentState = FOLLOWER
	}
	rf.currentTerm = args.ClaimTerm
	reply.ClaimTerm = args.ClaimTerm

	if len(rf.logs) > args.PrevLogIndex && rf.logs[args.PrevLogIndex].Term == args.PrevLogTerm {
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		for i := 0; i < len(args.Entries); i++ {
			rf.logs = append(rf.logs, args.Entries[i])
		}
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		}
		reply.PrevLogIndex = len(rf.logs) - 1
		reply.ClaimGranted = true
		// INFO("Server %d accepted append entry from Server %d and append logs", rf.me, args.ClaimMe)
	} else {
		if args.PrevLogIndex-1 >= rf.commitIndex {
			reply.PrevLogIndex = rf.commitIndex
		} else {
			reply.PrevLogIndex = args.PrevLogIndex - 1
		}
		reply.ClaimGranted = false
		// INFO("Server %d accepted append entry from Server %d but find prevlog mismatched", rf.me, args.ClaimMe)
	}

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
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term, isLeader := rf.GetState()

	// Your code here (2B).

	if isLeader { // only append when it is leader
		rf.mu.Lock()
		index = len(rf.logs)
		entry := LogEntry{Term: term, Command: command}
		rf.logs = append(rf.logs, entry)
		rf.nextIndex[rf.me] = index + 1
		rf.matchIndex[rf.me] = index
		rf.persist()
		rf.mu.Unlock()

		// are these lines necessary? (2020.05.03)
		// go rf.raiseAppendEntry()
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	rf.disconnectTimeOut = time.Duration(rand.Intn(DisconnectTimeOutRandom)+DisconnectTimeOutLowerBound) * time.Millisecond
	rf.electionTimeOut = time.Duration(rand.Intn(ElectionTimeOutRandom)+ElectionTimeOutLowerBound) * time.Millisecond

	rf.voteFor = -1

	rf.currentTerm = 0
	rf.currentState = FOLLOWER

	rf.getHeartbeat = make(chan bool)
	rf.becomeLeader = make(chan bool)

	rf.getRequestVote = make(chan bool)

	rf.logs = []LogEntry{}
	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//start a new goroutine for election
	go rf.startElectionLoop()
	go rf.updateCommit()
	go rf.apply2State(applyCh)

	return rf
}

//
// loop for a server, call different function according to current state
//
func (rf *Raft) startElectionLoop() {
	for {
		// the lock here looks ugly, but i can't find a better way
		// you have to lock current state when switching
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		switch rf.currentState {
		case FOLLOWER:
			rf.mu.Unlock()
			rf.asFollower()
		case CANDIDATE:
			rf.mu.Unlock()
			rf.asCandidate()
		case LEADER:
			rf.mu.Unlock()
			rf.asLeader()
		}
		// give the loop a break as said in the guide
		time.Sleep(ShortBreath * time.Millisecond)
	}
}

//
// behave as a follower
//
func (rf *Raft) asFollower() {
	// rf.RaftPrintf()
	select {
	case <-rf.getHeartbeat:
	case <-rf.getRequestVote:
	case <-time.After(rf.disconnectTimeOut):
		rf.mu.Lock()
		rf.currentState = CANDIDATE
		rf.voteFor = rf.me
		rf.currentTerm++
		rf.persist()
		rf.mu.Unlock()
	}
}

//
// behave as a candidate
// note we have to check whether the server is still candidate
// even if becomeLeader gets true
// and we have to check whether the server still candidate
// after election timeout and no heartbeat received
// note we use ElectionTimeOut twice in this function
// they serve for different purposes
//
func (rf *Raft) asCandidate() {
	// rf.RaftPrintf()

	// eat previous becomeLeader if any
	// I don't know whether this helps actually
	// but it won't hurt anyway
	select {
	case <-rf.becomeLeader:
	default:
	}

	go rf.raiseElection()

	ElectionTimeOut := time.Duration(rand.Intn(ElectionTimeOutRandom)+ElectionTimeOutLowerBound) * time.Millisecond
	select {
	case <-time.After(ElectionTimeOut):
		rf.mu.Lock()
		if rf.currentState == CANDIDATE {
			rf.voteFor = rf.me
			rf.currentTerm++
			rf.persist()
		}
		rf.mu.Unlock()
	case <-rf.getHeartbeat:
		// rf.mu.Lock()
		// rf.currentState = FOLLOWER
		// rf.voteFor = -1
		// rf.mu.Unlock()
	case <-rf.becomeLeader:
		rf.mu.Lock()
		if rf.currentState == CANDIDATE {
			rf.currentState = LEADER
			// rf.voteFor = -1
			for i := 0; i < len(rf.peers); i++ {
				rf.nextIndex[i] = len(rf.logs)
				rf.matchIndex[i] = 0
			}
			rf.matchIndex[rf.me] = len(rf.logs) - 1
			go rf.raiseAppendEntry()
		}
		rf.mu.Unlock()
	}
}

//
//behave as a leader
//
func (rf *Raft) asLeader() {
	// rf.RaftPrintf()
	go rf.raiseAppendEntry()
	time.Sleep(HeartbeatInterval * time.Millisecond)
}

//
// raise a turn of election as candidate
// even we enter this function because of a candidate state
// we still need to check whether the server is still candidate
// pay attention to becomeLeader from last turn
//
func (rf *Raft) raiseElection() {

	args := &RequestVoteArgs{}
	rf.mu.Lock()
	if rf.currentState == CANDIDATE {
		args.ClaimTerm = rf.currentTerm
		args.ClaimMe = rf.me
		args.LastLogIndex = len(rf.logs) - 1
		args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		rf.mu.Unlock()
	} else {
		rf.mu.Unlock()
		return
	}

	c := make(chan *RequestVoteReply, len(rf.peers)-1)
	for k := 0; k < len(rf.peers); k++ {
		go func(temp int) {
			if temp == args.ClaimMe { //pass itself
				return
			}
			reply := &RequestVoteReply{ClaimTerm: 0, ClaimGranted: false}
			rf.sendRequestVote(temp, args, reply)
			// WARNING("Server %d gets result from Server %d, %t to be leader of term %d", args.ClaimMe, temp, reply.ClaimGranted, args.ClaimTerm)
			c <- reply
		}(k)
	}

	// gather results
	finish := 1
	count := 1
	var vote *RequestVoteReply
	for {
		vote = <-c
		rf.mu.Lock()
		if rf.currentTerm != args.ClaimTerm || rf.currentState != CANDIDATE {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		if vote.ClaimGranted {
			count++
			if 2*count > len(rf.peers) {
				// rf.mu.Lock()
				// WARNING("Server %d is going to be leader of term %d", rf.me, args.ClaimTerm)
				// rf.mu.Unlock()
				rf.becomeLeader <- true
				break
			}
		} else {
			rf.mu.Lock()
			if vote.ClaimTerm > rf.currentTerm {
				rf.currentTerm = vote.ClaimTerm
				rf.currentState = FOLLOWER
				rf.voteFor = -1
				rf.persist()
				rf.mu.Unlock()
				// rf.becomeLeader <- false
				break
			}
			rf.mu.Unlock()
		}
		finish++
		if finish == len(rf.peers) {
			// rf.becomeLeader <- false
			break
		}
	}
}

//
// raise an append entry as a leader
// even we enter this function because of a leader state
// we still need to check whether the server is still leader
// in case it updates term as a follower, and then packs up args
//
func (rf *Raft) raiseAppendEntry() {

	// c := make(chan *AppendEntryReply, len(rf.peers)-1)
	for k := 0; k < len(rf.peers); k++ {
		go func(temp int) {
			if temp == rf.me { //pass itself
				return
			}
			rf.mu.Lock()
			if rf.currentState == LEADER {
				args := &AppendEntryArgs{}
				args.ClaimTerm = rf.currentTerm
				args.ClaimMe = rf.me
				args.LeaderCommit = rf.commitIndex
				if rf.nextIndex[temp] > len(rf.logs) {
					rf.nextIndex[temp] = len(rf.logs)
				}
				args.PrevLogIndex = rf.nextIndex[temp] - 1
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				args.Entries = rf.logs[args.PrevLogIndex+1:]
				rf.mu.Unlock()
				// INFO("Sending %v from Server %d to Server %d", args, args.ClaimMe, temp)
				reply := &AppendEntryReply{}
				ok := rf.sendAppendEntry(temp, args, reply)
				// c <- reply
				// INFO("Receiving %v at Server %d from Server %d    Timeout? %t", reply, args.ClaimMe, temp, !ok)
				if ok {
					rf.mu.Lock()
					rf.nextIndex[temp] = reply.PrevLogIndex + 1
					if rf.currentState == LEADER {
						if !reply.ClaimGranted && rf.currentTerm < reply.ClaimTerm {
							rf.currentTerm = reply.ClaimTerm
							rf.currentState = FOLLOWER
							rf.voteFor = -1
							rf.persist()
						}
						if reply.ClaimGranted && reply.PrevLogIndex > rf.matchIndex[temp] {
							rf.matchIndex[temp] = reply.PrevLogIndex
						}
					}
					rf.mu.Unlock()
				}
			} else {
				rf.mu.Unlock()
			}
		}(k)
	}

	// the code bellow is my attempt to write raiseAppendEntry
	// similarly to raiseRequestVote however i failed

	// finish := 1
	// var vote *AppendEntryReply
	// for {
	// 	vote = <-c
	// 	finish++
	// 	if finish == len(rf.peers) {
	// 		break
	// 	}
	// }
}

//
// if leader find most servers agree on new logs, update commitIndex
// we conduct this by check the median of matchIndex
// if Median(matchIndex) > commitIndex, it means we can safely commit more logs
// !!!!! checking median is not enough !!!!! (2020.05.03)
// you have to ensure that logs[Median(matchIndex)].Term == currentTerm
// note this is an independent goroutine called by Make()
//
func (rf *Raft) updateCommit() {
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		if rf.currentState == LEADER {
			matchIndexCopy := make([]int, len(rf.peers))
			copy(matchIndexCopy, rf.matchIndex)
			sort.Ints(matchIndexCopy)
			majorityCommitIndex := matchIndexCopy[(len(matchIndexCopy)-1)/2]
			minimumCommitIndex := matchIndexCopy[0]
			// maximumCommitIndex := matchIndexCopy[len(matchIndexCopy)-1]
			if minimumCommitIndex > rf.commitIndex {
				// INFO("Server %d update commit index from %d to %d", rf.me, rf.commitIndex, minimumCommitIndex)
				rf.commitIndex = minimumCommitIndex
			}
			if majorityCommitIndex > rf.commitIndex && rf.logs[majorityCommitIndex].Term == rf.currentTerm {
				// INFO("Server %d update commit index from %d to %d", rf.me, rf.commitIndex, majorityCommitIndex)
				rf.commitIndex = majorityCommitIndex
			}
		}
		rf.persist()
		rf.mu.Unlock()
		time.Sleep(HeartbeatInterval * time.Millisecond)
	}
}

//
// if lastApplied < commitIndex, commit these logs to state machine
// note this is an independent goroutine called by Make()
//
func (rf *Raft) apply2State(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			break
		}
		if rf.lastApplied < rf.commitIndex {
			// INFO("Server %d apply entry from %d to %d", rf.me, rf.lastApplied, rf.commitIndex)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				Msg := ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
				applyCh <- Msg
			}
			rf.lastApplied = rf.commitIndex
		}
		rf.persist()
		rf.mu.Unlock()
		time.Sleep(HeartbeatInterval * time.Millisecond)
	}
}
