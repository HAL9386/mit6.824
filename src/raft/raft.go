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
	//	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const DEBUG = true
func (rf *Raft) mlog(msg string, args ...interface{}) {
	if !DEBUG {
		return
	}
	str := fmt.Sprintf(msg, args...)
	var state string
	switch rf.state {
	case FollowerState:
		state = "Follower"
	case CandidateState:
		state = "Candidate"
	case LeaderState:
		state = "Leader"
	}
	fmt.Printf("Peer: %2d, Term: %4d, State: %10s, %s\n", rf.me, rf.term, state, str)
}

const (
	FollowerState  = 0
	CandidateState = 1
	LeaderState    = 2
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
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

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	term      int
	leader    int
	state     int // Follower, Candidate, Leader
	myVotes   int // votes received in this term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
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
	// Your code here (3C).
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
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	// Your code here (3D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	candidateId int
	term        int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	voteGranted bool 
	term        int
}

type AppendEntriesArgs struct {
	isHeartbeat  bool          // differentiate between heartbeat and log append
	term         int           // leader's term
	leaderId     int           // so follower can redirect clients
	prevLogIndex int           // index of log entry immediately preceding new ones
	prevLogTerm  int           // term of prevLogIndex entry
	entries      []interface{} // log entries to store (empty for heartbeat; may send more than one for efficiency)
	leaderCommit int           // leader's commitIndex
}

type AppendEntriesReply struct {
	success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	term    int  // currentTerm, for leader to update itself
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// i am bigger than you, reject
	if (args.term < rf.term) {
		reply.term = rf.term
		reply.voteGranted = false
		return
	}
	if (rf.state == FollowerState) {
		// you are bigger than me, accept
		reply.voteGranted = true
		reply.term = args.term
		rf.term = args.term
	} else if (rf.state == CandidateState || rf.state == LeaderState) {
		// we are equal, reject
		if (args.term == rf.term) {
			reply.voteGranted = false
			reply.term = rf.term
		} else { // you are bigger than me, accept
			rf.state = FollowerState
			rf.term = args.term
			reply.voteGranted = true
		}
	}
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

	// Your code here (3B).


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

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		// electionTimeout := 200 + (rand.Int63() % 300)
		// rf.mlog("electionTimeout: %d", electionTimeout)

		// // pause for a random amount of time between 50 and 350
		// // milliseconds.
		// time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
		// rf.state = CandidateState
		// rf.term += 1
		// rf.myVotes = 1
		// rf.sendElectionRequest()
		switch rf.state {
		case FollowerState:

		}
	}
}

func (rf *Raft) sendElectionRequest() {
	args := RequestVoteArgs{}
	args.term = rf.term
	args.candidateId = rf.me

	for peer := 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		// send RequestVote RPC to all other servers
		go func(peer int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(peer, &args, &reply)
			if ok {
				rf.handleRequestVoteReply(&reply)
			}
		}(peer)
	}
	if rf.state == LeaderState {
		rf.sendLeaderHeartbeat()
	}
}

func (rf *Raft) handleRequestVoteReply(reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.voteGranted {
		rf.myVotes += 1	
		if rf.myVotes > len(rf.peers)/2 {
			rf.state = LeaderState
		}
		rf.leader = rf.me
	} else if reply.term > rf.term {
		// I am not the leader anymore
		rf.state = FollowerState
		rf.term = reply.term
		rf.leader = -1
	}
}

func (rf *Raft) sendLeaderHeartbeat() {

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

	// Your initialization code here (3A, 3B, 3C).
	rf.term = 0
	rf.leader = -1
	rf.state = FollowerState

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
