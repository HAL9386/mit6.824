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

	nextIndex	  []int // for each server, index of the next log entry to send to that server
	matchIndex	[]int // for each server, index of highest log entry known to be replicated on server

	// Persistent state on all servers:
	log 			  []LogEntry // log entries
	currentTerm int // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int // candidateId that received vote in current term (or null if none)

	// Volatile state on all servers:
	state             int32         // Follower, Candidate, Leader
	electionTimeout   time.Duration // timeout for elections
	lastElectionReset time.Time     // last time a election relevant message was received
	votesReceived     int           // number of votes received in current term
	commitIndex       int           // index of highest log entry known to be committed
	lastApplied       int           // index of highest log entry applied to state machine
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.state == Leader
	rf.mu.Unlock()
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
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  // term reply to candidate for update or not
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// 5.6s
	if rf.currentTerm >= args.Term {
		reply.VoteGranted = false
		return
	}
	rf.convertToFollower(args.Term, -1)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
	   rf.compareLogEntries(args.LastLogTerm, args.LastLogIndex) <= 0 {
		rf.convertToFollower(args.Term, args.CandidateId)
		reply.VoteGranted = true
	}
	// 14s
	// if args.Term < rf.currentTerm {
	// 	reply.VoteGranted = false
	// 	return
	// }
	// if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
	// 	reply.VoteGranted = false
	// 	return
	// }
	// // check if the candidate's log is up to date
	// if rf.compareLogEntries(args.LastLogTerm, args.LastLogIndex) > 0 {
	// 	reply.VoteGranted = false
	// 	return
	// }
	// // voter for candidate
	// rf.convertToFollower(args.Term, args.CandidateId)
	// reply.VoteGranted = true

	// 7s
	// check if the term is up to date
	// if args.Term > rf.currentTerm {
	// 	rf.convertToFollower(args.Term, args.CandidateId)
	// 	reply.VoteGranted = true
	// } else if args.Term == rf.currentTerm {
	// 	// already voted for someone else, reject
	// 	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
	// 		reply.VoteGranted = false
	// 		return
	// 	}
	// 	// else
	// 	// votedFor == -1 || votedFor == args.CandidateId
	// 	// check if the candidate's log is up to date
	// 	// if rf.getLastLogTerm() > args.LastLogTerm || rf.getLastLogIndex() > args.LastLogIndex {
	// 	if rf.compareLogEntries(args.LastLogTerm, args.LastLogIndex) > 0 {
	// 		reply.VoteGranted = false
	// 		return
	// 	}
	// 	rf.convertToFollower(args.Term, args.CandidateId)
	// 	reply.VoteGranted = true
	// } else if args.Term < rf.currentTerm {
	// 	reply.VoteGranted = false
	// }
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int // so follower can redirect clients
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader's commitIndex
}
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// receiver implementation 1. reply false if im bigger
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term, -1)
	}
	// consistency check
	// receiver implementation 2. reply false if log doesnt contain an entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIndex >= len(rf.log) ||                  // check if the prevLogIndex is in the log
	   rf.log[args.PrevLogIndex].Term != args.PrevLogTerm { // check if the term of the prevLogIndex is correct
		reply.Success = false
		rf.resetElectionTimer()
		return
	}
	// append entries
	newIndex := args.PrevLogIndex + 1
	// receiver implementation 3. if an existing entry conflicts with a new one (same index but different term), delete the existing entry and all that follow it
	for i, entry := range args.Entries {
		if newIndex+i >= len(rf.log) {
			break
		}
		if rf.log[newIndex+i].Term != entry.Term {
			rf.log = rf.log[:newIndex+i]
			break
		}
	}
	// receiver implementation 4. append any new entries not already in the log
	for i := newIndex; i < newIndex+len(args.Entries); i++ {
		if i < len(rf.log) {
			continue
		}
		rf.log = append(rf.log, args.Entries[i-newIndex])
	}
	// receiver implementation 5. if leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		// rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		if (args.LeaderCommit < rf.getLastLogIndex()) {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = rf.getLastLogIndex()
		}
	}

	rf.resetElectionTimer()
	reply.Success = true
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (3B).
	rf.mu.Lock()
	// defer rf.mu.Unlock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return -1, -1, false
	}
	currentTerm := rf.currentTerm
	newIndex := rf.getLastLogIndex() + 1
	newEntry := LogEntry{
		Index: newIndex,
		Term:  rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newEntry)
	for peer := range rf.peers {
		if rf.nextIndex[peer] != 0 {
			continue
		}
		rf.nextIndex[peer] = len(rf.log) // nextIndex is the index of the next log entry to send to the peer
		rf.matchIndex[peer] = 0
	}
	rf.mu.Unlock()
	rf.broadcastAppendEntries()

	return newIndex, currentTerm, true
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

		// rf.mu.Lock()
		// state := rf.state
		// rf.mu.Unlock()
		state := atomic.LoadInt32(&rf.state)
		switch state {
		case Follower:
			if rf.checkTimeout() {
				rf.startElection()
			}
		case Candidate:
			if rf.checkTimeout() {
				rf.startElection()
				rf.broadcastRequestVote()
			}
		case Leader:
			rf.startLeader()
		}
		
		time.Sleep(100 * time.Millisecond)

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		// ms := 50 + (rand.Int63() % 300)
		// time.Sleep(time.Duration(ms) * time.Millisecond)
	}
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
	rf.dead          = 0
	rf.state         = Follower
	rf.currentTerm   = 0
	rf.votedFor      = -1
	rf.votesReceived = 0
	rf.resetElectionTimer()

	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: 0, Command: nil, Index: 0})

	// for leader
	rf.nextIndex   = make([]int, len(peers))
	rf.matchIndex  = make([]int, len(peers))
	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applier(applyCh)

	return rf
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}	
			rf.mu.Unlock()
			applyCh <- applyMsg
			rf.mu.Lock()
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}