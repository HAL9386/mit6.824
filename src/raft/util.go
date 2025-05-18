package raft

import (
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// type StateType int32
const (
	Follower int32 = iota
	Candidate
	Leader
)

const HeartbeatTimeout = 100 * time.Millisecond

type LogEntry struct {
	Term    int         // term of the entry
	Command interface{} // command for the entry
	Index   int         // index of the entry, increased from 1
}

// Randomly generate a timeout between 100ms and 400ms
func randomElectionTimeout() time.Duration {
	return time.Duration(100 + rand.Intn(300)) * time.Millisecond
}

func (rf *Raft) checkTimeout() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return time.Since(rf.lastElectionReset) > rf.electionTimeout 
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log) - 1].Index
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log) - 1].Term
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = randomElectionTimeout()
	rf.lastElectionReset = time.Now()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state              = Candidate
	rf.currentTerm       += 1
	rf.votedFor           = rf.me
	rf.votesReceived      = 1         // vote for self
	rf.mu.Unlock()
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	term         := rf.currentTerm
	me           := rf.me
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm  := rf.getLastLogTerm()
	rf.resetElectionTimer()
	rf.mu.Unlock()
	// broadcast RequestVote RPCs to all other servers
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// concurrent RPC calls
		go func(peer int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := RequestVoteReply{}
			if !rf.sendRequestVote(peer, &args, &reply) {
				return
			}
			rf.handleRequestVoteReply(&reply, term)
		}(peer)
	}
}

func (rf *Raft) handleRequestVoteReply(reply *RequestVoteReply, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// check if the term is still the same
	if rf.state != Candidate || term != rf.currentTerm {
		return
	}
	// handle the reply
	if reply.VoteGranted {
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 { // receive majority votes
			rf.state = Leader
			return 
		}
	} else if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term, -1)
	}
}

func (rf *Raft) startLeader() {
	rf.mu.Lock()
	lastIndex := rf.getLastLogIndex()
	for peer := range rf.peers {
		rf.nextIndex[peer] = lastIndex + 1
		rf.matchIndex[peer] = 0
	}
	rf.mu.Unlock()

	for atomic.LoadInt32(&rf.state) == Leader {
		rf.broadcastAppendEntries()
		time.Sleep(HeartbeatTimeout)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term     := rf.currentTerm
	leaderId := rf.me
	rf.mu.Unlock()

	// broadcast AppendEntries RPCs to all other servers
	for peer := range rf.peers {
		if peer == leaderId {
			continue
		}
		// concurrent RPC calls
		go func(peer int) {
			rf.mu.Lock()
			prevLogIndex := rf.nextIndex[peer] - 1 // index of the entry preceding the new one
			prevLogTerm  := rf.log[prevLogIndex].Term // term of the entry preceding the new one
			entries      := make([]LogEntry, len(rf.log[rf.nextIndex[peer]:])) // entries to send, starting from nextIndex
			copy(entries, rf.log[rf.nextIndex[peer]:])
			leaderCommitIndex := rf.commitIndex
			rf.mu.Unlock()

			args := AppendEntriesArgs{
				Term        : term,
				LeaderId    : leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm : prevLogTerm,
				Entries     : entries,
				LeaderCommit: leaderCommitIndex,
			}
			reply := AppendEntriesReply{}
			if !rf.sendAppendEntries(peer, &args, &reply) {
				return
			}
			rf.handleAppendEntriesReply(&args, &reply, peer)
		}(peer)
	}
}

func (rf *Raft) handleAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply, peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > args.Term {
		rf.convertToFollower(reply.Term, -1)
		return
	}
	if reply.Success {
		rf.matchIndex[peer]	= args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer]	= rf.matchIndex[peer] + 1
		rf.maybeAdvanceCommitIndex()
	} else {
		rf.nextIndex[peer] -= 1
	}
}

func (rf *Raft) maybeAdvanceCommitIndex() {
	for N := rf.commitIndex + 1; N <= rf.getLastLogIndex(); N++ {
		if rf.log[N].Term != rf.currentTerm {
			continue
		}
		count := 1
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			if rf.matchIndex[peer] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			// rf.applyEntries()
			break
		}
	}
}

func (rf *Raft) convertToFollower(term int, votedFor int) {
	rf.state         = Follower
	rf.currentTerm   = term
	rf.votedFor      = votedFor
	rf.votesReceived = 0
	rf.resetElectionTimer()
}

func (rf *Raft) compareLogEntries(term int, index int) int {
	myTerm  := rf.getLastLogTerm()
	myIndex := rf.getLastLogIndex()
	if myTerm != term {
		return myTerm - term
	}
	return myIndex - index
}
