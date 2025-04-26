package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

type StateType int
const (
	Follower StateType = iota
	Candidate
	Leader
)

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
	// if len(rf.log) == 0 {
	// 	return 0
	// }
	// return rf.log[len(rf.log)-1].Index
	return 0
}

func (rf *Raft) getLastLogTerm() int {
	// if len(rf.log) == 0 {
	// 	return 0
	// }
	// return rf.log[len(rf.log)-1].Term
	return 0
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.state              = Candidate
	rf.currentTerm       += 1
	rf.votedFor           = rf.me
	rf.votesReceived      = 1                    // vote for self
	term                 := rf.currentTerm
	me                   := rf.me
	lastLogIndex         := rf.getLastLogIndex()
	lastLogTerm          := rf.getLastLogTerm()
	rf.lastElectionReset  = time.Now()
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
	// defer rf.mu.Unlock()
	// check if the term is still the same
	if rf.state != Candidate || term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	// handle the reply
	if reply.VoteGranted {
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 {
			rf.state = Leader
			rf.mu.Unlock()
			rf.startLeader()
			return 
		}
	} else if reply.Term > rf.currentTerm {
		rf.state             = Follower
		rf.currentTerm       = reply.Term
		rf.votedFor          = -1
		rf.lastElectionReset = time.Now()
	}
	rf.mu.Unlock()
}

func (rf *Raft) startLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// initialize nextIndex and matchIndex
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.broadcastAppendEntries()
	go rf.leaderHeartbeat()
}
