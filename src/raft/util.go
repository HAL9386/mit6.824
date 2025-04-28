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

// type StateType int32
const (
	Follower int32 = iota
	Candidate
	Leader
)

const HeartbeatTimeout = 100 * time.Millisecond

type LogEntry struct {
	Term    int
	Command interface{}
	Index   int
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
	// term                 := rf.currentTerm
	// me                   := rf.me
	// lastLogIndex         := rf.getLastLogIndex()
	// lastLogTerm          := rf.getLastLogTerm()
	// rf.electionTimeout    = randomElectionTimeout()
	// rf.lastElectionReset  = time.Now()
	rf.mu.Unlock()
	// // broadcast RequestVote RPCs to all other servers
	// for peer := range rf.peers {
	// 	if peer == rf.me {
	// 		continue
	// 	}
	// 	// concurrent RPC calls
	// 	go func(peer int) {
	// 		args := RequestVoteArgs{
	// 			Term:         term,
	// 			CandidateId:  me,
	// 			LastLogIndex: lastLogIndex,
	// 			LastLogTerm:  lastLogTerm,
	// 		}
	// 		reply := RequestVoteReply{}
	// 		for !rf.sendRequestVote(peer, &args, &reply) {
	// 			return
	// 		}
	// 		rf.handleRequestVoteReply(&reply, term)
	// 	}(peer)
	// }
}

func (rf *Raft) broadcastRequestVote() {
	rf.mu.Lock()
	term                 := rf.currentTerm
	me                   := rf.me
	lastLogIndex         := rf.getLastLogIndex()
	lastLogTerm          := rf.getLastLogTerm()
	rf.lastElectionReset	= time.Now()
	rf.electionTimeout    = randomElectionTimeout()
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
		// rf.mu.Unlock()
		return
	}
	// handle the reply
	if reply.VoteGranted {
		rf.votesReceived++
		if rf.votesReceived > len(rf.peers)/2 { // receive majority votes
			rf.state = Leader
			// rf.mu.Unlock()
			// rf.startLeader()
			return 
		}
	} else if reply.Term > rf.currentTerm {
		rf.state             = Follower
		rf.currentTerm       = reply.Term
		rf.votedFor          = -1
		rf.electionTimeout	 = randomElectionTimeout()
		rf.lastElectionReset = time.Now()
	}
	// rf.mu.Unlock()
}

func (rf *Raft) startLeader() {
	for {
		rf.broadcastHeartBeat()
		time.Sleep(HeartbeatTimeout)
	}
}

func (rf *Raft) broadcastHeartBeat() {
	rf.mu.Lock()
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	term := rf.currentTerm
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
			prevLogIndex := 0
			prevLogTerm := 0
			rf.mu.Unlock()
			args := AppendEntriesArgs{
				Term        : term,
				LeaderId    : leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm : prevLogTerm,
				Entries     : nil,
				LeaderCommit: 0,
			}
			reply := AppendEntriesReply{}
			if !rf.sendAppendEntries(peer, &args, &reply) {
				return
			}
			rf.handleAppendEntriesReply(&reply, term)
		}(peer)
	}
}

func (rf *Raft) handleAppendEntriesReply(reply *AppendEntriesReply, term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > term {
		rf.state             = Follower
		rf.currentTerm       = reply.Term
		rf.votedFor          = -1
		rf.electionTimeout   = randomElectionTimeout()
		rf.lastElectionReset = time.Now()
		return
	}
}
