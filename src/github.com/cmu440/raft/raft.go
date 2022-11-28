//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me", its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cmu440/rpc"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = false

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	log         []LogEntry
	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	//
	term int
	//0 is Follower, 1 is Leader, 2 is Candidate
	role     int
	votedFor int

	receiveheartbeat bool
	receivevote      bool

	//chan
	applyCh         chan ApplyCommand
	reuestStateChan chan bool
	stateChan       chan *State
	askedToVote     chan *VoteRequest
	requestFinished chan bool
	voteResult      chan *VoteRequest
	commandChan     chan interface{}

	askedToAppend         chan *AppendRequest
	appendRequestFinished chan bool
	appendResult          chan *AppendRequest
	commandReplyChan      chan *PutReply

	//arguments for candidate
	receivedYesVotes int
}
type State struct {
	me       int
	term     int
	isleader bool
}

// GetState()
// ==========
//
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	// Your code here (2A)
	rf.reuestStateChan <- true
	st := <-rf.stateChan
	return st.me, st.term, st.isleader
}

// RequestVoteArgs
// ===============
//
// # Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int //candidate term
	CandidateId  int //candidate id
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A)
	Term        int
	VoteGranted bool
}

type VoteRequest struct {
	args  *RequestVoteArgs
	reply *RequestVoteReply
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (2A)
	Term    int
	Success bool
}

type AppendRequest struct {
	args   *AppendEntriesArgs
	reply  *AppendEntriesReply
	server int
}

// RequestVote
// ===========
//
// Example RequestVote RPC handler
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	newrequest := &VoteRequest{
		args:  args,
		reply: reply,
	}
	rf.askedToVote <- newrequest
	<-rf.requestFinished

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B)
	newrequest := &AppendRequest{
		args:   args,
		reply:  reply,
		server: rf.me,
	}
	rf.askedToAppend <- newrequest
	<-rf.appendRequestFinished

}

// sendRequestVote
// ===============
//
// # Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// # Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// # Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		RequestVoteResult := &VoteRequest{
			args:  args,
			reply: reply,
		}
		rf.voteResult <- RequestVoteResult
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		AppendEntriesResult := &AppendRequest{
			args:   args,
			reply:  reply,
			server: server,
		}
		rf.appendResult <- AppendEntriesResult
	}
	return ok
}

type LogEntry struct {
	Term       int
	LogCommand interface{}
}

type PutReply struct {
	Index int
	Term  int
	ok    bool
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// # If this server is not the leader, return false
//
// # Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// # The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	// Your code here (2B)
	// Put command in the channel to get state of the server
	rf.commandChan <- command
	reply := <-rf.commandReplyChan

	return reply.Index, reply.Term, reply.ok
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	// Your code here, if desired
}

// NewPeer
// ====
//
// # The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	//initilize all fileds in rf
	rf := &Raft{
		term:            0,
		role:            0,
		votedFor:        -1,
		reuestStateChan: make(chan bool),
		stateChan:       make(chan *State),

		askedToVote:     make(chan *VoteRequest),
		requestFinished: make(chan bool),
		voteResult:      make(chan *VoteRequest),

		askedToAppend:         make(chan *AppendRequest),
		appendRequestFinished: make(chan bool),
		appendResult:          make(chan *AppendRequest),
		commandChan:           make(chan interface{}),
		receivedYesVotes:      0,
		applyCh:               applyCh,
		commandReplyChan:      make(chan *PutReply),

		receiveheartbeat: false,
		receivevote:      false,

		log:         make([]LogEntry, 0),
		commitIndex: 0,
		lastApplied: 0,
	}
	rf.peers = peers
	rf.me = me
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	//enable logger
	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}

	// run mainroutine
	go rf.mainRoutine()

	return rf
}

func (rf *Raft) mainRoutine() {
	for {
		//everytie check the status of the server
		switch rf.role {
		//Follower
		case 0:
			rf.FollowerAction()
		//Leader
		case 1:
			rf.LeaderAction()
		//Candidate
		case 2:
			rf.CandidateAction()
		}
	}
}

func (rf *Raft) FollowerAction() {
	//set a timer for timeout
	random := rand.Intn(150) + 150
	timer := time.NewTimer(time.Duration(random) * time.Millisecond)
	for {
		//check if it is at the same status
		if rf.role != 0 {
			return
		}
		select {
		case <-rf.reuestStateChan:
			var isLeader bool
			if rf.role == 1 {
				isLeader = true
			} else {
				isLeader = false
			}
			st := &State{
				me:       rf.me,
				term:     rf.term,
				isleader: isLeader,
			}
			rf.stateChan <- st
		case <-timer.C:
			//timeout for Follower,change to Candidate and start vote request
			if rf.receiveheartbeat {
				//next timer
				random = rand.Intn(150) + 150
				timer = time.NewTimer(time.Duration(random) * time.Millisecond)
				rf.receiveheartbeat = false
			} else {
				//no heartbeat change to candidate
				rf.role = 2
				rf.NewRequestVoteRound()
			}
		case newrequest := <-rf.askedToVote:
			args := newrequest.args
			reply := newrequest.reply
			//check the term to decide append log
			if rf.term < args.Term {
				rf.term = args.Term
				if rf.checkLastLog(args.LastLogIndex, args.LastLogTerm) {
					rf.votedFor = args.CandidateId
					reply.VoteGranted = true
					reply.Term = args.Term
				} else {
					rf.votedFor = -1
					reply.VoteGranted = false
					reply.Term = args.Term
				}
			} else if rf.term == args.Term {
				if rf.votedFor == -1 {
					if rf.checkLastLog(args.LastLogIndex, args.LastLogTerm) {
						rf.votedFor = args.CandidateId
						reply.VoteGranted = true
						reply.Term = args.Term
					} else {
						rf.votedFor = -1
						reply.VoteGranted = false
						reply.Term = args.Term
					}
				} else {
					reply.VoteGranted = false
					reply.Term = args.Term
				}
			} else {
				reply.VoteGranted = false
				reply.Term = rf.term
			}
			rf.requestFinished <- true
		case appendrequest := <-rf.askedToAppend:
			//receiving heartbeat set true
			rf.receiveheartbeat = true

			args := appendrequest.args
			reply := appendrequest.reply
			if rf.term < args.Term {
				rf.term = args.Term
				rf.votedFor = -1
				reply.Success = false
				reply.Term = args.Term
			} else if rf.term == args.Term {
				//add entries
				rf.UpdateLogandReply(args, reply)
			} else {
				reply.Success = false
				reply.Term = rf.term
			}
			rf.logger.Printf("Server: %v now have log %v\n", rf.me, rf.log)
			rf.appendRequestFinished <- true
		case <-rf.appendResult:
		case <-rf.voteResult:
		case <-rf.commandChan:
			reply := &PutReply{
				Index: len(rf.log),
				Term:  rf.term,
				ok:    false,
			}
			rf.commandReplyChan <- reply

		}
	}
}

func (rf *Raft) LeaderAction() {
	timer := time.NewTimer(time.Duration(100) * time.Millisecond)
	for {
		if rf.role != 1 {
			return
		}
		select {
		case <-rf.reuestStateChan:
			var isLeader bool
			if rf.role == 1 {
				isLeader = true
			} else {
				isLeader = false
			}
			st := &State{
				me:       rf.me,
				term:     rf.term,
				isleader: isLeader,
			}
			rf.stateChan <- st
		case <-timer.C:
			//send heartbeat to all peers
			timer = time.NewTimer(time.Duration(100) * time.Millisecond)
			rf.BroadcastAppend()
		case newrequest := <-rf.askedToVote:
			//receive a request to vote
			args := newrequest.args
			reply := newrequest.reply
			if rf.term < args.Term {
				rf.role = 0
				rf.term = args.Term
				if rf.checkLastLog(args.LastLogIndex, args.LastLogTerm) {
					rf.votedFor = args.CandidateId
					reply.VoteGranted = true
					reply.Term = args.Term
				} else {
					rf.votedFor = -1
					reply.VoteGranted = false
					reply.Term = args.Term
				}
			} else {
				reply.VoteGranted = false
				reply.Term = rf.term
			}
			rf.requestFinished <- true
		case appendrequest := <-rf.askedToAppend:
			//handle appenentry
			args := appendrequest.args
			reply := appendrequest.reply
			if rf.term < args.Term {
				//become a follower
				rf.role = 0
				rf.term = args.Term
				rf.votedFor = -1
				rf.UpdateLogandReply(args, reply)
			} else {
				reply.Success = false
				reply.Term = rf.term
			}
			rf.logger.Printf("Server: %v now have log %v\n", rf.me, rf.log)
			rf.appendRequestFinished <- true
		case appendresult := <-rf.appendResult:
			//collect result from all follower
			args := appendresult.args
			reply := appendresult.reply
			index := appendresult.server
			if reply.Term > rf.term {
				rf.role = 0
				rf.term = reply.Term
				rf.votedFor = -1
			} else {
				if reply.Success {
					//update  matchindex
					rf.matchIndex[index] = args.PrevLogIndex + len(args.Entries)
					//update
					rf.nextIndex[index] = args.PrevLogIndex + len(args.Entries) + 1
					for {
						if rf.commitIndex >= len(rf.log) {
							break
						}
						newindex := rf.commitIndex + 1
						count := 1
						success := false
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								continue
							}
							if rf.matchIndex[i] >= newindex {
								count += 1
							}
							if 2*count > len(rf.peers) {
								rf.commitIndex = newindex
								success = true

								break
							}
						}
						if !success {
							break
						}
					}
					if rf.commitIndex > rf.lastApplied {
						for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
							message := ApplyCommand{
								Index:   i,
								Command: rf.log[i-1].LogCommand,
							}
							rf.applyCh <- message
						}
						rf.lastApplied = rf.commitIndex
					}

				} else {
					//resend
					if rf.nextIndex[index] > 1 {
						rf.nextIndex[index] -= 1
					}
					rf.SpecificAppend(index)
				}

			}
		case <-rf.voteResult:
		case command := <-rf.commandChan:
			//putcommand input a new command
			entry := LogEntry{
				Term:       rf.term,
				LogCommand: command,
			}
			oldentries := rf.log
			newentries := append(oldentries, entry)
			rf.log = newentries
			reply := &PutReply{
				Index: len(rf.log),
				Term:  rf.term,
				ok:    true,
			}
			rf.commandReplyChan <- reply

		}
	}
}

func (rf *Raft) CandidateAction() {
	random := rand.Intn(300) + 300
	timer := time.NewTimer(time.Duration(random) * time.Millisecond)
	for {
		if rf.role != 2 {
			return
		}
		select {
		case <-rf.reuestStateChan:
			var isLeader bool
			if rf.role == 1 {
				isLeader = true
			} else {
				isLeader = false
			}
			st := &State{
				me:       rf.me,
				term:     rf.term,
				isleader: isLeader,
			}
			rf.stateChan <- st
		case <-timer.C:
			//timeout for Candidate,start a new vote request
			random = rand.Intn(300) + 300
			timer = time.NewTimer(time.Duration(random) * time.Millisecond)
			if rf.receivevote {
				//next timer
				rf.receivevote = false
			} else {
				rf.NewRequestVoteRound()
			}
		case newrequest := <-rf.askedToVote:
			//chose to vote
			args := newrequest.args
			reply := newrequest.reply
			if rf.term < args.Term {
				rf.role = 0
				rf.term = args.Term
				if rf.checkLastLog(args.LastLogIndex, args.LastLogTerm) {
					rf.votedFor = args.CandidateId
					reply.VoteGranted = true
					reply.Term = args.Term
				} else {
					rf.votedFor = -1
					reply.VoteGranted = false
					reply.Term = args.Term
				}
			} else {
				reply.VoteGranted = false
				reply.Term = rf.term
			}
			rf.requestFinished <- true
		case newreply := <-rf.voteResult:
			rf.receivevote = true
			//new reply from others
			reply := newreply.reply
			if reply.Term == rf.term {
				if reply.VoteGranted {
					rf.receivedYesVotes += 1
					if rf.receivedYesVotes*2 > len(rf.peers) {
						rf.role = 1
						lastlog := len(rf.log)
						for index := range rf.nextIndex {
							rf.nextIndex[index] = lastlog + 1
						}
						rf.logger.Printf("\n\nserver %v become leader reset!!! %v\n\n", rf.me, rf.nextIndex)
						rf.BroadcastAppend()
					}
				}
			} else if reply.Term < rf.term {
				rf.role = 0
				rf.votedFor = -1
				rf.term = reply.Term
			}
		case appendrequest := <-rf.askedToAppend:
			args := appendrequest.args
			reply := appendrequest.reply
			if rf.term <= args.Term {
				//become a follower
				rf.role = 0
				rf.term = args.Term
				rf.votedFor = -1
				//add entries
				rf.UpdateLogandReply(args, reply)
			} else {
				reply.Success = false
				reply.Term = rf.term
			}
			rf.logger.Printf("Server: %v now have log %v\n", rf.me, rf.log)
			rf.appendRequestFinished <- true
		case <-rf.appendResult:
		case <-rf.commandChan:
			reply := &PutReply{
				Index: len(rf.log),
				Term:  rf.term,
				ok:    false,
			}
			rf.commandReplyChan <- reply

		}
	}
}

func (rf *Raft) NewRequestVoteRound() {
	rf.term += 1
	//send vote to all peers
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		lastLogIndex := len(rf.log)
		lastLogTerm := 0
		if lastLogIndex > 0 {
			lastLogTerm = rf.log[lastLogIndex-1].Term
		}
		args := &RequestVoteArgs{
			Term:         rf.term,
			CandidateId:  rf.me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(index, args, reply)
	}
	rf.receivedYesVotes = 1
	rf.votedFor = rf.me
}

func (rf *Raft) BroadcastAppend() {
	//send vote to all peers
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		rf.SpecificAppend(index)
	}
}
func (rf *Raft) SpecificAppend(index int) {
	//send heartbeat or entry to a specific server
	prevLogIndex := rf.nextIndex[index] - 1
	prevTerm := -1
	if prevLogIndex > 0 {
		prevTerm = rf.log[prevLogIndex-1].Term
	}
	var entry []LogEntry
	if len(rf.log) > 0 {
		entry = make([]LogEntry, 0)
		for i := prevLogIndex; i < len(rf.log); i++ {
			entry = append(entry, rf.log[i])
		}
	}
	//rf.logger.Printf("new append entry to server %v nextIndex %v \n", index, rf.nextIndex)
	args := &AppendEntriesArgs{
		Term:         rf.term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entry,
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntriesReply{}
	go rf.sendAppendEntries(index, args, reply)
}

func (rf *Raft) UpdateLogandReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//update log according to the appendentry received
	if args.PrevLogIndex == 0 {
		entries := make([]LogEntry, 0)
		newentries := append(entries, args.Entries...)
		rf.log = newentries
		reply.Success = true
		rf.commitIndex = args.LeaderCommit
		//rf.logger.Printf("Server: %v new commit log %v\n", rf.me, rf.log)
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				message := ApplyCommand{
					Index:   i,
					Command: rf.log[i-1].LogCommand,
				}
				rf.applyCh <- message
			}
			rf.lastApplied = rf.commitIndex
		}
	} else {
		if args.PrevLogIndex > len(rf.log) {
			reply.Success = false
		} else {
			term := rf.log[args.PrevLogIndex-1].Term
			if term == args.Term {
				//rf.logger.Printf("Server: %v new commit log %v\n", rf.me, rf.log)
				reply.Success = true
				entries := rf.log[:args.PrevLogIndex]
				newentries := append(entries, args.Entries...)
				rf.log = newentries
				rf.commitIndex = args.LeaderCommit
				if rf.commitIndex > rf.lastApplied {
					for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
						message := ApplyCommand{
							Index:   i,
							Command: rf.log[i-1].LogCommand,
						}
						rf.applyCh <- message
					}
					rf.lastApplied = rf.commitIndex
				}
			} else {
				reply.Success = false
			}
		}
	}
	reply.Term = args.Term

}
func (rf *Raft) checkLastLog(LastLogIndex int, LastLogTerm int) bool {
	//check if the vote is accepted
	rfLastIndex := len(rf.log)
	rfLastTerm := 0
	if rfLastIndex > 0 {
		rfLastTerm = rf.log[rfLastIndex-1].Term
	}
	if LastLogTerm > rfLastTerm {
		return true
	}
	if LastLogTerm == rfLastTerm && LastLogIndex >= rfLastIndex {
		return true
	}
	return false
}
