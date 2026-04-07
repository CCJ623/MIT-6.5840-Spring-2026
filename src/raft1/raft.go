package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type RoleType int

const (
	Follower RoleType = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command_ interface{}
	Term_    uint64
}

const HEARTBEAT_INTERVAL = 100 * time.Millisecond
const ELECTION_TIMEOUT = 500 * time.Millisecond
const DEBUG = false

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role_ RoleType

	// persistent state on all servers
	current_term_ uint64
	voted_for_    int // index of the peer into peers[] which this peer voted for
	logs_         []LogEntry

	// volatile state on all servers
	commit_index_ uint64
	last_applied_ uint64

	// volatile state on leaders
	next_index_  []uint64
	match_index_ []uint64

	last_heartbeat_time_   time.Time
	apply_message_channel_ chan raftapi.ApplyMsg
}

func (rf *Raft) roleName() string {
	switch rf.role_ {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (rf *Raft) debugf(format string, args ...interface{}) {
	if DEBUG {
		prefix := fmt.Sprintf("[%v][%v]: ", rf.me, rf.roleName())
		fmt.Printf(prefix+format+"\n", args...)
	}
}

func (rf *Raft) sendCommittedLogsToApplyChannel(committed_logs []LogEntry, start_index int) {
	for offset := range committed_logs {
		index := start_index + offset
		msg := raftapi.ApplyMsg{CommandValid: true,
			Command:      committed_logs[offset].Command_,
			CommandIndex: int(index),
		}
		rf.debugf("applying msg: index=%v cmd=%v", index, committed_logs[offset].Command_)
		rf.apply_message_channel_ <- msg
		rf.debugf("applied msg: index=%v cmd=%v", index, committed_logs[offset].Command_)
	}
}

func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role_ != Leader {
		return
	}

	// build a sorted copy of match indices; treat self as fully matched
	matched := slices.Clone(rf.match_index_)
	matched[rf.me] = uint64(len(rf.logs_) - 1)
	slices.SortFunc(matched, func(a, b uint64) int {
		if a > b {
			return -1
		} else if a < b {
			return 1
		}
		return 0
	}) // descending

	// sorted[len/2] is the highest index held by a majority:
	// for N=3: sorted[1] => at least 2 servers have it; for N=5: sorted[2] => at least 3 servers have it
	majority_index := matched[len(rf.peers)/2]

	if majority_index > rf.commit_index_ && rf.logs_[majority_index].Term_ == rf.current_term_ {
		old_commit := rf.commit_index_
		rf.commit_index_ = majority_index
		rf.last_applied_ = rf.commit_index_

		start_index := old_commit + 1
		recent_commited_logs_range := slices.Clone(rf.logs_[start_index:rf.commit_index_+1])
		go rf.sendCommittedLogsToApplyChannel(recent_commited_logs_range, int(start_index))

		rf.debugf("leader commit advanced from %v to %v", old_commit, rf.commit_index_)
		tester.Annotate(fmt.Sprintf("[%v][%v][%v]", rf.me, rf.roleName(), rf.current_term_), "leader commit advanced", fmt.Sprintf("from=%v to=%v", old_commit, rf.commit_index_))
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = int(rf.current_term_)
	isleader = (rf.role_ == Leader)
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
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
	Term_         uint64
	Candidate_ID_ int
	LastLogIndex_ uint64
	LastLogTerm_  uint64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term_        uint64
	VoteGranted_ bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debugf("received RequestVote from %v for term %v", args.Candidate_ID_, args.Term_)

	if args.Term_ < rf.current_term_ {
		// candidate is behind
		rf.debugf("rejecting RequestVote from %v (term %v < current %v)", args.Candidate_ID_, args.Term_, rf.current_term_)
		reply.Term_ = rf.current_term_
		reply.VoteGranted_ = false
		return
	}
	if args.Term_ > rf.current_term_ {
		// i am outdated
		// get into a new term
		rf.current_term_ = args.Term_
		rf.role_ = Follower
		rf.voted_for_ = -1
		rf.persist()
	}
	if rf.voted_for_ == -1 && args.LastLogIndex_ >= rf.commit_index_ {
		// i have not voted and candidate is qualified, i can vote for it
		rf.debugf("granting RequestVote to %v for term %v", args.Candidate_ID_, args.Term_)
		rf.voted_for_ = args.Candidate_ID_
		rf.current_term_ = args.Term_
		rf.last_heartbeat_time_ = time.Now()
		rf.persist()

		reply.Term_ = rf.current_term_
		reply.VoteGranted_ = true
		return
	}
	if rf.voted_for_ == args.Candidate_ID_ {
		// i already vote for it, replicated vote request
		rf.debugf("already voted for %v, granting again", args.Candidate_ID_)
		reply.VoteGranted_ = true
		rf.last_heartbeat_time_ = time.Now()
		return
	}

	// i have already voted for someone else, sorry pal
	rf.debugf("rejecting RequestVote from %v (already voted for %v)", args.Candidate_ID_, rf.voted_for_)
	reply.VoteGranted_ = false
	reply.Term_ = rf.current_term_
	return
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

type AppendEntriesArgs struct {
	Term_             uint64
	LeaderID_         int
	PreviousLogIndex_ uint64
	PreviousLogTerm_  uint64
	Entries_          []LogEntry
	LeaderCommit_     uint64
}

type AppendEntriesReplys struct {
	Term_    uint64
	Success_ bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReplys) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debugf("received AppendEntries from %v: term=%v prevLogIdx=%v prevLogTerm=%v entries=%v leaderCommit=%v",
		args.LeaderID_, args.Term_, args.PreviousLogIndex_, args.PreviousLogTerm_, len(args.Entries_), args.LeaderCommit_)

	if args.Term_ < rf.current_term_ {
		// old leader
		rf.debugf("rejecting AppendEntries from %v (term %v < current %v)", args.LeaderID_, args.Term_, rf.current_term_)
		reply.Term_ = rf.current_term_
		reply.Success_ = false
		return
	}

	// receive from right leader, update heartbeat
	rf.last_heartbeat_time_ = time.Now()
	rf.role_ = Follower
	if (uint64(len(rf.logs_)) <= args.PreviousLogIndex_) ||
		(rf.logs_[args.PreviousLogIndex_].Term_ != args.PreviousLogTerm_) {
		// previous log is wrong
		rf.debugf("rejecting AppendEntries from %v (prev log wrong)", args.LeaderID_)
		reply.Success_ = false
		return
	}

	// previous log is right, leader found my last correct log entry
	// now correct my current log
	for relative_index, leader_log_entry := range args.Entries_ {
		index := args.PreviousLogIndex_ + 1 + uint64(relative_index)
		if index == uint64(len(rf.logs_)) {
			// append new entry
			rf.logs_ = append(rf.logs_, leader_log_entry)
			rf.debugf("appended new entry at idx=%v term=%v cmd=%v", index, leader_log_entry.Term_, leader_log_entry.Command_)
			continue
		}

		curr_log_entry := &rf.logs_[index]
		if curr_log_entry.Term_ != leader_log_entry.Term_ {
			// conflict: truncate and overwrite
			rf.debugf("conflict at idx=%v (have term=%v want term=%v), truncating log", index, curr_log_entry.Term_, leader_log_entry.Term_)
			curr_log_entry.Term_ = leader_log_entry.Term_
			curr_log_entry.Command_ = leader_log_entry.Command_
			rf.logs_ = rf.logs_[:index+1]
		}

		// log is correct, do nothing
	}

	if args.LeaderCommit_ > rf.commit_index_ {
		old_commit := rf.commit_index_
		// catch up leader's commit index
		rf.commit_index_ = min(args.LeaderCommit_, uint64(len(rf.logs_))-1)
		start_index := old_commit + 1
		recent_commited_logs_range := slices.Clone(rf.logs_[start_index:rf.commit_index_+1])
		go rf.sendCommittedLogsToApplyChannel(recent_commited_logs_range, int(start_index))

		rf.debugf("commit_index advanced from %v to %v (leaderCommit=%v)", old_commit, rf.commit_index_, args.LeaderCommit_)
		tester.Annotate(fmt.Sprintf("[%v][%v][%v]", rf.me, rf.roleName(), rf.current_term_), "commit advanced", fmt.Sprintf("from=%v to=%v", old_commit, rf.commit_index_))
	}

	rf.persist()
	rf.debugf("accepted AppendEntries from %v: log len now=%v commit_index=%v", args.LeaderID_, len(rf.logs_)-1, rf.commit_index_)
	reply.Success_ = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReplys) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		// RPC failed
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role_ != Leader {
		// role not match
		return
	}
	if rf.current_term_ < reply.Term_ {
		// i am outdated
		rf.debugf("AppendEntries reply contained higher term %v, stepping down", reply.Term_)
		tester.Annotate(fmt.Sprintf("[%v][%v][%v]", rf.me, rf.roleName(), rf.current_term_), "stepped down", fmt.Sprintf("saw term=%v from peer=%v", reply.Term_, server))
		rf.role_ = Follower
		rf.current_term_ = reply.Term_
		rf.voted_for_ = -1
		rf.persist()
		return
	}

	if !reply.Success_ {
		// follower is outdated
		// try to update
		rf.debugf("AppendEntries failed for %v, backing up next_index", server)
		rf.next_index_[server]--
		return
	}

	// follower reply success
	logs_length := len(args.Entries_)
	new_match := args.PreviousLogIndex_ + uint64(logs_length)
	if new_match > rf.match_index_[server] {
		rf.match_index_[server] = new_match
		rf.next_index_[server] = rf.match_index_[server] + 1
	}
	if logs_length > 0 {
		rf.debugf("AppendEntries success for %v, updated match_index to %v", server, rf.match_index_[server])
		go rf.commit()
	}
}

func (rf *Raft) broadcastHeartbeats() {
	rf.debugf("broadcasting heartbeats for term %v", rf.current_term_)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func() {
			rf.mu.Lock()
			args := &AppendEntriesArgs{
				Term_:             rf.current_term_,
				LeaderID_:         rf.me,
				PreviousLogIndex_: rf.next_index_[index] - 1,
				PreviousLogTerm_:  rf.logs_[rf.next_index_[index]-1].Term_,
				Entries_:          make([]LogEntry, 0),
				LeaderCommit_:     rf.commit_index_,
			}
			reply := &AppendEntriesReplys{}
			rf.mu.Unlock()
			rf.sendAppendEntries(index, args, reply)
		}()
	}
}

func (rf *Raft) startElection(election_done chan bool) {

	args := &RequestVoteArgs{}

	rf.mu.Lock()

	rf.current_term_++
	rf.voted_for_ = rf.me
	rf.last_heartbeat_time_ = time.Now()
	rf.persist()

	rf.debugf("startElection for term %v", rf.current_term_)
	tester.Annotate(fmt.Sprintf("[%v][%v][%v]", rf.me, rf.roleName(), rf.current_term_), "election started", fmt.Sprintf("term=%v", rf.current_term_))

	args.Term_ = rf.current_term_
	args.Candidate_ID_ = rf.me
	args.LastLogIndex_ = uint64(len(rf.logs_)) - 1
	args.LastLogTerm_ = rf.logs_[args.LastLogIndex_].Term_

	rf.mu.Unlock()
	vote_count := 1

	// send to all server except self
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		// work function
		go func(server int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if !ok {
				// no reply
				return
			}

			rf.mu.Lock()

			if rf.role_ != Candidate || rf.current_term_ != args.Term_ {
				// state is outdated
				rf.mu.Unlock()
				return
			}

			if reply.VoteGranted_ {
				// vote success
				vote_count++
				rf.debugf("got vote from %v for term %v (total: %v)", server, args.Term_, vote_count)
				if vote_count > len(rf.peers)/2 {
					// i am leader now
					rf.debugf("became leader for term %v", args.Term_)
					tester.Annotate(fmt.Sprintf("[%v][%v][%v]", rf.me, rf.roleName(), rf.current_term_), "became leader", fmt.Sprintf("term=%v votes=%v/%v", args.Term_, vote_count, len(rf.peers)))
					rf.role_ = Leader
					go rf.broadcastHeartbeats()
					rf.mu.Unlock()
					election_done <- true
					return
				}
				rf.mu.Unlock()
				return
			}
			if reply.Term_ > rf.current_term_ {
				// i am a loser
				rf.debugf("stepped down to Follower (reply term %v > current %v)", reply.Term_, rf.current_term_)
				tester.Annotate(fmt.Sprintf("[%v][%v][%v]", rf.me, rf.roleName(), rf.current_term_), "stepped down", fmt.Sprintf("saw term=%v during election", reply.Term_))
				rf.current_term_ = reply.Term_
				rf.role_ = Follower
				rf.voted_for_ = -1
				rf.persist()

				for index := range rf.next_index_ {
					rf.next_index_[index] = uint64(len(rf.logs_))
				}
				for index := range rf.match_index_ {
					rf.match_index_[index] = 0
				}
				rf.mu.Unlock()
				election_done <- true
				return
			}
			rf.mu.Unlock()
		}(index)
	}
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debugf("Start() called with command %v (role=%v term=%v)", command, rf.roleName(), rf.current_term_)

	if rf.role_ != Leader {
		// i am not a leader
		isLeader = false
		index = int(rf.commit_index_)
		term = int(rf.current_term_)
		rf.debugf("Start() rejected: not leader")
		return index, term, isLeader
	}

	// i am a leader, append log entry
	rf.logs_ = append(rf.logs_, LogEntry{Command_: command, Term_: rf.current_term_})
	index = len(rf.logs_) - 1
	term = int(rf.current_term_)
	rf.persist()
	rf.debugf("Start() appended cmd=%v at idx=%v term=%v, log len=%v", command, index, term, len(rf.logs_)-1)
	tester.Annotate(fmt.Sprintf("[%v][%v][%v]", rf.me, rf.roleName(), rf.current_term_), "cmd appended", fmt.Sprintf("idx=%v term=%v cmd=%v", index, term, command))

	for peer_index := range rf.peers {
		if peer_index == rf.me {
			continue
		}
		previous_log_index := rf.next_index_[peer_index] - 1
		previous_log_term := rf.logs_[previous_log_index].Term_
		entries := make([]LogEntry, 0)
		if rf.next_index_[peer_index] == rf.match_index_[peer_index]+1 {
			// found our common history
			entries = rf.logs_[previous_log_index+1:]
		}
		rf.debugf("Start() sending %v entries to peer %v (prevIdx=%v prevTerm=%v)", len(entries), peer_index, previous_log_index, previous_log_term)
		args := AppendEntriesArgs{
			Term_:             rf.current_term_,
			LeaderID_:         rf.me,
			PreviousLogIndex_: previous_log_index,
			PreviousLogTerm_:  previous_log_term,
			Entries_:          entries,
			LeaderCommit_:     rf.commit_index_,
		}
		go func() {
			reply := AppendEntriesReplys{}
			rf.sendAppendEntries(peer_index, &args, &reply)
		}()
	}

	return index, term, isLeader
}

func (rf *Raft) ticker() {
	for true {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		switch rf.role_ {
		case Follower:
			sleep_time := ELECTION_TIMEOUT - time.Since(rf.last_heartbeat_time_)
			rf.mu.Unlock()
			time.Sleep(sleep_time)
			rf.mu.Lock()
			// check heartbeat: only convert if no heartbeat arrived during sleep
			if rf.role_ == Follower && time.Since(rf.last_heartbeat_time_) >= ELECTION_TIMEOUT {
				// election timeout, transform to candidate
				rf.debugf("election timeout, converting to Candidate")
				tester.Annotate(fmt.Sprintf("[%v][%v][%v]", rf.me, rf.roleName(), rf.current_term_), "election timeout", fmt.Sprintf("term=%v", rf.current_term_))
				rf.role_ = Candidate
			}
			rf.mu.Unlock()
		case Candidate:
			rf.mu.Unlock()
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			ms := 150 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)

			rf.mu.Lock()
			if rf.role_ != Candidate {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			// start election
			rf.debugf("starting election")
			election_done := make(chan bool, 1)
			rf.startElection(election_done)

			select {
			case <-election_done:
				rf.debugf("election done")
			case <-time.After(ELECTION_TIMEOUT):
				rf.debugf("election timeout")
			}
		case Leader:
			rf.mu.Unlock()
			rf.broadcastHeartbeats()
			time.Sleep(HEARTBEAT_INTERVAL)
		default:
			rf.mu.Unlock()
			return
		}

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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.debugf("Make: creating Raft server")
	rf.role_ = Follower

	rf.current_term_ = 0
	rf.voted_for_ = -1
	rf.logs_ = make([]LogEntry, 1)

	rf.commit_index_ = 0
	rf.last_applied_ = 0

	rf.next_index_ = make([]uint64, len(rf.peers))
	for i := range rf.next_index_ {
		rf.next_index_[i] = rf.commit_index_ + 1
	}

	rf.match_index_ = make([]uint64, len(rf.peers))
	for i := range rf.match_index_ {
		rf.match_index_[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.apply_message_channel_ = applyCh

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
