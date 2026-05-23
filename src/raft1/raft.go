package raft

// The file ../raftapi/raftapi.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// In addition,  Make() creates a new raft peer that implements the
// raft interface.

import (
	//	"bytes"

	"bytes"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type RoleType int
type LogicalIndex uint64

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
const ELECTION_TIMEOUT = 400 * time.Millisecond
const RANDOM_SLEEP_MIN = 0 * time.Millisecond
const RANDOM_SLEEP_MAX = 300 * time.Millisecond
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
	current_term_        uint64
	voted_for_           int // index of the peer into peers[] which this peer voted for
	logs_                []LogEntry
	last_included_index_ LogicalIndex
	last_included_term_  uint64
	snapshot_            []byte

	// volatile state on all servers
	commit_index_ LogicalIndex
	last_applied_ LogicalIndex

	// volatile state on leaders
	next_index_  []LogicalIndex
	match_index_ []LogicalIndex

	last_heartbeat_time_   time.Time
	apply_message_channel_ chan raftapi.ApplyMsg
	apply_cond_            *sync.Cond
	send_entries_cond_     *sync.Cond
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

func (rf *Raft) logicalIndexToPhysicalIndex(index LogicalIndex) uint64 {
	if index < rf.last_included_index_ {
		panic(fmt.Sprintf("Logic Error: index %v is before snapshot %v", index, rf.last_included_index_))
	}
	return uint64(index - rf.last_included_index_)
}

func (rf *Raft) getLogEntry(index LogicalIndex) *LogEntry {
	return &rf.logs_[rf.logicalIndexToPhysicalIndex(index)]
}

func (rf *Raft) getLogSlice(begin, end LogicalIndex) []LogEntry {
	if begin < rf.last_included_index_ || end > rf.getLogLength() {
		panic(fmt.Sprintf("Logic Error: [begin, end] [%v,%v] is out of range [%v,%v]", begin, end, rf.last_included_index_, rf.getLogLength()))
	}
	return rf.logs_[rf.logicalIndexToPhysicalIndex(begin):rf.logicalIndexToPhysicalIndex(end)]
}

func (rf *Raft) getLogSliceCopy(begin, end LogicalIndex) []LogEntry {
	return slices.Clone(rf.getLogSlice(begin, end))
}

func (rf *Raft) getLogLength() LogicalIndex {
	return rf.last_included_index_ + LogicalIndex(len(rf.logs_))
}

func (rf *Raft) debugf(format string, args ...interface{}) {
	if DEBUG {
		prefix := fmt.Sprintf("[%v][%v]: ", rf.me, rf.roleName())
		fmt.Printf(prefix+format+"\n", args...)
	}
}

func randomSleep(min_time time.Duration, max_time time.Duration) {
	if max_time <= min_time {
		return
	}

	time_range := max_time - min_time
	sleep_time := int64(min_time) + (rand.Int63() % int64(time_range))
	time.Sleep(time.Duration(sleep_time))
}

func (rf *Raft) applier() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for {
		for rf.last_applied_ == rf.commit_index_ {
			// keep waiting if nothing to commit
			rf.apply_cond_.Wait()
		}

		// A snapshot was installed that covers entries we haven't delivered yet.
		// Deliver a SnapshotValid message so the service can update its state.
		if rf.last_applied_ < rf.last_included_index_ {
			snapshot := rf.snapshot_
			snapIndex := rf.last_included_index_
			snapTerm := rf.last_included_term_
			rf.last_applied_ = rf.last_included_index_
			rf.mu.Unlock()
			rf.debugf("applier: delivering snapshot index=%v term=%v", snapIndex, snapTerm)
			rf.apply_message_channel_ <- raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snapshot,
				SnapshotIndex: int(snapIndex),
				SnapshotTerm:  int(snapTerm),
			}
			rf.mu.Lock()
		}

		if rf.last_applied_ == rf.commit_index_ {
			continue
		}

		start := rf.last_applied_ + 1
		entries := rf.getLogSliceCopy(start, rf.commit_index_+1)
		rf.last_applied_ = rf.commit_index_
		rf.mu.Unlock()
		for offset, entry := range entries {
			index := int(start) + offset
			rf.mu.Lock()
			rf.debugf("applying msg: index=%v cmd=%#v", index, entry.Command_)
			rf.mu.Unlock()
			rf.apply_message_channel_ <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command_,
				CommandIndex: index,
			}
			rf.mu.Lock()
			rf.debugf("applied msg: index=%v cmd=%#v", index, entry.Command_)
			rf.mu.Unlock()
		}
		rf.mu.Lock()
	}
}

// only called by leader
func (rf *Raft) commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role_ != Leader {
		return
	}

	// build a sorted copy of match indices; treat self as fully matched
	matched := slices.Clone(rf.match_index_)
	matched[rf.me] = rf.getLogLength() - 1
	slices.SortFunc(matched, func(a, b LogicalIndex) int {
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

	if majority_index > rf.commit_index_ && rf.getLogEntry(majority_index).Term_ == rf.current_term_ {
		old_commit := rf.commit_index_
		rf.commit_index_ = majority_index
		rf.apply_cond_.Signal()

		rf.debugf("leader commit advanced from %v to %v", old_commit, rf.commit_index_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "leader commit advanced", fmt.Sprintf("role=%v term=%v from=%v to=%v", rf.roleName(), rf.current_term_, old_commit, rf.commit_index_))
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

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	encoder.Encode(rf.current_term_)
	encoder.Encode(rf.voted_for_)
	encoder.Encode(rf.logs_)
	encoder.Encode(rf.last_included_index_)
	encoder.Encode(rf.last_included_term_)

	raft_state := buffer.Bytes()
	rf.persister.Save(raft_state, rf.snapshot_)
	rf.debugf("persisted state: term=%v votedFor=%v logLen=%v", rf.current_term_, rf.voted_for_, rf.getLogLength())
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "persisted", fmt.Sprintf("term=%v votedFor=%v logLen=%v", rf.current_term_, rf.voted_for_, rf.getLogLength()))
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

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	var current_term uint64
	var voted_for int
	logs := make([]LogEntry, 0)
	var last_included_index LogicalIndex
	var last_included_term uint64

	if decoder.Decode(&current_term) != nil ||
		decoder.Decode(&voted_for) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&last_included_index) != nil ||
		decoder.Decode(&last_included_term) != nil {
		// error
		return
	}

	rf.current_term_ = current_term
	rf.voted_for_ = voted_for
	rf.logs_ = logs
	rf.last_included_index_ = last_included_index
	rf.last_included_term_ = last_included_term
	rf.snapshot_ = rf.persister.ReadSnapshot()

	rf.debugf("restored persisted state: term=%v votedFor=%v logLen=%v", rf.current_term_, rf.voted_for_, rf.getLogLength())
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "restored", fmt.Sprintf("term=%v votedFor=%v logLen=%v", rf.current_term_, rf.voted_for_, rf.getLogLength()))
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	logical_index := LogicalIndex(index)
	if logical_index > rf.last_applied_ ||
		logical_index <= rf.last_included_index_ {
		// invalid index
		return
	}

	rf.snapshot_ = snapshot
	rf.logs_ = rf.getLogSliceCopy(logical_index, rf.getLogLength())
	old_last_included := rf.last_included_index_
	rf.last_included_index_ = logical_index
	rf.last_included_term_ = rf.getLogEntry(logical_index).Term_
	rf.persist()

	rf.debugf("snapshot: trimmed log from %v to %v, new logLen=%v", old_last_included, rf.last_included_index_, rf.getLogLength())
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "snapshot", fmt.Sprintf("role=%v term=%v lastIncludedIndex=%v lastIncludedTerm=%v logLen=%v", rf.roleName(), rf.current_term_, rf.last_included_index_, rf.last_included_term_, rf.getLogLength()))
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term_         uint64
	Candidate_ID_ int
	LastLogIndex_ LogicalIndex
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
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "vote rejected(stale term)", fmt.Sprintf("role=%v term=%v candidate=%v candidateTerm=%v", rf.roleName(), rf.current_term_, args.Candidate_ID_, args.Term_))
		reply.Term_ = rf.current_term_
		reply.VoteGranted_ = false
		return
	}
	if args.Term_ > rf.current_term_ {
		// i am outdated
		// get into a new term
		rf.debugf("stepping down: saw higher term %v from candidate %v", args.Term_, args.Candidate_ID_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "stepped down(RequestVote)", fmt.Sprintf("role=%v term=%v->%v from peer=%v", rf.roleName(), rf.current_term_, args.Term_, args.Candidate_ID_))
		rf.current_term_ = args.Term_
		rf.role_ = Follower
		rf.voted_for_ = -1
		rf.persist()
	}

	// reach here, rf.current_term == args.Term_
	if rf.voted_for_ == args.Candidate_ID_ {
		// i already vote for it, replicated vote request
		rf.debugf("already voted for %v, granting again", args.Candidate_ID_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "vote granted(dup)", fmt.Sprintf("role=%v term=%v candidate=%v", rf.roleName(), rf.current_term_, args.Candidate_ID_))
		reply.VoteGranted_ = true
		rf.last_heartbeat_time_ = time.Now()
		return
	}
	if rf.voted_for_ != -1 {
		// i already voted for someone else
		rf.debugf("rejecting RequestVote from %v (already voted for %v)", args.Candidate_ID_, rf.voted_for_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "vote rejected(already voted)", fmt.Sprintf("role=%v term=%v candidate=%v votedFor=%v", rf.roleName(), rf.current_term_, args.Candidate_ID_, rf.voted_for_))
		reply.Term_ = rf.current_term_
		reply.VoteGranted_ = false
		return
	}
	if args.LastLogTerm_ > rf.getLogEntry(rf.getLogLength()-1).Term_ {
		// candidate's log is ahead, grant vote
		rf.debugf("granting RequestVote to %v (candidate last log term %v > my last log term %v)", args.Candidate_ID_, args.LastLogTerm_, rf.getLogEntry(rf.getLogLength()-1).Term_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "vote granted(candidate log ahead)", fmt.Sprintf("role=%v term=%v candidate=%v candidateLastLogTerm=%v myLastLogTerm=%v", rf.roleName(), rf.current_term_, args.Candidate_ID_, args.LastLogTerm_, rf.getLogEntry(rf.getLogLength()-1).Term_))
		rf.voted_for_ = args.Candidate_ID_
		rf.last_heartbeat_time_ = time.Now()
		rf.persist()
		reply.Term_ = rf.current_term_
		reply.VoteGranted_ = true
		return
	}
	if args.LastLogTerm_ < rf.getLogEntry(rf.getLogLength()-1).Term_ {
		// candidate's log is behind
		rf.debugf("rejecting RequestVote from %v (candidate last log term %v < my last log term %v)", args.Candidate_ID_, args.LastLogTerm_, rf.getLogEntry(rf.getLogLength()-1).Term_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "vote rejected(log behind)", fmt.Sprintf("role=%v term=%v candidate=%v candidateLastLogTerm=%v myLastLogTerm=%v", rf.roleName(), rf.current_term_, args.Candidate_ID_, args.LastLogTerm_, rf.getLogEntry(rf.getLogLength()-1).Term_))
		reply.Term_ = rf.current_term_
		reply.VoteGranted_ = false
		return
	}
	if args.LastLogIndex_ >= rf.getLogLength()-1 {
		// i have not voted and candidate is qualified, i can vote for it
		rf.debugf("granting RequestVote to %v for term %v", args.Candidate_ID_, args.Term_)
		rf.voted_for_ = args.Candidate_ID_
		rf.current_term_ = args.Term_
		rf.last_heartbeat_time_ = time.Now()
		rf.persist()
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "vote granted", fmt.Sprintf("role=%v term=%v candidate=%v", rf.roleName(), rf.current_term_, args.Candidate_ID_))

		reply.Term_ = rf.current_term_
		reply.VoteGranted_ = true
		return
	}

	// candidate is not qualified
	rf.debugf("rejecting RequestVote from %v (not qualified)", args.Candidate_ID_)
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "vote rejected(not qualified)", fmt.Sprintf("role=%v term=%v candidate=%v votedFor=%v", rf.roleName(), rf.current_term_, args.Candidate_ID_, rf.voted_for_))
	reply.VoteGranted_ = false
	reply.Term_ = rf.current_term_
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
	PreviousLogIndex_ LogicalIndex
	PreviousLogTerm_  uint64
	Entries_          []LogEntry
	LeaderCommit_     LogicalIndex
}

type AppendEntriesReplys struct {
	Term_                          uint64
	Success_                       bool
	ConflictEntryTerm_             uint64
	FirstIndexOfConflictEntryTerm_ LogicalIndex
	LogLenth_                      LogicalIndex
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
	if args.Term_ > rf.current_term_ {
		rf.current_term_ = args.Term_
		rf.voted_for_ = -1
		rf.persist()
	}

	// receive from right leader, update heartbeat
	rf.last_heartbeat_time_ = time.Now()
	rf.role_ = Follower

	if rf.getLogLength() <= args.PreviousLogIndex_ {
		// my log is short
		rf.debugf("rejecting AppendEntries from %v (my log is short)", args.LeaderID_)
		reply.Success_ = false
		reply.LogLenth_ = rf.getLogLength()
		return
	}

	if args.PreviousLogIndex_ < rf.last_included_index_ {
		// try to append entries before snapshot, reject this stale request
		rf.debugf("rejecting AppendEntries from %v (entries before snapshot)", args.LeaderID_)
		reply.Success_ = true
		reply.LogLenth_ = rf.getLogLength()
		return
	}

	if conflict_term := rf.getLogEntry(args.PreviousLogIndex_).Term_; conflict_term != args.PreviousLogTerm_ {
		// previous log is wrong
		rf.debugf("rejecting AppendEntries from %v (prev log wrong)", args.LeaderID_)
		first_conflict_index := args.PreviousLogIndex_
		for ; first_conflict_index > rf.last_included_index_+1 &&
			rf.getLogEntry(first_conflict_index-1).Term_ == conflict_term; first_conflict_index-- {
		}

		reply.Success_ = false
		reply.LogLenth_ = rf.getLogLength()
		reply.ConflictEntryTerm_ = conflict_term
		reply.FirstIndexOfConflictEntryTerm_ = first_conflict_index
		return
	}

	// previous log is right, leader found my last correct log entry
	// now correct my current log
	reply.Success_ = true
	is_logs_modified := false
	for relative_index, leader_log_entry := range args.Entries_ {
		index := args.PreviousLogIndex_ + 1 + LogicalIndex(relative_index)
		if index == rf.getLogLength() {
			// append new entry
			rf.logs_ = append(rf.logs_, leader_log_entry)
			is_logs_modified = true
			rf.debugf("appended new entry at idx=%v term=%v cmd=%#v", index, leader_log_entry.Term_, leader_log_entry.Command_)
			continue
		}

		curr_log_entry := rf.getLogEntry(index)
		if curr_log_entry.Term_ != leader_log_entry.Term_ {
			// conflict: truncate and overwrite
			rf.debugf("conflict at idx=%v (have term=%v want term=%v), truncating log", index, curr_log_entry.Term_, leader_log_entry.Term_)
			curr_log_entry.Term_ = leader_log_entry.Term_
			curr_log_entry.Command_ = leader_log_entry.Command_
			rf.logs_ = rf.getLogSlice(rf.last_included_index_, index+1)
			is_logs_modified = true
		}

		// log is correct, do nothing
	}

	if is_logs_modified {
		// persist log
		rf.persist()
	}

	// avoid out of range of rf.logs
	new_commit_index := min(args.LeaderCommit_, rf.getLogLength()-1)
	// avoid out of range of correct log
	// only args.entries covered is correct
	new_commit_index = min(new_commit_index, args.PreviousLogIndex_+LogicalIndex(len(args.Entries_)))
	if new_commit_index <= rf.commit_index_ {
		// nothing to commit
		return
	}

	old_commit := rf.commit_index_
	// catch up leader's commit index
	rf.commit_index_ = new_commit_index
	rf.persist()
	rf.debugf("accepted AppendEntries from %v: log len now=%v commit_index=%v", args.LeaderID_, rf.getLogLength()-1, rf.commit_index_)
	rf.apply_cond_.Signal()

	rf.debugf("commit_index advanced from %v to %v (leaderCommit=%v)", old_commit, rf.commit_index_, args.LeaderCommit_)
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "commit advanced", fmt.Sprintf("role=%v term=%v from=%v to=%v", rf.roleName(), rf.current_term_, old_commit, rf.commit_index_))
}

type InstallSnapshotArgs struct {
	Term_              uint64
	LeaderID_          int
	LastIncludedIndex_ LogicalIndex
	LastIncludedTerm_  uint64
	Data_              []byte
}

type InstallSnapshotReplys struct {
	Term_ uint64
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReplys) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.debugf("received InstallSnapshot from %v: term=%v lastIncludedIndex=%v lastIncludedTerm=%v",
		args.LeaderID_, args.Term_, args.LastIncludedIndex_, args.LastIncludedTerm_)

	reply.Term_ = rf.current_term_
	if args.Term_ < rf.current_term_ {
		// old leader
		rf.debugf("rejecting InstallSnapshot from %v (term %v < current %v)", args.LeaderID_, args.Term_, rf.current_term_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "InstallSnapshot rejected(stale term)", fmt.Sprintf("role=%v term=%v leaderTerm=%v", rf.roleName(), rf.current_term_, args.Term_))
		return
	}
	if args.LastIncludedIndex_ <= rf.last_included_index_ {
		// stale RPC
		rf.debugf("rejecting InstallSnapshot from %v (stale: lastIncludedIndex %v <= my %v)", args.LeaderID_, args.LastIncludedIndex_, rf.last_included_index_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "InstallSnapshot rejected(stale)", fmt.Sprintf("role=%v term=%v", rf.roleName(), rf.current_term_))
		return
	}

	if rf.getLogLength() > args.LastIncludedIndex_ &&
		rf.getLogEntry(args.LastIncludedIndex_).Term_ == args.LastIncludedTerm_ {
		// truncate my log
		rf.logs_ = rf.getLogSliceCopy(args.LastIncludedIndex_, rf.getLogLength())
	} else {
		rf.logs_ = make([]LogEntry, 1)
	}

	rf.snapshot_ = args.Data_
	rf.last_included_index_ = args.LastIncludedIndex_
	rf.last_included_term_ = args.LastIncludedTerm_
	rf.getLogEntry(rf.last_included_index_).Term_ = rf.last_included_term_
	// make sure commit_index_ >= snapshot
	rf.commit_index_ = max(rf.commit_index_, args.LastIncludedIndex_)
	// Do NOT set last_applied_ here; the applier goroutine will deliver
	// a SnapshotValid ApplyMsg and advance last_applied_ itself.
	rf.persist()

	rf.debugf("installed snapshot: lastIncludedIndex=%v lastIncludedTerm=%v newLogLen=%v", rf.last_included_index_, rf.last_included_term_, rf.getLogLength())
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "InstallSnapshot accepted", fmt.Sprintf("role=%v term=%v lastIncludedIndex=%v lastIncludedTerm=%v logLen=%v", rf.roleName(), rf.current_term_, rf.last_included_index_, rf.last_included_term_, rf.getLogLength()))
	rf.apply_cond_.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	args := &InstallSnapshotArgs{Term_: rf.current_term_,
		LeaderID_:          rf.me,
		LastIncludedIndex_: rf.last_included_index_,
		LastIncludedTerm_:  rf.last_included_term_,
		Data_:              rf.snapshot_}
	reply := &InstallSnapshotReplys{}
	rf.debugf("sending InstallSnapshot to %v: lastIncludedIndex=%v lastIncludedTerm=%v", server, args.LastIncludedIndex_, args.LastIncludedTerm_)
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "sending InstallSnapshot", fmt.Sprintf("role=%v term=%v to=%v lastIncludedIndex=%v", rf.roleName(), rf.current_term_, server, args.LastIncludedIndex_))
	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok {
		// RPC failed
		rf.debugf("InstallSnapshot RPC to %v failed", server)
		return
	}
	if reply.Term_ > rf.current_term_ {
		// i am outdated
		rf.debugf("stepping down: InstallSnapshot reply from %v has higher term %v", server, reply.Term_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "stepped down(InstallSnapshot)", fmt.Sprintf("role=%v term=%v->%v from peer=%v", rf.roleName(), rf.current_term_, reply.Term_, server))
		rf.current_term_ = reply.Term_
		rf.role_ = Follower
		rf.voted_for_ = -1
		rf.persist()
		return
	}

	// install success
	rf.debugf("InstallSnapshot to %v succeeded, updating match/next index", server)
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "InstallSnapshot success", fmt.Sprintf("role=%v term=%v to=%v lastIncludedIndex=%v", rf.roleName(), rf.current_term_, server, args.LastIncludedIndex_))
	rf.match_index_[server] = args.LastIncludedIndex_
	rf.next_index_[server] = args.LastIncludedIndex_ + 1
	go rf.commit()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReplys) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		// RPC failed
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term_ > rf.current_term_ {
		// i am outdated
		rf.debugf("AppendEntries reply contained higher term %v, stepping down", reply.Term_)
		tester.Annotate(fmt.Sprintf("server%v", rf.me), "stepped down(AppendEntries)", fmt.Sprintf("role=%v term=%v->%v from peer=%v", rf.roleName(), rf.current_term_, reply.Term_, server))
		rf.role_ = Follower
		rf.current_term_ = reply.Term_
		rf.voted_for_ = -1
		rf.persist()
		return
	}
	if rf.role_ != Leader || rf.current_term_ != args.Term_ {
		// stale reply
		return
	}

	if !reply.Success_ {
		// follower is outdated, try to update
		// check to avoid stale reply
		if rf.match_index_[server] >= args.PreviousLogIndex_ {
			return
		}

		var new_next_index LogicalIndex = 0
		if reply.LogLenth_ <= args.PreviousLogIndex_ {
			new_next_index = reply.LogLenth_
		} else if reply.ConflictEntryTerm_ > args.PreviousLogTerm_ {
			// i don't have it's term in previous log
			// need to delete it's whole log of conflict term
			new_next_index = reply.FirstIndexOfConflictEntryTerm_
		} else {
			// i have been through it's term in previous log
			// try to find it
			start_index := args.PreviousLogIndex_
			for ; start_index > rf.last_included_index_ && rf.getLogEntry(start_index).Term_ > reply.ConflictEntryTerm_; start_index-- {
			}

			if start_index > rf.last_included_index_ && rf.getLogEntry(start_index).Term_ == reply.ConflictEntryTerm_ {
				// i have it's term
				// now it's our common history
				new_next_index = start_index + 1
			} else {
				// i don't have it's term
				new_next_index = reply.FirstIndexOfConflictEntryTerm_
			}
		}

		if rf.match_index_[server] >= new_next_index {
			// stale reply
			return
		}
		if new_next_index <= rf.last_included_index_ {
			// new_next_index before snapshot, no logs entries any more
			// install snapshot instead
			go rf.sendInstallSnapshot(server)
			rf.debugf("AppendEntries failed for %v, sending snapshot", server)
			return
		}

		rf.next_index_[server] = new_next_index
		rf.debugf("AppendEntries failed for %v, backing up next_index", server)

		// immediately proceed
		rf.sendAppendEntriesHelper(server)
		return
	}

	// follower reply success
	logs_length := len(args.Entries_)
	new_match := args.PreviousLogIndex_ + LogicalIndex(logs_length)
	if new_match > rf.match_index_[server] {
		rf.match_index_[server] = new_match
		rf.next_index_[server] = rf.match_index_[server] + 1
		rf.debugf("AppendEntries success for %v, updated match_index to %v", server, rf.match_index_[server])
	}
	go rf.commit()
}

// start a goroutine to send AppendEntries to server
// send probe if i do NOT find follower's common history
// send log entries if i DO find follower's common history
func (rf *Raft) sendAppendEntriesHelper(server int) {
	if rf.next_index_[server] <= rf.last_included_index_ {
		// next_index is before our snapshot; send snapshot instead
		rf.debugf("sendAppendEntriesHelper: next_index[%v]=%v <= last_included_index_=%v, sending snapshot", server, rf.next_index_[server], rf.last_included_index_)
		go rf.sendInstallSnapshot(server)
		return
	}
	previous_log_index := rf.next_index_[server] - 1
	previous_log_term := rf.getLogEntry(previous_log_index).Term_
	entries := make([]LogEntry, 0)
	if rf.next_index_[server] == rf.match_index_[server]+1 {
		// found our common history
		entries = rf.getLogSliceCopy(previous_log_index+1, rf.getLogLength())
	}
	rf.debugf("Start() sending %v entries to peer %v (prevIdx=%v prevTerm=%v)", len(entries), server, previous_log_index, previous_log_term)
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
		rf.sendAppendEntries(server, &args, &reply)
	}()
}

// sync all log entries to followers
func (rf *Raft) entriesSender() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for true {
		rf.send_entries_cond_.Wait()

		if rf.role_ != Leader {
			// do nothing if i ain't leader
			continue
		}

		for peer_index := range rf.peers {
			if peer_index == rf.me {
				continue
			}
			rf.sendAppendEntriesHelper(peer_index)
		}
	}
}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	rf.debugf("broadcasting heartbeats for term %v", rf.current_term_)
	rf.mu.Unlock()
	rf.send_entries_cond_.Signal()
}

func (rf *Raft) startElection(election_done chan bool) {

	args := &RequestVoteArgs{}

	rf.mu.Lock()

	rf.current_term_++
	rf.voted_for_ = rf.me
	rf.last_heartbeat_time_ = time.Now()
	rf.persist()

	rf.debugf("startElection for term %v", rf.current_term_)
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "election started", fmt.Sprintf("role=%v term=%v", rf.roleName(), rf.current_term_))

	args.Term_ = rf.current_term_
	args.Candidate_ID_ = rf.me
	args.LastLogIndex_ = rf.getLogLength() - 1
	args.LastLogTerm_ = rf.getLogEntry(args.LastLogIndex_).Term_

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

			if reply.Term_ > rf.current_term_ {
				// i am a loser
				rf.debugf("stepped down to Follower (reply term %v > current %v)", reply.Term_, rf.current_term_)
				tester.Annotate(fmt.Sprintf("server%v", rf.me), "stepped down(Election)", fmt.Sprintf("role=%v term=%v->%v during election", rf.roleName(), rf.current_term_, reply.Term_))
				rf.current_term_ = reply.Term_
				rf.role_ = Follower
				rf.voted_for_ = -1
				rf.persist()
				rf.mu.Unlock()
				election_done <- true
				return
			}
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
					tester.Annotate(fmt.Sprintf("server%v", rf.me), "became leader", fmt.Sprintf("role=%v term=%v votes=%v/%v", rf.roleName(), args.Term_, vote_count, len(rf.peers)))
					rf.role_ = Leader
					for i := range rf.next_index_ {
						rf.next_index_[i] = rf.getLogLength()
						//rf.next_index_[i] = rf.commit_index_ + 1
						rf.match_index_[i] = 0
					}

					go rf.broadcastHeartbeats()
					rf.mu.Unlock()
					election_done <- true
					return
				}
				rf.mu.Unlock()
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

	rf.debugf("Start() called with command %#v (role=%v term=%v)", command, rf.roleName(), rf.current_term_)

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
	index = int(rf.getLogLength()) - 1
	term = int(rf.current_term_)
	rf.persist()
	rf.debugf("Start() appended cmd=%#v at idx=%v term=%v, log len=%v", command, index, term, rf.getLogLength()-1)
	tester.Annotate(fmt.Sprintf("server%v", rf.me), "cmd appended", fmt.Sprintf("role=%v term=%v idx=%v cmd=%#v", rf.roleName(), rf.current_term_, index, command))

	rf.send_entries_cond_.Signal()

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
				tester.Annotate(fmt.Sprintf("server%v", rf.me), "election timeout", fmt.Sprintf("role=%v term=%v", rf.roleName(), rf.current_term_))
				rf.role_ = Candidate
			}
			rf.mu.Unlock()
		case Candidate:
			rf.mu.Unlock()
			// pause for a random amount of time between 50 and 350
			// milliseconds.
			randomSleep(RANDOM_SLEEP_MIN, RANDOM_SLEEP_MAX)

			rf.mu.Lock()
			if rf.role_ != Candidate {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			// start election
			rf.mu.Lock()
			rf.debugf("starting election")
			rf.mu.Unlock()
			election_done := make(chan bool, 1)
			rf.startElection(election_done)

			select {
			case <-election_done:
				rf.mu.Lock()
				rf.debugf("election done")
				rf.mu.Unlock()
			case <-time.After(ELECTION_TIMEOUT):
				rf.mu.Lock()
				rf.debugf("election timeout")
				rf.mu.Unlock()
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
	rf.last_included_index_ = 0
	rf.last_included_term_ = 0

	rf.commit_index_ = 0
	rf.last_applied_ = 0
	rf.apply_cond_ = sync.NewCond(&rf.mu)
	rf.send_entries_cond_ = sync.NewCond(&rf.mu)

	rf.next_index_ = make([]LogicalIndex, len(rf.peers))
	rf.match_index_ = make([]LogicalIndex, len(rf.peers))
	for i := range rf.next_index_ {
		rf.next_index_[i] = rf.getLogLength()
		rf.match_index_[i] = 0
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.apply_message_channel_ = applyCh

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	go rf.entriesSender()

	return rf
}
