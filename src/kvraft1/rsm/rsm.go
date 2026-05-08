package rsm

import (
	"sync"
	"time"

	"math/rand/v2"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command_ interface{}
	Id_      uint64
}

type OperationResult struct {
	// op received from raft
	operation_ Op
	// result return by DoOp()
	result_ any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	// key is log index, value is condition
	operation_channels_ map[int]chan OperationResult
}

func applyReader(rsm *RSM) {
	for {
		apply_message := <-rsm.applyCh

		// invalid message
		if !apply_message.CommandValid && !apply_message.SnapshotValid {
			continue
		}

		// command
		if apply_message.CommandValid {
			operation := apply_message.Command.(Op)
			result := rsm.sm.DoOp(operation.Command_)

			// logs to big, do snapshot
			if rsm.maxraftstate != -1 && rsm.rf.PersistBytes() > rsm.maxraftstate {
				rsm.rf.Snapshot(apply_message.CommandIndex, rsm.sm.Snapshot())
			}

			rsm.mu.Lock()
			channel, ok := rsm.operation_channels_[apply_message.CommandIndex]
			rsm.mu.Unlock()

			// channel not exist
			if !ok {
				continue
			}

			channel <- OperationResult{operation_: operation, result_: result}
			continue
		}

		// snapshot
		rsm.sm.Restore(apply_message.Snapshot)
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:                  me,
		maxraftstate:        maxraftstate,
		applyCh:             make(chan raftapi.ApplyMsg),
		sm:                  sm,
		operation_channels_: make(map[int]chan OperationResult, 1),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}

	if snapshot := persister.ReadSnapshot(); len(snapshot) > 0 {
		sm.Restore(snapshot)
	}

	go applyReader(rsm)
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	operation := Op{Command_: req, Id_: rand.Uint64()}
	log_index, _, is_leader := rsm.Raft().Start(operation)

	// not a leader
	if !is_leader {
		return rpc.ErrWrongLeader, nil // i'm dead, try another server.
	}

	rsm.operation_channels_[log_index] = make(chan OperationResult, 1)
	defer delete(rsm.operation_channels_, log_index)
	channel := rsm.operation_channels_[log_index]

	// get apply message
	rsm.mu.Unlock()
	select {
	case operation_result := <-channel:
		rsm.mu.Lock()
		if operation_result.operation_.Id_ != operation.Id_ {
			return rpc.ErrWrongLeader, nil
		}
		return rpc.OK, operation_result.result_
	case <-time.After(2000 * time.Millisecond):
		rsm.mu.Lock()
		return rpc.ErrWrongLeader, nil
	}
}
