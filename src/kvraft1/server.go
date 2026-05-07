package kvraft

import (
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ValueType struct {
	value_   string
	version_ rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// Your definitions here.
	mu      sync.Mutex
	kv_map_ map[string]ValueType
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}

		value, is_key_exist := kv.kv_map_[args.Key]

		if !is_key_exist {
			reply.Err = rpc.ErrNoKey
			return reply
		}

		reply.Err = rpc.OK
		reply.Value = value.value_
		reply.Version = value.version_
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}

		value, is_key_exist := kv.kv_map_[args.Key]

		if !is_key_exist {
			// new kv
			if args.Version == 0 {
				new_value := ValueType{value_: args.Value, version_: args.Version + 1}
				kv.kv_map_[args.Key] = new_value
				reply.Err = rpc.OK
				return reply
			}

			reply.Err = rpc.ErrNoKey
			return reply
		}

		if args.Version != value.version_ {
			reply.Err = rpc.ErrVersion
			return reply
		}

		new_value := ValueType{value_: args.Value, version_: args.Version + 1}
		kv.kv_map_[args.Key] = new_value
		reply.Err = rpc.OK
		return reply
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	error, result := kv.rsm.Submit(*args)

	// wrong leader
	if error == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	error, result := kv.rsm.Submit(*args)

	// wrong leader
	if error == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = result.(rpc.PutReply)
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.kv_map_ = make(map[string]ValueType)
	// You may need initialization code here.
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
