package shardgrp

import (
	"bytes"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type ValueType struct {
	Value_   string
	Version_ rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// Your code here
	mu      sync.Mutex
	kv_map_ map[string]ValueType
	config_ *shardcfg.ShardConfig
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}

		shard_id := shardcfg.Key2Shard(args.Key)
		group_id := kv.config_.Shards[shard_id]
		if group_id != kv.gid {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		value, is_key_exist := kv.kv_map_[args.Key]

		if !is_key_exist {
			reply.Err = rpc.ErrNoKey
			return reply
		}

		reply.Err = rpc.OK
		reply.Value = value.Value_
		reply.Version = value.Version_
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}

		shard_id := shardcfg.Key2Shard(args.Key)
		group_id := kv.config_.Shards[shard_id]
		if group_id != kv.gid {
			reply.Err = rpc.ErrWrongGroup
			return reply
		}

		value, is_key_exist := kv.kv_map_[args.Key]

		if !is_key_exist {
			// new kv
			if args.Version == 0 {
				new_value := ValueType{Value_: args.Value, Version_: args.Version + 1}
				kv.kv_map_[args.Key] = new_value
				reply.Err = rpc.OK
				return reply
			}

			reply.Err = rpc.ErrNoKey
			return reply
		}

		if args.Version != value.Version_ {
			reply.Err = rpc.ErrVersion
			return reply
		}

		new_value := ValueType{Value_: args.Value, Version_: args.Version + 1}
		kv.kv_map_[args.Key] = new_value
		reply.Err = rpc.OK
		return reply
	default:
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)

	if err := encoder.Encode(kv.kv_map_); err != nil {
		panic(err)
	}
	if err := encoder.Encode(kv.config_); err != nil {
		panic(err)
	}

	return buffer.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// no data
	if len(data) < 1 {
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	var new_kv_map map[string]ValueType
	var new_config shardcfg.ShardConfig

	if err := decoder.Decode(&new_kv_map); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&new_config); err != nil {
		panic(err)
	}

	kv.mu.Lock()
	kv.kv_map_ = new_kv_map
	kv.config_ = &new_config
	kv.mu.Unlock()
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	error, result := kv.rsm.Submit(*args)

	// wrong leader
	if error == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	error, result := kv.rsm.Submit(*args)

	// wrong leader
	if error == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = result.(rpc.PutReply)
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})
	labgob.Register(shardcfg.ShardConfig{})

	config := shardcfg.MakeShardConfig()
	config.Num = 0
	if gid == shardcfg.Gid1 {
		for i := 0; i < len(config.Shards); i++ {
			config.Shards[i] = shardcfg.Gid1
		}
	}

	kv := &KVServer{gid: gid, me: me, kv_map_: make(map[string]ValueType), config_: config}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
