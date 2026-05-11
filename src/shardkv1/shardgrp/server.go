package shardgrp

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const Debug = false

func (kv *KVServer) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		prefix := fmt.Sprintf(" [Gid %d, Srv %d] ", kv.gid, kv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

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
	mu                           sync.Mutex
	kv_map_                      map[string]ValueType
	is_my_shards                 [shardcfg.NShards]bool
	frozen_shards                [shardcfg.NShards]bool
	latest_config_num_for_shards [shardcfg.NShards]shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch args := req.(type) {
	case rpc.GetArgs:
		reply := rpc.GetReply{}

		shard_id := shardcfg.Key2Shard(args.Key)
		if !kv.is_my_shards[shard_id] {
			reply.Err = rpc.ErrWrongGroup
			kv.DPrintf("DoOp: Get(Key=%s) -> ErrWrongGroup\n", args.Key)
			return reply
		}

		value, is_key_exist := kv.kv_map_[args.Key]

		if !is_key_exist {
			reply.Err = rpc.ErrNoKey
			kv.DPrintf("DoOp: Get(Key=%s) -> ErrNoKey\n", args.Key)
			return reply
		}

		reply.Err = rpc.OK
		reply.Value = value.Value_
		reply.Version = value.Version_
		kv.DPrintf("DoOp: Get(Key=%s) -> OK (Val=%s)\n", args.Key, reply.Value)
		return reply
	case rpc.PutArgs:
		reply := rpc.PutReply{}

		shard_id := shardcfg.Key2Shard(args.Key)
		if kv.frozen_shards[shard_id] {
			reply.Err = rpc.ErrWrongGroup
			kv.DPrintf("DoOp: Put(Key=%s) -> ErrWrongGroup (Frozen)\n", args.Key)
			return reply
		}

		if !kv.is_my_shards[shard_id] {
			reply.Err = rpc.ErrWrongGroup
			kv.DPrintf("DoOp: Put(Key=%s) -> ErrWrongGroup (Not mine)\n", args.Key)
			return reply
		}

		value, is_key_exist := kv.kv_map_[args.Key]

		if !is_key_exist {
			// new kv
			if args.Version == 0 {
				new_value := ValueType{Value_: args.Value, Version_: args.Version + 1}
				kv.kv_map_[args.Key] = new_value
				reply.Err = rpc.OK
				kv.DPrintf("DoOp: Put(Key=%s, Val=%s, Ver=%d) -> OK (New)\n", args.Key, args.Value, args.Version)
				return reply
			}

			reply.Err = rpc.ErrNoKey
			kv.DPrintf("DoOp: Put(Key=%s) -> ErrNoKey\n", args.Key)
			return reply
		}

		if args.Version != value.Version_ {
			reply.Err = rpc.ErrVersion
			kv.DPrintf("DoOp: Put(Key=%s, Ver=%d) -> ErrVersion (Expected=%d)\n", args.Key, args.Version, value.Version_)
			return reply
		}

		new_value := ValueType{Value_: args.Value, Version_: args.Version + 1}
		kv.kv_map_[args.Key] = new_value
		reply.Err = rpc.OK
		kv.DPrintf("DoOp: Put(Key=%s, Val=%s, Ver=%d) -> OK (Update)\n", args.Key, args.Value, args.Version)
		return reply
	case shardrpc.FreezeShardArgs:
		reply := shardrpc.FreezeShardReply{}

		// stale RPC
		if args.Num < kv.latest_config_num_for_shards[args.Shard] {
			reply.Err = rpc.ErrWrongGroup
			reply.Num = kv.latest_config_num_for_shards[args.Shard]
			kv.DPrintf("DoOp: FreezeShard(Shard=%d, Num=%d) -> ErrWrongGroup (Stale: Latest=%d)\n", args.Shard, args.Num, kv.latest_config_num_for_shards[args.Shard])
			return reply
		}

		// duplicate request, send empty data to remind caller
		if args.Num == kv.latest_config_num_for_shards[args.Shard] && !kv.is_my_shards[args.Shard] {
			reply.Err = rpc.OK
			reply.Num = kv.latest_config_num_for_shards[args.Shard]
			reply.State = []byte{}
			kv.DPrintf("DoOp: FreezeShard(Shard=%d, Num=%d) -> OK (duplicate request)\n", args.Shard, args.Num)
			return reply
		}

		// not my shard
		if !kv.is_my_shards[args.Shard] {
			reply.Err = rpc.ErrWrongGroup
			reply.Num = kv.latest_config_num_for_shards[args.Shard]
			kv.DPrintf("DoOp: FreezeShard(Shard=%d, Num=%d) -> ErrWrongGroup (Not mine)\n", args.Shard, args.Num)
			return reply
		}

		kv.frozen_shards[args.Shard] = true
		kv.latest_config_num_for_shards[args.Shard] = args.Num
		shard_data := make(map[string]ValueType)

		// find all target kv
		for key, value := range kv.kv_map_ {
			shard_id := shardcfg.Key2Shard(key)
			if shard_id == args.Shard {
				shard_data[key] = value
			}
		}

		buffer := new(bytes.Buffer)
		encoder := labgob.NewEncoder(buffer)
		if err := encoder.Encode(shard_data); err != nil {
			panic(err)
		}

		reply.State = buffer.Bytes()
		reply.Num = kv.latest_config_num_for_shards[args.Shard]
		reply.Err = rpc.OK
		kv.DPrintf("DoOp: FreezeShard(Shard=%d, Num=%d) -> OK\n", args.Shard, args.Num)
		return reply
	case shardrpc.InstallShardArgs:
		reply := shardrpc.InstallShardReply{}

		// stale RPC
		if args.Num < kv.latest_config_num_for_shards[args.Shard] {
			reply.Err = rpc.ErrWrongGroup
			kv.DPrintf("DoOp: InstallShard(Shard=%d, Num=%d) -> ErrWrongGroup (Stale: Latest=%d)\n", args.Shard, args.Num, kv.latest_config_num_for_shards[args.Shard])
			return reply
		}

		kv.is_my_shards[args.Shard] = true
		kv.frozen_shards[args.Shard] = false
		kv.latest_config_num_for_shards[args.Shard] = args.Num

		shard_data := make(map[string]ValueType)
		buffer := bytes.NewBuffer(args.State)
		decoder := labgob.NewDecoder(buffer)
		if err := decoder.Decode(&shard_data); err != nil {
			panic(err)
		}

		// install all shard kv
		for key, value := range shard_data {
			kv.kv_map_[key] = value
		}

		reply.Err = rpc.OK
		kv.DPrintf("DoOp: InstallShard(Shard=%d, Num=%d) -> OK\n", args.Shard, args.Num)
		return reply
	case shardrpc.DeleteShardArgs:
		reply := shardrpc.DeleteShardReply{}

		// stale RPC
		if args.Num < kv.latest_config_num_for_shards[args.Shard] {
			reply.Err = rpc.ErrWrongGroup
			kv.DPrintf("DoOp: DeleteShard(Shard=%d, Num=%d) -> ErrWrongGroup (Stale: Latest=%d)\n", args.Shard, args.Num, kv.latest_config_num_for_shards[args.Shard])
			return reply
		}

		// not my shard
		if !kv.is_my_shards[args.Shard] {
			reply.Err = rpc.OK
			kv.DPrintf("DoOp: DeleteShard(Shard=%d, Num=%d) -> OK (Already not mine)\n", args.Shard, args.Num)
			return reply
		}

		kv.is_my_shards[args.Shard] = false
		kv.frozen_shards[args.Shard] = false
		kv.latest_config_num_for_shards[args.Shard] = args.Num

		// delete all target kv
		for key := range kv.kv_map_ {
			shard_id := shardcfg.Key2Shard(key)
			if shard_id == args.Shard {
				delete(kv.kv_map_, key)
			}
		}

		reply.Err = rpc.OK
		kv.DPrintf("DoOp: DeleteShard(Shard=%d, Num=%d) -> OK\n", args.Shard, args.Num)
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
	if err := encoder.Encode(kv.is_my_shards); err != nil {
		panic(err)
	}
	if err := encoder.Encode(kv.frozen_shards); err != nil {
		panic(err)
	}
	if err := encoder.Encode(kv.latest_config_num_for_shards); err != nil {
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
	var new_is_my_shard [shardcfg.NShards]bool
	var new_frozen_shard [shardcfg.NShards]bool
	var new_latest_config_num_for_shards [shardcfg.NShards]shardcfg.Tnum

	if err := decoder.Decode(&new_kv_map); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&new_is_my_shard); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&new_frozen_shard); err != nil {
		panic(err)
	}
	if err := decoder.Decode(&new_latest_config_num_for_shards); err != nil {
		panic(err)
	}

	kv.mu.Lock()
	kv.kv_map_ = new_kv_map
	kv.is_my_shards = new_is_my_shard
	kv.frozen_shards = new_frozen_shard
	kv.latest_config_num_for_shards = new_latest_config_num_for_shards
	kv.mu.Unlock()

	kv.DPrintf("Restore: Restored state (MapSize=%d)\n", len(new_kv_map))
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.DPrintf("RPC: Get(Key=%s)\n", args.Key)
	error, result := kv.rsm.Submit(*args)

	// wrong leader
	if error == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = result.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.DPrintf("RPC: Put(Key=%s, Val=%s, Ver=%d)\n", args.Key, args.Value, args.Version)
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
	kv.mu.Lock()
	latest_config_num := kv.latest_config_num_for_shards[args.Shard]
	is_my_shard := kv.is_my_shards[args.Shard]
	kv.mu.Unlock()

	kv.DPrintf("RPC: FreezeShard(Shard=%d, Num=%d)\n", args.Shard, args.Num)

	// stale RPC
	if args.Num < latest_config_num {
		reply.Err = rpc.ErrWrongGroup
		reply.Num = latest_config_num
		kv.DPrintf("RPC: FreezeShard(Shard=%d, Num=%d) -> ErrWrongGroup (Stale: Latest=%d)\n", args.Shard, args.Num, kv.latest_config_num_for_shards[args.Shard])
		return
	}

	// duplicate request, send empty data to remind caller
	if args.Num == latest_config_num && !is_my_shard {
		reply.Err = rpc.OK
		reply.Num = latest_config_num
		reply.State = []byte{}
		kv.DPrintf("RPC: FreezeShard(Shard=%d, Num=%d) -> OK (duplicate request)\n", args.Shard, args.Num)
		return
	}

	// not my shard
	if !is_my_shard {
		reply.Err = rpc.ErrWrongGroup
		reply.Num = latest_config_num
		kv.DPrintf("RPC: FreezeShard(Shard=%d, Num=%d) -> ErrWrongGroup (Not mine)\n", args.Shard, args.Num)
		return
	}

	error, result := kv.rsm.Submit(*args)

	// wrong leader
	if error == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = result.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	kv.mu.Lock()
	latest_config_num := kv.latest_config_num_for_shards[args.Shard]
	kv.mu.Unlock()

	kv.DPrintf("RPC: InstallShard(Shard=%d, Num=%d)\n", args.Shard, args.Num)

	// stale RPC
	if args.Num < latest_config_num {
		reply.Err = rpc.ErrWrongGroup
		kv.DPrintf("RPC: InstallShard(Shard=%d, Num=%d) -> ErrWrongGroup (Stale: Latest=%d)\n", args.Shard, args.Num, kv.latest_config_num_for_shards[args.Shard])
		return
	}

	error, result := kv.rsm.Submit(*args)

	// wrong leader
	if error == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = result.(shardrpc.InstallShardReply)
	kv.DPrintf("RPC: InstallShard(Shard=%d, Num=%d) -> Result=%v\n", args.Shard, args.Num, reply.Err)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	kv.mu.Lock()
	latest_config_num := kv.latest_config_num_for_shards[args.Shard]
	is_my_shard := kv.is_my_shards[args.Shard]
	kv.mu.Unlock()

	kv.DPrintf("RPC: DeleteShard(Shard=%d, Num=%d)\n", args.Shard, args.Num)

	// stale RPC
	if args.Num < latest_config_num {
		reply.Err = rpc.ErrWrongGroup
		kv.DPrintf("RPC: DeleteShard(Shard=%d, Num=%d) -> ErrWrongGroup (Stale: Latest=%d)\n", args.Shard, args.Num, kv.latest_config_num_for_shards[args.Shard])
		return
	}

	// not my shard
	if !is_my_shard {
		reply.Err = rpc.OK
		kv.DPrintf("RPC: DeleteShard(Shard=%d, Num=%d) -> OK (Already not mine)\n", args.Shard, args.Num)
		return
	}

	error, result := kv.rsm.Submit(*args)

	// wrong leader
	if error == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}

	*reply = result.(shardrpc.DeleteShardReply)

}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.PutReply{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(rpc.GetReply{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.FreezeShardReply{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.InstallShardReply{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(shardrpc.DeleteShardReply{})
	labgob.Register(rsm.Op{})
	labgob.Register(shardcfg.ShardConfig{})

	kv := &KVServer{gid: gid, me: me, kv_map_: make(map[string]ValueType)}
	for i := 0; i < shardcfg.NShards; i++ {
		kv.is_my_shards[i] = (gid == shardcfg.Gid1)
		kv.frozen_shards[i] = false
		kv.latest_config_num_for_shards[i] = 0
	}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Your code here

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
