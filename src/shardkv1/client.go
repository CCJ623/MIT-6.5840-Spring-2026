package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client uses the shardctrler to query for the current
// configuration and find the assignment of shards (keys) to groups,
// and then talks to the group that holds the key's shard.
//

import (
	"log"
	"sync"
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const RPC_RETRY_INTERVAL = 10 * time.Millisecond

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// You will have to modify this struct.
	lock sync.Mutex
}

// The tester calls MakeClerk and passes in a shardctrler so that
// client can call it's Query method
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// You'll have to add code here.

	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// Get a key from a shardgrp.  You can use shardcfg.Key2Shard(key) to
// find the shard responsible for the key and ck.sck.Query() to read
// the current configuration and lookup the servers in the group
// responsible for key.  You can make a clerk for that group by
// calling shardgrp.MakeClerk(ck.clnt, servers).
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	shard_id := shardcfg.Key2Shard(key)
	for {
		config := ck.sck.Query()
		group_id, servers, ok := config.GidServers(shard_id)
		if !ok || len(servers) < 1 {
			DPrintf("[Clnt] Get(Key=%s) -> Shard %d: No group found, retrying...\n", key, shard_id)
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		ck.lock.Lock()
		ck.rcks[group_id] = shardgrp.MakeClerk(ck.clnt, servers)
		clerk := ck.rcks[group_id]
		ck.lock.Unlock()

		DPrintf("[Clnt] Get(Key=%s) -> Shard %d, Gid %d | Sending RPC\n", key, shard_id, group_id)
		value, version, err := clerk.Get(key)

		if err == rpc.ErrWrongGroup {
			DPrintf("[Clnt] Get(Key=%s) -> Shard %d, Gid %d | ErrWrongGroup, refreshing config...\n", key, shard_id, group_id)
			continue
		}

		DPrintf("[Clnt] Get(Key=%s) -> Shard %d, Gid %d | Result=%v\n", key, shard_id, group_id, err)
		return value, version, err
	}
}

// Put a key to a shard group.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	shard_id := shardcfg.Key2Shard(key)
	for {
		config := ck.sck.Query()
		group_id, servers, ok := config.GidServers(shard_id)
		if !ok || len(servers) < 1 {
			DPrintf("[Clnt] Put(Key=%s, Ver=%d) -> Shard %d: No group found, retrying...\n", key, version, shard_id)
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		ck.lock.Lock()
		ck.rcks[group_id] = shardgrp.MakeClerk(ck.clnt, servers)
		clerk := ck.rcks[group_id]
		ck.lock.Unlock()

		DPrintf("[Clnt] Put(Key=%s, Ver=%d) -> Shard %d, Gid %d | Sending RPC\n", key, version, shard_id, group_id)
		err := clerk.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			DPrintf("[Clnt] Put(Key=%s, Ver=%d) -> Shard %d, Gid %d | ErrWrongGroup, refreshing config...\n", key, version, shard_id, group_id)
			continue
		}

		DPrintf("[Clnt] Put(Key=%s, Ver=%d) -> Shard %d, Gid %d | Result=%v\n", key, version, shard_id, group_id, err)
		return err
	}

}

