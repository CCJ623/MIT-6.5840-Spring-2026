package shardgrp

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const RPC_RETRY_INTERVAL = 100 * time.Millisecond

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	lock sync.Mutex
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	ck.lock.Lock()
	defer ck.lock.Unlock()
	return ck.leader
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	for {
		leader := ck.Leader()
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)

		// network failed or some error
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		return reply.Value, reply.Version, reply.Err
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	is_resend_ := false

	for {
		leader := ck.Leader()
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		// network error or wrong leader
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			is_resend_ = true
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		// error version
		if reply.Err == rpc.ErrVersion && !is_resend_ {
			return rpc.ErrVersion
		}

		// unsure
		if reply.Err == rpc.ErrVersion && is_resend_ {
			return rpc.ErrMaybe
		}

		return reply.Err
	}

}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}

	for {
		leader := ck.Leader()
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)

		// network failed or some error
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		return reply.State, reply.Err
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}

	for {
		leader := ck.Leader()
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)

		// network failed or some error
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		return reply.Err
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}

	for {
		leader := ck.Leader()
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.DeleteShard", &args, &reply)

		// network failed or some error
		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		return reply.Err
	}
}
