package shardgrp

import (
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const GrpClntDebug = false

func GCDPrintf(format string, a ...interface{}) (n int, err error) {
	if GrpClntDebug {
		log.Printf(format, a...)
	}
	return
}

const RPC_RETRY_INTERVAL = 1 * time.Millisecond

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // last successful leader (index into servers[])
	// You can  add to this struct.
	lock                sync.Mutex
	rpc_max_retry_times uint
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers, rpc_max_retry_times: uint(len(servers) * 2)}

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
	retry_times := uint(0)

	for {
		leader := ck.Leader()
		GCDPrintf("[GrpClnt] RPC: Get(Key=%s) -> Srv %d\n", key, leader)
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.Get", &args, &reply)

		// network failed or some error
		if !ok || reply.Err == rpc.ErrWrongLeader {
			retry_times++
			if retry_times >= ck.rpc_max_retry_times {
				return "", 0, rpc.ErrWrongGroup
			}

			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			GCDPrintf("[GrpClnt] RPC: Get(Key=%s) -> Srv %d | Failed/WrongLeader (ok=%v, err=%v)\n", key, leader, ok, reply.Err)
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		GCDPrintf("[GrpClnt] RPC: Get(Key=%s) -> Srv %d | OK (Val=%s, Err=%v)\n", key, leader, reply.Value, reply.Err)
		return reply.Value, reply.Version, reply.Err
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	is_resend_ := false
	retry_times := uint(0)

	for {
		leader := ck.Leader()
		GCDPrintf("[GrpClnt] RPC: Put(Key=%s, Ver=%d) -> Srv %d\n", key, version, leader)
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.Put", &args, &reply)

		// network error or wrong leader
		if !ok || reply.Err == rpc.ErrWrongLeader {
			retry_times++
			if retry_times >= ck.rpc_max_retry_times {
				return rpc.ErrWrongGroup
			}

			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			GCDPrintf("[GrpClnt] RPC: Put(Key=%s, Ver=%d) -> Srv %d | Failed/WrongLeader (ok=%v, err=%v)\n", key, version, leader, ok, reply.Err)
			is_resend_ = true
			time.Sleep(RPC_RETRY_INTERVAL)

			continue
		}

		// error version
		if reply.Err == rpc.ErrVersion && !is_resend_ {
			GCDPrintf("[GrpClnt] RPC: Put(Key=%s, Ver=%d) -> Srv %d | ErrVersion\n", key, version, leader)
			return rpc.ErrVersion
		}

		// unsure
		if reply.Err == rpc.ErrVersion && is_resend_ {
			GCDPrintf("[GrpClnt] RPC: Put(Key=%s, Ver=%d) -> Srv %d | ErrMaybe\n", key, version, leader)
			return rpc.ErrMaybe
		}

		GCDPrintf("[GrpClnt] RPC: Put(Key=%s, Ver=%d) -> Srv %d | Result=%v\n", key, version, leader, reply.Err)
		return reply.Err
	}

}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}
	retry_times := uint(0)

	for {
		leader := ck.Leader()
		GCDPrintf("[GrpClnt] RPC: FreezeShard(Shard=%d, Num=%d) -> Srv %d\n", s, num, leader)
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.FreezeShard", &args, &reply)

		// network failed or some error
		if !ok || reply.Err == rpc.ErrWrongLeader {
			retry_times++
			if retry_times >= ck.rpc_max_retry_times {
				if !ok {
					return []byte{}, rpc.ErrMaybe
				} else {
					return []byte{}, reply.Err
				}
			}

			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			GCDPrintf("[GrpClnt] RPC: FreezeShard(Shard=%d, Num=%d) -> Srv %d | Failed/WrongLeader\n", s, num, leader)
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		GCDPrintf("[GrpClnt] RPC: FreezeShard(Shard=%d, Num=%d) -> Srv %d | Result=%v\n", s, num, leader, reply.Err)
		return reply.State, reply.Err
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}
	retry_times := uint(0)

	for {
		leader := ck.Leader()
		GCDPrintf("[GrpClnt] RPC: InstallShard(Shard=%d, Num=%d) -> Srv %d\n", s, num, leader)
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.InstallShard", &args, &reply)

		// network failed or some error
		if !ok || reply.Err == rpc.ErrWrongLeader {
			retry_times++
			if retry_times >= ck.rpc_max_retry_times {
				if !ok {
					return rpc.ErrMaybe
				} else {
					return reply.Err
				}
			}

			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			GCDPrintf("[GrpClnt] RPC: InstallShard(Shard=%d, Num=%d) -> Srv %d | Failed/WrongLeader\n", s, num, leader)
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		GCDPrintf("[GrpClnt] RPC: InstallShard(Shard=%d, Num=%d) -> Srv %d | Result=%v\n", s, num, leader, reply.Err)
		return reply.Err
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}
	retry_times := uint(0)

	for {
		leader := ck.Leader()
		GCDPrintf("[GrpClnt] RPC: DeleteShard(Shard=%d, Num=%d) -> Srv %d\n", s, num, leader)
		ok := ck.Clnt.Call(ck.servers[leader], "KVServer.DeleteShard", &args, &reply)

		// network failed or some error
		if !ok || reply.Err == rpc.ErrWrongLeader {
			retry_times++
			if retry_times >= ck.rpc_max_retry_times {
				if !ok {
					return rpc.ErrMaybe
				} else {
					return reply.Err
				}
			}

			ck.lock.Lock()
			curr_leader := ck.leader
			if curr_leader == leader {
				ck.leader = (ck.leader + 1) % len(ck.servers)
			}
			ck.lock.Unlock()

			GCDPrintf("[GrpClnt] RPC: DeleteShard(Shard=%d, Num=%d) -> Srv %d | Failed/WrongLeader\n", s, num, leader)
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		GCDPrintf("[GrpClnt] RPC: DeleteShard(Shard=%d, Num=%d) -> Srv %d | Result=%v\n", s, num, leader, reply.Err)
		return reply.Err
	}
}
