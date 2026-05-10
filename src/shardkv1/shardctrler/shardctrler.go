package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"log"
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

const Debug = false
const RPC_RETRY_INTERVAL = 100 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	str := cfg.String()

	for {
		err := sck.IKVClerk.Put("config", str, 0)
		if err == rpc.OK {
			break
		}
	}
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	for {
		DPrintf("[Ctrler] ChangeConfigTo: Target Num=%d | Started\n", new.Num)

		old_config_str, version, err := sck.IKVClerk.Get("config")
		if err != rpc.OK {
			DPrintf("[Ctrler] ChangeConfigTo: Target Num=%d | Failed to get current config (Err: %v)\n", new.Num, err)
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		old_config := shardcfg.FromString(old_config_str)
		if new.Num <= old_config.Num {
			DPrintf("[Ctrler] ChangeConfigTo: Target Num=%d | Aborted (Already at Num=%d)\n", new.Num, old_config.Num)
			return
		}

		is_success := true
		for i := 0; i < len(old_config.Shards); i++ {
			old_group_id := old_config.Shards[i]
			new_group_id := new.Shards[i]

			// no need to move shard
			if old_group_id == new_group_id {
				continue
			}

			var shard_data []byte
			shard_id := shardcfg.Tshid(i)
			old_shard_group_clerk := shardgrp.MakeClerk(sck.clnt, old_config.Groups[old_group_id])
			new_shard_group_clerk := shardgrp.MakeClerk(sck.clnt, new.Groups[new_group_id])

			DPrintf("[Ctrler] Shard=%d | Move Gid=%d -> Gid=%d | Starting Move\n", shard_id, old_group_id, new_group_id)

			// all operation below, ErrWrongGroup means config is stale, we can return
			// get and freeze old shard
			data, err := old_shard_group_clerk.FreezeShard(shard_id, old_config.Num)
			DPrintf("[Ctrler] Shard=%d | Move Gid=%d -> Gid=%d | Freeze | Err=%v\n", shard_id, old_group_id, new_group_id, err)
			if err != rpc.OK {
				is_success = false
				break
			}
			shard_data = data

			err = new_shard_group_clerk.InstallShard(shard_id, shard_data, new.Num)
			DPrintf("[Ctrler] Shard=%d | Move Gid=%d -> Gid=%d | Install | Err=%v\n", shard_id, old_group_id, new_group_id, err)
			if err != rpc.OK {
				is_success = false
				break
			}

			err = old_shard_group_clerk.DeleteShard(shard_id, old_config.Num)
			DPrintf("[Ctrler] Shard=%d | Move Gid=%d -> Gid=%d | Delete | Err=%v\n", shard_id, old_group_id, new_group_id, err)
			if err != rpc.OK {
				is_success = false
				break
			}
		}

		if !is_success {
			time.Sleep(RPC_RETRY_INTERVAL)
			continue
		}

		err = sck.IKVClerk.Put("config", new.String(), version)
		DPrintf("[Ctrler] ChangeConfigTo: Target Num=%d | PutConfig Result=%v\n", new.Num, err)
		if err == rpc.OK {
			return
		}
	}
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	for {
		str, _, err := sck.IKVClerk.Get("config")
		if err == rpc.OK {
			return shardcfg.FromString(str)
		}
	}
}
