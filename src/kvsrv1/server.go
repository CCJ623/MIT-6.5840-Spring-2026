package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueType struct {
	value_   string
	version_ rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	kv_map_ map[string]ValueType
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.kv_map_ = make(map[string]ValueType)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value, ok := kv.kv_map_[key]
	if !ok {
		// no key
		reply.Err = rpc.ErrNoKey
		return
	}

	// key exist
	reply.Value = value.value_
	reply.Version = value.version_
	reply.Err = rpc.OK
	return
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, ok := kv.kv_map_[args.Key]
	if !ok {
		// no key
		if args.Version == 0 {
			// new key
			value_struct := ValueType{}
			value_struct.value_ = args.Value
			value_struct.version_ = args.Version + 1
			kv.kv_map_[args.Key] = value_struct

			reply.Err = rpc.OK
			return
		}

		// error
		reply.Err = rpc.ErrNoKey
		return
	}

	// key exist
	if value.version_ != args.Version {
		// wrong version
		reply.Err = rpc.ErrVersion
		return
	}

	// version match
	value_struct := ValueType{}
	value_struct.value_ = args.Value
	value_struct.version_ = args.Version + 1
	kv.kv_map_[args.Key] = value_struct

	reply.Err = rpc.OK
	return
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
