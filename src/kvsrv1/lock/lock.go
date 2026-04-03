package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

const UNLOCK_NAME = "None"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lock_name_ string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here

	// make sure lock exist in server
	for {
		_, _, get_error := lk.ck.Get(lockname)
		if get_error == rpc.OK {
			break
		}
		if get_error == rpc.ErrNoKey {
			lk.ck.Put(lockname, UNLOCK_NAME, 0)
			break
		}
	}

	lk.ck = ck
	lk.lock_name_ = lockname
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		value, version, _ := lk.ck.Get(lk.lock_name_)

		if value != UNLOCK_NAME{
			// lock is already acquired
			continue
		}

		id := kvtest.RandValue(8)
		put_error := lk.ck.Put(lk.lock_name_, id, version)

		switch put_error {
		case rpc.OK:
			// acquire success
			return
		case rpc.ErrNoKey:
		case rpc.ErrVersion:
			// acquire failed
			continue
		case rpc.ErrMaybe:
			// not sure
			// verify again
			value, _, _ := lk.ck.Get(lk.lock_name_)
			if value == id {
				// acquire sucess
				return
			} else {
				// acquire failed
				continue
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		my_id, version, _ := lk.ck.Get(lk.lock_name_)
		put_error := lk.ck.Put(lk.lock_name_, UNLOCK_NAME, version)

		switch put_error {
		case rpc.OK:
			// release success
			return
		case rpc.ErrNoKey:
		case rpc.ErrVersion:
			// release failed
			continue
		case rpc.ErrMaybe:
			// not sure
			// verify again
			value, _, _ := lk.ck.Get(lk.lock_name_)
			if value != my_id {
				// release sucess
				return
			} else {
				// release failed
				continue
			}
		}
	}
}
