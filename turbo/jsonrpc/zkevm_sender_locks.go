package jsonrpc

import (
	"sync"

	"github.com/ledgerwatch/erigon-lib/common"
)

// SenderLock is a map of sender addresses to the number of locks they have
// This is used to ensure that any calls for an account nonce will wait until all
// pending transactions for that account have been processed.  Without this you can
// get strange race behaviour where a nonce will come back too low if the pool is taking
// a long time to process a transaction.
type SenderLock struct {
	mtx   sync.RWMutex
	locks map[common.Address]uint64
}

func NewSenderLock() *SenderLock {
	return &SenderLock{
		locks: make(map[common.Address]uint64),
	}
}

func (sl *SenderLock) GetLock(sender common.Address) uint64 {
	sl.mtx.Lock()
	defer sl.mtx.Unlock()
	return sl.locks[sender]
}

func (sl *SenderLock) ReleaseLock(sender common.Address) {
	sl.mtx.Lock()
	defer sl.mtx.Unlock()
	if current, ok := sl.locks[sender]; ok {
		if current <= 1 {
			delete(sl.locks, sender)
		} else {
			sl.locks[sender] = current - 1
		}
	}
}

func (sl *SenderLock) AddLock(sender common.Address) {
	sl.mtx.Lock()
	defer sl.mtx.Unlock()
	sl.locks[sender]++
}
