package state

import (
	"github.com/ledgerwatch/erigon-lib/kv"
	"sync"
)

type ReconState1 struct {
	lock          sync.RWMutex
	queue         theap[uint64]
	changes       map[string]map[string][]byte
	sizeEstimate  uint64
	rollbackCount uint64
}

func NewReconState1() *ReconState1 {
	rs := &ReconState1{
		changes: map[string]map[string][]byte{},
	}
	return rs
}

func (rs *ReconState1) Put(table string, key, val []byte) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		t = map[string][]byte{}
		rs.changes[table] = t
	}
	t[string(key)] = val
	rs.sizeEstimate += uint64(len(key)) + uint64(len(val))
}

func (rs *ReconState1) Delete(table string, key []byte) {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	t, ok := rs.changes[table]
	if !ok {
		t = map[string][]byte{}
		rs.changes[table] = t
	}
	t[string(key)] = nil
	rs.sizeEstimate += uint64(len(key))
}

func (rs *ReconState1) Get(table string, key []byte) []byte {
	rs.lock.RLock()
	defer rs.lock.RUnlock()
	t, ok := rs.changes[table]
	if !ok {
		return nil
	}
	return t[string(key)]
}

func (rs *ReconState1) Flush(rwTx kv.RwTx) error {
	rs.lock.Lock()
	defer rs.lock.Unlock()
	for table, t := range rs.changes {
		for ks, val := range t {
			if len(val) == 0 {
				if err := rwTx.Delete(table, []byte(ks), nil); err != nil {
					return err
				}
			} else {
				if err := rwTx.Put(table, []byte(ks), val); err != nil {
					return err
				}
			}
		}
	}
	rs.changes = map[string]map[string][]byte{}
	rs.sizeEstimate = 0
	return nil
}
