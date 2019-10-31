package ethdb

import (
	"bytes"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/petar/GoLLRB/llrb"
)

type PutItem struct {
	key, value []byte
}

func (a *PutItem) Less(b llrb.Item) bool {
	bi := b.(*PutItem)
	return bytes.Compare(a.key, bi.key) < 0
}

type mutation struct {
	puts map[string]*llrb.LLRB // Map buckets to RB tree containing items
	//map[timestamp]map[hBucket]listOfChangedKeys
	suffixkeys map[uint64]map[string][]dbutils.Change
	mu         sync.RWMutex
	db         Database
}

func (m *mutation) getMem(bucket, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.puts[string(bucket)]; ok {
		i := t.Get(&PutItem{key: key})
		if i == nil {
			return nil, false
		}
		if item, ok := i.(*PutItem); ok {
			if item.value == nil {
				return nil, true
			}
			v := make([]byte, len(item.value))
			copy(v, item.value)
			return v, true
		}
		return nil, false
	}
	return nil, false
}

// Can only be called from the worker thread
func (m *mutation) Get(bucket, key []byte) ([]byte, error) {
	if value, ok := m.getMem(bucket, key); ok {
		if value == nil {
			return nil, ErrKeyNotFound
		}
		return value, nil
	}
	if m.db != nil {
		return m.db.Get(bucket, key)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) GetS(hBucket, key []byte, timestamp uint64) ([]byte, error) {
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	return m.Get(hBucket, composite)
}

func (m *mutation) getNoLock(bucket, key []byte) ([]byte, error) {
	if t, ok := m.puts[string(bucket)]; ok {
		i := t.Get(&PutItem{key: key})
		if i != nil {
			if item, ok := i.(*PutItem); ok {
				if item.value == nil {
					return nil, ErrKeyNotFound
				}
				return common.CopyBytes(item.value), nil
			}
		}
	}
	if m.db != nil {
		return m.db.Get(bucket, key)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) hasMem(bucket, key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.puts[string(bucket)]; ok {
		return t.Has(&PutItem{key: key})
	}
	return false
}

func (m *mutation) Has(bucket, key []byte) (bool, error) {
	if m.hasMem(bucket, key) {
		return true, nil
	}
	if m.db != nil {
		return m.db.Has(bucket, key)
	}
	return false, nil
}

func (m *mutation) Size() int {
	if m.db == nil {
		return 0
	}
	return m.db.Size()
}

func (m *mutation) Put(bucket, key []byte, value []byte) error {
	bb := make([]byte, len(bucket))
	copy(bb, bucket)
	k := make([]byte, len(key))
	copy(k, key)
	v := make([]byte, len(value))
	copy(v, value)
	m.mu.Lock()
	defer m.mu.Unlock()
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(bb)]; !ok {
		t = llrb.New()
		m.puts[string(bb)] = t
	}
	t.ReplaceOrInsert(&PutItem{key: k, value: v})
	return nil
}

// Assumes that bucket, key, and value won't be modified
func (m *mutation) PutS(hBucket, key, value []byte, timestamp uint64, noHistory bool) error {
	//fmt.Printf("PutS bucket %x key %x value %x timestamp %d\n", bucket, key, value, timestamp)
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	suffixM, ok := m.suffixkeys[timestamp]
	if !ok {
		suffixM = make(map[string][]dbutils.Change)
		m.suffixkeys[timestamp] = suffixM
	}
	suffixL, ok := suffixM[string(hBucket)]
	if !ok {
		suffixL = make([]dbutils.Change, 0)
	}
	suffixL = append(suffixL, dbutils.Change{
		Key:   key,
		Value: value,
	})
	suffixM[string(hBucket)] = suffixL
	if noHistory {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var ht *llrb.LLRB
	if ht, ok = m.puts[string(hBucket)]; !ok {
		ht = llrb.New()
		m.puts[string(hBucket)] = ht
	}
	ht.ReplaceOrInsert(&PutItem{key: composite, value: value})
	return nil
}

func (m *mutation) MultiPut(tuples ...[]byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := len(tuples)
	for i := 0; i < l; i += 3 {
		var t *llrb.LLRB
		var ok bool
		if t, ok = m.puts[string(tuples[i])]; !ok {
			t = llrb.New()
			m.puts[string(tuples[i])] = t
		}
		t.ReplaceOrInsert(&PutItem{key: tuples[i+1], value: tuples[i+2]})
	}
	return 0, nil
}

func (m *mutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	size := 0
	for _, t := range m.puts {
		size += t.Len()
	}
	return size
}

func (m *mutation) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	if m.db == nil {
		panic("Not implemented")
	} else {
		return m.db.GetAsOf(bucket, hBucket, key, timestamp)
	}
}

func (m *mutation) walkMem(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error {
	fixedbytes, mask := bytesmask(fixedbits)
	m.mu.RLock()
	defer m.mu.RUnlock()
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(bucket)]; !ok {
		return nil
	}
	for nextkey := startkey; nextkey != nil; {
		from := nextkey
		nextkey = nil
		var extErr error
		t.AscendGreaterOrEqual(&PutItem{key: from}, func(i llrb.Item) bool {
			item := i.(*PutItem)
			if item.value == nil {
				return true
			}
			if fixedbits > 0 && (!bytes.Equal(item.key[:fixedbytes-1], startkey[:fixedbytes-1]) || (item.key[fixedbytes-1]&mask) != (startkey[fixedbytes-1]&mask)) {
				return true
			}
			goOn, err := walker(item.key, item.value)
			if err != nil {
				extErr = err
				return false
			}
			return goOn
		})
		if extErr != nil {
			return extErr
		}
	}
	return nil
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) Walk(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error {
	if m.db == nil {
		return m.walkMem(bucket, startkey, fixedbits, walker)
	}
	return m.db.Walk(bucket, startkey, fixedbits, walker)
}

func (m *mutation) multiWalkMem(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error {
	panic("Not implemented")
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) (bool, error)) error {
	if m.db == nil {
		return m.multiWalkMem(bucket, startkeys, fixedbits, walker)
	}
	return m.db.MultiWalk(bucket, startkeys, fixedbits, walker)
}

func (m *mutation) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	if m.db == nil {
		panic("Not implemented")
	}

	return m.db.WalkAsOf(bucket, hBucket, startkey, fixedbits, timestamp, walker)
}

func (m *mutation) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) (bool, error)) error {
	if m.db == nil {
		panic("Not implemented")
	}
	return m.db.MultiWalkAsOf(bucket, hBucket, startkeys, fixedbits, timestamp, walker)
}

func (m *mutation) RewindData(timestampSrc, timestampDst uint64, df func(hBucket, key, value []byte) error) error {
	return rewindData(m, timestampSrc, timestampDst, df)
}

func (m *mutation) Delete(bucket, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	bb := make([]byte, len(bucket))
	copy(bb, bucket)
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(bb)]; !ok {
		t = llrb.New()
		m.puts[string(bb)] = t
	}
	k := make([]byte, len(key))
	copy(k, key)
	t.ReplaceOrInsert(&PutItem{key: k, value: nil})
	return nil
}

// Deletes all keys with specified suffix from all the buckets
func (m *mutation) DeleteTimestamp(timestamp uint64) error {
	suffix := dbutils.EncodeTimestamp(timestamp)
	m.mu.Lock()
	defer m.mu.Unlock()
	var t *llrb.LLRB
	var ok bool
	if t, ok = m.puts[string(dbutils.ChangeSetBucket)]; !ok {
		t = llrb.New()
		m.puts[string(dbutils.ChangeSetBucket)] = t
	}
	err := m.Walk(dbutils.ChangeSetBucket, suffix, uint(8*len(suffix)), func(k, v []byte) (bool, error) {
		hBucket := k[len(suffix):]
		changedAccounts, err := dbutils.Decode(v)
		if err != nil {
			return false, err
		}

		keycount := changedAccounts.KeyCount()
		var ht *llrb.LLRB
		var ok bool
		if keycount > 0 {
			hBucketStr := string(common.CopyBytes(hBucket))
			if ht, ok = m.puts[hBucketStr]; !ok {
				ht = llrb.New()
				m.puts[hBucketStr] = ht
			}
		}

		err = changedAccounts.Walk(func(kk, _ []byte) error {
			kk = append(kk, suffix...)
			ht.ReplaceOrInsert(&PutItem{key: kk, value: nil})
			return nil
		})
		if err != nil {
			return false, err
		}
		t.ReplaceOrInsert(&PutItem{key: common.CopyBytes(k), value: nil})
		return true, nil
	})
	return err
}

func (m *mutation) Commit() (uint64, error) {
	if m.db == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var t *llrb.LLRB
	var ok bool
	if len(m.suffixkeys) > 0 {
		if t, ok = m.puts[string(dbutils.ChangeSetBucket)]; !ok {
			t = llrb.New()
			m.puts[string(dbutils.ChangeSetBucket)] = t
		}
		for timestamp, suffixM := range m.suffixkeys {
			suffix := dbutils.EncodeTimestamp(timestamp)
			for bucketStr, suffixL := range suffixM {
				hBucket := []byte(bucketStr)
				suffixkey := make([]byte, len(suffix)+len(hBucket))
				copy(suffixkey, suffix)
				copy(suffixkey[len(suffix):], hBucket)
				dat, err := m.getNoLock(dbutils.ChangeSetBucket, suffixkey)
				if err != nil && err != ErrKeyNotFound {
					return 0, err
				}

				changedAccounts, err := dbutils.Decode(dat)
				if err != nil {
					log.Error("Commit Decode suffix err", "err", err)
				}

				changedAccounts = changedAccounts.MultiAdd(suffixL)
				changedRLP, err := dbutils.Encode(changedAccounts)
				if err != nil {
					log.Error("Commit Decode suffix err", "err", err)
					return 0, err
				}

				t.ReplaceOrInsert(&PutItem{key: suffixkey, value: changedRLP})
			}
		}
	}

	m.suffixkeys = make(map[uint64]map[string][]dbutils.Change)
	size := 0
	for _, t := range m.puts {
		size += t.Len()
	}
	tuples := make([][]byte, size*3)
	var index int
	for bucketStr, bt := range m.puts {
		bucketB := []byte(bucketStr)
		bt.AscendGreaterOrEqual(&PutItem{}, func(i llrb.Item) bool {
			item := i.(*PutItem)
			tuples[index] = bucketB
			index++
			tuples[index] = item.key
			index++
			tuples[index] = item.value
			index++
			return true
		})
	}
	var written uint64
	var putErr error
	if written, putErr = m.db.MultiPut(tuples...); putErr != nil {
		return 0, putErr
	}
	m.puts = make(map[string]*llrb.LLRB)
	return written, nil
}

func (m *mutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.suffixkeys = make(map[uint64]map[string][]dbutils.Change)
	m.puts = make(map[string]*llrb.LLRB)
}

func (m *mutation) Keys() ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	size := 0
	for _, t := range m.puts {
		size += t.Len()
	}
	pairs := make([][]byte, 2*size)
	idx := 0
	for bucketStr, bt := range m.puts {
		bucketB := []byte(bucketStr)
		bt.AscendGreaterOrEqual(&PutItem{}, func(i llrb.Item) bool {
			item := i.(*PutItem)
			pairs[idx] = bucketB
			idx++
			pairs[idx] = item.key
			idx++
			return true
		})
	}
	return pairs, nil
}

func (m *mutation) Close() {
	m.Rollback()
}

func (m *mutation) NewBatch() DbWithPendingMutations {
	mm := &mutation{
		db:         m,
		puts:       make(map[string]*llrb.LLRB),
		suffixkeys: make(map[uint64]map[string][]dbutils.Change),
	}
	return mm
}

func (m *mutation) MemCopy() Database {
	panic("Not implemented")
}
