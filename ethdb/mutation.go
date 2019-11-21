package ethdb

import (
	"errors"
	"sort"
	"sync"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/log"
)

type puts map[string]putsBucket //map[bucket]putsBucket

func newPuts() puts {
	return make(puts)
}

func (p puts) Set(bucket, key, value []byte) {
	var bucketPuts putsBucket
	var ok bool
	if bucketPuts, ok = p[string(bucket)]; !ok {
		bucketPuts = make(putsBucket)
		p[string(bucket)] = bucketPuts
	}
	bucketPuts[string(key)] = value
}

func (p puts) Delete(bucket, key []byte) {
	p.Set(bucket, key, nil)
}

func (p puts) SetStr(bucket string, key, value []byte) {
	var bucketPuts putsBucket
	var ok bool
	if bucketPuts, ok = p[bucket]; !ok {
		bucketPuts = make(putsBucket)
		p[bucket] = bucketPuts
	}
	bucketPuts[string(key)] = value
}

func (p puts) DeleteStr(bucket string, key []byte) {
	p.SetStr(bucket, key, nil)
}

func (p puts) Size() int {
	var size int
	for _, put := range p {
		size += len(put)
	}
	return size
}

type putsBucket map[string][]byte //map[key]value

func (pb putsBucket) Get(key []byte) ([]byte, bool) {
	value, ok := pb[string(key)]
	if !ok {
		return nil, false
	}

	if value == nil {
		return nil, true
	}

	v := make([]byte, len(value))
	copy(v, value)

	return v, true
}

func (pb putsBucket) GetStr(key string) ([]byte, bool) {
	value, ok := pb[key]
	if !ok {
		return nil, false
	}

	if value == nil {
		return nil, true
	}

	v := make([]byte, len(value))
	copy(v, value)

	return v, true
}

type mutation struct {
	puts puts // Map buckets to map[key]value
	//map[timestamp]map[hBucket]listOfChangedKeys
	changeSetByBlock map[uint64]map[string][]dbutils.Change
	mu               sync.RWMutex
	db               Database
}

func (m *mutation) getMem(bucket, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if t, ok := m.puts[string(bucket)]; ok {
		return t.Get(key)
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
		value, ok := t.Get(key)
		if ok && value == nil {
			return nil, ErrKeyNotFound
		}
		return value, nil
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
		_, ok = t.Get(key)
		return ok
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

	var t putsBucket
	var ok bool
	if t, ok = m.puts[string(bb)]; !ok {
		t = make(putsBucket)
		m.puts[string(bb)] = t
	}
	t[string(k)] = v
	return nil
}

// Assumes that bucket, key, and value won't be modified
func (m *mutation) PutS(hBucket, key, value []byte, timestamp uint64, noHistory bool) error {
	//fmt.Printf("PutS bucket %x key %x value %x timestamp %d\n", bucket, key, value, timestamp)
	composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
	changesByBucket, ok := m.changeSetByBlock[timestamp]
	if !ok {
		changesByBucket = make(map[string][]dbutils.Change)
		m.changeSetByBlock[timestamp] = changesByBucket
	}
	changes, ok := changesByBucket[string(hBucket)]
	if !ok {
		changes = make([]dbutils.Change, 0)
	}
	changes = append(changes, dbutils.Change{
		Key:   key,
		Value: value,
	})
	changesByBucket[string(hBucket)] = changes
	if noHistory {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts.Set(hBucket, composite, value)
	return nil
}

func (m *mutation) MultiPut(tuples ...[]byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := len(tuples)
	for i := 0; i < l; i += 3 {
		m.puts.Set(tuples[i], tuples[i+1], tuples[i+2])
	}
	return 0, nil
}

func (m *mutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.puts.Size()
}

func (m *mutation) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	if m.db == nil {
		panic("Not implemented")
	} else {
		return m.db.GetAsOf(bucket, hBucket, key, timestamp)
	}
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) Walk(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error {
	if m.db == nil {
		panic("not implemented")
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
	m.puts.Delete(bucket, key)
	return nil
}

// Deletes all keys with specified suffix(blockNum) from all the buckets
func (m *mutation) DeleteTimestamp(timestamp uint64) error {
	encodedTS := dbutils.EncodeTimestamp(timestamp)
	m.mu.Lock()
	defer m.mu.Unlock()
	err := m.Walk(dbutils.ChangeSetBucket, encodedTS, uint(8*len(encodedTS)), func(k, v []byte) (bool, error) {
		// k = encodedTS + hBucket
		hBucketStr := string(k[len(encodedTS):])
		changedAccounts, err := dbutils.Decode(v)
		if err != nil {
			return false, err
		}

		err = changedAccounts.Walk(func(kk, _ []byte) error {
			m.puts.DeleteStr(hBucketStr, kk)
			return nil
		})
		if err != nil {
			return false, err
		}
		m.puts.DeleteStr(hBucketStr, k)
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
	if len(m.changeSetByBlock) > 0 {
		changeSetStr := string(dbutils.ChangeSetBucket)
		for timestamp, changesByBucket := range m.changeSetByBlock {
			encodedTS := dbutils.EncodeTimestamp(timestamp)
			for bucketStr, changes := range changesByBucket {
				hBucket := []byte(bucketStr)
				changeSetKey := dbutils.CompositeChangeSetKey(encodedTS, hBucket)
				dat, err := m.getNoLock(dbutils.ChangeSetBucket, changeSetKey)
				if err != nil && err != ErrKeyNotFound {
					return 0, err
				}

				changedAccounts, err := dbutils.Decode(dat)
				if err != nil {
					log.Error("Decode changedAccounts error on commit", "err", err)
				}

				changedAccounts = changedAccounts.MultiAdd(changes)
				changedRLP, err := dbutils.Encode(changedAccounts)
				if err != nil {
					log.Error("Encode changedAccounts error on commit", "err", err)
					return 0, err
				}

				m.puts.SetStr(changeSetStr, changeSetKey, changedRLP)
			}
		}
	}

	m.changeSetByBlock = make(map[uint64]map[string][]dbutils.Change)

	tuples := common.NewTuples(m.puts.Size(), 3, 1)
	for bucketStr, bt := range m.puts {
		bucketB := []byte(bucketStr)
		for key := range bt {
			value, _ := bt.GetStr(key)
			if err := tuples.Append(bucketB, []byte(key), value); err != nil {
				return 0, err
			}
		}
	}
	sort.Sort(tuples)

	written, err := m.db.MultiPut(tuples.Values...)
	if err != nil {
		return 0, err
	}
	m.puts = make(puts)
	return written, nil
}

func (m *mutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.changeSetByBlock = make(map[uint64]map[string][]dbutils.Change)
	m.puts = make(puts)
}

func (m *mutation) Keys() ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tuples := common.NewTuples(m.puts.Size(), 2, 1)
	for bucketStr, bt := range m.puts {
		bucketB := []byte(bucketStr)
		for key := range bt {
			if err := tuples.Append(bucketB, []byte(key)); err != nil {
				return nil, err
			}
		}
	}
	sort.Sort(tuples)
	return tuples.Values, nil
}

func (m *mutation) Close() {
	m.Rollback()
}

func (m *mutation) NewBatch() DbWithPendingMutations {
	mm := &mutation{
		db:               m,
		puts:             newPuts(),
		changeSetByBlock: make(map[uint64]map[string][]dbutils.Change),
	}
	return mm
}

func (m *mutation) MemCopy() Database {
	panic("Not implemented")
}

var errNotSupported = errors.New("not supported")

// [TURBO-GETH] Freezer support (not implemented yet)
// Ancients returns an error as we don't have a backing chain freezer.
func (m *mutation) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// TruncateAncients returns an error as we don't have a backing chain freezer.
func (m *mutation) TruncateAncients(items uint64) error {
	return errNotSupported
}
