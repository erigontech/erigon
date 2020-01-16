package ethdb

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ledgerwatch/turbo-geth/common/debug"

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
	//map[blockNumber]listOfChangedKeys
	accountChangeSetByBlock map[uint64]*dbutils.ChangeSet
	storageChangeSetByBlock map[uint64]*dbutils.ChangeSet
	mu                      sync.RWMutex
	db                      Database
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

func (m *mutation) getChangeSetByBlockNoLock(bucket []byte, timestamp uint64) (*dbutils.ChangeSet, error) {
	switch {
	case bytes.Equal(bucket, dbutils.AccountsHistoryBucket):
		if _, ok := m.accountChangeSetByBlock[timestamp]; !ok {
			m.accountChangeSetByBlock[timestamp] = dbutils.NewChangeSet()
		}
		return m.accountChangeSetByBlock[timestamp], nil
	case bytes.Equal(bucket, dbutils.StorageHistoryBucket):
		if _, ok := m.storageChangeSetByBlock[timestamp]; !ok {
			m.storageChangeSetByBlock[timestamp] = dbutils.NewChangeSet()
		}
		return m.storageChangeSetByBlock[timestamp], nil
	default:
		panic("incorrect bucket")
	}
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

func (m *mutation) DiskSize() int64 {
	if m.db == nil {
		return 0
	}
	return m.db.DiskSize()
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

	m.puts.Set(bb, key, value)
	return nil
}

// Assumes that bucket, key, and value won't be modified
func (m *mutation) PutS(hBucket, key, value []byte, timestamp uint64, noHistory bool) error {
	//fmt.Printf("PutS bucket %x key %x value %x timestamp %d\n", bucket, key, value, timestamp)
	m.mu.Lock()
	defer m.mu.Unlock()

	changeSet, err := m.getChangeSetByBlockNoLock(hBucket, timestamp)
	if err != nil {
		return err
	}

	err = changeSet.Add(key, value)
	if err != nil {
		return err
	}

	if noHistory {
		return nil
	}

	if !debug.IsThinHistory() {
		composite, _ := dbutils.CompositeKeySuffix(key, timestamp)
		m.puts.Set(hBucket, composite, value)
	}

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

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (m *mutation) IdealBatchSize() int {
	return m.db.IdealBatchSize()
}

func (m *mutation) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	m.panicOnEmptyDB()
	return m.db.GetAsOf(bucket, hBucket, key, timestamp)
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) Walk(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	return m.db.Walk(bucket, startkey, fixedbits, walker)
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.MultiWalk(bucket, startkeys, fixedbits, walker)
}

func (m *mutation) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	return m.db.WalkAsOf(bucket, hBucket, startkey, fixedbits, timestamp, walker)
}

func (m *mutation) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.MultiWalkAsOf(bucket, hBucket, startkeys, fixedbits, timestamp, walker)
}

func (m *mutation) RewindData(timestampSrc, timestampDst uint64, df func(hBucket, key, value []byte) error) error {
	return RewindData(m, timestampSrc, timestampDst, df)
}

func (m *mutation) Delete(bucket, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts.Delete(bucket, key)
	return nil
}

// Deletes all keys with specified suffix(blockNum) from all the buckets
func (m *mutation) DeleteTimestamp(timestamp uint64) error {
	changeSetKey := dbutils.EncodeTimestamp(timestamp)
	changedAccounts, err := m.Get(dbutils.AccountChangeSetBucket, changeSetKey)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	changedStorage, err := m.Get(dbutils.StorageChangeSetBucket, changeSetKey)
	if err != nil && err != ErrKeyNotFound {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if debug.IsThinHistory() {
		if len(changedAccounts) > 0 {
			innerErr := dbutils.Walk(changedAccounts, func(kk, _ []byte) error {
				indexBytes, getErr := m.getNoLock(dbutils.AccountsHistoryBucket, kk)
				if getErr != nil {
					return nil
				}
				var (
					v         []byte
					isEmpty   bool
					removeErr error
				)

				v, isEmpty, removeErr = RemoveFromIndex(indexBytes, timestamp)
				if removeErr != nil {
					return removeErr
				}
				if isEmpty {
					m.puts.DeleteStr(string(dbutils.AccountsHistoryBucket), kk)
				} else {
					m.puts.SetStr(string(dbutils.AccountsHistoryBucket), kk, v)
				}
				return nil
			})
			if innerErr != nil {
				return innerErr
			}
			m.puts.DeleteStr(string(dbutils.AccountChangeSetBucket), changeSetKey)
		}

		if len(changedStorage) > 0 {
			innerErr := dbutils.Walk(changedStorage, func(kk, _ []byte) error {
				indexBytes, getErr := m.getNoLock(dbutils.StorageHistoryBucket, kk)
				if getErr != nil {
					return nil
				}
				var (
					v         []byte
					isEmpty   bool
					removeErr error
				)

				v, isEmpty, removeErr = RemoveFromStorageIndex(indexBytes, timestamp)
				if removeErr != nil {
					return removeErr
				}
				if isEmpty {
					m.puts.DeleteStr(string(dbutils.StorageHistoryBucket), kk)
				} else {
					m.puts.SetStr(string(dbutils.StorageHistoryBucket), kk, v)
				}
				return nil
			})
			if innerErr != nil {
				return innerErr
			}
			m.puts.DeleteStr(string(dbutils.StorageChangeSetBucket), changeSetKey)
		}

	} else {
		if len(changedAccounts) > 0 {
			innerErr := dbutils.Walk(changedAccounts, func(kk, _ []byte) error {
				composite, _ := dbutils.CompositeKeySuffix(kk, timestamp)
				m.puts.DeleteStr(string(dbutils.AccountsHistoryBucket), composite)
				return nil
			})

			if innerErr != nil {
				return innerErr
			}
			m.puts.DeleteStr(string(dbutils.AccountChangeSetBucket), changeSetKey)
		}
		if len(changedStorage) > 0 {
			innerErr := dbutils.Walk(changedStorage, func(kk, _ []byte) error {
				composite, _ := dbutils.CompositeKeySuffix(kk, timestamp)
				m.puts.DeleteStr(string(dbutils.StorageHistoryBucket), composite)
				return nil
			})

			if innerErr != nil {
				return innerErr
			}
			m.puts.DeleteStr(string(dbutils.StorageChangeSetBucket), changeSetKey)
		}
	}
	return nil
}

func (m *mutation) Commit() (uint64, error) {
	if m.db == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.accountChangeSetByBlock) > 0 {
		for timestamp, changes := range m.accountChangeSetByBlock {
			if debug.IsThinHistory() {
				changedKeys := changes.ChangedKeys()
				for k := range changedKeys {
					var (
						v   []byte
						key []byte
						err error
					)
					value, err := m.getNoLock(dbutils.AccountsHistoryBucket, key)
					if err == ErrKeyNotFound {
						if m.db != nil {
							value, err = m.db.Get(dbutils.AccountsHistoryBucket, key)
							if err != nil && err != ErrKeyNotFound {
								return 0, err
							}
						}
					}
					v, err = AppendToIndex(value, timestamp)
					if err != nil {
						log.Error("mutation, append to index", "err", err, "timestamp", timestamp)
						continue
					}
					m.puts.Set(dbutils.AccountsHistoryBucket, []byte(k), v)
				}
			}
			sort.Sort(changes)
			dat, err := changes.Encode()
			if err != nil {
				return 0, err
			}
			m.puts.Set(dbutils.AccountChangeSetBucket, dbutils.EncodeTimestamp(timestamp), dat)
		}

	}
	if len(m.storageChangeSetByBlock) > 0 {
		for timestamp, changes := range m.storageChangeSetByBlock {
			if debug.IsThinHistory() {
				changedKeys := changes.ChangedKeys()
				for k := range changedKeys {
					var (
						v   []byte
						key []byte
						err error
					)
					key = []byte(k)[:common.HashLength+common.IncarnationLength]
					value, err := m.getNoLock(dbutils.StorageHistoryBucket, key)
					if err == ErrKeyNotFound {
						if m.db != nil {
							value, err = m.db.Get(dbutils.StorageHistoryBucket, key)
							if err != nil && err != ErrKeyNotFound {
								return 0, err
							}
						}
					}

					v, err = AppendToStorageIndex(value, []byte(k)[common.HashLength+common.IncarnationLength:common.HashLength+common.IncarnationLength+common.HashLength], timestamp)
					if err != nil {
						log.Error("mutation, append to storage index", "err", err, "timestamp", timestamp)
						continue
					}
					m.puts.Set(dbutils.StorageHistoryBucket, key, v)
				}
			}
			sort.Sort(changes)
			dat, err := changes.Encode()
			if err != nil {
				return 0, err
			}
			m.puts.Set(dbutils.StorageChangeSetBucket, dbutils.EncodeTimestamp(timestamp), dat)
		}
	}

	m.accountChangeSetByBlock = make(map[uint64]*dbutils.ChangeSet)
	m.storageChangeSetByBlock = make(map[uint64]*dbutils.ChangeSet)

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
	m.accountChangeSetByBlock = make(map[uint64]*dbutils.ChangeSet)
	m.storageChangeSetByBlock = make(map[uint64]*dbutils.ChangeSet)
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
		db:                      m,
		puts:                    newPuts(),
		accountChangeSetByBlock: make(map[uint64]*dbutils.ChangeSet),
		storageChangeSetByBlock: make(map[uint64]*dbutils.ChangeSet),
	}
	return mm
}

func (m *mutation) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

func (m *mutation) MemCopy() Database {
	m.panicOnEmptyDB()
	return m.db
}

func (m *mutation) ID() uint64 {
	return m.db.ID()
}

// [TURBO-GETH] Freezer support (not implemented yet)
// Ancients returns an error as we don't have a backing chain freezer.
func (m *mutation) Ancients() (uint64, error) {
	return 0, errNotSupported
}

// TruncateAncients returns an error as we don't have a backing chain freezer.
func (m *mutation) TruncateAncients(items uint64) error {
	return errNotSupported
}

func NewRWDecorator(db Database) *RWCounterDecorator {
	return &RWCounterDecorator{
		db,
		DBCounterStats{},
	}
}

type RWCounterDecorator struct {
	Database
	DBCounterStats
}

type DBCounterStats struct {
	Put             uint64
	PutS            uint64
	Get             uint64
	GetS            uint64
	GetAsOf         uint64
	Has             uint64
	Walk            uint64
	WalkAsOf        uint64
	MultiWalk       uint64
	MultiWalkAsOf   uint64
	Delete          uint64
	DeleteTimestamp uint64
	MultiPut        uint64
}

func (d *RWCounterDecorator) Put(bucket, key, value []byte) error {
	atomic.AddUint64(&d.DBCounterStats.Put, 1)
	fmt.Println("PUT", string(bucket), key)
	return d.Database.Put(bucket, key, value)
}

func (d *RWCounterDecorator) PutS(hBucket, key, value []byte, timestamp uint64, changeSetBucketOnly bool) error {
	atomic.AddUint64(&d.DBCounterStats.PutS, 1)
	return d.Database.PutS(hBucket, key, value, timestamp, changeSetBucketOnly)
}
func (d *RWCounterDecorator) Get(bucket, key []byte) ([]byte, error) {
	atomic.AddUint64(&d.DBCounterStats.Get, 1)
	return d.Database.Get(bucket, key)
}

func (d *RWCounterDecorator) GetAsOf(bucket, hBucket, key []byte, timestamp uint64) ([]byte, error) {
	atomic.AddUint64(&d.DBCounterStats.GetAsOf, 1)
	return d.Database.GetAsOf(bucket, hBucket, key, timestamp)
}
func (d *RWCounterDecorator) Has(bucket, key []byte) (bool, error) {
	atomic.AddUint64(&d.DBCounterStats.Has, 1)
	return d.Database.Has(bucket, key)
}
func (d *RWCounterDecorator) Walk(bucket, startkey []byte, fixedbits uint, walker func([]byte, []byte) (bool, error)) error {
	atomic.AddUint64(&d.DBCounterStats.Walk, 1)
	return d.Database.Walk(bucket, startkey, fixedbits, walker)
}
func (d *RWCounterDecorator) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []uint, walker func(int, []byte, []byte) error) error {
	atomic.AddUint64(&d.DBCounterStats.MultiWalk, 1)
	return d.Database.MultiWalk(bucket, startkeys, fixedbits, walker)
}
func (d *RWCounterDecorator) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits uint, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	atomic.AddUint64(&d.DBCounterStats.WalkAsOf, 1)
	return d.Database.WalkAsOf(bucket, hBucket, startkey, fixedbits, timestamp, walker)
}
func (d *RWCounterDecorator) MultiWalkAsOf(bucket, hBucket []byte, startkeys [][]byte, fixedbits []uint, timestamp uint64, walker func(int, []byte, []byte) error) error {
	atomic.AddUint64(&d.DBCounterStats.MultiWalkAsOf, 1)
	return d.Database.MultiWalkAsOf(bucket, hBucket, startkeys, fixedbits, timestamp, walker)
}
func (d *RWCounterDecorator) Delete(bucket, key []byte) error {
	atomic.AddUint64(&d.DBCounterStats.Delete, 1)
	return d.Database.Delete(bucket, key)
}
func (d *RWCounterDecorator) DeleteTimestamp(timestamp uint64) error {
	atomic.AddUint64(&d.DBCounterStats.DeleteTimestamp, 1)
	return d.Database.DeleteTimestamp(timestamp)
}
func (d *RWCounterDecorator) MultiPut(tuples ...[]byte) (uint64, error) {
	atomic.AddUint64(&d.DBCounterStats.MultiPut, 1)
	return d.Database.MultiPut(tuples...)
}
func (d *RWCounterDecorator) MemCopy() Database {
	return d.Database.MemCopy()
}
func (d *RWCounterDecorator) NewBatch() DbWithPendingMutations {
	mm := &mutation{
		db:                      d,
		puts:                    newPuts(),
		accountChangeSetByBlock: make(map[uint64]*dbutils.ChangeSet),
		storageChangeSetByBlock: make(map[uint64]*dbutils.ChangeSet),
	}
	return mm
}
