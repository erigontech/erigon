package ethdb

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/common"
)

type mutation struct {
	puts *puts // Map buckets to map[key]value
	mu   sync.RWMutex
	db   Database
}

func (m *mutation) KV() *bolt.DB {
	if casted, ok := m.db.(HasKV); !ok {
		return nil
	} else {
		return casted.KV()
	}
}

func (m *mutation) AbstractKV() KV {
	if casted, ok := m.db.(HasAbstractKV); ok {
		return casted.AbstractKV()
	}
	return nil
}

func (m *mutation) getMem(bucket, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.puts.get(bucket, key)
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

func (m *mutation) getNoLock(bucket, key []byte) ([]byte, error) {
	if value, ok := m.puts.get(bucket, key); ok {
		return value, nil
	}
	if m.db != nil {
		return m.db.Get(bucket, key)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) GetIndexChunk(bucket, key []byte, timestamp uint64) ([]byte, error) {
	if m.db != nil {
		return m.db.GetIndexChunk(bucket, key, timestamp)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) hasMem(bucket, key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.puts.get(bucket, key)
	return ok
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
	m.mu.Lock()
	defer m.mu.Unlock()

	m.puts.set(bucket, key, value)
	return nil
}

func (m *mutation) MultiPut(tuples ...[]byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := len(tuples)
	for i := 0; i < l; i += 3 {
		m.puts.set(tuples[i], tuples[i+1], tuples[i+2])
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
func (m *mutation) Walk(bucket, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	return m.db.Walk(bucket, startkey, fixedbits, walker)
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.MultiWalk(bucket, startkeys, fixedbits, walker)
}

func (m *mutation) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits int, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	return m.db.WalkAsOf(bucket, hBucket, startkey, fixedbits, timestamp, walker)
}

func (m *mutation) Delete(bucket, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts.Delete(bucket, key)
	return nil
}

func (m *mutation) Commit() (uint64, error) {
	if m.db == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	tuples := make(MultiPutTuples, 0, m.puts.Len()*3)
	for bucketStr, bt := range m.puts.mp {
		bucketB := []byte(bucketStr)
		for key := range bt {
			value, _ := bt.GetStr(key)
			tuples = append(tuples, bucketB, []byte(key), value)
		}
	}
	sort.Sort(tuples)

	written, err := m.db.MultiPut(tuples...)
	if err != nil {
		return 0, fmt.Errorf("db.MultiPut failed: %w", err)
	}

	m.puts = newPuts()
	return written, nil
}

func (m *mutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = newPuts()
}

func (m *mutation) Keys() ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tuples := common.NewTuples(m.puts.Len(), 2, 1)
	for bucketStr, bt := range m.puts.mp {
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
		db:   m,
		puts: newPuts(),
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
	Put           uint64
	Get           uint64
	GetS          uint64
	GetAsOf       uint64
	Has           uint64
	Walk          uint64
	WalkAsOf      uint64
	MultiWalk     uint64
	MultiWalkAsOf uint64
	Delete        uint64
	MultiPut      uint64
}

func (d *RWCounterDecorator) Put(bucket, key, value []byte) error {
	atomic.AddUint64(&d.DBCounterStats.Put, 1)
	return d.Database.Put(bucket, key, value)
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
func (d *RWCounterDecorator) Walk(bucket, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	atomic.AddUint64(&d.DBCounterStats.Walk, 1)
	return d.Database.Walk(bucket, startkey, fixedbits, walker)
}
func (d *RWCounterDecorator) MultiWalk(bucket []byte, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {
	atomic.AddUint64(&d.DBCounterStats.MultiWalk, 1)
	return d.Database.MultiWalk(bucket, startkeys, fixedbits, walker)
}
func (d *RWCounterDecorator) WalkAsOf(bucket, hBucket, startkey []byte, fixedbits int, timestamp uint64, walker func([]byte, []byte) (bool, error)) error {
	atomic.AddUint64(&d.DBCounterStats.WalkAsOf, 1)
	return d.Database.WalkAsOf(bucket, hBucket, startkey, fixedbits, timestamp, walker)
}
func (d *RWCounterDecorator) Delete(bucket, key []byte) error {
	atomic.AddUint64(&d.DBCounterStats.Delete, 1)
	return d.Database.Delete(bucket, key)
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
		db:   d,
		puts: newPuts(),
	}
	return mm
}
