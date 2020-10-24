package ethdb

import (
	"context"
	"fmt"
	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/metrics"
	"sort"
	"sync"
	"sync/atomic"
)

var (
	dbCommitBigBatchTimer   = metrics.NewRegisteredTimer("db/commit/big_batch", nil)
	dbCommitSmallBatchTimer = metrics.NewRegisteredTimer("db/commit/small_batch", nil)
)

type mutation struct {
	puts   *puts // Map buckets to map[key]value
	mu     sync.RWMutex
	db     Database
	tuples MultiPutTuples
}

func (m *mutation) KV() KV {
	if casted, ok := m.db.(HasKV); ok {
		return casted.KV()
	}
	return nil
}

func (m *mutation) getMem(bucket string, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.puts.get(bucket, key)
}

// Can only be called from the worker thread
func (m *mutation) Get(bucket string, key []byte) ([]byte, error) {
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

func (m *mutation) Last(bucket string) ([]byte, []byte, error) {
	return m.db.Last(bucket)
}

func (m *mutation) Reserve(bucket string, key []byte, i int) ([]byte, error) {
	return m.db.(DbWithPendingMutations).Reserve(bucket, key, i)
}

func (m *mutation) GetIndexChunk(bucket string, key []byte, timestamp uint64) ([]byte, error) {
	if m.db != nil {
		return m.db.GetIndexChunk(bucket, key, timestamp)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) hasMem(bucket string, key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.puts.get(bucket, key)
	return ok
}

func (m *mutation) Has(bucket string, key []byte) (bool, error) {
	if m.hasMem(bucket, key) {
		return true, nil
	}
	if m.db != nil {
		return m.db.Has(bucket, key)
	}
	return false, nil
}

func (m *mutation) DiskSize(ctx context.Context) (common.StorageSize, error) {
	if m.db == nil {
		return 0, nil
	}
	sz, err := m.db.(HasStats).DiskSize(ctx)
	if err != nil {
		return 0, err
	}
	return common.StorageSize(sz), nil
}

func (m *mutation) Put(bucket string, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.puts.set(bucket, key, value)
	return nil
}

func (m *mutation) Append(bucket string, key []byte, value []byte) error {
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
		m.puts.set(string(tuples[i]), tuples[i+1], tuples[i+2])
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
	return int(512 * datasize.MB)
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) Walk(bucket string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	return m.db.Walk(bucket, startkey, fixedbits, walker)
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) MultiWalk(bucket string, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.MultiWalk(bucket, startkeys, fixedbits, walker)
}

func (m *mutation) Delete(bucket string, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts.Delete(bucket, key)
	return nil
}

func (m *mutation) CommitAndBegin(ctx context.Context) error {
	_, err := m.Commit()
	return err
}

func (m *mutation) RollbackAndBegin(ctx context.Context) error {
	m.Rollback()
	return nil
}

func (m *mutation) Commit() (uint64, error) {
	if m.db == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tuples == nil {
		m.tuples = make(MultiPutTuples, 0, m.puts.Len()*3)
	}
	m.tuples = m.tuples[:0]
	for bucketStr, bt := range m.puts.mp {
		bucketB := []byte(bucketStr)
		for key := range bt {
			value, _ := bt.GetStr(key)
			m.tuples = append(m.tuples, bucketB, []byte(key), value)
		}
		delete(m.puts.mp, bucketStr)
	}
	sort.Sort(m.tuples)

	written, err := m.db.MultiPut(m.tuples...)
	if err != nil {
		return 0, fmt.Errorf("db.MultiPut failed: %w", err)
	}

	m.puts = newPuts()
	m.tuples = nil
	return written, nil
}

func (m *mutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = newPuts()
	m.tuples = nil
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

func (m *mutation) Begin(ctx context.Context, writable bool) (DbWithPendingMutations, error) {
	return m.db.Begin(ctx, writable)
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

func (d *RWCounterDecorator) Put(bucket string, key, value []byte) error {
	atomic.AddUint64(&d.DBCounterStats.Put, 1)
	return d.Database.Put(bucket, key, value)
}

func (d *RWCounterDecorator) Get(bucket string, key []byte) ([]byte, error) {
	atomic.AddUint64(&d.DBCounterStats.Get, 1)
	return d.Database.Get(bucket, key)
}

func (d *RWCounterDecorator) Has(bucket string, key []byte) (bool, error) {
	atomic.AddUint64(&d.DBCounterStats.Has, 1)
	return d.Database.Has(bucket, key)
}
func (d *RWCounterDecorator) Walk(bucket string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	atomic.AddUint64(&d.DBCounterStats.Walk, 1)
	return d.Database.Walk(bucket, startkey, fixedbits, walker)
}
func (d *RWCounterDecorator) MultiWalk(bucket string, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {
	atomic.AddUint64(&d.DBCounterStats.MultiWalk, 1)
	return d.Database.MultiWalk(bucket, startkeys, fixedbits, walker)
}
func (d *RWCounterDecorator) Delete(bucket string, key []byte) error {
	atomic.AddUint64(&d.DBCounterStats.Delete, 1)
	return d.Database.Delete(bucket, key)
}
func (d *RWCounterDecorator) MultiPut(tuples ...[]byte) (uint64, error) {
	atomic.AddUint64(&d.DBCounterStats.MultiPut, 1)
	return d.Database.MultiPut(tuples...)
}
func (d *RWCounterDecorator) NewBatch() DbWithPendingMutations {
	mm := &mutation{
		db:   d,
		puts: newPuts(),
	}
	return mm
}
