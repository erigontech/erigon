package ethdb

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/google/btree"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/metrics"
)

var (
	dbCommitBigBatchTimer   = metrics.NewRegisteredTimer("db/commit/big_batch", nil)
	dbCommitSmallBatchTimer = metrics.NewRegisteredTimer("db/commit/small_batch", nil)
)

type mutation struct {
	puts       *btree.BTree
	mu         sync.RWMutex
	searchItem MutationItem
	size       int
	db         Database
}

type MutationItem struct {
	table string
	key   []byte
	value []byte
}

func (mi *MutationItem) Less(than btree.Item) bool {
	i := than.(*MutationItem)
	c := strings.Compare(mi.table, i.table)
	if c != 0 {
		return c < 0
	}
	return bytes.Compare(mi.key, i.key) < 0
}

func (m *mutation) KV() KV {
	if casted, ok := m.db.(HasKV); ok {
		return casted.KV()
	}
	return nil
}

func (m *mutation) getMem(table string, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.searchItem.table = table
	m.searchItem.key = key
	i := m.puts.Get(&m.searchItem)
	if i == nil {
		return nil, false
	}
	return i.(*MutationItem).value, true
}

func (m *mutation) Sequence(bucket string, amount uint64) (res uint64, err error) {
	return m.db.Sequence(bucket, amount)
}

// Can only be called from the worker thread
func (m *mutation) Get(table string, key []byte) ([]byte, error) {
	if value, ok := m.getMem(table, key); ok {
		if value == nil {
			return nil, ErrKeyNotFound
		}
		return value, nil
	}
	if m.db != nil {
		return m.db.Get(table, key)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) Last(table string) ([]byte, []byte, error) {
	return m.db.Last(table)
}

func (m *mutation) Reserve(table string, key []byte, i int) ([]byte, error) {
	return m.db.(DbWithPendingMutations).Reserve(table, key, i)
}

func (m *mutation) GetIndexChunk(table string, key []byte, timestamp uint64) ([]byte, error) {
	if m.db != nil {
		return m.db.GetIndexChunk(table, key, timestamp)
	}
	return nil, ErrKeyNotFound
}

func (m *mutation) hasMem(table string, key []byte) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	m.searchItem.table = table
	m.searchItem.key = key
	return m.puts.Has(&m.searchItem)
}

func (m *mutation) Has(table string, key []byte) (bool, error) {
	if m.hasMem(table, key) {
		return true, nil
	}
	if m.db != nil {
		return m.db.Has(table, key)
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

func (m *mutation) Put(table string, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	newMi := &MutationItem{table: table, key: key, value: value}
	i := m.puts.ReplaceOrInsert(newMi)
	m.size += int(unsafe.Sizeof(newMi)) + len(key) + len(value)
	if i != nil {
		oldMi := i.(*MutationItem)
		m.size -= (int(unsafe.Sizeof(oldMi)) + len(oldMi.key) + len(oldMi.value))
	}
	return nil
}

func (m *mutation) Append(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *mutation) MultiPut(tuples ...[]byte) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l := len(tuples)
	for i := 0; i < l; i += 3 {
		newMi := &MutationItem{table: string(tuples[i]), key: tuples[i+1], value: tuples[i+2]}
		i := m.puts.ReplaceOrInsert(newMi)
		m.size += int(unsafe.Sizeof(newMi)) + len(newMi.key) + len(newMi.value)
		if i != nil {
			oldMi := i.(*MutationItem)
			m.size -= (int(unsafe.Sizeof(oldMi)) + len(oldMi.key) + len(oldMi.value))
		}
	}
	return 0, nil
}

func (m *mutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// IdealBatchSize defines the size of the data batches should ideally add in one write.
func (m *mutation) IdealBatchSize() int {
	return int(512 * datasize.MB)
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) Walk(table string, startkey []byte, fixedbits int, walker func([]byte, []byte) (bool, error)) error {
	m.panicOnEmptyDB()
	return m.db.Walk(table, startkey, fixedbits, walker)
}

// WARNING: Merged mem/DB walk is not implemented
func (m *mutation) MultiWalk(table string, startkeys [][]byte, fixedbits []int, walker func(int, []byte, []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.MultiWalk(table, startkeys, fixedbits, walker)
}

func (m *mutation) Delete(table string, k, v []byte) error {
	if v != nil {
		return fmt.Errorf("mutation doesn't implement dupsort values deletion yet")
	}
	//m.puts.Delete(table, k)
	return m.Put(table, k, nil)
}

func (m *mutation) CommitAndBegin(ctx context.Context) error {
	_, err := m.Commit()
	return err
}

func (m *mutation) RollbackAndBegin(ctx context.Context) error {
	m.Rollback()
	return nil
}

func (m *mutation) doCommit(tx Tx) error {
	var prevTable string
	var c Cursor
	var innerErr error
	var isEndOfBucket bool
	logEvery := time.NewTicker(30 * time.Second)
	defer logEvery.Stop()
	count := 0
	total := float64(m.puts.Len())

	m.puts.Ascend(func(i btree.Item) bool {
		mi := i.(*MutationItem)
		if mi.table != prevTable {
			if c != nil {
				c.Close()
			}
			c = tx.Cursor(mi.table)
			prevTable = mi.table
			firstKey, _, err := c.Seek(mi.key)
			if err != nil {
				innerErr = err
				return false
			}
			isEndOfBucket = firstKey == nil
		}
		if isEndOfBucket {
			if len(mi.value) > 0 {
				if err := c.Append(mi.key, mi.value); err != nil {
					innerErr = err
					return false
				}
			}
		} else if len(mi.value) == 0 {
			if err := c.Delete(mi.key, nil); err != nil {
				innerErr = err
				return false
			}
		} else {
			if err := c.Put(mi.key, mi.value); err != nil {
				innerErr = err
				return false
			}
		}

		count++

		select {
		default:
		case <-logEvery.C:
			progress := fmt.Sprintf("%.1fM/%.1fM", float64(count)/1_000_000, total/1_000_000)
			log.Info("Write to db", "progress", progress, "current table", mi.table)
		}
		return true
	})
	return innerErr
}

func (m *mutation) Commit() (uint64, error) {
	if m.db == nil {
		return 0, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if tx, ok := m.db.(HasTx); ok {
		if err := m.doCommit(tx.Tx()); err != nil {
			return 0, err
		}
	} else {
		if err := m.db.(HasKV).KV().Update(context.Background(), func(tx Tx) error {
			return m.doCommit(tx)
		}); err != nil {
			return 0, err
		}
	}

	m.puts.Clear(false /* addNodesToFreelist */)
	m.size = 0
	return 0, nil
}

func (m *mutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts.Clear(false /* addNodesToFreelist */)
	m.size = 0
}

func (m *mutation) Keys() ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	tuples := common.NewTuples(m.puts.Len(), 2, 1)
	var innerErr error
	m.puts.Ascend(func(i btree.Item) bool {
		mi := i.(*MutationItem)
		if err := tuples.Append([]byte(mi.table), mi.key); err != nil {
			innerErr = err
			return false
		}
		return true
	})
	return tuples.Values, innerErr
}

func (m *mutation) Close() {
	m.Rollback()
}

func (m *mutation) NewBatch() DbWithPendingMutations {
	mm := &mutation{
		db:   m,
		puts: btree.New(32),
	}
	return mm
}

func (m *mutation) Begin(ctx context.Context, flags TxFlags) (DbWithPendingMutations, error) {
	return m.db.Begin(ctx, flags)
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

func (m *mutation) SetKV(kv KV) {
	m.db.(HasKV).SetKV(kv)
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
func (d *RWCounterDecorator) Delete(bucket string, k, v []byte) error {
	atomic.AddUint64(&d.DBCounterStats.Delete, 1)
	return d.Database.Delete(bucket, k, v)
}
func (d *RWCounterDecorator) MultiPut(tuples ...[]byte) (uint64, error) {
	atomic.AddUint64(&d.DBCounterStats.MultiPut, 1)
	return d.Database.MultiPut(tuples...)
}
func (d *RWCounterDecorator) NewBatch() DbWithPendingMutations {
	mm := &mutation{
		db:   d,
		puts: btree.New(32),
	}
	return mm
}
