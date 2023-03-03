package olddb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/google/btree"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/log/v3"
)

type mutation struct {
	puts       *btree.BTree
	db         kv.RwTx
	quit       <-chan struct{}
	clean      func()
	searchItem MutationItem
	mu         sync.RWMutex
	size       int
}

type MutationItem struct {
	table string
	key   []byte
	value []byte
}

// NewBatch - starts in-mem batch
//
// Common pattern:
//
// batch := db.NewBatch()
// defer batch.Rollback()
// ... some calculations on `batch`
// batch.Commit()
func NewBatch(tx kv.RwTx, quit <-chan struct{}) *mutation {
	clean := func() {}
	if quit == nil {
		ch := make(chan struct{})
		clean = func() { close(ch) }
		quit = ch
	}
	return &mutation{
		db:    tx,
		puts:  btree.New(32),
		quit:  quit,
		clean: clean,
	}
}

func (mi *MutationItem) Less(than btree.Item) bool {
	i, ok := than.(*MutationItem)
	if !ok {
		log.Warn("Failed to convert btree.Item to MutationItem pointer")
	}
	c := strings.Compare(mi.table, i.table)
	if c != 0 {
		return c < 0
	}
	return bytes.Compare(mi.key, i.key) < 0
}

func (m *mutation) ReadOnly() bool { return false }
func (m *mutation) RwKV() kv.RwDB {
	if casted, ok := m.db.(ethdb.HasRwKV); ok {
		return casted.RwKV()
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

func (m *mutation) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}

	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	newVBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(newVBytes, currentV+amount)
	if err = m.Put(kv.Sequence, []byte(bucket), newVBytes); err != nil {
		return 0, err
	}

	return currentV, nil
}
func (m *mutation) ReadSequence(bucket string) (res uint64, err error) {
	v, ok := m.getMem(kv.Sequence, []byte(bucket))
	if !ok && m.db != nil {
		v, err = m.db.GetOne(kv.Sequence, []byte(bucket))
		if err != nil {
			return 0, err
		}
	}
	var currentV uint64 = 0
	if len(v) > 0 {
		currentV = binary.BigEndian.Uint64(v)
	}

	return currentV, nil
}

// Can only be called from the worker thread
func (m *mutation) GetOne(table string, key []byte) ([]byte, error) {
	if value, ok := m.getMem(table, key); ok {
		if value == nil {
			return nil, nil
		}
		return value, nil
	}
	if m.db != nil {
		// TODO: simplify when tx can no longer be parent of mutation
		value, err := m.db.GetOne(table, key)
		if err != nil {
			return nil, err
		}

		return value, nil
	}
	return nil, nil
}

// Can only be called from the worker thread
func (m *mutation) Get(table string, key []byte) ([]byte, error) {
	value, err := m.GetOne(table, key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, ethdb.ErrKeyNotFound
	}

	return value, nil
}

func (m *mutation) Last(table string) ([]byte, []byte, error) {
	c, err := m.db.Cursor(table)
	if err != nil {
		return nil, nil, err
	}
	defer c.Close()
	return c.Last()
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

func (m *mutation) Put(table string, k, v []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	newMi := &MutationItem{table: table, key: k, value: v}
	i := m.puts.ReplaceOrInsert(newMi)
	m.size += int(unsafe.Sizeof(newMi)) + len(k) + len(v)
	if i != nil {
		oldMi := i.(*MutationItem)
		m.size -= int(unsafe.Sizeof(oldMi)) + len(oldMi.key) + len(oldMi.value)
	}
	return nil
}

func (m *mutation) Append(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *mutation) AppendDup(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *mutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

func (m *mutation) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForEach(bucket, fromPrefix, walker)
}

func (m *mutation) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForPrefix(bucket, prefix, walker)
}

func (m *mutation) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForAmount(bucket, prefix, amount, walker)
}

func (m *mutation) Delete(table string, k []byte) error {
	//m.puts.Delete(table, k)
	return m.Put(table, k, nil)
}

func (m *mutation) doCommit(tx kv.RwTx) error {
	var prevTable string
	var c kv.RwCursor
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
			var err error
			c, err = tx.RwCursor(mi.table)
			if err != nil {
				innerErr = err
				return false
			}
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
			if err := c.Delete(mi.key); err != nil {
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
			tx.CollectMetrics()
		case <-m.quit:
			innerErr = common.ErrStopped
			return false
		}
		return true
	})
	tx.CollectMetrics()
	return innerErr
}

func (m *mutation) Commit() error {
	if m.db == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.doCommit(m.db); err != nil {
		return err
	}

	m.puts.Clear(false /* addNodesToFreelist */)
	m.size = 0
	m.clean()
	return nil
}

func (m *mutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts.Clear(false /* addNodesToFreelist */)
	m.size = 0
	m.clean()
}

func (m *mutation) Close() {
	m.Rollback()
}

func (m *mutation) Begin(ctx context.Context, flags ethdb.TxFlags) (ethdb.DbWithPendingMutations, error) {
	panic("mutation can't start transaction, because doesn't own it")
}

func (m *mutation) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

func (m *mutation) SetRwKV(kv kv.RwDB) {
	m.db.(ethdb.HasRwKV).SetRwKV(kv)
}
