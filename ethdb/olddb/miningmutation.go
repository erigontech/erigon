package olddb

import (
	"context"
	"encoding/binary"
	"sort"
	"sync"
	"unsafe"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/ethdb"
)

type miningmutation struct {
	puts          map[string]map[string][]byte
	hashedStorage map[string][][]byte

	db kv.Tx
	mu sync.RWMutex
}

// NewBatch - starts in-mem batch
//
// Common pattern:
//
// batch := db.NewBatch()
// defer batch.Rollback()
// ... some calculations on `batch`
// batch.Commit()
func NewMiningBatch(tx kv.Tx) *miningmutation {
	return &miningmutation{
		db:            tx,
		puts:          make(map[string]map[string][]byte),
		hashedStorage: make(map[string][][]byte),
	}
}

func (m *miningmutation) RwKV() kv.RwDB {
	if casted, ok := m.db.(ethdb.HasRwKV); ok {
		return casted.RwKV()
	}
	return nil
}

// getMem Retrieve database entry from memory (hashed storage will be left out for now because it is the only non auto-DupSorted table)
func (m *miningmutation) getMem(table string, key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if _, ok := m.puts[table]; !ok {
		return nil, false
	}
	if value, ok := m.puts[table][*(*string)(unsafe.Pointer(&key))]; ok {
		return value, ok
	}

	return nil, false
}

func (m *miningmutation) IncrementSequence(bucket string, amount uint64) (res uint64, err error) {
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

func (m *miningmutation) ReadSequence(bucket string) (res uint64, err error) {
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
func (m *miningmutation) GetOne(table string, key []byte) ([]byte, error) {
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
func (m *miningmutation) Get(table string, key []byte) ([]byte, error) {
	value, err := m.GetOne(table, key)
	if err != nil {
		return nil, err
	}

	if value == nil {
		return nil, ethdb.ErrKeyNotFound
	}

	return value, nil
}

func (m *miningmutation) Last(table string) ([]byte, []byte, error) {
	panic("not implemented. (miningmutation.Last)")
}

// Has return whether a key is present in a certain table.
func (m *miningmutation) Has(table string, key []byte) (bool, error) {
	if _, ok := m.getMem(table, key); ok {
		return ok, nil
	}
	if m.db != nil {
		return m.db.Has(table, key)
	}
	return false, nil
}

// Put insert a new entry in the database, if it is hashed storage it will add it to a slice instead of a map.
func (m *miningmutation) Put(table string, key []byte, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.puts[table]; !ok {
		m.puts[table] = make(map[string][]byte)
	}
	stringKey := *(*string)(unsafe.Pointer(&key))

	if table == kv.HashedStorage {
		if _, ok := m.hashedStorage[stringKey]; !ok {
			m.hashedStorage[stringKey] = [][]byte{}
		}
		m.hashedStorage[stringKey] = append(m.hashedStorage[stringKey], value)
		return nil
	}
	m.puts[table][stringKey] = value
	return nil
}

func (m *miningmutation) Append(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *miningmutation) AppendDup(table string, key []byte, value []byte) error {
	return m.Put(table, key, value)
}

func (m *miningmutation) BatchSize() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return 0
}

func (m *miningmutation) ForEach(bucket string, fromPrefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForEach(bucket, fromPrefix, walker)
}

func (m *miningmutation) ForPrefix(bucket string, prefix []byte, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForPrefix(bucket, prefix, walker)
}

func (m *miningmutation) ForAmount(bucket string, prefix []byte, amount uint32, walker func(k, v []byte) error) error {
	m.panicOnEmptyDB()
	return m.db.ForAmount(bucket, prefix, amount, walker)
}

func (m *miningmutation) Delete(table string, k, v []byte) error {
	return m.Put(table, k, nil)
}

func (m *miningmutation) Commit() error {
	return nil
}

func (m *miningmutation) Rollback() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts = map[string]map[string][]byte{}
}

func (m *miningmutation) Close() {
	m.Rollback()
}

func (m *miningmutation) Begin(ctx context.Context, flags ethdb.TxFlags) (ethdb.DbWithPendingMutations, error) {
	panic("mutation can't start transaction, because doesn't own it")
}

func (m *miningmutation) panicOnEmptyDB() {
	if m.db == nil {
		panic("Not implemented")
	}
}

func (m *miningmutation) SetRwKV(kv kv.RwDB) {
	m.db.(ethdb.HasRwKV).SetRwKV(kv)
}

func (m *miningmutation) BucketSize(bucket string) (uint64, error) {
	panic("Not implemented")
}

func (m *miningmutation) DropBucket(bucket string) error {
	panic("Not implemented")
}

func (m *miningmutation) ExistsBucket(bucket string) (bool, error) {
	panic("Not implemented")
}

func (m *miningmutation) ListBuckets() ([]string, error) {
	panic("Not implemented")
}

func (m *miningmutation) ClearBucket(bucket string) error {
	return nil
}

func (m *miningmutation) CollectMetrics() {
}

func (m *miningmutation) CreateBucket(bucket string) error {
	panic("Not implemented")
}

// Cursor creates a new cursor (the real fun begins here)
func (m *miningmutation) makeCursor(bucket string) (kv.RwCursorDupSort, error) {
	c := &miningmutationcursor{}
	// We can filter duplicates in dup sorted table
	filterMap := make(map[string]struct{})
	c.table = bucket
	var err error
	if bucket != kv.HashedStorage {
		for key, value := range m.puts[bucket] { 
			c.pairs = append(c.pairs, cursorentry{[]byte(key), value})
		}
		c.cursor, err = m.db.Cursor(bucket)
	} else {
		c.dupCursor, err = m.db.CursorDupSort(bucket)
		c.cursor = c.dupCursor
		for key, values := range m.hashedStorage {
			for _, value := range values {
				dbKey, _, err := c.dupCursor.SeekBothExact([]byte(key), value)
				if err != nil {
					return &miningmutationcursor{}, err
				}
				if _, ok := filterMap[string(append([]byte(key), value...))]; dbKey == nil && !ok {
					c.pairs = append(c.pairs, cursorentry{[]byte(key), value})
				}
				filterMap[string(append([]byte(key), value...))] = struct{}{}
			}
		}
	}
	sort.Sort(c.pairs)
	c.mutation = m
	return c, err
}

// Cursor creates a new cursor (the real fun begins here)
func (m *miningmutation) RwCursorDupSort(bucket string) (kv.RwCursorDupSort, error) {
	return m.makeCursor(bucket)
}

// Cursor creates a new cursor (the real fun begins here)
func (m *miningmutation) RwCursor(bucket string) (kv.RwCursor, error) {
	return m.makeCursor(bucket)
}

// Cursor creates a new cursor (the real fun begins here)
func (m *miningmutation) CursorDupSort(bucket string) (kv.CursorDupSort, error) {
	return m.makeCursor(bucket)
}

// Cursor creates a new cursor (the real fun begins here)
func (m *miningmutation) Cursor(bucket string) (kv.Cursor, error) {
	return m.makeCursor(bucket)
}

// ViewID creates a new cursor (the real fun begins here)
func (m *miningmutation) ViewID() uint64 {
	panic("ViewID Not implemented")
}
