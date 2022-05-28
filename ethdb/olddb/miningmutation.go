package olddb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
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
func (m *miningmutation) RwCursorDupSort(bucket string) (kv.RwCursorDupSort, error) {
	c := &miningmutationcursor{}
	c.dupsortedEntries = make(map[string]struct{})
	c.table = bucket
	var err error
	if bucket != kv.HashedStorage {
		keyMap := make(map[string]bool)
		for key, value := range m.puts[bucket] {
			if _, ok := keyMap[string(key)]; !ok {
				c.pairs = append(c.pairs, cursorentry{[]byte(key), value})
			}
			keyMap[string(key)] = true
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
				if _, ok := c.dupsortedEntries[string(append([]byte(key), value...))]; dbKey == nil && !ok {
					c.pairs = append(c.pairs, cursorentry{[]byte(key), value})
				}
				c.dupsortedEntries[string(append([]byte(key), value...))] = struct{}{}
			}
		}
	}
	sort.Sort(c.pairs)
	c.mutation = m
	return c, err
}

// Cursor creates a new cursor (the real fun begins here)
func (m *miningmutation) RwCursor(bucket string) (kv.RwCursor, error) {
	return m.RwCursorDupSort(bucket)
}

// ViewID creates a new cursor (the real fun begins here)
func (m *miningmutation) ViewID() uint64 {
	panic("ViewID Not implemented")
}

// Cursor creates a new cursor (the real fun begins here)
func (m *miningmutation) CursorDupSort(bucket string) (kv.CursorDupSort, error) {
	return m.RwCursorDupSort(bucket)
}

// Cursor creates a new cursor (the real fun begins here)
func (m *miningmutation) Cursor(bucket string) (kv.Cursor, error) {
	return m.RwCursorDupSort(bucket)
}

// entry for the cursor
type cursorentry struct {
	key   []byte
	value []byte
}

func compareEntries(a, b cursorentry) bool {
	if bytes.Compare(a.key, b.key) == 0 {
		return bytes.Compare(a.value, b.value) < 0
	}
	return bytes.Compare(a.key, b.key) < 0
}

type cursorentries []cursorentry

func (cur cursorentries) Less(i, j int) bool {
	return compareEntries(cur[i], cur[j])
}

func (cur cursorentries) Len() int {
	return len(cur)
}

func (cur cursorentries) Swap(i, j int) {
	cur[j], cur[i] = cur[i], cur[j]
}

// cursor
type miningmutationcursor struct {
	// sorted slice of the entries in the bucket we iterate on.
	pairs cursorentries

	// we can keep one cursor type if we store 2 of each kind.
	cursor    kv.Cursor
	dupCursor kv.CursorDupSort
	// We use a map to keep track of which elements we got for dupsorted table
	dupsortedEntries map[string]struct{}
	// we keep the index in the slice of pairs we are at.
	current int
	// current cursor entry
	currentPair cursorentry
	// we keep the mining mutation so that we can insert new elements in db
	mutation *miningmutation
	table    string
}

func (m *miningmutationcursor) endOfNextDb() (bool, error) {
	dbCurrK, dbCurrV, err := m.cursor.Current()
	if err != nil {
		return false, err
	}

	lastK, lastV, err := m.cursor.Last()
	if err != nil {
		return false, err
	}

	currK, currV, _ := m.Current()

	if m.dupCursor != nil {
		_, err = m.dupCursor.SeekBothRange(dbCurrK, dbCurrV)
	} else {
		_, _, err = m.cursor.Seek(dbCurrK)
	}
	if bytes.Compare(lastK, currK) == 0 {
		return bytes.Compare(lastV, currV) <= 0, nil
	}
	return bytes.Compare(lastK, currK) <= 0, nil
}

func (m miningmutationcursor) isDupsortedEnabled() bool {
	return m.dupCursor != nil
}

// First move cursor to first position and return key and value accordingly.
func (m *miningmutationcursor) First() ([]byte, []byte, error) {
	if m.pairs.Len() == 0 {
		m.current = 0
		return m.cursor.First()
	}

	dbKey, dbValue, err := m.cursor.First()
	if err != nil {
		return nil, nil, err
	}

	m.current = 0
	// is this db less than memory?
	if (compareEntries(cursorentry{dbKey, dbValue}, m.pairs[0])) {
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}

	if !m.isDupsortedEnabled() && bytes.Compare(dbKey, m.pairs[0].key) == 0 {
		if _, _, err := m.cursor.Next(); err != nil {
			return nil, nil, err
		}
	}

	m.currentPair = cursorentry{m.pairs[0].key, m.pairs[0].value}
	return m.pairs[0].key, m.pairs[0].value, nil
}

// Current return the current key and values the cursor is on.
func (m *miningmutationcursor) Current() ([]byte, []byte, error) {
	return m.currentPair.key, m.currentPair.value, nil
}

// isPointingOnDb checks if the cursor is pointing on the db cursor or on the memory slice.
func (m *miningmutationcursor) isPointingOnDb() (bool, error) {
	dbKey, dbValue, err := m.cursor.Current()
	if err != nil {
		return false, err
	}
	// if current pointer in memory is lesser than pointer in db then we are in memory and viceversa
	return compareEntries(cursorentry{dbKey, dbValue}, m.pairs[m.current]), nil
}

// Next returns the next element of the mutation.
func (m *miningmutationcursor) Next() ([]byte, []byte, error) {
	if m.pairs.Len()-1 < m.current {
		nextK, nextV, err := m.cursor.Next()
		if err != nil {
			return nil, nil, err
		}

		if nextK != nil {
			m.currentPair = cursorentry{nextK, nextV}
		}
		return nextK, nextV, nil
	}

	isEndDb, err := m.endOfNextDb()
	if err != nil {
		return nil, nil, err
	}

	if isEndDb {
		if m.current+1 > m.pairs.Len()-1 {
			return nil, nil, nil
		}
		m.current++
		m.currentPair = cursorentry{m.pairs[m.current].key, m.pairs[m.current].value}
		return m.pairs[m.current].key, m.pairs[m.current].value, nil
	}

	isOnDb, err := m.isPointingOnDb()
	if err != nil {
		return nil, nil, err
	}
	if isOnDb {
		// we check current of memory against next in db
		dbKey, dbValue, err := m.cursor.Next()
		if err != nil {
			return nil, nil, err
		}
		// is this db less than memory?
		if (dbKey != nil && compareEntries(cursorentry{dbKey, dbValue}, m.pairs[m.current])) {
			m.currentPair = cursorentry{dbKey, dbValue}
			return dbKey, dbValue, nil
		}

		if !m.isDupsortedEnabled() && bytes.Compare(dbKey, m.pairs[m.current].key) == 0 {
			if _, _, err := m.cursor.Next(); err != nil {
				return nil, nil, err
			}
		}
		m.currentPair = cursorentry{m.pairs[m.current].key, m.pairs[m.current].value}
		// return current
		return m.pairs[m.current].key, m.pairs[m.current].value, nil
	}
	// We check current of memory, against next in db
	dbKey, dbValue, err := m.cursor.Current()
	if err != nil {
		return nil, nil, err
	}

	m.current++
	// is this db less than memory?
	if (m.current > m.pairs.Len()-1) || (dbKey != nil && compareEntries(cursorentry{dbKey, dbValue}, m.pairs[m.current])) {
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}
	if !m.isDupsortedEnabled() && bytes.Compare(dbKey, m.pairs[m.current].key) == 0 {
		if _, _, err := m.cursor.Next(); err != nil {
			return nil, nil, err
		}
	}
	m.currentPair = cursorentry{m.pairs[m.current].key, m.pairs[m.current].value}
	return m.pairs[m.current].key, m.pairs[m.current].value, nil
}

// NextDup returns the next dupsorted element of the mutation (We do not apply recovery when ending of nextDup)
func (m *miningmutationcursor) NextDup() ([]byte, []byte, error) {
	currK, _, _ := m.Current()

	nextK, nextV, err := m.Next()
	if err != nil {
		return nil, nil, err
	}

	if bytes.Compare(currK, nextK) != 0 {
		return nil, nil, nil
	}
	return nextK, nextV, nil
}

// Seek move pointer to a key at a certain position.
func (m *miningmutationcursor) Seek(seek []byte) ([]byte, []byte, error) {
	dbKey, dbValue, err := m.cursor.Seek(seek)
	if err != nil {
		return nil, nil, err
	}
	m.current = 0
	// TODO(Giulio2002): Use Golang search
	for _, pair := range m.pairs {
		if len(pair.key) >= len(seek) && bytes.Compare(pair.key[:len(seek)], seek) >= 0 {
			if (dbKey != nil && compareEntries(cursorentry{dbKey, dbValue}, pair)) {
				m.currentPair = cursorentry{dbKey, dbValue}
				return dbKey, dbValue, nil
			}

			if !m.isDupsortedEnabled() && bytes.Compare(dbKey, pair.key) == 0 {
				if _, _, err := m.cursor.Next(); err != nil {
					return nil, nil, err
				}
			}
			m.currentPair = cursorentry{pair.key, pair.value}
			// return current
			return pair.key, pair.value, nil
		}
		m.current++
	}

	return dbKey, dbValue, nil
}

// Seek move pointer to a key at a certain position.
func (m *miningmutationcursor) SeekExact(seek []byte) ([]byte, []byte, error) {
	current := -1
	for i, pair := range m.pairs {
		if bytes.Compare(pair.key, seek) == 0 {
			current = i
			break
		}
	}
	fmt.Println(current)

	if current >= 0 {
		m.current = current
		dbKey, dbValue, err := m.cursor.Seek(seek)
		if err != nil {
			return nil, nil, err
		}
		if (dbKey != nil && compareEntries(cursorentry{dbKey, dbValue}, m.pairs[m.current])) {
			m.currentPair = cursorentry{dbKey, dbValue}
			return dbKey, dbValue, nil
		}
		m.currentPair = cursorentry{m.pairs[m.current].key, m.pairs[m.current].value}
		return m.pairs[m.current].key, m.pairs[m.current].value, nil
	}

	dbKey, dbValue, err := m.cursor.SeekExact(seek)
	if err != nil {
		return nil, nil, err
	}
	if dbKey != nil {
		m.currentPair = cursorentry{dbKey, dbValue}
		m.current = len(m.pairs)
	}
	return dbKey, dbValue, err
}

func (m *miningmutationcursor) Put(k, v []byte) error {
	return m.mutation.Put(m.table, k, v)
}

func (m *miningmutationcursor) Append(k []byte, v []byte) error {
	return m.mutation.Put(m.table, k, v)
}

func (m *miningmutationcursor) AppendDup(k []byte, v []byte) error {
	return m.mutation.Put(m.table, k, v)
}

func (m *miningmutationcursor) PutNoDupData(key, value []byte) error {
	panic("DeleteCurrentDuplicates Not implemented")
}

func (m *miningmutationcursor) Delete(k, v []byte) error {
	panic("Delete Not implemented")
}

func (m *miningmutationcursor) DeleteCurrent() error {
	panic("DeleteCurrent Not implemented")
}

func (m *miningmutationcursor) DeleteCurrentDuplicates() error {
	panic("DeleteCurrentDuplicates Not implemented")
}

func (m *miningmutationcursor) SeekBothRange(key, value []byte) ([]byte, error) {
	panic("SeekBothRange Not implemented")
}

func (m *miningmutationcursor) Last() ([]byte, []byte, error) {
	m.current = len(m.pairs)
	dbKey, dbValue, err := m.cursor.Last()
	if err != nil {
		return nil, nil, err
	}

	if m.current == 0 {
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}

	if (compareEntries(cursorentry{dbKey, dbValue}, m.pairs[m.current-1])) {
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}
	m.current--
	m.currentPair = cursorentry{m.pairs[m.current].key, m.pairs[m.current].value}
	return m.pairs[m.current].key, m.pairs[m.current].value, nil
}

func (m *miningmutationcursor) Prev() ([]byte, []byte, error) {
	panic("Prev is not implemented!")
}

func (m *miningmutationcursor) Close() {
	return
}

func (m *miningmutationcursor) Count() (uint64, error) {
	panic("Not implemented")
}

func (m *miningmutationcursor) FirstDup() ([]byte, error) {
	panic("Not implemented")
}

func (m *miningmutationcursor) NextNoDup() ([]byte, []byte, error) {
	panic("Not implemented")
}

func (m *miningmutationcursor) LastDup() ([]byte, error) {
	panic("Not implemented")
}

func (m *miningmutationcursor) CountDuplicates() (uint64, error) {
	panic("Not implemented")
}

func (m *miningmutationcursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	panic("SeekBothExact Not implemented")
}
