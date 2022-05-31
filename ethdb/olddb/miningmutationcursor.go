package olddb

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
)

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
	// we keep the index in the slice of pairs we are at.
	current      int
	isPrevFromDb bool
	// current cursor entry
	currentPair cursorentry
	// we keep the mining mutation so that we can insert new elements in db
	mutation *miningmutation
	table    string
}

func (m *miningmutationcursor) endOfNextDb() (bool, error) {
	dbCurrK, dbCurrV, _ := m.cursor.Current()
	fmt.Println()
	lastK, lastV, err := m.cursor.Last()
	if err != nil {
		return false, err
	}
	if m.table == kv.HashedStorage && len(dbCurrK) == 72 {
		dbCurrK = dbCurrK[:40]
		dbCurrV = append(dbCurrK[40:], dbCurrV...)
		fmt.Println("enddb")
		fmt.Println(common.Bytes2Hex(dbCurrK))
		fmt.Println(common.Bytes2Hex(dbCurrV))
	}

	if m.table == kv.HashedStorage && len(lastK) == 72 {
		lastK = lastK[:40]
		lastV = append(lastK[40:], lastV...)
	}
	currK, currV, _ := m.Current()
	if m.dupCursor != nil {
		_, err = m.dupCursor.SeekBothRange(dbCurrK, dbCurrV)
	} else {
		_, _, err = m.cursor.Seek(dbCurrK)
	}
	if err != nil {
		return false, err
	}

	if bytes.Compare(lastK, currK) == 0 {
		return bytes.Compare(lastV, currV) <= 0, nil
	}
	return bytes.Compare(lastK, currK) <= 0, nil
}

func (m *miningmutationcursor) convertToHashedStoraFormat(k []byte, v []byte) ([]byte, []byte, error) {
	if len(k) == 72 && m.table == kv.HashedStorage {
		return k[:40], append(k[40:], v...), nil
	}
	return k, v, nil
}

func (m miningmutationcursor) isDupsortedEnabled() bool {
	return m.dupCursor != nil
}

// First move cursor to first position and return key and value accordingly.
func (m *miningmutationcursor) First() ([]byte, []byte, error) {
	m.current = 0

	if m.cursor == nil {
		return m.goForward(nil, nil)
	}

	dbKey, dbValue, err := m.cursor.First()
	if err != nil {
		return nil, nil, err
	}

	return m.goForward(dbKey, dbValue)
}

// Current return the current key and values the cursor is on.
func (m *miningmutationcursor) Current() ([]byte, []byte, error) {
	return common.CopyBytes(m.currentPair.key), common.CopyBytes(m.currentPair.value), nil
}

func (m *miningmutationcursor) goForward(dbKey, dbValue []byte) ([]byte, []byte, error) {
	// is this db less than memory?
	if m.current > m.pairs.Len()-1 {
		m.isPrevFromDb = true
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}
	var err error
	if !m.isDupsortedEnabled() && bytes.Compare(dbKey, m.pairs[m.current].key) == 0 {
		if dbKey, dbValue, err = m.cursor.Next(); err != nil {
			return nil, nil, err
		}
	} else if m.isDupsortedEnabled() && bytes.Compare(dbValue, m.pairs[m.current].value) == 0 {
		if dbKey, dbValue, err = m.dupCursor.NextDup(); err != nil {
			return nil, nil, err
		}
	} else if m.table == kv.HashedStorage && len(dbKey) == 40 && len(dbValue) >= 32 &&
		bytes.Compare(append(dbKey, dbValue[:32]...), append(m.pairs[m.current].key, m.pairs[m.current].value[:32]...)) == 0 {
		if dbKey, dbValue, err = m.dupCursor.NextDup(); err != nil {
			return nil, nil, err
		}
	}

	if dbKey != nil && dbValue != nil && compareEntries(cursorentry{dbKey, dbValue}, m.pairs[m.current]) {
		m.isPrevFromDb = true
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}
	m.isPrevFromDb = false
	m.currentPair = cursorentry{common.CopyBytes(m.pairs[m.current].key), common.CopyBytes(m.pairs[m.current].value)}
	// return current
	return common.CopyBytes(m.pairs[m.current].key), common.CopyBytes(m.pairs[m.current].value), nil
}

func (m *miningmutationcursor) goNext(nextDup bool) ([]byte, []byte, error) {
	if m.cursor == nil {
		return m.goForward(nil, nil)
	}
	var err error
	if m.pairs.Len()-1 < m.current {
		if m.table == kv.HashedStorage {
			fmt.Println("almost dead 2")
		}
		var nextK, nextV []byte
		if m.isPrevFromDb {
			if nextDup {
				nextK, nextV, err = m.dupCursor.NextDup()
			} else {
				nextK, nextV, err = m.cursor.Next()
			}
		} else {
			nextK, nextV, err = m.cursor.Current()
			if m.table == kv.HashedStorage && len(nextK) == 72 {
				nextK = nextK[:40]
				nextV = append(nextK[40:], nextV...)
			}
		}
		if err != nil {
			return nil, nil, err
		}

		if nextK != nil {
			m.currentPair = cursorentry{nextK, nextV}
		}
		m.isPrevFromDb = true
		return nextK, nextV, nil
	}

	isEndDb, err := m.endOfNextDb()
	if err != nil {
		return nil, nil, err
	}
	if isEndDb {
		if !m.isPrevFromDb {
			m.current++
		}
		m.isPrevFromDb = false

		if m.current > m.pairs.Len()-1 {
			return nil, nil, nil
		}
		m.currentPair = cursorentry{common.CopyBytes(m.pairs[m.current].key), common.CopyBytes(m.pairs[m.current].value)}
		return common.CopyBytes(m.pairs[m.current].key), common.CopyBytes(m.pairs[m.current].value), nil
	}

	if m.isPrevFromDb {
		var dbKey, dbValue []byte
		// we check current of memory against next in db
		if nextDup {
			dbKey, dbValue, err = m.dupCursor.NextDup()
		} else {
			dbKey, dbValue, err = m.cursor.Next()
		}
		if err != nil {
			return nil, nil, err
		}
		return m.goForward(dbKey, dbValue)
	}
	// We check current of memory, against next in db
	var dbKey, dbValue []byte
	if nextDup {
		dbKey, dbValue, err = m.dupCursor.Current()
	} else {
		dbKey, dbValue, err = m.cursor.Current()
	}

	if err != nil {
		return nil, nil, err
	}

	m.current++
	if m.table == kv.HashedStorage && len(dbKey) == 72 {
		return m.goForward(common.CopyBytes(dbKey[:40]), append(dbKey[40:], dbValue...))
	}
	return m.goForward(dbKey, dbValue)
}

// Next returns the next element of the mutation.
func (m *miningmutationcursor) Next() ([]byte, []byte, error) {
	return m.goNext(false)
}

// NextDup returns the next dupsorted element of the mutation (We do not apply recovery when ending of nextDup)
func (m *miningmutationcursor) NextDup() ([]byte, []byte, error) {
	currK := m.currentPair.key
	nextK, nextV, err := m.goNext(true)
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
	var dbKey, dbValue []byte
	var err error
	if m.cursor != nil {
		dbKey, dbValue, err = m.cursor.Seek(seek)
		if err != nil {
			return nil, nil, err
		}
	}

	// TODO(Giulio2002): Use Golang search
	for i := range m.pairs {
		if len(m.pairs[i].key) >= len(seek) && bytes.Compare(m.pairs[i].key[:len(seek)], seek) >= 0 {
			m.current = i
			return m.goForward(dbKey, dbValue)
		}
	}
	m.current = len(m.pairs)
	m.isPrevFromDb = true
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

	if current >= 0 {
		m.current = current
		dbKey, dbValue, err := m.cursor.Seek(seek)
		if err != nil {
			return nil, nil, err
		}
		return m.goForward(dbKey, dbValue)
	}

	dbKey, dbValue, err := m.cursor.SeekExact(seek)
	if err != nil {
		return nil, nil, err
	}
	if dbKey != nil {
		m.currentPair = cursorentry{dbKey, dbValue}
		m.current = len(m.pairs)
	}
	m.isPrevFromDb = true
	return dbKey, dbValue, err
}

func (m *miningmutationcursor) Put(k, v []byte) error {
	return m.mutation.Put(m.table, k, v)
}

func (m *miningmutationcursor) Append(k []byte, v []byte) error {
	return m.Put(k, v)
}

func (m *miningmutationcursor) AppendDup(k []byte, v []byte) error {
	return m.Put(k, v)
}

func (m *miningmutationcursor) PutNoDupData(key, value []byte) error {
	panic("DeleteCurrentDuplicates Not implemented")
}

func (m *miningmutationcursor) Delete(k, v []byte) error {
	return m.mutation.Delete(m.table, k, v)
}

func (m *miningmutationcursor) DeleteCurrent() error {
	panic("DeleteCurrent Not implemented")
}

func (m *miningmutationcursor) DeleteCurrentDuplicates() error {
	panic("DeleteCurrentDuplicates Not implemented")
}

// Seek move pointer to a key at a certain position.
func (m *miningmutationcursor) SeekBothRange(key, value []byte) ([]byte, error) {
	if value == nil {
		k, v, err := m.SeekExact(key)
		m.currentPair = cursorentry{common.CopyBytes(k), common.CopyBytes(v)}
		m.isPrevFromDb = true
		return v, err
	}
	dbValue, err := m.dupCursor.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}
	// TODO(Giulio2002): Use Golang search
	for i := range m.pairs {
		if bytes.Compare(m.pairs[i].key, key) == 0 && len(m.pairs[i].value) >= len(value) && bytes.Compare(m.pairs[i].value[:len(value)], value) >= 0 {
			m.current = i
			_, retValue, err := m.goForward(key, dbValue)
			return retValue, err
		}
	}
	m.currentPair = cursorentry{common.CopyBytes(key), common.CopyBytes(dbValue)}
	m.current = len(m.pairs)
	m.isPrevFromDb = true
	return dbValue, nil
}

func (m *miningmutationcursor) Last() ([]byte, []byte, error) {
	m.current = len(m.pairs) - 1
	if m.cursor == nil {
		if m.current == -1 {
			return nil, nil, nil
		}
		return m.goForward(nil, nil)
	}
	dbKey, dbValue, err := m.cursor.Last()
	if err != nil {
		return nil, nil, err
	}

	if m.current == -1 {
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}

	return m.goForward(dbKey, dbValue)
}

func (m *miningmutationcursor) Prev() ([]byte, []byte, error) {
	panic("Prev is not implemented!")
}

func (m *miningmutationcursor) Close() {
	if m.cursor != nil {
		m.cursor.Close()
	}
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
