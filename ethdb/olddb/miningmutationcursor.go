package olddb

import (
	"bytes"

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
	current int
	// current cursor entry
	currentPair cursorentry
	// we keep the mining mutation so that we can insert new elements in db
	mutation *miningmutation
	table    string
}

func (m *miningmutationcursor) endOfNextDb() (bool, error) {
	dbCurrK, dbCurrV, _ := m.cursor.Current()

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
	if err != nil {
		return false, err
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
	return common.CopyBytes(m.pairs[0].key), common.CopyBytes(m.pairs[0].value), nil
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

func (m *miningmutationcursor) goForward(dbKey, dbValue []byte) ([]byte, []byte, error) {
	// is this db less than memory?
	if m.current > m.pairs.Len()-1 {
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}

	if !m.isDupsortedEnabled() && bytes.Compare(dbKey, m.pairs[m.current].key) == 0 {
		if _, _, err := m.cursor.Next(); err != nil {
			return nil, nil, err
		}
	}

	if dbKey != nil && compareEntries(cursorentry{dbKey, dbValue}, m.pairs[m.current]) {
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}

	m.currentPair = cursorentry{m.pairs[m.current].key, m.pairs[m.current].value}
	// return current
	return common.CopyBytes(m.pairs[m.current].key), common.CopyBytes(m.pairs[m.current].value), nil
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
		return common.CopyBytes(m.pairs[m.current].key), common.CopyBytes(m.pairs[m.current].value), nil
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
		return m.goForward(dbKey, dbValue)
	}
	// We check current of memory, against next in db
	dbKey, dbValue, err := m.cursor.Current()
	if err != nil {
		return nil, nil, err
	}

	m.current++
	return m.goForward(dbKey, dbValue)
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
			return m.goForward(dbKey, dbValue)
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
		_, v, err := m.SeekExact(key)
		return v, err
	}
	dbValue, err := m.dupCursor.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}
	m.current = 0
	// TODO(Giulio2002): Use Golang search
	for _, pair := range m.pairs {
		if bytes.Compare(pair.key, key) == 0 && len(pair.value) >= len(value) && bytes.Compare(pair.value[:len(value)], value) >= 0 {
			_, retValue, err := m.goForward(key, dbValue)
			return retValue, err
		}
		m.current++
	}
	return dbValue, nil
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

	m.current--
	return m.goForward(dbKey, dbValue)
}

func (m *miningmutationcursor) Prev() ([]byte, []byte, error) {
	panic("Prev is not implemented!")
}

func (m *miningmutationcursor) Close() {
	m.cursor.Close()
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
