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

// cursor
type miningmutationcursor struct {
	// we can keep one cursor type if we store 2 of each kind.
	cursor    kv.Cursor
	dupCursor kv.CursorDupSort
	// Mem cursors
	memCursor    kv.RwCursor
	memDupCursor kv.RwCursorDupSort
	// we keep the index in the slice of pairs we are at.
	isPrevFromDb bool
	// Flag for dupsort mode
	isDupsort bool
	// entry history
	currentPair     cursorentry
	currentDbEntry  cursorentry
	currentMemEntry cursorentry
	// we keep the mining mutation so that we can insert new elements in db
	mutation *miningmutation
	table    string
}

// First move cursor to first position and return key and value accordingly.
func (m *miningmutationcursor) First() ([]byte, []byte, error) {
	memKey, memValue, err := m.memCursor.First()
	if err != nil {
		return nil, nil, err
	}

	dbKey, dbValue, err := m.cursor.First()
	if err != nil {
		return nil, nil, err
	}

	return m.goForward(memKey, memValue, dbKey, dbValue)
}

// Current return the current key and values the cursor is on.
func (m *miningmutationcursor) Current() ([]byte, []byte, error) {
	return common.CopyBytes(m.currentPair.key), common.CopyBytes(m.currentPair.value), nil
}

func (m *miningmutationcursor) goForward(memKey, memValue, dbKey, dbValue []byte) ([]byte, []byte, error) {
	var err error
	if memValue == nil && dbValue == nil {
		return nil, nil, nil
	}
	// Check for duplicates
	if bytes.Compare(memKey, dbKey) == 0 {
		if !m.isDupsort {
			if dbKey, dbValue, err = m.cursor.Next(); err != nil {
				return nil, nil, err
			}
		} else if bytes.Compare(memValue, dbValue) == 0 {
			if dbKey, dbValue, err = m.dupCursor.NextDup(); err != nil {
				return nil, nil, err
			}
		} else if len(memValue) >= 32 && len(dbValue) >= 32 && m.table == kv.HashedStorage && bytes.Compare(memValue[:32], dbValue[:32]) == 0 {
			if dbKey, dbValue, err = m.dupCursor.NextDup(); err != nil {
				return nil, nil, err
			}
		}
	}
	m.currentDbEntry = cursorentry{dbKey, dbValue}
	m.currentMemEntry = cursorentry{memKey, memValue}
	// compare entries
	if bytes.Compare(memKey, dbKey) == 0 {
		m.isPrevFromDb = dbValue != nil && (memValue == nil || bytes.Compare(memValue, dbValue) > 0)
	} else {
		m.isPrevFromDb = dbValue != nil && (memKey == nil || bytes.Compare(memKey, dbKey) > 0)
	}
	if dbValue == nil {
		m.currentDbEntry = cursorentry{}
	}
	if memValue == nil {
		m.currentMemEntry = cursorentry{}
	}
	if m.isPrevFromDb {
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}

	m.currentPair = cursorentry{memKey, memValue}
	return memKey, memValue, nil
}

// Next returns the next element of the mutation.
func (m *miningmutationcursor) Next() ([]byte, []byte, error) {
	if m.isPrevFromDb {
		k, v, err := m.cursor.Next()
		if err != nil {
			return nil, nil, err
		}
		return m.goForward(m.currentMemEntry.key, m.currentMemEntry.value, k, v)
	}

	memK, memV, err := m.memCursor.Next()
	if err != nil {
		return nil, nil, err
	}

	return m.goForward(memK, memV, m.currentDbEntry.key, m.currentDbEntry.value)
}

// NextDup returns the next element of the mutation.
func (m *miningmutationcursor) NextDup() ([]byte, []byte, error) {
	if m.isPrevFromDb {
		k, v, err := m.dupCursor.NextDup()

		if err != nil {
			return nil, nil, err
		}
		return m.goForward(m.currentMemEntry.key, m.currentMemEntry.value, k, v)
	}

	memK, memV, err := m.memDupCursor.NextDup()
	if err != nil {
		return nil, nil, err
	}

	return m.goForward(memK, memV, m.currentDbEntry.key, m.currentDbEntry.value)
}

// Seek move pointer to a key at a certain position.
func (m *miningmutationcursor) Seek(seek []byte) ([]byte, []byte, error) {
	dbKey, dbValue, err := m.cursor.Seek(seek)
	if err != nil {
		return nil, nil, err
	}

	memKey, memValue, err := m.memCursor.Seek(seek)
	if err != nil {
		return nil, nil, err
	}
	return m.goForward(memKey, memValue, dbKey, dbValue)
}

// Seek move pointer to a key at a certain position.
func (m *miningmutationcursor) SeekExact(seek []byte) ([]byte, []byte, error) {
	memKey, memValue, err := m.memCursor.SeekExact(seek)
	if err != nil {
		return nil, nil, err
	}

	if memKey != nil {
		m.currentMemEntry.key = memKey
		m.currentMemEntry.value = memValue
		m.currentDbEntry.key, m.currentDbEntry.value, err = m.cursor.Seek(seek)
		m.isPrevFromDb = false
		m.currentPair = cursorentry{memKey, memValue}
		return memKey, memValue, err
	}

	dbKey, dbValue, err := m.cursor.SeekExact(seek)
	if err != nil {
		return nil, nil, err
	}

	if dbKey != nil {
		m.currentDbEntry.key = dbKey
		m.currentDbEntry.value = dbValue
		m.currentMemEntry.key, m.currentMemEntry.value, err = m.memCursor.Seek(seek)
		m.isPrevFromDb = true
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, err
	}
	return nil, nil, nil
}

func (m *miningmutationcursor) Put(k, v []byte) error {
	return m.mutation.Put(m.table, common.CopyBytes(k), common.CopyBytes(v))
}

func (m *miningmutationcursor) Append(k []byte, v []byte) error {
	return m.mutation.Put(m.table, common.CopyBytes(k), common.CopyBytes(v))

}

func (m *miningmutationcursor) AppendDup(k []byte, v []byte) error {
	return m.memDupCursor.AppendDup(common.CopyBytes(k), common.CopyBytes(v))
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

	memValue, err := m.memDupCursor.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}
	_, retValue, err := m.goForward(key, memValue, key, dbValue)
	return retValue, err
}

func (m *miningmutationcursor) Last() ([]byte, []byte, error) {
	// TODO(Giulio2002): make fixes.
	memKey, memValue, err := m.memCursor.Last()
	if err != nil {
		return nil, nil, err
	}

	dbKey, dbValue, err := m.cursor.Last()
	if err != nil {
		return nil, nil, err
	}

	return m.goForward(memKey, memValue, dbKey, dbValue)
}

func (m *miningmutationcursor) Prev() ([]byte, []byte, error) {
	panic("Prev is not implemented!")
}

func (m *miningmutationcursor) Close() {
	if m.cursor != nil {
		m.cursor.Close()
	}
	if m.memCursor != nil {
		m.memCursor.Close()
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
