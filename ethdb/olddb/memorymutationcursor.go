/*
   Copyright 2022 Erigon contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

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
type memorymutationcursor struct {
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
	mutation *memorymutation
	table    string
}

// First move cursor to first position and return key and value accordingly.
func (m *memorymutationcursor) First() ([]byte, []byte, error) {
	memKey, memValue, err := m.memCursor.First()
	if err != nil {
		return nil, nil, err
	}

	dbKey, dbValue, err := m.cursor.First()
	if err != nil {
		return nil, nil, err
	}

	if dbKey != nil && m.mutation.isEntryDeleted(m.table, dbKey) {
		if dbKey, dbValue, err = m.getNextOnDb(false); err != nil {
			return nil, nil, err
		}
	}

	return m.goForward(memKey, memValue, dbKey, dbValue)
}

func (m *memorymutationcursor) getNextOnDb(dup bool) (key []byte, value []byte, err error) {
	if dup {
		key, value, err = m.dupCursor.NextDup()
		if err != nil {
			return
		}
	} else {
		key, value, err = m.cursor.Next()
		if err != nil {
			return
		}
	}

	for key != nil && value != nil && m.mutation.isEntryDeleted(m.table, m.convertAutoDupsort(key, value)) {
		if dup {
			key, value, err = m.dupCursor.NextDup()
			if err != nil {
				return
			}
		} else {
			key, value, err = m.cursor.Next()
			if err != nil {
				return
			}
		}
	}
	return
}

func (m *memorymutationcursor) convertAutoDupsort(key []byte, value []byte) []byte {
	// The only dupsorted table we are interested is HashedStorage
	if m.table != kv.HashedStorage {
		return key
	}
	return append(key, value[:32]...)
}

// Current return the current key and values the cursor is on.
func (m *memorymutationcursor) Current() ([]byte, []byte, error) {
	return common.CopyBytes(m.currentPair.key), common.CopyBytes(m.currentPair.value), nil
}

func (m *memorymutationcursor) skipIntersection(memKey, memValue, dbKey, dbValue []byte) (newDbKey []byte, newDbValue []byte, err error) {
	newDbKey = dbKey
	newDbValue = dbValue
	// Check for duplicates
	if bytes.Compare(memKey, dbKey) == 0 {
		if !m.isDupsort {
			if newDbKey, newDbValue, err = m.getNextOnDb(false); err != nil {
				return
			}
		} else if bytes.Compare(memValue, dbValue) == 0 {
			if newDbKey, newDbValue, err = m.getNextOnDb(true); err != nil {
				return
			}
		} else if len(memValue) >= 32 && len(dbValue) >= 32 && m.table == kv.HashedStorage && bytes.Compare(memValue[:32], dbValue[:32]) == 0 {
			if newDbKey, newDbValue, err = m.getNextOnDb(true); err != nil {
				return
			}
		}
	}
	return
}

func (m *memorymutationcursor) goForward(memKey, memValue, dbKey, dbValue []byte) ([]byte, []byte, error) {
	var err error
	if memValue == nil && dbValue == nil {
		return nil, nil, nil
	}

	dbKey, dbValue, err = m.skipIntersection(memKey, memValue, dbKey, dbValue)
	if err != nil {
		return nil, nil, err
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
func (m *memorymutationcursor) Next() ([]byte, []byte, error) {
	if m.isPrevFromDb {
		k, v, err := m.getNextOnDb(false)
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
func (m *memorymutationcursor) NextDup() ([]byte, []byte, error) {
	if m.isPrevFromDb {
		k, v, err := m.getNextOnDb(true)

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
func (m *memorymutationcursor) Seek(seek []byte) ([]byte, []byte, error) {
	dbKey, dbValue, err := m.cursor.Seek(seek)
	if err != nil {
		return nil, nil, err
	}

	// If the entry is marked as DB find one that is not
	if dbKey != nil && m.mutation.isEntryDeleted(m.table, dbKey) {
		dbKey, dbValue, err = m.getNextOnDb(false)
		if err != nil {
			return nil, nil, err
		}
	}

	memKey, memValue, err := m.memCursor.Seek(seek)
	if err != nil {
		return nil, nil, err
	}
	return m.goForward(memKey, memValue, dbKey, dbValue)
}

// Seek move pointer to a key at a certain position.
func (m *memorymutationcursor) SeekExact(seek []byte) ([]byte, []byte, error) {
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

	if dbKey != nil && !m.mutation.isEntryDeleted(m.table, seek) {
		m.currentDbEntry.key = dbKey
		m.currentDbEntry.value = dbValue
		m.currentMemEntry.key, m.currentMemEntry.value, err = m.memCursor.Seek(seek)
		m.isPrevFromDb = true
		m.currentPair = cursorentry{dbKey, dbValue}
		return dbKey, dbValue, err
	}
	return nil, nil, nil
}

func (m *memorymutationcursor) Put(k, v []byte) error {
	return m.mutation.Put(m.table, common.CopyBytes(k), common.CopyBytes(v))
}

func (m *memorymutationcursor) Append(k []byte, v []byte) error {
	return m.mutation.Put(m.table, common.CopyBytes(k), common.CopyBytes(v))

}

func (m *memorymutationcursor) AppendDup(k []byte, v []byte) error {
	return m.memDupCursor.AppendDup(common.CopyBytes(k), common.CopyBytes(v))
}

func (m *memorymutationcursor) PutNoDupData(key, value []byte) error {
	panic("DeleteCurrentDuplicates Not implemented")
}

func (m *memorymutationcursor) Delete(k, v []byte) error {
	return m.mutation.Delete(m.table, k, v)
}

func (m *memorymutationcursor) DeleteCurrent() error {
	panic("DeleteCurrent Not implemented")
}

func (m *memorymutationcursor) DeleteCurrentDuplicates() error {
	panic("DeleteCurrentDuplicates Not implemented")
}

// Seek move pointer to a key at a certain position.
func (m *memorymutationcursor) SeekBothRange(key, value []byte) ([]byte, error) {
	if value == nil {
		_, v, err := m.SeekExact(key)
		return v, err
	}

	dbValue, err := m.dupCursor.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}

	if dbValue != nil && m.mutation.isEntryDeleted(m.table, m.convertAutoDupsort(key, dbValue)) {
		_, dbValue, err = m.getNextOnDb(true)
		if err != nil {
			return nil, err
		}
	}

	memValue, err := m.memDupCursor.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}
	_, retValue, err := m.goForward(key, memValue, key, dbValue)
	return retValue, err
}

func (m *memorymutationcursor) Last() ([]byte, []byte, error) {
	// TODO(Giulio2002): make fixes.
	memKey, memValue, err := m.memCursor.Last()
	if err != nil {
		return nil, nil, err
	}

	dbKey, dbValue, err := m.cursor.Last()
	if err != nil {
		return nil, nil, err
	}

	dbKey, dbValue, err = m.skipIntersection(memKey, memValue, dbKey, dbValue)
	if err != nil {
		return nil, nil, err
	}

	m.currentDbEntry = cursorentry{dbKey, dbValue}
	m.currentMemEntry = cursorentry{memKey, memValue}

	// Basic checks
	if dbKey != nil && m.mutation.isEntryDeleted(m.table, dbKey) {
		m.currentDbEntry = cursorentry{}
		m.isPrevFromDb = false
		return memKey, memValue, nil
	}

	if dbValue == nil {
		m.isPrevFromDb = false
		return memKey, memValue, nil
	}

	if memValue == nil {
		m.isPrevFromDb = true
		return dbKey, dbValue, nil
	}
	// Check which one is last and return it
	keyCompare := bytes.Compare(memKey, dbKey)
	if keyCompare == 0 {
		if bytes.Compare(memValue, dbValue) > 0 {
			m.currentDbEntry = cursorentry{}
			m.isPrevFromDb = false
			return memKey, memValue, nil
		}
		m.currentMemEntry = cursorentry{}
		m.isPrevFromDb = true
		return dbKey, dbValue, nil
	}

	if keyCompare > 0 {
		m.currentDbEntry = cursorentry{}
		m.isPrevFromDb = false
		return memKey, memValue, nil
	}

	m.currentMemEntry = cursorentry{}
	m.isPrevFromDb = true
	return dbKey, dbValue, nil
}

func (m *memorymutationcursor) Prev() ([]byte, []byte, error) {
	panic("Prev is not implemented!")
}

func (m *memorymutationcursor) Close() {
	if m.cursor != nil {
		m.cursor.Close()
	}
	if m.memCursor != nil {
		m.memCursor.Close()
	}
	return
}

func (m *memorymutationcursor) Count() (uint64, error) {
	panic("Not implemented")
}

func (m *memorymutationcursor) FirstDup() ([]byte, error) {
	panic("Not implemented")
}

func (m *memorymutationcursor) NextNoDup() ([]byte, []byte, error) {
	panic("Not implemented")
}

func (m *memorymutationcursor) LastDup() ([]byte, error) {
	panic("Not implemented")
}

func (m *memorymutationcursor) CountDuplicates() (uint64, error) {
	panic("Not implemented")
}

func (m *memorymutationcursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	panic("SeekBothExact Not implemented")
}
