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

package membatchwithdb

import (
	"bytes"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
)

type NextType int

const (
	Normal NextType = iota
	Dup
	NoDup
)

// entry for the cursor
type cursorEntry struct {
	key   []byte
	value []byte
}

// cursor
type memoryMutationCursor struct {
	// entry history
	cursor    kv.CursorDupSort
	memCursor kv.RwCursorDupSort
	// we keep the mining mutation so that we can insert new elements in db
	mutation        *MemoryMutation
	table           string
	currentPair     cursorEntry
	currentDbEntry  cursorEntry
	currentMemEntry cursorEntry
	isPrevFromDb    bool
}

func (m *memoryMutationCursor) isTableCleared() bool {
	return m.mutation.isTableCleared(m.table)
}

func (m *memoryMutationCursor) isEntryDeleted(key []byte, value []byte, t NextType) bool {
	if t == Normal {
		return m.mutation.isEntryDeleted(m.table, key)
	} else {
		return m.mutation.isEntryDeleted(m.table, m.convertAutoDupsort(key, value))
	}
}

// First move cursor to first position and return key and value accordingly.
func (m *memoryMutationCursor) First() ([]byte, []byte, error) {
	memKey, memValue, err := m.memCursor.First()
	if err != nil || m.isTableCleared() {
		return memKey, memValue, err
	}

	dbKey, dbValue, err := m.cursor.First()
	if err != nil {
		return nil, nil, err
	}

	if dbKey != nil && m.isEntryDeleted(dbKey, dbValue, Normal) {
		if dbKey, dbValue, err = m.getNextOnDb(Normal); err != nil {
			return nil, nil, err
		}
	}

	return m.resolveCursorPriority(memKey, memValue, dbKey, dbValue, Normal)
}

func (m *memoryMutationCursor) getNextOnDb(t NextType) (key []byte, value []byte, err error) {
	switch t {
	case Normal:
		key, value, err = m.cursor.Next()
		if err != nil {
			return
		}
	case Dup:
		key, value, err = m.cursor.NextDup()
		if err != nil {
			return
		}
	case NoDup:
		key, value, err = m.cursor.NextNoDup()
		if err != nil {
			return
		}
	default:
		err = fmt.Errorf("invalid next type")
		return
	}

	for key != nil && value != nil && m.isEntryDeleted(key, value, t) {
		switch t {
		case Normal:
			key, value, err = m.cursor.Next()
			if err != nil {
				return
			}
		case Dup:
			key, value, err = m.cursor.NextDup()
			if err != nil {
				return
			}
		case NoDup:
			key, value, err = m.cursor.NextNoDup()
			if err != nil {
				return
			}
		default:
			err = fmt.Errorf("invalid next type")
			return
		}
	}
	return
}

func (m *memoryMutationCursor) convertAutoDupsort(key []byte, value []byte) []byte {
	config, ok := kv.ChaindataTablesCfg[m.table]
	// If we do not have the configuration we assume it is not dupsorted
	if !ok || !config.AutoDupSortKeysConversion {
		return key
	}
	if len(key) != config.DupToLen {
		return key
	}
	return append(key, value[:config.DupFromLen-config.DupToLen]...)
}

// Current return the current key and values the cursor is on.
func (m *memoryMutationCursor) Current() ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.Current()
	}
	return common.Copy(m.currentPair.key), common.Copy(m.currentPair.value), nil
}

func (m *memoryMutationCursor) skipIntersection(memKey, memValue, dbKey, dbValue []byte, t NextType) (newDbKey []byte, newDbValue []byte, err error) {
	newDbKey = dbKey
	newDbValue = dbValue
	config, ok := kv.ChaindataTablesCfg[m.table]
	dupSortTable := ok && ((config.Flags & kv.DupSort) != 0)
	autoKeyConversion := ok && config.AutoDupSortKeysConversion
	dupsortOffset := 0
	if autoKeyConversion {
		dupsortOffset = config.DupFromLen - config.DupToLen
	}
	// Check for duplicates
	if bytes.Equal(memKey, dbKey) {
		var skip bool
		if t == Normal {
			skip = !dupSortTable || autoKeyConversion || bytes.Equal(memValue, dbValue)
		} else {
			skip = bytes.Equal(memValue, dbValue) ||
				(dupsortOffset != 0 && len(memValue) >= dupsortOffset && len(dbValue) >= dupsortOffset && bytes.Equal(memValue[:dupsortOffset], dbValue[:dupsortOffset]))
		}
		if skip {
			if newDbKey, newDbValue, err = m.getNextOnDb(t); err != nil {
				return
			}
		}
	}
	return
}

func (m *memoryMutationCursor) resolveCursorPriority(memKey, memValue, dbKey, dbValue []byte, t NextType) ([]byte, []byte, error) {
	if memValue == nil && dbValue == nil {
		return nil, nil, nil
	}

	var err error
	dbKey, dbValue, err = m.skipIntersection(memKey, memValue, dbKey, dbValue, t)
	if err != nil {
		return nil, nil, err
	}

	m.currentDbEntry = cursorEntry{dbKey, dbValue}
	m.currentMemEntry = cursorEntry{memKey, memValue}
	// compare entries
	if bytes.Equal(memKey, dbKey) {
		m.isPrevFromDb = dbValue != nil && (memValue == nil || bytes.Compare(memValue, dbValue) > 0)
	} else {
		m.isPrevFromDb = dbValue != nil && (memKey == nil || bytes.Compare(memKey, dbKey) > 0)
	}
	if dbValue == nil {
		m.currentDbEntry = cursorEntry{}
	}
	if memValue == nil {
		m.currentMemEntry = cursorEntry{}
	}
	if m.isPrevFromDb {
		m.currentPair = cursorEntry{dbKey, dbValue}
		return dbKey, dbValue, nil
	}

	m.currentPair = cursorEntry{memKey, memValue}
	return memKey, memValue, nil
}

// Next returns the next element of the mutation.
func (m *memoryMutationCursor) Next() ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.Next()
	}

	if m.isPrevFromDb {
		k, v, err := m.getNextOnDb(Normal)
		if err != nil {
			return nil, nil, err
		}
		return m.resolveCursorPriority(m.currentMemEntry.key, m.currentMemEntry.value, k, v, Normal)
	}

	memK, memV, err := m.memCursor.Next()
	if err != nil {
		return nil, nil, err
	}

	return m.resolveCursorPriority(memK, memV, m.currentDbEntry.key, m.currentDbEntry.value, Normal)
}

// NextDup returns the next element of the mutation.
func (m *memoryMutationCursor) NextDup() ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.NextDup()
	}

	if m.isPrevFromDb {
		k, v, err := m.getNextOnDb(Dup)

		if err != nil {
			return nil, nil, err
		}
		return m.resolveCursorPriority(m.currentMemEntry.key, m.currentMemEntry.value, k, v, Dup)
	}

	memK, memV, err := m.memCursor.NextDup()
	if err != nil {
		return nil, nil, err
	}

	return m.resolveCursorPriority(memK, memV, m.currentDbEntry.key, m.currentDbEntry.value, Dup)
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursor) Seek(seek []byte) ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.Seek(seek)
	}

	dbKey, dbValue, err := m.cursor.Seek(seek)
	if err != nil {
		return nil, nil, err
	}

	// If the entry is marked as deleted find one that is not
	if dbKey != nil && m.isEntryDeleted(dbKey, dbValue, Normal) {
		dbKey, dbValue, err = m.getNextOnDb(Normal)
		if err != nil {
			return nil, nil, err
		}
	}

	memKey, memValue, err := m.memCursor.Seek(seek)
	if err != nil {
		return nil, nil, err
	}

	return m.resolveCursorPriority(memKey, memValue, dbKey, dbValue, Normal)
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursor) SeekExact(seek []byte) ([]byte, []byte, error) {
	memKey, memValue, err := m.memCursor.SeekExact(seek)
	if err != nil || m.isTableCleared() {
		return memKey, memValue, err
	}

	if memKey != nil {
		m.currentMemEntry.key = memKey
		m.currentMemEntry.value = memValue
		m.currentDbEntry.key, m.currentDbEntry.value, err = m.cursor.Seek(seek)
		m.isPrevFromDb = false
		m.currentPair = cursorEntry{memKey, memValue}
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
		m.currentPair = cursorEntry{dbKey, dbValue}
		return dbKey, dbValue, err
	}
	return nil, nil, nil
}

func (m *memoryMutationCursor) Put(k, v []byte) error {
	return m.mutation.Put(m.table, common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursor) Append(k []byte, v []byte) error {
	return m.mutation.Append(m.table, common.Copy(k), common.Copy(v))

}

func (m *memoryMutationCursor) AppendDup(k []byte, v []byte) error {
	return m.memCursor.AppendDup(common.Copy(k), common.Copy(v))
}

func (m *memoryMutationCursor) PutNoDupData(key, value []byte) error {
	panic("Not implemented")
}

func (m *memoryMutationCursor) Delete(k []byte) error {
	return m.mutation.Delete(m.table, k)
}

func (m *memoryMutationCursor) DeleteCurrent() error {
	panic("DeleteCurrent Not implemented")
}
func (m *memoryMutationCursor) DeleteExact(_, _ []byte) error {
	panic("DeleteExact Not implemented")
}

func (m *memoryMutationCursor) DeleteCurrentDuplicates() error {
	config, ok := kv.ChaindataTablesCfg[m.table]
	autoKeyConversion := ok && config.AutoDupSortKeysConversion
	if autoKeyConversion {
		panic("DeleteCurrentDuplicates Not implemented for AutoDupSortKeysConversion tables")
	}

	k, _, err := m.Current()
	if err != nil {
		return err
	}
	if k != nil {
		return m.Delete(k)
	}
	return nil
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursor) SeekBothRange(key, value []byte) ([]byte, error) {
	if m.isTableCleared() {
		return m.memCursor.SeekBothRange(key, value)
	}

	dbValue, err := m.cursor.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}

	if dbValue != nil && m.isEntryDeleted(key, dbValue, Dup) {
		_, dbValue, err = m.getNextOnDb(Dup)
		if err != nil {
			return nil, err
		}
	}

	memValue, err := m.memCursor.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}
	_, retValue, err := m.resolveCursorPriority(key, memValue, key, dbValue, Dup)
	return retValue, err
}

func (m *memoryMutationCursor) Last() ([]byte, []byte, error) {
	memKey, memValue, err := m.memCursor.Last()
	if err != nil || m.isTableCleared() {
		return memKey, memValue, err
	}

	dbKey, dbValue, err := m.cursor.Last()
	if err != nil {
		return nil, nil, err
	}

	dbKey, dbValue, err = m.skipIntersection(memKey, memValue, dbKey, dbValue, Normal)
	if err != nil {
		return nil, nil, err
	}

	m.currentDbEntry = cursorEntry{dbKey, dbValue}
	m.currentMemEntry = cursorEntry{memKey, memValue}

	// Basic checks
	if dbKey != nil && m.isEntryDeleted(dbKey, dbValue, Normal) {
		m.currentDbEntry = cursorEntry{}
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
			m.currentDbEntry = cursorEntry{}
			m.isPrevFromDb = false
			return memKey, memValue, nil
		}
		m.currentMemEntry = cursorEntry{}
		m.isPrevFromDb = true
		return dbKey, dbValue, nil
	}

	if keyCompare > 0 {
		m.currentDbEntry = cursorEntry{}
		m.isPrevFromDb = false
		return memKey, memValue, nil
	}

	m.currentMemEntry = cursorEntry{}
	m.isPrevFromDb = true
	return dbKey, dbValue, nil
}

func (m *memoryMutationCursor) Prev() ([]byte, []byte, error) {
	panic("Prev is not implemented!")
}
func (m *memoryMutationCursor) PrevDup() ([]byte, []byte, error) {
	panic("Prev is not implemented!")
}
func (m *memoryMutationCursor) PrevNoDup() ([]byte, []byte, error) {
	panic("Prev is not implemented!")
}

func (m *memoryMutationCursor) Close() {
	if m.cursor != nil {
		m.cursor.Close()
	}
	if m.memCursor != nil {
		m.memCursor.Close()
	}
}

func (m *memoryMutationCursor) Count() (uint64, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursor) FirstDup() ([]byte, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursor) NextNoDup() ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.NextNoDup()
	}

	if m.isPrevFromDb {
		k, v, err := m.getNextOnDb(NoDup)
		if err != nil {
			return nil, nil, err
		}
		return m.resolveCursorPriority(m.currentMemEntry.key, m.currentMemEntry.value, k, v, NoDup)
	}

	memK, memV, err := m.memCursor.NextNoDup()
	if err != nil {
		return nil, nil, err
	}

	return m.resolveCursorPriority(memK, memV, m.currentDbEntry.key, m.currentDbEntry.value, NoDup)
}

func (m *memoryMutationCursor) LastDup() ([]byte, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursor) CountDuplicates() (uint64, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	panic("SeekBothExact Not implemented")
}
