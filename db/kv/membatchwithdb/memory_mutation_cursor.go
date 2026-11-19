// Copyright 2022 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package membatchwithdb

import (
	"bytes"
	"errors"

	"github.com/erigontech/erigon/db/kv"
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
	// deleted marks a mem tombstone that sorts after the current db entry.
	deleted bool
}

func memCursorEntry(k, v []byte, deleted bool) cursorEntry {
	return cursorEntry{key: k, value: v, deleted: deleted}
}

// cursor
type memoryMutationCursor struct {
	// entry history
	cursor    kv.CursorDupSort
	memCursor *memStoreCursor
	// we keep the mining mutation so that we can insert new elements in db
	mutation        *MemoryMutation
	table           string
	currentPair     cursorEntry
	currentDbEntry  cursorEntry
	currentMemEntry cursorEntry
	isPrevFromDb    bool
	pureDupSort     bool
}

func (m *memoryMutationCursor) isTableCleared() bool {
	m.memCursor.store.rlock()
	defer m.memCursor.store.runlock()
	return m.memCursor.table.cleared
}

func (m *memoryMutationCursor) isEntryDeleted(key []byte, value []byte) bool {
	m.memCursor.store.rlock()
	defer m.memCursor.store.runlock()
	return m.memCursor.table.hides(key, value)
}

// First move cursor to first position and return key and value accordingly.
func (m *memoryMutationCursor) First() ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.First()
	}
	mem := memCursorEntry(m.memCursor.firstRaw())

	dbKey, dbValue, err := m.cursor.First()
	if err != nil {
		return nil, nil, err
	}

	if dbKey != nil && m.isEntryDeleted(dbKey, dbValue) {
		if dbKey, dbValue, err = m.getNextOnDb(Normal); err != nil {
			return nil, nil, err
		}
	}

	return m.resolveCursorPriority(mem, dbKey, dbValue, Normal)
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
		err = errors.New("invalid next type")
		return
	}

	for key != nil && value != nil && m.isEntryDeleted(key, value) {
		switch t {
		case Normal, NoDup: // NoDup has left the old key: a hidden pair hides only itself
			key, value, err = m.cursor.Next()
			if err != nil {
				return
			}
		case Dup:
			key, value, err = m.cursor.NextDup()
			if err != nil {
				return
			}
		default:
			err = errors.New("invalid next type")
			return
		}
	}
	return
}

// Current return the current key and values the cursor is on.
func (m *memoryMutationCursor) Current() ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.Current()
	}
	return bytes.Clone(m.currentPair.key), bytes.Clone(m.currentPair.value), nil
}

func (m *memoryMutationCursor) skipIntersection(memKey, memValue, dbKey, dbValue []byte, t NextType) (newDbKey []byte, newDbValue []byte, err error) {
	newDbKey = dbKey
	newDbValue = dbValue
	dupSortTable := m.memCursor.table.dupSort
	// Check for duplicates
	if bytes.Equal(memKey, dbKey) {
		var skip bool
		if t == Normal {
			skip = !dupSortTable || bytes.Equal(memValue, dbValue)
		} else {
			skip = bytes.Equal(memValue, dbValue)
		}
		if skip {
			if newDbKey, newDbValue, err = m.getNextOnDb(t); err != nil {
				return
			}
		}
	}
	return
}

// skipMemTombstones moves the mem side past the tombstones that sort at or before
// the db entry, or past all of them once the db side is exhausted. Tombstones
// further ahead stay unvisited: the db rows they hide are filtered by isEntryDeleted.
func (m *memoryMutationCursor) skipMemTombstones(mem cursorEntry, dbKey, dbValue []byte) cursorEntry {
	for mem.deleted {
		if dbValue != nil {
			if c := bytes.Compare(mem.key, dbKey); c > 0 || c == 0 && bytes.Compare(mem.value, dbValue) > 0 {
				break
			}
		}
		mem = memCursorEntry(m.memCursor.nextRaw())
	}
	return mem
}

func (m *memoryMutationCursor) resolveCursorPriority(mem cursorEntry, dbKey, dbValue []byte, t NextType) ([]byte, []byte, error) {
	if mem.deleted {
		if mem = m.skipMemTombstones(mem, dbKey, dbValue); mem.deleted {
			m.currentMemEntry = mem
			m.currentDbEntry = cursorEntry{key: dbKey, value: dbValue}
			m.currentPair = m.currentDbEntry
			m.isPrevFromDb = true
			return dbKey, dbValue, nil
		}
	}
	memKey, memValue := mem.key, mem.value
	if memValue == nil && dbValue == nil {
		return nil, nil, nil
	}

	var err error
	dbKey, dbValue, err = m.skipIntersection(memKey, memValue, dbKey, dbValue, t)
	if err != nil {
		return nil, nil, err
	}

	m.currentDbEntry = cursorEntry{key: dbKey, value: dbValue}
	m.currentMemEntry = cursorEntry{key: memKey, value: memValue}
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
		m.currentPair = cursorEntry{key: dbKey, value: dbValue}
		return dbKey, dbValue, nil
	}

	m.currentPair = cursorEntry{key: memKey, value: memValue}
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
		return m.resolveCursorPriority(m.currentMemEntry, k, v, Normal)
	}

	return m.resolveCursorPriority(memCursorEntry(m.memCursor.nextRaw()), m.currentDbEntry.key, m.currentDbEntry.value, Normal)
}

// NextDup returns the next value of the current key, or nil past the last one, which keeps the cursor in place
// like MDBX. Each side moves within the key; a side moves to the next key only when the other side still has
// a value of this key.
func (m *memoryMutationCursor) NextDup() ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.NextDup()
	}
	key := m.currentPair.key
	if !m.memCursor.table.dupSort || key == nil {
		return nil, nil, nil
	}
	if m.isPrevFromDb {
		mem := m.currentMemEntry
		for mem.deleted && bytes.Equal(mem.key, key) {
			mem = memCursorEntry(m.memCursor.nextRaw())
		}
		m.currentMemEntry = mem
		dbKey, dbValue, err := m.getNextOnDb(Dup)
		if err != nil {
			return nil, nil, err
		}
		if dbKey == nil {
			if mem.deleted || mem.value == nil || !bytes.Equal(mem.key, key) { // a nil value is no entry
				return nil, nil, nil
			}
			if dbKey, dbValue, err = m.getNextOnDb(Normal); err != nil {
				return nil, nil, err
			}
		}
		return m.resolveCursorPriority(mem, dbKey, dbValue, Normal)
	}

	memKey, memValue, err := m.memCursor.NextDup()
	if err != nil {
		return nil, nil, err
	}
	mem := cursorEntry{key: memKey, value: memValue}
	if memKey == nil {
		if m.currentDbEntry.value == nil || !bytes.Equal(m.currentDbEntry.key, key) {
			return nil, nil, nil
		}
		mem = memCursorEntry(m.memCursor.nextRaw())
	}
	return m.resolveCursorPriority(mem, m.currentDbEntry.key, m.currentDbEntry.value, Normal)
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
	if dbKey != nil && m.isEntryDeleted(dbKey, dbValue) {
		dbKey, dbValue, err = m.getNextOnDb(Normal)
		if err != nil {
			return nil, nil, err
		}
	}

	return m.resolveCursorPriority(memCursorEntry(m.memCursor.seekRaw(seek)), dbKey, dbValue, Normal)
}

// Seek move pointer to a key at a certain position.
func (m *memoryMutationCursor) SeekExact(seek []byte) ([]byte, []byte, error) {
	k, v, err := m.Seek(seek)
	if err != nil {
		return nil, nil, err
	}
	if k != nil && bytes.Equal(k, seek) {
		return k, v, nil
	}
	return nil, nil, nil
}

func (m *memoryMutationCursor) Put(k, v []byte) error {
	return m.mutation.Put(m.table, bytes.Clone(k), bytes.Clone(v))
}

func (m *memoryMutationCursor) Append(k []byte, v []byte) error {
	return m.mutation.Append(m.table, bytes.Clone(k), bytes.Clone(v))
}

func (m *memoryMutationCursor) AppendDup(k []byte, v []byte) error {
	return m.memCursor.AppendDup(bytes.Clone(k), bytes.Clone(v))
}

func (m *memoryMutationCursor) PutNoDupData(key, value []byte) error {
	panic("Not implemented")
}

func (m *memoryMutationCursor) PutCurrent(key, value []byte) error {
	panic("Not implemented")
}

func (m *memoryMutationCursor) Delete(k []byte) error {
	return m.mutation.Delete(m.table, k)
}

func (m *memoryMutationCursor) DeleteCurrent() error {
	if !m.pureDupSort {
		return m.mutation.Delete(m.table, m.currentPair.key)
	}
	return m.mutation.deleteExact(m.table, m.currentPair.key, m.currentPair.value)
}

func (m *memoryMutationCursor) DeleteExact(k, v []byte) error {
	return m.mutation.deleteExact(m.table, k, v)
}

func (m *memoryMutationCursor) DeleteCurrentDuplicates() error {
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

	if dbValue != nil && m.isEntryDeleted(key, dbValue) {
		_, dbValue, err = m.getNextOnDb(Dup)
		if err != nil {
			return nil, err
		}
	}

	memValue, err := m.memCursor.SeekBothRange(key, value)
	if err != nil {
		return nil, err
	}
	_, retValue, err := m.resolveCursorPriority(cursorEntry{key: key, value: memValue}, key, dbValue, Dup)
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
	if dbKey != nil && m.isEntryDeleted(dbKey, dbValue) {
		if dbKey, dbValue, err = m.lastLiveOnDb(); err != nil {
			return nil, nil, err
		}
	}

	dbKey, dbValue, err = m.skipIntersection(memKey, memValue, dbKey, dbValue, Normal)
	if err != nil {
		return nil, nil, err
	}

	m.currentDbEntry = cursorEntry{key: dbKey, value: dbValue}
	m.currentMemEntry = cursorEntry{key: memKey, value: memValue}

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

// lastLiveOnDb returns the last backing-db entry that is not deleted on this overlay.
// Cursors of nested memory batches do not implement Prev, so for them it falls back
// to a forward scan.
func (m *memoryMutationCursor) lastLiveOnDb() ([]byte, []byte, error) {
	if _, nested := m.cursor.(*memoryMutationCursor); !nested {
		k, v, err := m.cursor.Last()
		for k != nil && err == nil && m.isEntryDeleted(k, v) {
			k, v, err = m.cursor.Prev()
		}
		return k, v, err
	}
	// Non-nil buffers: Last reads a nil value as "no db entry", so an empty value must stay empty.
	lastK, lastV, found := []byte{}, []byte{}, false
	k, v, err := m.cursor.First()
	for ; k != nil && err == nil; k, v, err = m.cursor.Next() {
		if !m.isEntryDeleted(k, v) {
			lastK, lastV, found = append(lastK[:0], k...), append(lastV[:0], v...), true
		}
	}
	if err != nil || !found {
		return nil, nil, err
	}
	return lastK, lastV, nil
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

func (m *memoryMutationCursor) FirstDup() ([]byte, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursor) NextNoDup() ([]byte, []byte, error) {
	if m.isTableCleared() {
		return m.memCursor.NextNoDup()
	}

	// both sides may hold values of the current key, so both move past it
	key, mem := m.currentPair.key, m.currentMemEntry
	dbKey, dbValue := m.currentDbEntry.key, m.currentDbEntry.value
	var err error
	if m.isPrevFromDb || bytes.Equal(dbKey, key) {
		if dbKey, dbValue, err = m.getNextOnDb(NoDup); err != nil {
			return nil, nil, err
		}
	}
	if !m.isPrevFromDb || bytes.Equal(mem.key, key) {
		mem = memCursorEntry(m.memCursor.nextNoDupRaw())
	}
	return m.resolveCursorPriority(mem, dbKey, dbValue, Normal)
}

func (m *memoryMutationCursor) LastDup() ([]byte, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursor) CountDuplicates() (uint64, error) {
	panic("Not implemented")
}

func (m *memoryMutationCursor) SeekBothExact(key, value []byte) ([]byte, []byte, error) {
	memKey, memValue, err := m.memCursor.SeekBothExact(key, value)
	if err != nil || m.isTableCleared() {
		return memKey, memValue, err
	}

	if memKey != nil {
		m.currentMemEntry = cursorEntry{key: memKey, value: memValue}
		m.currentDbEntry = cursorEntry{key: key}
		m.currentDbEntry.value, err = m.cursor.SeekBothRange(key, value)
		m.isPrevFromDb = false
		m.currentPair = cursorEntry{key: memKey, value: memValue}
		return memKey, memValue, err
	}

	dbKey, dbValue, err := m.cursor.SeekBothExact(key, value)
	if err != nil {
		return nil, nil, err
	}

	if dbKey != nil && !m.isEntryDeleted(key, value) {
		m.currentDbEntry = cursorEntry{key: dbKey, value: dbValue}
		m.currentMemEntry = cursorEntry{key: key}
		m.currentMemEntry.value, err = m.memCursor.SeekBothRange(key, value)
		m.isPrevFromDb = true
		m.currentPair = cursorEntry{key: dbKey, value: dbValue}
		return dbKey, dbValue, err
	}
	return nil, nil, nil
}
