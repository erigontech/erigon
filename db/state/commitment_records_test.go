// Copyright 2026 The Erigon Authors
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

package state

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

type commitmentRecordTestEntry struct {
	key []byte
	val []byte
}

type commitmentRecordTestCursor struct {
	entries   []commitmentRecordTestEntry
	index     int
	nextCalls *int
}

func (c *commitmentRecordTestCursor) Key() []byte   { return c.entries[c.index].key }
func (c *commitmentRecordTestCursor) Value() []byte { return c.entries[c.index].val }
func (c *commitmentRecordTestCursor) Next() bool {
	(*c.nextCalls)++
	c.index++
	return c.index < len(c.entries)
}
func (*commitmentRecordTestCursor) Close() {}

func commitmentRecordTestSeek(entries []commitmentRecordTestEntry, nextCalls *int, seeks *int) func([]byte) (commitmentRecordCursor, error) {
	return func(key []byte) (commitmentRecordCursor, error) {
		(*seeks)++
		index := sort.Search(len(entries), func(i int) bool { return bytes.Compare(entries[i].key, key) >= 0 })
		if index == len(entries) {
			return nil, nil
		}
		return &commitmentRecordTestCursor{entries: entries, index: index, nextCalls: nextCalls}, nil
	}
}

func TestScanCommitmentRecordRunCoversSlotsAcrossFiles(t *testing.T) {
	t.Parallel()

	nodeKey := nibbles.EncodeKeyV3([]byte{1, 2})
	foreign := append(append([]byte(nil), nibbles.ChildKeyV3(nodeKey, 0)...), 0x00, 0x80)
	firstFile := []commitmentRecordTestEntry{
		{key: nibbles.ChildKeyV3(nodeKey, 0), val: []byte{0xa0}},
		{key: foreign, val: []byte{0xff}},
	}
	secondFile := []commitmentRecordTestEntry{
		{key: nibbles.ChildKeyV3(nodeKey, 1), val: []byte{0xa1}},
	}
	wanted := uint16(1<<0 | 1<<1)
	var records [16][]byte
	firstNext, firstSeeks := 0, 0
	present, err := scanCommitmentRecordRunInto(nodeKey, wanted, 0, &records, commitmentRecordTestSeek(firstFile, &firstNext, &firstSeeks))
	require.NoError(t, err)
	require.Equal(t, uint16(1<<0), present)
	require.Equal(t, 2, firstSeeks, "the foreign subtree must be skipped by seeking the next expected child")
	require.Equal(t, 1, firstNext)

	secondNext, secondSeeks := 0, 0
	present, err = scanCommitmentRecordRunInto(nodeKey, wanted, present, &records, commitmentRecordTestSeek(secondFile, &secondNext, &secondSeeks))
	require.NoError(t, err)
	require.Equal(t, wanted, present)
	require.Equal(t, 1, secondSeeks)
	require.Equal(t, []byte{0xa0}, records[0])
	require.Equal(t, []byte{0xa1}, records[1])
	// The first file scan stops after the second file supplies the last wanted slot;
	// it never walks a descendant after the mask is covered.
	require.Equal(t, 0, secondNext)
}

// commitmentRecordTestSeekBounded fails the walk instead of letting it spin, so a scan that stops
// making progress surfaces as a named error rather than a hung test.
func commitmentRecordTestSeekBounded(entries []commitmentRecordTestEntry, nextCalls, seeks *int, limit int) func([]byte) (commitmentRecordCursor, error) {
	inner := commitmentRecordTestSeek(entries, nextCalls, seeks)
	return func(key []byte) (commitmentRecordCursor, error) {
		if *seeks >= limit {
			return nil, fmt.Errorf("seek limit %d exceeded at key %x: the sibling walk is not advancing", limit, key)
		}
		return inner(key)
	}
}

// A descendant record can sort inside the direct-child run when the file lacks that child's own
// record, which is what a compacted tombstone leaves behind. The walk has to step past it.
func TestScanCommitmentRecordRunAdvancesPastIntruderWhenSlotAbsent(t *testing.T) {
	t.Parallel()

	nodeKey := nibbles.EncodeKeyV3([]byte{1, 2})
	intruder := append(append([]byte(nil), nibbles.ChildKeyV3(nodeKey, 0)...), 0x00, 0x80)
	file := []commitmentRecordTestEntry{
		{key: intruder, val: []byte{0xff}},
		{key: nibbles.ChildKeyV3(nodeKey, 3), val: []byte{0xa3}},
	}

	wanted := uint16(1<<0 | 1<<3)
	var records [16][]byte
	next, seeks := 0, 0
	present, err := scanCommitmentRecordRunInto(nodeKey, wanted, 0, &records, commitmentRecordTestSeekBounded(file, &next, &seeks, 32))
	require.NoError(t, err)
	require.Equal(t, uint16(1<<3), present, "child 0 is absent from this file and child 3 must still be collected")
	require.Equal(t, []byte{0xa3}, records[3])
	require.Nil(t, records[0])
}

// A nil *btindex.Cursor returned straight into the interface is a non-nil interface holding
// a nil pointer, so scanCommitmentRecordRun's == nil check misses it and Key() segfaults.
func TestCommitmentCursorPastEndOfIndexIsNilInterface(t *testing.T) {
	t.Parallel()

	cursor, err := commitmentCursor(nil, nil)
	require.NoError(t, err)
	require.True(t, cursor == nil, "a nil *btindex.Cursor must not become a non-nil interface")
}

// scanCommitmentRecordRunInto is the file-path shape of the run: collect every record found.
func scanCommitmentRecordRunInto(nodeKey []byte, wanted, present uint16, records *[16][]byte,
	seek func([]byte) (commitmentRecordCursor, error)) (uint16, error) {
	childKey := make([]byte, len(nodeKey)+1)
	copy(childKey, nodeKey)
	return scanCommitmentRecordRun(nodeKey, childKey, wanted, present, seek,
		func(nibble int, cursor commitmentRecordCursor) bool {
			records[nibble] = bytes.Clone(cursor.Value())
			return true
		})
}
