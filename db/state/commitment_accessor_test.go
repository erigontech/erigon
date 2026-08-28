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
	"testing"

	"github.com/stretchr/testify/require"
	btree2 "github.com/tidwall/btree"

	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment/nibbles"
)

func buildCommitmentAccessorTestDomain(t *testing.T, keys [][]byte) (*Domain, *DomainRoTx, kv.Tx) {
	t.Helper()

	ctx := t.Context()
	db, d := testDbAndDomainOfStep(t, statecfg.Schema.CommitmentDomain, 16, log.New())
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)

	dt := d.beginForTests()
	writer := dt.NewWriter()
	for i, key := range keys {
		require.NoError(t, writer.PutWithPrev(key, []byte{byte(i + 1)}, uint64(i+1), nil))
	}
	require.NoError(t, writer.Flush(ctx, tx))
	writer.Close()
	dt.Close()

	require.NoError(t, d.collateBuildIntegrate(ctx, 0, tx, background.NewProgressSet()))
	require.NoError(t, tx.Commit())

	roTx, err := db.BeginRo(ctx)
	require.NoError(t, err)
	t.Cleanup(roTx.Rollback)
	dt = d.beginForTests()
	t.Cleanup(func() {
		dt.Close()
	})
	return d, dt, roTx
}

func scanCommitmentChildRecords(t *testing.T, dt *DomainRoTx, nodeKey []byte, depth int) (keys [][]byte, seeks, nexts, foreignSeen int) {
	t.Helper()

	item := dt.files[0].src
	reader := dt.dataReader(item.decompressor)
	_, hi := nibbles.ChildRangeBoundsV3(nodeKey)
	expectedLength := nibbles.ChildKeyLenForDepth(depth)
	for nibble := byte(0); nibble < 16; {
		expected := nibbles.ChildKeyV3(nodeKey, nibble)
		seeks++
		cursor, err := item.bindex.Seek(reader, expected)
		require.NoError(t, err)
		if cursor == nil {
			nibble++
			continue
		}

		for {
			key := cursor.Key()
			if key == nil || bytes.Compare(key, hi) >= 0 {
				cursor.Close()
				nibble++
				break
			}
			if len(key) == expectedLength && nibbles.IsChildKeyForNodeV3(nodeKey, key) && bytes.Equal(key, expected) {
				keys = append(keys, bytes.Clone(key))
				nibble++
				if nibble == 16 {
					cursor.Close()
					return keys, seeks, nexts, foreignSeen
				}
				nexts++
				if !cursor.Next() {
					cursor.Close()
					break
				}
				nextExpected := nibbles.ChildKeyV3(nodeKey, nibble)
				key = cursor.Key()
				if bytes.Equal(key, nextExpected) {
					expected = nextExpected
					continue
				}
				if len(key) != expectedLength || !nibbles.IsChildKeyForNodeV3(nodeKey, key) {
					foreignSeen++
				}
				cursor.Close()
				break
			}
			if len(key) != expectedLength || !nibbles.IsChildKeyForNodeV3(nodeKey, key) {
				foreignSeen++
			}
			cursor.Close()
			nibble++
			break
		}
	}
	return keys, seeks, nexts, foreignSeen
}

func TestCommitmentDomainUsesOrderedAccessors(t *testing.T) {
	t.Parallel()

	nodePath := []byte{0x1, 0x2}
	nodeKey := nibbles.EncodeKeyV3(nodePath)
	keys := [][]byte{
		nibbles.ChildKeyV3(nodeKey, 0),
		nibbles.ChildKeyV3(nodeKey, 7),
		nibbles.ChildKeyV3(nodeKey, 15),
	}
	_, dt, roTx := buildCommitmentAccessorTestDomain(t, keys)

	require.Equal(t, statecfg.AccessorBTree|statecfg.AccessorExistence, dt.d.Accessors)
	item := dt.files[0].src
	require.NotNil(t, item.bindex)
	require.NotNil(t, item.existence)
	require.Nil(t, item.index)
	require.EqualValues(t, len(keys), item.bindex.KeyCount())

	reader := dt.dataReader(item.decompressor)
	cursor, err := item.bindex.Seek(reader, keys[1])
	require.NoError(t, err)
	require.NotNil(t, cursor)
	require.Equal(t, keys[1], cursor.Key())
	cursor.Close()

	var ramIter btree2.MapIter[string, []dataWithTxNum]
	var got [][]byte
	require.NoError(t, dt.debugIteratePrefixLatest(nodeKey, ramIter, func(key, _ []byte) (bool, error) {
		got = append(got, bytes.Clone(key))
		return true, nil
	}, roTx))
	require.Equal(t, keys, got)
}

func TestCommitmentChildRangeScanFiltersForeignSubtree(t *testing.T) {
	t.Parallel()

	nodePath := []byte{0x1, 0x2}
	nodeKey := nibbles.EncodeKeyV3(nodePath)
	keys := [][]byte{
		nibbles.ChildKeyV3(nodeKey, 0),
		nibbles.ChildKeyV3(nodeKey, 8),
		nibbles.ChildKeyV3(nodeKey, 15),
	}
	for i := range 128 {
		path := append(bytes.Clone(nodePath), 0x0, 0x0, 0x8, 0x0, byte(i>>4), byte(i&0xf), 0x0, 0x0)
		foreign := nibbles.ChildKeyV3(nibbles.EncodeKeyV3(path), 0)
		lo, hi := nibbles.ChildRangeBoundsV3(nodeKey)
		require.Less(t, bytes.Compare(lo, foreign), 0)
		require.Less(t, bytes.Compare(foreign, hi), 0)
		keys = append(keys, foreign)
	}

	_, dt, _ := buildCommitmentAccessorTestDomain(t, keys)
	got, seeks, nexts, foreignSeen := scanCommitmentChildRecords(t, dt, nodeKey, len(nodePath))
	require.Equal(t, [][]byte{
		nibbles.ChildKeyV3(nodeKey, 0),
		nibbles.ChildKeyV3(nodeKey, 8),
		nibbles.ChildKeyV3(nodeKey, 15),
	}, got)
	require.Equal(t, 16, seeks)
	require.Equal(t, 1, foreignSeen, "only the first key of the foreign subtree should be inspected")
	require.Less(t, nexts, 128, "foreign subtree must be skipped by re-seeking the next expected child key")
}
