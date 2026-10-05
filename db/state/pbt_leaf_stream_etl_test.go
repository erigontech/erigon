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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func pbinTestOp(key, value []byte) pbt.Op {
	op := pbt.Op{Key: key}
	copy(op.Value[:], value)
	return op
}

func TestPBinLeafStreamDeduplicatesAndRejectsConflicts(t *testing.T) {
	collector := etl.NewCollector("pbin-leaf-stream-test", t.TempDir(), etl.NewSortableBuffer(1024), log.Root())
	defer collector.Close()
	key := eip8297.TreeKeyCodeChunk(common.Hash{1}, 0)
	value := make([]byte, eip8297.ValueLength)
	value[eip8297.ValueLength-1] = 1
	require.NoError(t, pbinCollectOp(collector, new([8 + eip8297.ValueLength]byte), pbinTestOp(key, value), 3))
	require.NoError(t, pbinCollectOp(collector, new([8 + eip8297.ValueLength]byte), pbinTestOp(key, value), 7))
	var leaves []PBinLeaf
	require.NoError(t, pbinLoadSortedLeaves(collector, func(leaf PBinLeaf) error {
		leaves = append(leaves, leaf)
		return nil
	}, &pbinStreamProgress{}))
	require.Len(t, leaves, 1)
	require.EqualValues(t, 7, leaves[0].Stamp)

	conflicting := etl.NewCollector("pbin-leaf-stream-conflict-test", t.TempDir(), etl.NewSortableBuffer(1024), log.Root())
	defer conflicting.Close()
	other := bytes.Clone(value)
	other[0] = 1
	require.NoError(t, pbinCollectOp(conflicting, new([8 + eip8297.ValueLength]byte), pbinTestOp(key, value), 1))
	require.NoError(t, pbinCollectOp(conflicting, new([8 + eip8297.ValueLength]byte), pbinTestOp(key, other), 2))
	require.Error(t, pbinLoadSortedLeaves(conflicting, func(PBinLeaf) error { return nil }, &pbinStreamProgress{}))
}
