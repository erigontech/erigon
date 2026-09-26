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
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func TestPBinRebuildBatchesFollowTreeKeyOrder(t *testing.T) {
	addressA := bytes.Repeat([]byte{0x01}, 20)
	addressB := bytes.Repeat([]byte{0x02}, 20)
	ops := []pbt.Op{
		{Key: eip8297.TreeKeyStorage(addressB, bytes.Repeat([]byte{0x02}, 32)), Value: [32]byte{1}},
		{Key: eip8297.TreeKeyAccount(addressA, eip8297.BasicDataLeafKey), Value: [32]byte{2}},
		{Key: eip8297.TreeKeyCodeChunk(common.BytesToHash(bytes.Repeat([]byte{0x03}, 32)), 0), Value: [32]byte{3}},
		{Key: eip8297.TreeKeyAccount(addressB, eip8297.BasicDataLeafKey), Value: [32]byte{4}},
	}

	batches, err := pbinRebuildBatches(ops, t.TempDir(), 2, 256)
	require.NoError(t, err)
	require.Len(t, batches, 2)
	require.Len(t, batches[0], 2)
	require.Len(t, batches[1], 2)
	var ordered [][]byte
	for _, batch := range batches {
		for _, op := range batch {
			ordered = append(ordered, op.Key)
		}
	}
	for i := 1; i < len(ordered); i++ {
		require.Less(t, bytes.Compare(ordered[i-1], ordered[i]), 0)
	}
}

func TestPBinRebuildBatchesEmpty(t *testing.T) {
	batches, err := pbinRebuildBatches(nil, t.TempDir(), 2, 256)
	require.NoError(t, err)
	require.Empty(t, batches)
}

func TestPBinRebuildBatchesSplitWhale(t *testing.T) {
	address := bytes.Repeat([]byte{0x7a}, 20)
	ops := make([]pbt.Op, 0, 6)
	for slot := byte(64); slot < 70; slot++ {
		key := bytes.Repeat([]byte{0}, 32)
		key[len(key)-1] = slot
		ops = append(ops, pbt.Op{Key: eip8297.TreeKeyStorage(address, key), Value: [32]byte{slot}})
	}

	batches, err := pbinRebuildBatches(ops, t.TempDir(), 2, 1<<20)
	require.NoError(t, err)
	require.Len(t, batches, 3)
	for _, batch := range batches {
		require.Len(t, batch, 2)
	}
	require.Equal(t, ops[0].Key, batches[0][0].Key)
	require.Equal(t, ops[len(ops)-1].Key, batches[2][1].Key)
}
