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
	"encoding/binary"
	"runtime"
	"syscall"
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

	batches, err := pbinRebuildBatches(ops, t.TempDir(), 2, 1<<20)
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
		key := make([]byte, 32)
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

func TestPBinRebuildBatchStreamPreservesOperations(t *testing.T) {
	address := bytes.Repeat([]byte{0x3a}, 20)
	ops := []pbt.Op{
		{Key: eip8297.TreeKeyStorage(address, bytes.Repeat([]byte{0x02}, 32)), Value: [32]byte{1}},
		{Key: eip8297.TreeKeyAccount(address, eip8297.BasicDataLeafKey), Value: [32]byte{2}},
		{Key: eip8297.TreeKeyStorage(address, bytes.Repeat([]byte{0x01}, 32)), Value: [32]byte{3}},
	}
	want, err := pbinRebuildBatches(ops, t.TempDir(), 10, 1<<20)
	require.NoError(t, err)
	var got []pbt.Op
	err = pbinForEachRebuildBatch(ops, t.TempDir(), 10, 1<<20, func(batch []pbt.Op, _ bool) error {
		got = append(got, batch...)
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, want[0], got)
}

func TestPBinRebuildBatchStreamBoundsLiveHeap(t *testing.T) {
	const (
		operationCount = 600_000
		maxOperations  = 100_000
		maxBytes       = 64 << 20
	)
	address := bytes.Repeat([]byte{0x5a}, 20)
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	var beforeUsage syscall.Rusage
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &beforeUsage))
	ops := make([]pbt.Op, operationCount)
	for i := range ops {
		slot := make([]byte, 32)
		binary.BigEndian.PutUint32(slot[28:], uint32(i))
		ops[i] = pbt.Op{Key: eip8297.TreeKeyStorage(address, slot), Value: [32]byte{1}}
	}
	var largest int
	var peak uint64
	err := pbinForEachRebuildBatch(ops, t.TempDir(), maxOperations, maxBytes, func(batch []pbt.Op, _ bool) error {
		if len(batch) > largest {
			largest = len(batch)
		}
		var current runtime.MemStats
		runtime.ReadMemStats(&current)
		if current.Alloc > peak {
			peak = current.Alloc
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, maxOperations, largest)
	runtime.GC()
	require.Len(t, ops, operationCount)
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	var afterUsage syscall.Rusage
	require.NoError(t, syscall.Getrusage(syscall.RUSAGE_SELF, &afterUsage))
	ceiling := uint64(maxBytes) + uint64(maxOperations)*128 + 32<<20
	retained := uint64(0)
	if stats.Alloc > baseline.Alloc {
		retained = stats.Alloc - baseline.Alloc
	}
	t.Logf("peak live heap: %d bytes; live heap after GC: %d bytes; retained: %d bytes; peak rss delta: %d; ceiling: %d bytes", peak, stats.Alloc, retained, afterUsage.Maxrss-beforeUsage.Maxrss, ceiling)
	require.Less(t, retained, ceiling)
}
