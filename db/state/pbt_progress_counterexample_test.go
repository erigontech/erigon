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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func TestPBTPartitionWriteProgressCountsEachOpOnce(t *testing.T) {
	previous := log.Root().GetHandler()
	previousInterval := PBTProgressInterval
	var logs bytes.Buffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	PBTProgressInterval = time.Nanosecond
	t.Cleanup(func() {
		log.Root().SetHandler(previous)
		PBTProgressInterval = previousInterval
	})
	const totalOps = 5000
	address := bytes.Repeat([]byte{1}, 20)
	ops := make([]pbt.Op, totalOps)
	for i := range ops {
		slot := make([]byte, 32)
		binary.BigEndian.PutUint64(slot[24:], uint64(i))
		var value [32]byte
		binary.BigEndian.PutUint64(value[24:], uint64(i))
		ops[i] = pbt.Op{Key: eip8297.TreeKeyStorage(address, slot), Value: value}
	}
	ops = pbinSortedOps(ops)
	batches, err := pbinRebuildBatches(ops, t.TempDir(), 1, 1<<20)
	require.NoError(t, err)
	var count int
	for _, batch := range batches {
		count += len(batch)
	}
	require.Equal(t, len(ops), count)
	require.Contains(t, logs.String(), `phase="pbin-ops file write" done=4096 `, "sampled progress must report the threshold")
	require.Contains(t, logs.String(), `phase="pbin-ops file write" done=5000 `, "all written operations must report done=5000")
}
