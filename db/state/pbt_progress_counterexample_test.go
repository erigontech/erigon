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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/commitment/eip8297"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func TestPBTPartitionWriteProgressCountsEachOpOnce(t *testing.T) {
	previous := log.Root().GetHandler()
	var logs bytes.Buffer
	log.Root().SetHandler(log.StreamHandler(&logs, log.LogfmtFormat()))
	t.Cleanup(func() { log.Root().SetHandler(previous) })
	ops := []pbt.Op{
		{Key: eip8297.TreeKeyAccount(bytes.Repeat([]byte{1}, 20), eip8297.BasicDataLeafKey), Value: [32]byte{1}},
		{Key: eip8297.TreeKeyAccount(bytes.Repeat([]byte{2}, 20), eip8297.BasicDataLeafKey), Value: [32]byte{2}},
	}
	batches, err := pbinRebuildBatches(ops, t.TempDir(), 1, 1<<20)
	require.NoError(t, err)
	var count int
	for _, batch := range batches {
		count += len(batch)
	}
	require.Equal(t, len(ops), count)
	require.Contains(t, logs.String(), `phase="pbin-ops file write" done=2 `, "two written operations must report done=2")
}
