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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
)

func TestRebuildShardMaxSteps(t *testing.T) {
	require.Equal(t, uint64(64), rebuildShardMaxSteps(0))
	require.Equal(t, uint64(64), rebuildShardMaxSteps(218_000))  // mainnet steps keep 64-step shards
	require.Equal(t, uint64(8192), rebuildShardMaxSteps(1_748))  // 3125-tx steps: 1.79G keys over 1,024,000 steps
	require.Equal(t, uint64(8), rebuildShardMaxSteps(1_890_433)) // dense steps: 121M keys in a 64-step shard ran out of 62 GB
	require.Equal(t, uint64(1), rebuildShardMaxSteps(1<<25))
	require.Equal(t, uint64(1<<24), rebuildShardMaxSteps(1))
}

func TestNextRebuildShardStaysInsideTheRange(t *testing.T) {
	const from, last = kv.Step(1024000), kv.Step(1040000)
	shardTo, size := from+8192, kv.Step(8192)
	for shardTo < last {
		var shardFrom kv.Step
		shardFrom, shardTo, size = nextRebuildShard(shardTo, size, last)
		require.Less(t, shardFrom, shardTo)
		require.LessOrEqual(t, shardTo, last)
	}
	require.Equal(t, last, shardTo)
}
