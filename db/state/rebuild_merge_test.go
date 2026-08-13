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

func mergeableRange(name kv.Domain, from, to uint64) DomainRanges {
	return DomainRanges{
		name:    name,
		values:  MergeRange{name: name.String(), needMerge: true, from: from, to: to},
		history: HistoryRanges{history: MergeRange{needMerge: true, from: from, to: to}},
		aggStep: 1,
	}
}

// The rebuild's shard loop holds live accounts/storage file streams for the whole
// range, so only commitment may collapse underneath it.
func TestKeepCommitmentMergeOnly(t *testing.T) {
	t.Parallel()

	r := &Ranges{}
	r.domain[kv.AccountsDomain] = mergeableRange(kv.AccountsDomain, 0, 100)
	r.domain[kv.StorageDomain] = mergeableRange(kv.StorageDomain, 0, 100)
	r.domain[kv.CodeDomain] = mergeableRange(kv.CodeDomain, 0, 100)
	r.domain[kv.CommitmentDomain] = mergeableRange(kv.CommitmentDomain, 0, 100)
	r.invertedIndex[0] = NewMergeRange("logaddrs", true, 0, 100)

	require.True(t, r.any())
	keepCommitmentMergeOnly(r)

	require.True(t, r.domain[kv.CommitmentDomain].any(), "commitment must stay mergeable")
	require.False(t, r.domain[kv.AccountsDomain].any(), "accounts must be held")
	require.False(t, r.domain[kv.StorageDomain].any(), "storage must be held")
	require.False(t, r.domain[kv.CodeDomain].any(), "code must be held")
	for i, ii := range r.invertedIndex {
		require.Nilf(t, ii, "inverted index %d must be held", i)
	}
}

func TestKeepCommitmentMergeOnlyNothingToDo(t *testing.T) {
	t.Parallel()

	r := &Ranges{}
	r.domain[kv.AccountsDomain] = mergeableRange(kv.AccountsDomain, 0, 100)

	keepCommitmentMergeOnly(r)
	require.False(t, r.any(), "with no commitment range there is nothing left to merge")
}

func TestRebuildShardSteps(t *testing.T) {
	t.Parallel()

	const gb = uint64(1) << 30
	// The range the live mainnet rebuild walks: 8192 steps, 1.79G keys.
	const mainnetKeysPerStep = 218706

	for _, tc := range []struct {
		name                      string
		totalMemory               uint64
		stepsInRange, keysPerStep uint64
		want                      uint64
	}{
		{"snap-arb1 mainnet", 125 * gb, 8192, mainnetKeysPerStep, 128},
		{"same box, denser range halves it", 125 * gb, 8192, 2 * mainnetKeysPerStep, 64},
		{"4x the RAM, 4x the shard", 500 * gb, 8192, mainnetKeysPerStep, 512},
		{"huge box is bounded by the range, not a constant", 64000 * gb, 8192, mainnetKeysPerStep, 8192},
		{"sparse range takes the whole thing", 125 * gb, 1024, 8, 1024},
		{"tiny box still makes progress", 1 * gb, 8192, mainnetKeysPerStep, 1},
		{"unknown density falls back to the range", 125 * gb, 256, 0, 256},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, rebuildShardSteps(tc.totalMemory, tc.stepsInRange, tc.keysPerStep))
		})
	}
}

// No constant ceiling: a wide enough range must keep getting bigger shards on a
// bigger box, or it lands back in the many-shard regime the sharding exists to avoid.
func TestRebuildShardStepsHasNoConstantCeiling(t *testing.T) {
	t.Parallel()

	const gb = uint64(1) << 30
	small := rebuildShardSteps(128*gb, 1<<20, 1000)
	large := rebuildShardSteps(8192*gb, 1<<20, 1000)
	require.Greater(t, large, small, "more RAM must buy a bigger shard")
	require.Greater(t, large, uint64(512), "must not stall at the old constant cap")
}

func TestRebuildShardStepsNeverExceedsRange(t *testing.T) {
	t.Parallel()

	const gb = uint64(1) << 30
	for _, stepsInRange := range []uint64{1, 2, 64, 256, 8192} {
		got := rebuildShardSteps(1<<50, stepsInRange, 1)
		require.LessOrEqual(t, got, stepsInRange, "a shard may never outgrow its range")
		require.Positive(t, got)
	}
}

// Shard boundaries feed the merge ranges, which are power-of-two aligned.
func TestRebuildShardStepsIsPowerOfTwo(t *testing.T) {
	t.Parallel()

	const gb = uint64(1) << 30
	for mem := uint64(1); mem <= 4096; mem *= 3 {
		for _, kps := range []uint64{1, 997, 218706, 5_000_000} {
			got := rebuildShardSteps(mem*gb, 8192, kps)
			require.Positive(t, got)
			require.Zerof(t, got&(got-1), "shard steps %d is not a power of two (mem=%dG kps=%d)", got, mem, kps)
		}
	}
}
