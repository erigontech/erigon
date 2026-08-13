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

func TestRebuildShardMaxSteps(t *testing.T) {
	t.Parallel()

	const gb = uint64(1) << 30
	for _, tc := range []struct {
		name  string
		total uint64
		want  uint64
	}{
		{"tiny", 8 * gb, 64},
		{"default class", 32 * gb, 64},
		{"64G", 64 * gb, 64},
		{"128G", 125 * gb, 128},
		{"256G", 256 * gb, 256},
		{"1T clamps", 1024 * gb, 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, rebuildShardMaxSteps(tc.total))
		})
	}
}

// A shard's peak anonymous memory scales with the keys it holds, so the choice
// must never grow faster than the RAM backing it.
func TestRebuildShardMaxStepsMonotonic(t *testing.T) {
	t.Parallel()

	const gb = uint64(1) << 30
	prev := uint64(0)
	for total := gb; total <= 2048*gb; total += 3 * gb {
		got := rebuildShardMaxSteps(total)
		require.GreaterOrEqual(t, got, prev, "must not shrink as RAM grows (at %d GiB)", total/gb)
		require.LessOrEqual(t, got, uint64(512), "must stay clamped")
		prev = got
	}
}
