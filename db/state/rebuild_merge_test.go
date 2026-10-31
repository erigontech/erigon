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

// A rebuild shard runs inside a range-scoped iterator over the accounts and
// storage files, so only commitment may collapse underneath it.
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
