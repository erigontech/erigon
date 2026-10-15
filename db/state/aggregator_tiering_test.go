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

func TestCommitmentHotMaxMergeSteps(t *testing.T) {
	const stepSize = 10
	ranges := []testFileRange{{0, 1}, {1, 2}, {2, 3}, {3, 4}, {4, 5}, {5, 6}, {6, 7}, {7, 8}}
	span := func(r MergeRange) [2]uint64 {
		if !r.needMerge {
			return [2]uint64{}
		}
		return [2]uint64{r.from / stepSize, r.to / stepSize}
	}
	for _, tc := range []struct {
		hotSteps   uint64
		commitment [2]uint64
	}{
		{hotSteps: 0, commitment: [2]uint64{0, 8}},
		{hotSteps: 1, commitment: [2]uint64{}},
	} {
		db, agg := testDbAndAggregatorv3(t, stepSize)
		agg.commitmentHotMaxMergeSteps = tc.hotSteps
		dirs := agg.Dirs()
		generateAccountsFile(t, dirs, ranges)
		generateStorageFile(t, dirs, ranges)
		generateCodeFile(t, dirs, ranges)
		generateCommitmentFile(t, dirs, ranges)
		require.NoError(t, agg.OpenFolder(db))

		aggTx := agg.BeginFilesRo()
		r := aggTx.findMergeRange(8*stepSize, stepSize, 32)
		aggTx.Close()

		require.Equal(t, [2]uint64{0, 8}, span(r.domain[kv.AccountsDomain].values), "hot=%d", tc.hotSteps)
		require.Equal(t, [2]uint64{0, 8}, span(r.domain[kv.StorageDomain].values), "hot=%d", tc.hotSteps)
		require.Equal(t, tc.commitment, span(r.domain[kv.CommitmentDomain].values), "hot=%d", tc.hotSteps)
	}
}
