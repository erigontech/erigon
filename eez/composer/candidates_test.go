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

package composer

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/execution/types"
)

// TestCandidatesOfWindow468: the blocks the entries of window 468's batch
// settle are the siblings of its Sync block carrying its prefixes.
func TestCandidatesOfWindow468(t *testing.T) {
	w := loadSyncWindow(t)
	ends := PairEnds(w.sync.Transactions())
	require.Equal(t, []int{2, 4, 5}, ends)
	got, err := Candidates(w.config, w.engine, w.sync, w.witness, ends)
	require.NoError(t, err)
	var want []common.Hash
	for _, e := range w.batch.Entries {
		want = append(want, e.RollupUpdates[0].NewRoot)
	}
	require.Equal(t, want, got)
	require.Equal(t, w.sync.Hash(), got[len(got)-1])
}

func TestCandidatesRefuse(t *testing.T) {
	w := loadSyncWindow(t)
	_, err := Candidates(w.config, w.engine, w.sync, w.witness, []int{6})
	require.Error(t, err, "a prefix longer than the block")
	h := types.CopyHeader(w.sync.Header())
	h.Root = common.Hash{1}
	forged := types.NewBlockFromStorage(h.Hash(), h, w.sync.Transactions(), nil, w.sync.Withdrawals(), nil)
	_, err = Candidates(w.config, w.engine, forged, w.witness, []int{5})
	require.ErrorIs(t, err, stateless.ErrStateRoot, "a block its witness does not execute to")
}
