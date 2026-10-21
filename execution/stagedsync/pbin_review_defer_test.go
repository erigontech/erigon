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

package stagedsync

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/v3/pbt"
)

func TestPBinReviewEmptyBlockAfterDeferredUnwind(t *testing.T) {
	tx, doms := pbinOnlyStepTest(t)
	setDeferCommitmentUpdates(doms, true, false, false)
	defer doms.SetDeferCommitmentUpdates(false)
	putPBinDeferAccounts(t, tx, doms, 1, 8, 0, 1)
	root1, err := doms.ComputeCommitment(t.Context(), tx, true, 1, 1, "review", nil)
	require.NoError(t, err)
	require.NoError(t, doms.FlushPendingUpdates(tx))
	putPBinDeferAccounts(t, tx, doms, 1, 8, 100, 2)
	_, err = doms.ComputeCommitment(t.Context(), tx, true, 2, 2, "review", nil)
	require.NoError(t, err)
	require.NoError(t, doms.FlushPendingUpdates(tx))
	doms.Unwind(2, nil)
	doms.GetCommitmentCtx().SetPBinFeed(&commitment.PBinFeed{})
	root, err := doms.ComputeCommitment(t.Context(), tx, true, 2, 2, "review empty replacement", nil)
	require.NoError(t, err)
	require.Equal(t, root1, root, "empty replacement must keep the pre-unwind root")
	domain := doms.GetCommitmentCtx().CommitmentDomain()
	rootRecord, _, err := doms.GetLatest(domain, tx, pbt.GlobalRootKey())
	require.NoError(t, err)
	state, _, err := doms.GetLatest(domain, tx, commitment.KeyCommitmentState)
	require.NoError(t, err)
	require.NotEmpty(t, rootRecord)
	require.True(t, bytes.HasSuffix(state, rootRecord), "state embeds the unwound block's root instead of the current root")
}
