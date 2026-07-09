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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/snapshotsync/blocksnapshots"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestRetireCutoffs_ConvertsBlockDistanceToTxNum pins the per-domain
// block-distance-to-txNum conversion: Default drives most domains, CommitmentDomain
// gets its own window, RCacheDomain follows the history window. The aggregator
// floors txNum to its file step, so this layer stays in txNum.
func TestRetireCutoffs_ConvertsBlockDistanceToTxNum(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)

	snaps := blocksnapshots.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs.Snap, logger)
	t.Cleanup(snaps.Close)
	br := freezeblocks.NewBlockReader(snaps, nil)

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// block b owns txNums [10*b, 10*b+9], so Min(b) = 10*b.
	const perBlock = uint64(10)
	const forward = uint64(30)
	for b := uint64(0); b <= forward; b++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, b, b*perBlock+perBlock-1))
	}

	t.Run("archive -> nothing retirable", func(t *testing.T) {
		cutoffs, err := historyRetireCutoffs(ctx, tx, br, prune.ArchiveMode, forward)
		require.NoError(t, err)
		require.True(t, cutoffs.IsNoop())
	})

	t.Run("history not accumulated yet -> nothing retirable", func(t *testing.T) {
		pm := prune.Mode{Initialised: true, History: prune.Distance(1_000_000)}
		cutoffs, err := historyRetireCutoffs(ctx, tx, br, pm, forward)
		require.NoError(t, err)
		require.True(t, cutoffs.IsNoop())
	})

	t.Run("finite history -> default txNum, commitment keep-all, rcache follows history", func(t *testing.T) {
		// PruneTo(30, Distance(10)) = block 20; Min(20) = txNum 200.
		pm := prune.Mode{Initialised: true, History: prune.Distance(10)}
		cutoffs, err := historyRetireCutoffs(ctx, tx, br, pm, forward)
		require.NoError(t, err)
		require.False(t, cutoffs.IsNoop())
		require.Equal(t, uint64(200), cutoffs.Default)
		require.Equal(t, uint64(0), cutoffs.PerDomain[kv.CommitmentDomain], "keep-all commitment -> no cutoff")
		require.Equal(t, uint64(200), cutoffs.PerDomain[kv.RCacheDomain], "rcache follows the history window")
	})

	t.Run("finite history + finite commitment -> independent cutoffs", func(t *testing.T) {
		// history Distance(10) -> block 20, txNum 200; commitment Distance(5) ->
		// block 25, txNum 250 (a narrower window is retired more aggressively).
		pm := prune.Mode{Initialised: true, History: prune.Distance(10), CommitmentHistory: prune.Distance(5)}
		cutoffs, err := historyRetireCutoffs(ctx, tx, br, pm, forward)
		require.NoError(t, err)
		require.False(t, cutoffs.IsNoop())
		require.Equal(t, uint64(200), cutoffs.Default)
		require.Equal(t, uint64(250), cutoffs.PerDomain[kv.CommitmentDomain])
	})

	t.Run("archive history + finite commitment -> commitment only", func(t *testing.T) {
		// History keep-all -> Default 0; commitment Distance(20) -> block 10, txNum 100.
		pm := prune.Mode{Initialised: true, History: prune.KeepAllBlocksPruneMode, CommitmentHistory: prune.Distance(20)}
		cutoffs, err := historyRetireCutoffs(ctx, tx, br, pm, forward)
		require.NoError(t, err)
		require.False(t, cutoffs.IsNoop())
		require.Equal(t, uint64(0), cutoffs.Default)
		require.Equal(t, uint64(100), cutoffs.PerDomain[kv.CommitmentDomain])
	})
}
