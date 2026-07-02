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
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// TestHistoryRetireCutoffStep_ConvertsBlockDistanceToStep pins the
// block-distance-to-step conversion PruneExecutionStage relies on.
func TestHistoryRetireCutoffStep_ConvertsBlockDistanceToStep(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	db := mdbx.New(dbcfg.ChainDB, logger).InMem(t, dirs.Chaindata).MustOpen()
	t.Cleanup(db.Close)

	snaps := freezeblocks.NewRoSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs.Snap, logger)
	t.Cleanup(snaps.Close)
	br := freezeblocks.NewBlockReader(snaps, nil)

	ctx := context.Background()
	tx, err := db.BeginRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	// block b owns txNum 10*b, matching stepSize below, for easy boundary math.
	const perBlock, stepSize = uint64(10), uint64(10)
	const forward = uint64(30)
	for b := uint64(0); b <= forward; b++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, b, b*perBlock+perBlock-1))
	}

	t.Run("History disabled -> no cutoff", func(t *testing.T) {
		cutoffStep, commitmentStep, err := historyRetireCutoffStep(ctx, tx, br, prune.ArchiveMode, stepSize, forward)
		require.NoError(t, err)
		require.Zero(t, cutoffStep)
		require.Zero(t, commitmentStep)
	})

	t.Run("not enough history accumulated yet -> no cutoff", func(t *testing.T) {
		pm := prune.Mode{Initialised: true, History: prune.Distance(1_000_000)}
		cutoffStep, _, err := historyRetireCutoffStep(ctx, tx, br, pm, stepSize, forward)
		require.NoError(t, err)
		require.Zero(t, cutoffStep)
	})

	t.Run("finite distance -> correct step boundary", func(t *testing.T) {
		// PruneTo(30, Distance(10)) = block 20; Min(20) = txNum 200; step = 20.
		pm := prune.Mode{Initialised: true, History: prune.Distance(10)}
		cutoffStep, _, err := historyRetireCutoffStep(ctx, tx, br, pm, stepSize, forward)
		require.NoError(t, err)
		require.Equal(t, kv.Step(20), cutoffStep)
	})

	t.Run("cutoff rounds down to step 0 -> no cutoff", func(t *testing.T) {
		// PruneTo(30, Distance(29)) = block 1; Min(1) = txNum 10; with a large
		// stepSize that floors to step 0, which must count as "nothing yet".
		pm := prune.Mode{Initialised: true, History: prune.Distance(29)}
		cutoffStep, _, err := historyRetireCutoffStep(ctx, tx, br, pm, 1000, forward)
		require.NoError(t, err)
		require.Equal(t, kv.Step(0), cutoffStep)
	})

	t.Run("commitment distance -> independent commitment cutoff", func(t *testing.T) {
		// Archive general history (cutoff 0) + commitment bounded at distance 10:
		// commitment step = 20, general stays 0.
		pm := prune.ArchiveMode.WithCommitmentHistory(10)
		cutoffStep, commitmentStep, err := historyRetireCutoffStep(ctx, tx, br, pm, stepSize, forward)
		require.NoError(t, err)
		require.Zero(t, cutoffStep)
		require.Equal(t, kv.Step(20), commitmentStep)
	})
}
