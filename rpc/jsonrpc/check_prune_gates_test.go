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

package jsonrpc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/state"
)

// pruneGatingReceiptsDistance is a receipt-cache window narrower than the
// history one, so a block can sit inside history and outside the receipts.
const pruneGatingReceiptsDistance = prune.Distance(5)

// TestPruneGateBoundary pins the exact block where each gate flips and which
// boundary its error names. The endpoint table probes blocks far from the
// boundary, so an off-by-one there would pass unnoticed.
func TestPruneGateBoundary(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: pruneGatingDistance},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	oldest := pruneGatingDistance.PruneTo(chainInfo.head)
	require.NotZero(t, oldest, "the chain must outgrow the prune distance for this to test anything")

	for _, tc := range []struct {
		name     string
		gate     func(block uint64) error
		boundary string
	}{
		{"blocks", func(b uint64) error { return apis.eth.checkPruneBlocks(ctx, tx, b) }, "blocks are available"},
		{"history", func(b uint64) error { return apis.eth.checkPruneHistory(ctx, tx, b) }, "history is available"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.gate(oldest), "the oldest retained block is served")

			err := tc.gate(oldest - 1)
			require.ErrorIs(t, err, state.PrunedError)
			require.Contains(t, err.Error(), tc.boundary, "the error must name the boundary that rejected")
		})
	}
}

// TestPruneGateArchive pins that an archive node never gates, including at
// genesis: its distances are sentinels that report themselves as disabled.
func TestPruneGateArchive(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, 0))
	require.NoError(t, apis.eth.checkPruneHistory(ctx, tx, 0))
	require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, tx, 0))
}

// TestReceiptsGateFollowsRetention pins checkReceiptsAvailable against the
// retention actually applied to the receipt cache. Enabling the cache says
// only that it exists on disk, not how much of it is kept: RCacheDomain is
// retired on its own --prune.receipts.distance window when one is set, and on
// the history window otherwise.
func TestReceiptsGateFollowsRetention(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Parallel()

	historyOldest := pruneGatingDistance.PruneTo(pruneGatingChainLen)
	receiptsOldest := pruneGatingReceiptsDistance.PruneTo(pruneGatingChainLen)
	require.Greater(t, receiptsOldest, historyOldest, "the receipt window must be the narrower one")

	for _, tc := range []struct {
		name    string
		cfg     pruneGatingConfig
		served  uint64 // oldest block the receipts of which must be served
		refused uint64 // newest block the receipts of which must be refused
	}{
		{
			// No cache: receipts are re-derived by re-executing, so they
			// follow the history window.
			name:    "no_cache",
			cfg:     pruneGatingConfig{mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode}},
			served:  historyOldest,
			refused: historyOldest - 1,
		},
		{
			// Cache on, no window of its own: it is retired alongside
			// history, so enabling it widens nothing.
			name:    "cache_follows_history",
			cfg:     pruneGatingConfig{mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode}, persistReceipts: true},
			served:  historyOldest,
			refused: historyOldest - 1,
		},
		{
			// Cache on with a narrower window of its own: receipts go
			// before history does.
			name: "cache_own_window",
			cfg: pruneGatingConfig{mode: prune.Mode{
				Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
				Receipts: pruneGatingReceiptsDistance,
			}, persistReceipts: true},
			served:  receiptsOldest,
			refused: receiptsOldest - 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			apis, _ := setupPruneGating(t, tc.cfg)
			ctx := t.Context()
			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, tx, tc.served))
			require.ErrorIs(t, apis.eth.checkReceiptsAvailable(ctx, tx, tc.refused), state.PrunedError)
		})
	}
}

// TestReceiptsGateKeepAll pins the one shape where enabling the cache does
// widen availability: an explicit keep-all retires nothing, so receipts
// outlive the history they would otherwise be re-derived from.
func TestReceiptsGateKeepAll(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: prune.KeepAllReceiptsPruneMode,
		},
		persistReceipts: true,
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, tx, 0))
	require.ErrorIs(t, apis.eth.checkPruneHistory(ctx, tx, 0), state.PrunedError,
		"history is still pruned; only the receipts survive")
}

// TestBlockReceiptsGateCombinesBothBoundaries pins that the composed gate rejects
// on either leg, and in particular that the blocks leg does the work where the
// receipts one would not: with the cache kept forever, only the missing body
// stands between the caller and an answer.
func TestBlockReceiptsGateCombinesBothBoundaries(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}
	t.Parallel()

	for _, tc := range []struct {
		name  string
		cfg   pruneGatingConfig
		fires bool
	}{
		{
			// Bodies kept, receipts kept forever: nothing is missing.
			name: "both_legs_pass",
			cfg: pruneGatingConfig{mode: prune.Mode{
				Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
				Receipts: prune.KeepAllReceiptsPruneMode,
			}, persistReceipts: true},
			fires: false,
		},
		{
			// Bodies kept, no cache: the receipts leg rejects on history.
			name: "receipts_leg_rejects",
			cfg: pruneGatingConfig{mode: prune.Mode{
				Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			}},
			fires: true,
		},
		{
			// Receipts kept forever but the body is gone: only the blocks leg
			// can catch this, which is why the gate composes the two.
			name: "blocks_leg_rejects",
			cfg: pruneGatingConfig{mode: prune.Mode{
				Initialised: true, History: pruneGatingDistance, Blocks: pruneGatingDistance,
				Receipts: prune.KeepAllReceiptsPruneMode,
			}, persistReceipts: true},
			fires: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			apis, chainInfo := setupPruneGating(t, tc.cfg)
			ctx := t.Context()
			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			err = apis.eth.checkBlockReceiptsAvailable(ctx, tx, chainInfo.old.num)
			if tc.fires {
				require.ErrorIs(t, err, state.PrunedError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
