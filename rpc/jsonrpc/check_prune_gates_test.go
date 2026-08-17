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
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/rpc/filters"
)

const (
	// pruneGatingReceiptsNarrow is a receipt-cache window narrower than the history
	// one: the cache stops covering a block that re-execution still reaches.
	pruneGatingReceiptsNarrow = prune.Distance(5)
	// pruneGatingReceiptsWide is a receipt-cache window wider than the history one:
	// the cache is the only thing that can answer for the blocks in between.
	pruneGatingReceiptsWide = prune.Distance(15)
)

// TestPruneGateBoundary pins the exact block where each gate flips and which
// boundary its error names. The endpoint table probes blocks far from the
// boundary, so an off-by-one there would pass unnoticed.
func TestPruneGateBoundary(t *testing.T) {
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
	t.Parallel()

	historyOldest := pruneGatingDistance.PruneTo(pruneGatingChainLen)
	narrowOldest := pruneGatingReceiptsNarrow.PruneTo(pruneGatingChainLen)
	wideOldest := pruneGatingReceiptsWide.PruneTo(pruneGatingChainLen)
	require.Greater(t, narrowOldest, historyOldest, "the narrow window must stop inside history")
	require.Less(t, wideOldest, historyOldest, "the wide window must outlive history")

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
			// Cache on with a window of its own wider than history: for the
			// blocks in between it is the only source, so it is what decides.
			name: "cache_window_wider_than_history",
			cfg: pruneGatingConfig{mode: prune.Mode{
				Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
				Receipts: pruneGatingReceiptsWide,
			}, persistReceipts: true},
			served:  wideOldest,
			refused: wideOldest - 1,
		},
		{
			// Cache on with a window narrower than history: past its cutoff the
			// receipts are re-derived by re-executing, so history decides and the
			// narrow cache costs the caller nothing.
			name: "cache_window_narrower_than_history",
			cfg: pruneGatingConfig{mode: prune.Mode{
				Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
				Receipts: pruneGatingReceiptsNarrow,
			}, persistReceipts: true},
			served:  historyOldest,
			refused: historyOldest - 1,
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

// TestUsesLogIndex pins the predicate against getTopicsBitmapV3, which skips
// empty topic positions: a criteria whose every position is empty matches any
// topic and searches no index.
func TestUsesLogIndex(t *testing.T) {
	t.Parallel()

	topicA := common.Hash{0x1}
	for _, tc := range []struct {
		name string
		crit filters.FilterCriteria
		want bool
	}{
		{"no_criteria", filters.FilterCriteria{}, false},
		{"one_address", filters.FilterCriteria{Addresses: []common.Address{testAddr}}, true},
		{"no_topic_position", filters.FilterCriteria{Topics: [][]common.Hash{}}, false},
		{"one_empty_position", filters.FilterCriteria{Topics: [][]common.Hash{{}}}, false},
		{"two_empty_positions", filters.FilterCriteria{Topics: [][]common.Hash{{}, {}}}, false},
		{"one_filled_position", filters.FilterCriteria{Topics: [][]common.Hash{{topicA}}}, true},
		{"filled_after_empty", filters.FilterCriteria{Topics: [][]common.Hash{{}, {topicA}}}, true},
		{"address_with_empty_position", filters.FilterCriteria{
			Addresses: []common.Address{testAddr}, Topics: [][]common.Hash{{}},
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, usesLogIndex(tc.crit))
		})
	}
}

// TestLogsGateTakesHistoryOnlyForIndexSearch pins that the history leg of the log
// gate follows the index search and not the mere presence of a topics field. The
// cache is kept forever here, so history is the only boundary that can reject.
func TestLogsGateTakesHistoryOnlyForIndexSearch(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
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

	topicA := common.Hash{0x1}
	for _, tc := range []struct {
		name  string
		crit  filters.FilterCriteria
		fires bool
	}{
		{"unfiltered", filters.FilterCriteria{}, false},
		{"empty_topic_position", filters.FilterCriteria{Topics: [][]common.Hash{{}}}, false},
		{"by_topic", filters.FilterCriteria{Topics: [][]common.Hash{{topicA}}}, true},
		{"by_address", filters.FilterCriteria{Addresses: []common.Address{testAddr}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := apis.eth.checkLogsAvailable(ctx, tx, chainInfo.old.num, tc.crit)
			if tc.fires {
				require.ErrorIs(t, err, state.PrunedError)
				require.Contains(t, err.Error(), "history is available")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestLogsGateRequiresBlockBodies pins the blocks leg of the log gate: serving a
// log means deriving its receipt from the block's transaction, so a pruned body
// makes the query unanswerable however long the receipts are kept.
func TestLogsGateRequiresBlockBodies(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: pruneGatingDistance,
			Receipts: prune.KeepAllReceiptsPruneMode,
		},
		persistReceipts: true,
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	err = apis.eth.checkLogsAvailable(ctx, tx, chainInfo.old.num, filters.FilterCriteria{})
	require.ErrorIs(t, err, state.PrunedError)
	require.Contains(t, err.Error(), "blocks are available")
}

// TestCheckTxFee pins the fee cap: the fee is gasPrice*gas in wei, compared
// against a cap expressed in ether, and a zero cap disables the check.
func TestCheckTxFee(t *testing.T) {
	t.Parallel()

	oneEtherGasPrice := new(big.Int).Div(big.NewInt(common.Ether), big.NewInt(21000))
	for _, tc := range []struct {
		name     string
		gasPrice *big.Int
		gas      uint64
		gasCap   float64
		wantErr  bool
	}{
		{"no_cap", oneEtherGasPrice, 21000, 0, false},
		{"under_cap", oneEtherGasPrice, 21000, 2, false},
		{"at_cap", oneEtherGasPrice, 21000, 1, false},
		{"over_cap", oneEtherGasPrice, 42000, 1, true},
		{"zero_fee", big.NewInt(0), 21000, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := checkTxFee(tc.gasPrice, tc.gas, tc.gasCap)
			if tc.wantErr {
				require.ErrorContains(t, err, "exceeds the configured cap")
			} else {
				require.NoError(t, err)
			}
		})
	}
}
