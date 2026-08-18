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
	"fmt"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	bortypes "github.com/erigontech/erigon/polygon/bor/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpccfg"
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

// TestReceiptsGateFollowsHistoryWherePostStateIsComputed pins the receipt paths that
// bypass the cache: a pre-Byzantium receipt carries a post state the cache does not
// store, so it is always re-executed and only history can answer for it — whatever
// the receipt retention says.
func TestReceiptsGateFollowsHistoryWherePostStateIsComputed(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: prune.KeepAllReceiptsPruneMode,
		},
		persistReceipts: true,
		chainConfig:     byzantiumChainConfig(pruneGatingByzantiumHeight),
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	historyOldest := pruneGatingDistance.PruneTo(pruneGatingChainLen)
	require.Less(t, historyOldest, pruneGatingByzantiumHeight, "history must reach below the fork for this to test anything")

	err = apis.eth.checkReceiptsAvailable(ctx, tx, historyOldest-1)
	require.ErrorIs(t, err, state.PrunedError, "a pre-Byzantium receipt below history cannot be re-executed")
	require.Contains(t, err.Error(), "history is available")

	require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, tx, historyOldest),
		"a pre-Byzantium receipt inside history is re-executed")
	require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, tx, pruneGatingByzantiumHeight+1),
		"from the fork on, the kept cache answers")
}

// TestBorStateSyncReceiptFollowsHistory pins the other cache-bypassing path: the
// synthetic state sync receipt is reconstructed from the state at the end of the
// block, so a block carrying events needs history however long receipts are kept.
func TestBorStateSyncReceiptFollowsHistory(t *testing.T) {
	t.Parallel()

	api, chainInfo := setupBorStateSyncGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: prune.KeepAllReceiptsPruneMode,
		},
		persistReceipts: true,
	}, pruneGatingChainLen, &types.Message{})
	ctx := t.Context()

	_, err := api.GetBlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.old.num)))
	require.ErrorIs(t, err, state.PrunedError)
	require.Contains(t, err.Error(), "history is available")
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

// TestBlocksGateServesLegacyArchive pins the shape that must not be read as chain
// history expiry: an archive datadir stored before keep-all became the default keeps
// the same sentinel in Blocks, but holds every body. The stored mode is what the RPC
// layer reads, and EnsureNotChanged corrects that shape in memory without rewriting
// it, so the gate has to tell the two apart by History carrying the sentinel too.
func TestBlocksGateServesLegacyArchive(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true,
			History:     prune.KeepPostMergeBlocksPruneMode,
			Blocks:      prune.KeepPostMergeBlocksPruneMode,
		},
		chainConfig: mergeHeightChainConfig(pruneGatingMergeHeight),
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, 0), "an archive node holds every body")
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num))
	require.NoError(t, apis.eth.checkPruneHistory(ctx, tx, 0))

	_, err = apis.eth.GetBlockByNumber(ctx, rpc.BlockNumber(chainInfo.old.num), false)
	require.NoError(t, err)
}

// TestBlocksGateAppliesChainHistoryExpiry pins the legacy full shape, where the
// blocks distance is a sentinel rather than a window: pre-merge bodies are never
// downloaded on a chain that declares a merge point, so the gate must refuse below
// it instead of reading the sentinel as "nothing is pruned".
func TestBlocksGateAppliesChainHistoryExpiry(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: prune.KeepAllBlocksPruneMode,
			Blocks: prune.KeepPostMergeBlocksPruneMode,
		},
		chainConfig: mergeHeightChainConfig(pruneGatingMergeHeight),
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	err = apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num)
	require.ErrorIs(t, err, state.PrunedError)
	require.Contains(t, err.Error(), fmt.Sprintf("blocks are available from block %d", pruneGatingMergeHeight))

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, pruneGatingMergeHeight),
		"the merge block itself is served")
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.recent.num))

	_, err = apis.eth.GetBlockByNumber(ctx, rpc.BlockNumber(chainInfo.old.num), false)
	require.ErrorIs(t, err, state.PrunedError, "the endpoints must see the same boundary")
}

// TestLogsByBlockHashNamesThePruneBoundary pins that a filter pinned to a block
// hash reports pruning rather than a missing block: the range is resolved from the
// retained header, so the gate speaks before any body is read.
func TestLogsByBlockHashNamesThePruneBoundary(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: pruneGatingDistance},
	})
	ctx := t.Context()

	// setupPruneGating stores the prune mode without pruning, so the body a pruned
	// node would no longer hold has to be removed here.
	rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	rawdb.DeleteBody(rwTx, chainInfo.old.hash, chainInfo.old.num)
	require.NoError(t, rwTx.Commit())

	hash := chainInfo.old.hash
	for _, tc := range []struct {
		name string
		call func() (any, error)
	}{
		{"eth_getLogs", func() (any, error) {
			return apis.eth.GetLogs(ctx, filters.FilterCriteria{BlockHash: &hash})
		}},
		{"overlay_getLogs", func() (any, error) {
			return apis.overlay.GetLogs(ctx, filters.FilterCriteria{BlockHash: &hash}, nil, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call()
			require.ErrorIs(t, err, state.PrunedError)
			require.Contains(t, err.Error(), "blocks are available")
		})
	}
}

// TestBlockHistoryGateCombinesBothBoundaries pins that the composed gate rejects on
// either leg and names the one that rejected, including the boundary block itself.
// Each leg is measured with the other kept in full, so neither can mask the other.
func TestBlockHistoryGateCombinesBothBoundaries(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		mode     prune.Mode
		boundary string
	}{
		{
			name: "both_legs_pass",
			mode: prune.Mode{Initialised: true, History: prune.KeepAllBlocksPruneMode, Blocks: prune.KeepAllBlocksPruneMode},
		},
		{
			name:     "blocks_leg_rejects",
			mode:     prune.Mode{Initialised: true, History: prune.KeepAllBlocksPruneMode, Blocks: pruneGatingDistance},
			boundary: "blocks are available",
		},
		{
			name:     "history_leg_rejects",
			mode:     prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode},
			boundary: "history is available",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: tc.mode})
			ctx := t.Context()
			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			err = apis.eth.checkBlockHistoryAvailable(ctx, tx, chainInfo.old.num)
			if tc.boundary == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, state.PrunedError)
			require.Contains(t, err.Error(), tc.boundary, "the error must name the leg that rejected")

			oldest := pruneGatingDistance.PruneTo(chainInfo.head)
			require.NoError(t, apis.eth.checkBlockHistoryAvailable(ctx, tx, oldest),
				"the oldest retained block is served")
		})
	}
}

// TestReplayLogEndpointsRequireBlockBodies pins the blocks leg of the log
// endpoints that re-execute: they read each transaction to replay it, so a
// pruned body leaves them answering with a silently incomplete result.
func TestReplayLogEndpointsRequireBlockBodies(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: prune.KeepAllBlocksPruneMode, Blocks: pruneGatingDistance,
		},
	})
	ctx := t.Context()

	for _, tc := range []struct {
		name string
		call func(block uint64) (any, error)
	}{
		{"erigon_getLatestLogs", func(block uint64) (any, error) {
			return apis.erigon.GetLatestLogs(ctx, blockFilter(block), filters.LogFilterOptions{LogCount: 10})
		}},
		{"overlay_getLogs", func(block uint64) (any, error) {
			return apis.overlay.GetLogs(ctx, blockFilter(block), nil, nil)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call(chainInfo.old.num)
			require.ErrorIs(t, err, state.PrunedError)
			require.Contains(t, err.Error(), "blocks are available")

			_, err = tc.call(chainInfo.recent.num)
			require.NoError(t, err, "a retained body is served even with history kept in full")
		})
	}
}

// TestBlockReceiptsGateServesEmptyBlocks pins that a block with no transactions
// is served from its body: it has no receipt to read, so neither the receipt
// cache nor state history is consulted.
func TestBlockReceiptsGateServesEmptyBlocks(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
		},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, chainInfo.empty.num))

	err = apis.eth.checkBlockReceiptsAvailable(ctx, tx, chainInfo.old.num)
	require.ErrorIs(t, err, state.PrunedError, "a block carrying transactions still needs its receipts")

	receipts, err := apis.eth.GetBlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.empty.num)))
	require.NoError(t, err)
	require.Empty(t, receipts)
}

// borStateSyncChainConfig puts the state sync txn lookup on the bridge instead of
// the block bodies. The rules name stays ethash to match the faker the tester
// picks for Bor.
func borStateSyncChainConfig() *chain.Config {
	cfg := pruneGatingChainConfig()
	cfg.Bor = &borcfg.BorConfig{}
	return cfg
}

// byzantiumChainConfig moves Byzantium and every later fork to at, so the blocks
// below it carry a post state the receipt cache does not store.
func byzantiumChainConfig(at uint64) *chain.Config {
	cfg := pruneGatingChainConfig()
	for _, fork := range []**uint64{
		&cfg.ByzantiumBlock, &cfg.ConstantinopleBlock, &cfg.PetersburgBlock,
		&cfg.IstanbulBlock, &cfg.MuirGlacierBlock, &cfg.BerlinBlock,
	} {
		*fork = &at
	}
	return cfg
}

// mergeHeightChainConfig declares a merge point, which is what turns
// KeepPostMergeBlocksPruneMode from a no-op into chain history expiry.
func mergeHeightChainConfig(height uint64) *chain.Config {
	cfg := pruneGatingChainConfig()
	cfg.MergeHeight = &height
	return cfg
}

// pruneGatingChainConfig mirrors TestChainBerlinConfig, which cannot be copied
// because chain.Config carries a sync.Once.
func pruneGatingChainConfig() *chain.Config {
	return &chain.Config{
		ChainID:               uint256.NewInt(1337),
		Rules:                 chain.EtHashRules,
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(0),
		ByzantiumBlock:        common.NewUint64(0),
		ConstantinopleBlock:   common.NewUint64(0),
		PetersburgBlock:       common.NewUint64(0),
		IstanbulBlock:         common.NewUint64(0),
		MuirGlacierBlock:      common.NewUint64(0),
		BerlinBlock:           common.NewUint64(0),
		Ethash:                new(chain.EthashConfig),
	}
}

// setupBorStateSyncGating builds a Bor chain and an eth API whose bridge resolves
// one state sync txn hash to stateSyncBlock. Such a txn is not part of any block
// body, so the regular txn lookup misses it and only the bridge can place it.
func setupBorStateSyncGating(t *testing.T, cfg pruneGatingConfig, stateSyncBlock uint64, events ...*types.Message) (*APIImpl, pruneGatingChain) {
	t.Helper()
	chainConfig := cfg.chainConfig
	if chainConfig == nil {
		chainConfig = borStateSyncChainConfig()
	}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chainConfig,
			Alloc:  types.GenesisAlloc{testAddr: {Balance: big.NewInt(1_000_000_000)}},
		}),
		execmoduletester.WithKey(testKey),
	)
	c, err := m.GenerateChain(pruneGatingChainLen, func(i int, block *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))

	tx, err := m.DB.BeginTemporalRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = prune.EnsureNotChanged(tx, cfg.mode)
	require.NoError(t, err)
	if cfg.persistReceipts {
		require.NoError(t, kvcfg.PersistReceipts.ForceWrite(tx, true))
	}
	require.NoError(t, tx.Commit())

	bridge := mockBridgeReader{stateSyncBlock: stateSyncBlock, stateSyncFound: true, events: events}
	base := NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, bridge, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	ref := func(idx int) pruneGatingRef {
		b := c.Blocks[idx]
		return pruneGatingRef{num: b.NumberU64(), hash: b.Hash()}
	}
	return newEthApiForTest(base, m.DB, nil, nil), pruneGatingChain{
		head:   pruneGatingChainLen,
		old:    ref(pruneGatingOldBlockIdx),
		recent: ref(pruneGatingChainLen - 1),
	}
}

// TestEmptyBlockBypassExcludesBor pins the Bor exclusion of the empty-block bypass:
// a state sync receipt is reconstructed from state history, so a Bor block carrying
// no transactions is still unanswerable below the history boundary.
func TestEmptyBlockBypassExcludesBor(t *testing.T) {
	t.Parallel()

	api, chainInfo := setupBorStateSyncGating(t, pruneGatingConfig{mode: prune.Mode{
		Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
	}}, pruneGatingChainLen)
	ctx := t.Context()
	tx, err := api.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	empty, err := api.blockHasNoReceipts(ctx, tx, chainInfo.old.num)
	require.NoError(t, err)
	require.False(t, empty, "the chain carries no transactions at all; only Bor keeps the gate on")

	err = api.checkBlockReceiptsAvailable(ctx, tx, chainInfo.old.num)
	require.ErrorIs(t, err, state.PrunedError)
	require.Contains(t, err.Error(), "history is available")
}

// TestTransactionReceiptGatesResolvedBorStateSyncBlock pins that the gate runs on
// the block the state sync txn actually belongs to. Its hash is absent from every
// body, so gating before the bridge resolves it measures the genesis block and
// refuses every state sync receipt on a body-pruned node.
func TestTransactionReceiptGatesResolvedBorStateSyncBlock(t *testing.T) {
	t.Parallel()

	mode := prune.Mode{Initialised: true, History: prune.KeepAllBlocksPruneMode, Blocks: pruneGatingDistance}

	t.Run("retained_block_is_served", func(t *testing.T) {
		t.Parallel()
		api, chainInfo := setupBorStateSyncGating(t, pruneGatingConfig{mode: mode}, pruneGatingChainLen-1)
		_, err := api.GetTransactionReceipt(t.Context(), bortypes.ComputeBorTxHash(chainInfo.recent.num, chainInfo.recent.hash))
		require.NotErrorIs(t, err, state.PrunedError)
	})

	t.Run("pruned_block_names_itself", func(t *testing.T) {
		t.Parallel()
		api, chainInfo := setupBorStateSyncGating(t, pruneGatingConfig{mode: mode}, uint64(pruneGatingOldBlockIdx+1))
		_, err := api.GetTransactionReceipt(t.Context(), bortypes.ComputeBorTxHash(chainInfo.old.num, chainInfo.old.hash))
		require.ErrorIs(t, err, state.PrunedError)
		require.Contains(t, err.Error(), fmt.Sprintf("requested block %d", chainInfo.old.num))
	})
}

// TestCapabilitiesAgreeWithGates pins eth_capabilities against the gates it
// describes: for every prune shape the oldest block a field advertises must be
// exactly the block where the matching gate stops refusing. Advertising more than
// the gate serves sends a caller after data it will be refused.
func TestCapabilitiesAgreeWithGates(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	for _, cfg := range pruneGatingConfigs {
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()
			apis, chainInfo := setupPruneGating(t, cfg)
			ctx := t.Context()
			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			caps, err := apis.eth.Capabilities(ctx)
			require.NoError(t, err)

			for _, pair := range []struct {
				name  string
				field CapabilityField
				gate  func(block uint64) error
			}{
				{"state", caps.State, func(b uint64) error { return apis.eth.checkPruneHistory(ctx, tx, b) }},
				{"blocks", caps.Blocks, func(b uint64) error { return apis.eth.checkPruneBlocks(ctx, tx, b) }},
				{"tx", caps.Tx, func(b uint64) error { return apis.eth.checkPruneBlocks(ctx, tx, b) }},
				{"receipts", caps.Receipts, func(b uint64) error {
					return apis.eth.checkBlockReceiptsAvailable(ctx, tx, b)
				}},
				{"logs", caps.Logs, func(b uint64) error {
					return apis.eth.checkLogsAvailable(ctx, tx, b, addressFilter(b))
				}},
			} {
				t.Run(pair.name, func(t *testing.T) {
					require.False(t, pair.field.Disabled)
					require.NotNil(t, pair.field.OldestBlock)
					oldest := uint64(*pair.field.OldestBlock)

					require.NoError(t, pair.gate(oldest), "the advertised oldest block must be served")
					if chainInfo.old.num >= oldest {
						require.NoError(t, pair.gate(chainInfo.old.num),
							"every block above the advertised oldest must be served")
					}
					if oldest == 0 {
						return
					}
					require.NotEqual(t, chainInfo.empty.num, oldest-1,
						"a transaction-free block is served by the receipt gate whatever the retention")
					require.ErrorIs(t, pair.gate(oldest-1), state.PrunedError,
						"the block below the advertised oldest must be refused")
				})
			}
		})
	}
}

// TestCapabilitiesAdvertiseTheReceiptWindow pins the shape the endpoint table does not
// carry: a receipt cache with a finite window of its own, wider than history. The
// receipts field must advertise that window and render it, while a filtered log query
// still stops at history.
func TestCapabilitiesAdvertiseTheReceiptWindow(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: pruneGatingReceiptsWide,
		},
		persistReceipts: true,
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	wideOldest := pruneGatingReceiptsWide.PruneTo(pruneGatingChainLen)
	historyOldest := pruneGatingDistance.PruneTo(pruneGatingChainLen)

	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)

	require.Equal(t, wideOldest, uint64(*caps.Receipts.OldestBlock))
	require.NotNil(t, caps.Receipts.DeleteStrategy)
	require.Equal(t, uint64(pruneGatingReceiptsWide), uint64(caps.Receipts.DeleteStrategy.RetentionBlocks),
		"the window that decides must be the one rendered")

	require.Equal(t, historyOldest, uint64(*caps.Logs.OldestBlock),
		"a filtered log query searches the indices, which follow history")

	require.NoError(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, wideOldest))
	require.ErrorIs(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, wideOldest-1), state.PrunedError)
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
