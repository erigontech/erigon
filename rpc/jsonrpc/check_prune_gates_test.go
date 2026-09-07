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
	"bytes"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
)

const (
	// pruneGatingReceiptsNarrow is a receipt-cache window narrower than the history
	// one: the cache stops covering a block that re-execution still reaches.
	pruneGatingReceiptsNarrow = prune.Distance(5)
	// pruneGatingReceiptsWide is a receipt-cache window wider than the history one:
	// the cache is the only thing that can answer for the blocks in between.
	pruneGatingReceiptsWide = prune.Distance(15)
	// pruneGatingReceiptsUnstarted is a receipt-cache window wider than the whole
	// test chain: it has pruned nothing yet, so its oldest block is still zero.
	pruneGatingReceiptsUnstarted = prune.Distance(pruneGatingChainLen * 3)
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
			// Cache on with a retention that is a sentinel rather than a window: only
			// an explicit keep-all outlives history, so this one is retired with it.
			name: "cache_sentinel_retention",
			cfg: pruneGatingConfig{mode: prune.Mode{
				Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
				Receipts: prune.KeepPostMergeBlocksPruneMode,
			}, persistReceipts: true},
			served:  historyOldest,
			refused: historyOldest - 1,
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

// TestReceiptsGateFollowsHistoryWhereTheCacheIsNotServed pins the gate against what the
// generator actually does: with receipt assertions on it reads the cached receipt to
// compare it, not to answer, so the block is re-executed and reaches only as far back as
// history whatever the receipt retention says.
//
// Not parallel: it flips a process-wide assertion flag.
func TestReceiptsGateFollowsHistoryWhereTheCacheIsNotServed(t *testing.T) {
	defer func(enabled bool) { dbg.AssertEnabled = enabled }(dbg.AssertEnabled)
	dbg.AssertEnabled = true

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

	require.ErrorIs(t, apis.eth.checkReceiptsAvailable(ctx, tx, 0), state.PrunedError,
		"a cache the generator will not serve does not widen availability")
}

// TestReceiptEndpointsCloseWhenTheCacheIsNotServed is the endpoint counterpart of
// TestReceiptsGateFollowsHistoryWhereTheCacheIsNotServed: a receipt retention outliving
// history opens the block only while the cache is served, and the endpoint has to
// surface the refusal rather than leave it at the gate.
//
// Not parallel: it flips a process-wide assertion flag.
func TestReceiptEndpointsCloseWhenTheCacheIsNotServed(t *testing.T) {
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: prune.KeepAllReceiptsPruneMode,
		},
		persistReceipts: true,
	})
	ctx := t.Context()
	old := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.old.num))

	served, err := apis.eth.GetBlockReceipts(ctx, old)
	require.NoError(t, err)
	require.Len(t, served, 1, "the retention reaches past the history cutoff")

	defer func(enabled bool) { dbg.AssertEnabled = enabled }(dbg.AssertEnabled)
	dbg.AssertEnabled = true

	_, err = apis.eth.GetBlockReceipts(ctx, old)
	require.ErrorIs(t, err, state.PrunedError,
		"without the cache the block is only reachable by re-executing, which history no longer allows")
}

// TestCapabilitiesFollowHistoryWhereTheCacheIsNotServed pins the same premise in the
// advertised boundary: it must not offer blocks the receipt endpoints would refuse.
//
// Not parallel: it flips a process-wide assertion flag.
func TestCapabilitiesFollowHistoryWhereTheCacheIsNotServed(t *testing.T) {
	defer func(enabled bool) { dbg.AssertEnabled = enabled }(dbg.AssertEnabled)
	dbg.AssertEnabled = true

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: prune.KeepAllReceiptsPruneMode,
		},
		persistReceipts: true,
	})

	caps, err := apis.eth.Capabilities(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(*caps.State.OldestBlock), uint64(*caps.Receipts.OldestBlock),
		"receipts reach as far as the re-execution that serves them")
	require.NotZero(t, uint64(*caps.Receipts.OldestBlock))
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

// TestReceiptsGateReadsFrozenBlocksNotStageProgress pins where the "does the datadir
// hold frozen blocks" question is answered: the block reader. The snapshots stage
// progress is no proxy — the stage records the minimum sync progress on a node with
// no snapshot file at all, so reading it makes a fresh chain skip the post-state
// computation and serve pre-Byzantium receipts with status instead of root.
func TestReceiptsGateReadsFrozenBlocksNotStageProgress(t *testing.T) {
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

	rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	require.NoError(t, stages.SaveStageProgress(rwTx, stages.Snapshots, pruneGatingChainLen))
	require.NoError(t, rwTx.Commit())

	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	historyOldest := pruneGatingDistance.PruneTo(pruneGatingChainLen)
	err = apis.eth.checkReceiptsAvailable(ctx, tx, historyOldest-1)
	require.ErrorIs(t, err, state.PrunedError,
		"no snapshot file is on disk, so the post state is still computed and follows history")
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

// TestLogsGateSkipsThePostStateLegPreByzantium pins that a log query does not inherit
// the post-state requirement of a full receipt. getLogsV3 asks for receipts without a
// post state, so the kept cache answers pre-Byzantium blocks that
// eth_getTransactionReceipt has to re-execute. An indexed filter still needs history.
func TestLogsGateSkipsThePostStateLegPreByzantium(t *testing.T) {
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

	below := pruneGatingDistance.PruneTo(pruneGatingChainLen) - 1
	require.Less(t, below, pruneGatingByzantiumHeight, "the probed block must sit below the fork")

	require.NoError(t, apis.eth.checkLogsAvailable(ctx, tx, below, filters.FilterCriteria{}),
		"an unfiltered query reads the kept cache, which carries every field it needs")
	require.ErrorIs(t, apis.eth.checkLogsAvailable(ctx, tx, below, addressFilter(below)), state.PrunedError,
		"an indexed filter searches LogAddrIdx, retired at the history cutoff")
	require.ErrorIs(t, apis.eth.checkReceiptsAvailable(ctx, tx, below), state.PrunedError,
		"a full receipt still needs the post state a re-execution computes")
}

// TestCapabilitiesAgreeWithTheLogsGatePreByzantium pins the advertised side of the
// same split: caps.Logs describes the indexed query, so it stays at the history
// cutoff below the fork, and an unfiltered query is served further back than
// advertised rather than the other way round.
func TestCapabilitiesAgreeWithTheLogsGatePreByzantium(t *testing.T) {
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

	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)
	require.NotNil(t, caps.Logs.OldestBlock)
	oldest := uint64(*caps.Logs.OldestBlock)
	require.Equal(t, pruneGatingDistance.PruneTo(pruneGatingChainLen), oldest)

	require.NoError(t, apis.eth.checkLogsAvailable(ctx, tx, oldest, addressFilter(oldest)),
		"the advertised oldest block must be served")
	require.ErrorIs(t, apis.eth.checkLogsAvailable(ctx, tx, oldest-1, addressFilter(oldest-1)), state.PrunedError,
		"the block below the advertised oldest must be refused")
	require.NoError(t, apis.eth.checkLogsAvailable(ctx, tx, oldest-1, filters.FilterCriteria{}),
		"an unfiltered query reads past the advertised boundary, never short of it")
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
// blocks distance is a sentinel rather than a window: pre-merge transactions are
// never downloaded on a chain that declares a merge point, so the gate must refuse
// below it instead of reading the sentinel as "nothing is pruned".
func TestBlocksGateAppliesChainHistoryExpiry(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: prune.KeepAllBlocksPruneMode,
			Blocks: prune.KeepPostMergeBlocksPruneMode,
		},
		chainConfig:     mergeHeightChainConfig(pruneGatingMergeHeight),
		dropPreMergeTxs: true,
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

// TestGatesTakeNoEmptyBlockExemption pins that a block without transactions is refused
// below the cutoff like any other. Its receipt list is empty and its body says so, but
// serving it would put the gate below the boundary eth_capabilities advertises, for a
// subset of blocks the caller cannot predict.
func TestGatesTakeNoEmptyBlockExemption(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	empty := chainInfo.empty.num
	require.Less(t, empty+1, pruneGatingDistance.PruneTo(pruneGatingChainLen),
		"the empty block and the one above it must sit below the history cutoff")

	require.ErrorIs(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, empty), state.PrunedError)
	require.ErrorIs(t, apis.eth.checkLogsAvailable(ctx, tx, empty, filters.FilterCriteria{}), state.PrunedError)

	for _, tc := range []struct {
		name string
		call func() (any, error)
	}{
		{"eth_getBlockReceipts", func() (any, error) {
			return apis.eth.GetBlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(empty)))
		}},
		{"eth_getLogs_single", func() (any, error) { return apis.eth.GetLogs(ctx, blockFilter(empty)) }},
		{"eth_getLogs_range", func() (any, error) { return apis.eth.GetLogs(ctx, rangeFilter(empty, empty+1)) }},
		{"erigon_getLogs_single", func() (any, error) { return apis.erigon.GetLogs(ctx, blockFilter(empty)) }},
		{"erigon_getLogsByHash", func() (any, error) { return apis.erigon.GetLogsByHash(ctx, chainInfo.empty.hash) }},
		{"ots_getBlockDetails", func() (any, error) { return apis.ots.GetBlockDetails(ctx, rpc.BlockNumber(empty)) }},
		{"graphql_getBlockDetails", func() (any, error) { return apis.graphql.GetBlockDetails(ctx, rpc.BlockNumber(empty)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call()
			require.ErrorIs(t, err, state.PrunedError)
		})
	}

	receipts, err := apis.eth.GetBlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.recent.num)))
	require.NoError(t, err, "above the cutoff the same endpoint answers")
	require.NotNil(t, receipts)
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

// TestCapabilitiesTakeThePreByzantiumRequirement pins the receipts field against the
// fork the gate honours: below Byzantium the receipt carries a post state the cache
// does not store, so those blocks are re-executed and reach only as far as history.
// Advertising them from genesis sends a routing client to a node that refuses them.
func TestCapabilitiesTakeThePreByzantiumRequirement(t *testing.T) {
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

	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)

	oldest := uint64(*caps.Receipts.OldestBlock)
	require.Equal(t, historyOldest, oldest, "below the fork the kept cache does not answer")
	require.NoError(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, oldest))
	require.ErrorIs(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, oldest-1), state.PrunedError)
}

// TestCapabilitiesRenderAWindowThatHasNotStarted pins the retention rendered when
// two policies have the same oldest block: a window wider than the chain has pruned
// nothing yet and reports zero like keep-all, so the oldest blocks alone cannot rank
// them. The category is pruned all the same, and the strategy has to say so.
func TestCapabilitiesRenderAWindowThatHasNotStarted(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: pruneGatingReceiptsUnstarted,
		},
		persistReceipts: true,
	})
	ctx := t.Context()

	require.Zero(t, pruneGatingReceiptsUnstarted.PruneTo(pruneGatingChainLen),
		"the window must not have started pruning for this to test anything")

	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)

	require.Zero(t, uint64(*caps.Receipts.OldestBlock))
	require.NotNil(t, caps.Receipts.DeleteStrategy, "the receipt window still decides the retention")
	require.Equal(t, uint64(pruneGatingReceiptsUnstarted), uint64(caps.Receipts.DeleteStrategy.RetentionBlocks))
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

// TestBlocksGateTellsExpiryFromLegacyArchive pins the shape the stored prune mode
// cannot resolve on its own: an archive datadir kept from before keep-all became the
// Blocks default and an operator asking for chain-history expiry on top of archive
// persist the same sentinel pair, while the downloader reads that pair as expiry and
// omits pre-merge bodies. What is on disk tells them apart.
func TestBlocksGateTellsExpiryFromLegacyArchive(t *testing.T) {
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

	rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	for num := uint64(1); num < pruneGatingMergeHeight; num++ {
		hash, ok, err := apis.eth._blockReader.CanonicalHash(ctx, rwTx, num)
		require.NoError(t, err)
		require.True(t, ok)
		rawdb.DeleteBody(rwTx, hash, num)
	}
	require.NoError(t, rwTx.Commit())

	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	oldest, err := apis.eth._blockReader.MinimumBlockAvailable(ctx, tx)
	require.NoError(t, err)
	require.Equal(t, pruneGatingMergeHeight, oldest, "the fixture must hold no body below the merge point")

	err = apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num)
	require.ErrorIs(t, err, state.PrunedError)
	require.Contains(t, err.Error(), fmt.Sprintf("blocks are available from block %d", pruneGatingMergeHeight))

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, pruneGatingMergeHeight))
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.recent.num))

	_, err = apis.eth.GetBlockByNumber(ctx, rpc.BlockNumber(chainInfo.old.num), false)
	require.ErrorIs(t, err, state.PrunedError, "the endpoints must see the same boundary")
}

// rangeFilter spans two blocks, the shape the single-block helpers cannot express.
func rangeFilter(begin, end uint64) filters.FilterCriteria {
	return filters.FilterCriteria{
		FromBlock: new(big.Int).SetUint64(begin),
		ToBlock:   new(big.Int).SetUint64(end),
	}
}

// noByzantiumChainConfig declares a chain that never reaches Byzantium, so every
// receipt on it carries a post state the cache does not store.
func noByzantiumChainConfig() *chain.Config {
	cfg := pruneGatingChainConfig()
	for _, fork := range []**uint64{
		&cfg.ByzantiumBlock, &cfg.ConstantinopleBlock, &cfg.PetersburgBlock,
		&cfg.IstanbulBlock, &cfg.MuirGlacierBlock, &cfg.BerlinBlock,
	} {
		*fork = nil
	}
	return cfg
}

// TestBlocksGateDoesNotSettleExpiryBeforeBlocksArrive pins that the archive/expiry
// question is resolved only from an observation that answers it. A node whose block
// data has not arrived holds nothing, which is not evidence of an archive datadir and
// must not be recorded as one for the life of the process.
func TestBlocksGateDoesNotSettleExpiryBeforeBlocksArrive(t *testing.T) {
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

	canonicalHash := func(num uint64) common.Hash {
		tx, err := apis.eth.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		hash, ok, err := apis.eth._blockReader.CanonicalHash(ctx, tx, num)
		require.NoError(t, err)
		require.True(t, ok)
		return hash
	}
	preMergeHash := canonicalHash(1)
	preMergeBodyKey := dbutils.BlockBodyKey(1, preMergeHash)

	oldestAvailable := func() uint64 {
		tx, err := apis.eth.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		oldest, err := apis.eth._blockReader.MinimumBlockAvailable(ctx, tx)
		require.NoError(t, err)
		return oldest
	}
	gateOnOldBlock := func() error {
		tx, err := apis.eth.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		return apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num)
	}
	write := func(fn func(rwTx kv.TemporalRwTx)) {
		rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		fn(rwTx)
		require.NoError(t, rwTx.Commit())
	}

	var preMergeBody []byte
	write(func(rwTx kv.TemporalRwTx) {
		value, err := rwTx.GetOne(kv.BlockBody, preMergeBodyKey)
		require.NoError(t, err)
		require.NotEmpty(t, value)
		preMergeBody = bytes.Clone(value)
		for num := uint64(1); num <= pruneGatingChainLen; num++ {
			rawdb.DeleteBody(rwTx, canonicalHash(num), num)
		}
	})
	require.Zero(t, oldestAvailable(), "the fixture must hold no body at all")
	require.ErrorIs(t, gateOnOldBlock(), state.PrunedError,
		"holding no body is not evidence of an archive datadir")

	write(func(rwTx kv.TemporalRwTx) {
		require.NoError(t, rwTx.Put(kv.BlockBody, preMergeBodyKey, preMergeBody))
	})
	require.NoError(t, gateOnOldBlock(),
		"a readable pre-merge block on disk makes the datadir an archive one")
}

// TestCapabilitiesFollowTheResolvedBlocksBoundary pins the blocks field against the
// boundary checkPruneBlocks resolves rather than the stored sentinel: the same prune
// mode means chain history expiry on one datadir and a legacy archive on another.
func TestCapabilitiesFollowTheResolvedBlocksBoundary(t *testing.T) {
	t.Parallel()

	cfg := pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true,
			History:     prune.KeepPostMergeBlocksPruneMode,
			Blocks:      prune.KeepPostMergeBlocksPruneMode,
		},
		chainConfig: mergeHeightChainConfig(pruneGatingMergeHeight),
	}

	t.Run("legacy_archive", func(t *testing.T) {
		t.Parallel()
		apis, _ := setupPruneGating(t, cfg)
		ctx := t.Context()
		tx, err := apis.eth.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()

		caps, err := apis.eth.Capabilities(ctx)
		require.NoError(t, err)
		require.Zero(t, uint64(*caps.Blocks.OldestBlock), "every body is on disk")
		require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, 0))
	})

	t.Run("chain_history_expiry", func(t *testing.T) {
		t.Parallel()
		apis, _ := setupPruneGating(t, cfg)
		ctx := t.Context()

		rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
		require.NoError(t, err)
		defer rwTx.Rollback()
		for num := uint64(1); num < pruneGatingMergeHeight; num++ {
			hash, ok, err := apis.eth._blockReader.CanonicalHash(ctx, rwTx, num)
			require.NoError(t, err)
			require.True(t, ok)
			rawdb.DeleteBody(rwTx, hash, num)
		}
		require.NoError(t, rwTx.Commit())

		caps, err := apis.eth.Capabilities(ctx)
		require.NoError(t, err)
		require.Equal(t, pruneGatingMergeHeight, uint64(*caps.Blocks.OldestBlock))
	})
}

// TestCapabilitiesOmitTheStrategyForKeptReceipts pins that an explicit keep-all
// receipt retention is not rendered as a deletion window: it deletes nothing, and its
// sentinel is not a block count.
func TestCapabilitiesOmitTheStrategyForKeptReceipts(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: prune.KeepAllReceiptsPruneMode,
		},
		persistReceipts: true,
	})

	caps, err := apis.eth.Capabilities(t.Context())
	require.NoError(t, err)
	require.Zero(t, uint64(*caps.Receipts.OldestBlock))
	require.Nil(t, caps.Receipts.DeleteStrategy, "keep-all deletes nothing")
}

// TestBlocksGateAppliesExpiryWhenOldestIsMidChain pins the settled expiry shape: the
// transaction segment spanning the merge point starts below it, so the oldest fully
// available block lands mid-chain while older bodies are still on disk. Data starting
// mid-chain is not evidence of an archive datadir, and the gate must refuse below the
// merge point.
func TestBlocksGateAppliesExpiryWhenOldestIsMidChain(t *testing.T) {
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

	rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	for num := uint64(1); num <= 2; num++ {
		hash, ok, err := apis.eth._blockReader.CanonicalHash(ctx, rwTx, num)
		require.NoError(t, err)
		require.True(t, ok)
		rawdb.DeleteBody(rwTx, hash, num)
	}
	require.NoError(t, rwTx.Commit())

	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	oldest, err := apis.eth._blockReader.MinimumBlockAvailable(ctx, tx)
	require.NoError(t, err)
	require.Greater(t, oldest, uint64(1))
	require.Less(t, oldest, pruneGatingMergeHeight,
		"the oldest available block must land strictly inside the pre-merge range")
	require.Less(t, oldest, chainInfo.old.num, "the probed block's body must still be on disk")

	err = apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num)
	require.ErrorIs(t, err, state.PrunedError)
	require.Contains(t, err.Error(), fmt.Sprintf("blocks are available from block %d", pruneGatingMergeHeight))

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, pruneGatingMergeHeight))
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.recent.num))
}

// TestBlocksGateRequiresPreMergeTransactions pins the other production expiry shape:
// the downloader blacklists only transaction segments, so every pre-merge body stays
// on disk while its transactions are missing. A pre-merge body is not evidence of an
// archive datadir; only a readable early transaction is.
func TestBlocksGateRequiresPreMergeTransactions(t *testing.T) {
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

	rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	txNumMin, err := apis.eth._txNumReader.Min(ctx, rwTx, 1)
	require.NoError(t, err)
	txNumMax, err := apis.eth._txNumReader.Max(ctx, rwTx, pruneGatingMergeHeight-1)
	require.NoError(t, err)
	for txNum := txNumMin; txNum <= txNumMax; txNum++ {
		require.NoError(t, rwTx.Delete(kv.EthTx, hexutil.EncodeTs(txNum)))
	}
	require.NoError(t, rwTx.Commit())

	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	oldest, err := apis.eth._blockReader.MinimumBlockAvailable(ctx, tx)
	require.NoError(t, err)
	require.LessOrEqual(t, oldest, uint64(1), "every body must stay on disk")
	body, _, err := apis.eth._blockReader.Body(ctx, tx, chainInfo.old.hash, chainInfo.old.num)
	require.NoError(t, err)
	require.NotNil(t, body, "the pre-merge body the probe must not trust")

	err = apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num)
	require.ErrorIs(t, err, state.PrunedError)
	require.Contains(t, err.Error(), fmt.Sprintf("blocks are available from block %d", pruneGatingMergeHeight))

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, pruneGatingMergeHeight))
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.recent.num))
}

// TestBlocksGateReopensWhenOlderBlocksArrive pins that a shape read as expiry is not
// settled: snapshot minima are live availability, and older segments opening later
// must reopen the gate. Only the archive observation is final.
func TestBlocksGateReopensWhenOlderBlocksArrive(t *testing.T) {
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

	type rawBody struct{ key, value []byte }
	var saved []rawBody
	rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	for num := uint64(1); num < pruneGatingMergeHeight; num++ {
		hash, ok, err := apis.eth._blockReader.CanonicalHash(ctx, rwTx, num)
		require.NoError(t, err)
		require.True(t, ok)
		key := dbutils.BlockBodyKey(num, hash)
		value, err := rwTx.GetOne(kv.BlockBody, key)
		require.NoError(t, err)
		require.NotEmpty(t, value)
		saved = append(saved, rawBody{key: key, value: bytes.Clone(value)})
		rawdb.DeleteBody(rwTx, hash, num)
	}
	require.NoError(t, rwTx.Commit())

	// The verdict is cached for a short TTL, which is what keeps a widening snapshot
	// set from being read on every request. This test is about the later observation
	// winning, not about how long the previous one lingers.
	apis.eth._preMergeData.SetTTL(0)

	gateOnOldBlock := func() error {
		tx, err := apis.eth.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		return apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num)
	}
	require.ErrorIs(t, gateOnOldBlock(), state.PrunedError,
		"without pre-merge blocks the datadir reads as expiry")

	rwTx, err = apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	for _, body := range saved {
		require.NoError(t, rwTx.Put(kv.BlockBody, body.key, body.value))
	}
	require.NoError(t, rwTx.Commit())

	require.NoError(t, gateOnOldBlock(),
		"pre-merge blocks arriving later must reopen the gate")
}

// TestLogsByHashGateAppliesOnCachedReceipts pins that a cached receipt set is gated
// like an uncached one: availability can move while an entry is still cached, and a
// cache hit must not answer below the advertised boundary.
func TestLogsByHashGateAppliesOnCachedReceipts(t *testing.T) {
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

	roTx, err := apis.erigon.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer roTx.Rollback()
	block, err := apis.erigon.blockByHashWithSenders(ctx, roTx, chainInfo.old.hash)
	require.NoError(t, err)
	require.NotNil(t, block)
	_, err = apis.erigon.getReceipts(ctx, roTx, block)
	require.NoError(t, err)
	roTx.Rollback()
	_, ok := apis.erigon.getCachedReceipts(ctx, chainInfo.old.hash)
	require.True(t, ok, "the premise is a warm block-receipts cache")

	rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	for num := uint64(1); num < pruneGatingMergeHeight; num++ {
		hash, ok, err := apis.eth._blockReader.CanonicalHash(ctx, rwTx, num)
		require.NoError(t, err)
		require.True(t, ok)
		rawdb.DeleteBody(rwTx, hash, num)
	}
	require.NoError(t, rwTx.Commit())

	_, err = apis.erigon.GetLogsByHash(ctx, chainInfo.old.hash)
	require.ErrorIs(t, err, state.PrunedError)
}

// TestCapabilitiesTakeTheNoByzantiumRequirement pins the same pre-Byzantium
// constraint on a chain that never reaches the fork: there every receipt is
// re-executed, so the kept cache never widens what the endpoints serve.
func TestCapabilitiesTakeTheNoByzantiumRequirement(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: prune.KeepAllReceiptsPruneMode,
		},
		persistReceipts: true,
		chainConfig:     noByzantiumChainConfig(),
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	historyOldest := pruneGatingDistance.PruneTo(pruneGatingChainLen)
	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)
	require.Equal(t, historyOldest, uint64(*caps.Receipts.OldestBlock))

	require.NoError(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, historyOldest))
	require.ErrorIs(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, historyOldest-1), state.PrunedError)
}

// TestLogsByBlockHashReportsAMissingBody pins that a block the gate serves but whose
// body is gone is reported as missing rather than answered with an empty log array.
// Turning a missing body into an empty result is only correct where the gate speaks.
func TestLogsByBlockHashReportsAMissingBody(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()

	rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	rawdb.DeleteBody(rwTx, chainInfo.old.hash, chainInfo.old.num)
	require.NoError(t, rwTx.Commit())

	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, apis.eth.checkLogsAvailable(ctx, tx, chainInfo.old.num, filters.FilterCriteria{}),
		"the fixture needs a mode where no gate refuses the block")

	hash := chainInfo.old.hash
	_, err = apis.eth.GetLogs(ctx, filters.FilterCriteria{BlockHash: &hash})
	require.ErrorContains(t, err, "block not found")
}

// TestCapabilitiesDropTheWindowAtTheForkBoundary pins the retention rendered when the
// receipt boundary is a fork height rather than a window: a client computing
// head - retentionBlocks from a window strategy would land far below the oldest block
// the same field advertises.
func TestCapabilitiesDropTheWindowAtTheForkBoundary(t *testing.T) {
	t.Parallel()

	const historyDistance = prune.Distance(8)
	const receiptsDistance = prune.Distance(15)

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: historyDistance, Blocks: prune.KeepAllBlocksPruneMode,
			Receipts: receiptsDistance,
		},
		persistReceipts: true,
		chainConfig:     byzantiumChainConfig(pruneGatingByzantiumHeight),
	})
	ctx := t.Context()

	require.GreaterOrEqual(t, historyDistance.PruneTo(pruneGatingChainLen), pruneGatingByzantiumHeight,
		"history must stay above the fork for the boundary to be pinned to it")
	require.Less(t, receiptsDistance.PruneTo(pruneGatingChainLen), pruneGatingByzantiumHeight,
		"the receipt window must reach below the fork")

	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)

	require.EqualValues(t, pruneGatingByzantiumHeight, uint64(*caps.Receipts.OldestBlock))
	require.Nil(t, caps.Receipts.DeleteStrategy, "a window cannot describe a fork height")
}

// TestBlocksGateServesAChainWithoutPreMergeTransactions pins the verdict on a chain that
// carries no transaction below its merge point: the retentions differ in the pre-merge
// transactions they keep, so with none to keep both hold everything there is and the
// gate has nothing to refuse.
func TestBlocksGateServesAChainWithoutPreMergeTransactions(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true,
			History:     prune.KeepPostMergeBlocksPruneMode,
			Blocks:      prune.KeepPostMergeBlocksPruneMode,
		},
		chainConfig: mergeHeightChainConfig(1),
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, 0),
		"with no pre-merge transaction to be missing the pre-merge blocks are served")
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, 1), "blocks from the merge point are served")
}

// TestBlocksGateResolvesExpiryFromDiskWhateverTheHistory pins that the archive/expiry
// question is answered from the block data on disk whatever the stored history
// retention is. The blocks sentinel alone is persisted both by a legacy archive datadir
// and by chain history expiry, and the history field says nothing about which.
func TestBlocksGateResolvesExpiryFromDiskWhateverTheHistory(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		dropPreMergeTxs bool
		served          bool
	}{
		{name: "pre_merge_transactions_on_disk", served: true},
		{name: "pre_merge_transactions_never_downloaded", dropPreMergeTxs: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
				mode: prune.Mode{
					Initialised: true, History: prune.KeepAllBlocksPruneMode,
					Blocks: prune.KeepPostMergeBlocksPruneMode,
				},
				chainConfig:     mergeHeightChainConfig(pruneGatingMergeHeight),
				dropPreMergeTxs: tc.dropPreMergeTxs,
			})
			ctx := t.Context()
			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			err = apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num)
			if tc.served {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, state.PrunedError)
		})
	}
}

// TestBlocksGateCachesTheVerdictForAShortWhile pins the shape of the archive/expiry
// answer: it reads live availability, so it is remembered briefly rather than settled,
// and a later observation wins once the window is over.
func TestBlocksGateCachesTheVerdictForAShortWhile(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true, History: prune.KeepAllBlocksPruneMode,
			Blocks: prune.KeepPostMergeBlocksPruneMode,
		},
		chainConfig: mergeHeightChainConfig(pruneGatingMergeHeight),
	})
	ctx := t.Context()
	gateOnOldBlock := func() error {
		tx, err := apis.eth.db.BeginTemporalRo(ctx)
		require.NoError(t, err)
		defer tx.Rollback()
		return apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num)
	}

	require.NoError(t, gateOnOldBlock(), "pre-merge transactions on disk read as archive")

	dropTransactions(t, apis.rwDB, 1, pruneGatingMergeHeight)
	require.NoError(t, gateOnOldBlock(), "within the window the remembered verdict answers")

	apis.eth._preMergeData.SetTTL(0)
	require.ErrorIs(t, gateOnOldBlock(), state.PrunedError, "past the window the datadir is read again")
}

// TestBlocksGateSkipsAnEmptySampledBlock pins that a sampled block without transactions
// is passed over rather than read as evidence: it holds no transaction whose absence
// could tell chain history expiry from a legacy archive, and the datadir has others.
func TestBlocksGateSkipsAnEmptySampledBlock(t *testing.T) {
	t.Parallel()

	// Halving this merge height lands the first candidate on the transaction-free block.
	const mergeHeight = 2*pruneGatingEmptyBlockIdx + 2

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{
			Initialised: true,
			History:     prune.KeepPostMergeBlocksPruneMode,
			Blocks:      prune.KeepPostMergeBlocksPruneMode,
		},
		chainConfig: mergeHeightChainConfig(mergeHeight),
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.Equal(t, chainInfo.empty.num, uint64(mergeHeight)/2, "the first sampled candidate must be the empty block")
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num),
		"an empty candidate is skipped, and a later one shows the datadir holds pre-merge transactions")
}

// prunedHistoryTx makes state history look retired above the whole chain. Preparing an
// execution environment reads that boundary before it can build a reader, so this is
// what a block whose receipts have to be re-executed hits on a pruned node.
type prunedHistoryTx struct {
	kv.TemporalTx
}

func (tx prunedHistoryTx) Debug() kv.TemporalDebugTx {
	return prunedHistoryDebugTx{tx.TemporalTx.Debug()}
}

type prunedHistoryDebugTx struct {
	kv.TemporalDebugTx
}

func (prunedHistoryDebugTx) HistoryStartFrom(kv.Domain) uint64 { return math.MaxUint64 }

// TestEmptyBlockReceiptsNeedNoStateHistory pins that a block without transactions is
// answered from its body: there is nothing to derive, so no execution environment is
// prepared and the unavailable state history is never reached. The block carrying
// transactions is the control — it must fail on the same view.
func TestEmptyBlockReceiptsNeedNoStateHistory(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	chainConfig, err := apis.eth.chainConfig(ctx, tx)
	require.NoError(t, err)
	view := prunedHistoryTx{tx}

	empty, err := apis.eth.blockByNumberWithSenders(ctx, tx, chainInfo.empty.num)
	require.NoError(t, err)
	require.NotNil(t, empty)
	require.Empty(t, empty.Transactions())

	receipts, err := apis.eth.receiptsGenerator.GetReceipts(ctx, chainConfig, view, empty, eth.ReceiptsOpts{})
	require.NoError(t, err)
	require.Empty(t, receipts)

	withTxns, err := apis.eth.blockByNumberWithSenders(ctx, tx, chainInfo.old.num)
	require.NoError(t, err)
	require.NotEmpty(t, withTxns.Transactions())

	_, err = apis.eth.receiptsGenerator.GetReceipts(ctx, chainConfig, view, withTxns, eth.ReceiptsOpts{})
	require.ErrorIs(t, err, state.PrunedError, "the control block must reach the unavailable history")
}

// TestFeeHistoryGateTakesTheOldestBlockOfTheRange pins that the reward-percentile gate
// looks at where the requested range starts, not where it ends: a range that reaches
// below the cutoff is refused even when its newest block is retained. The header series
// is served for the same range.
func TestFeeHistoryGateTakesTheOldestBlockOfTheRange(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: prune.KeepAllBlocksPruneMode, Blocks: pruneGatingDistance},
	})
	ctx := t.Context()
	head := chainInfo.head
	oldest := pruneGatingDistance.PruneTo(head)
	retained := rpc.DecimalOrHex(head - oldest + 1)

	_, err := apis.eth.FeeHistory(ctx, retained+1, rpc.BlockNumber(head), []float64{50})
	require.ErrorIs(t, err, state.PrunedError)
	require.Contains(t, err.Error(), "blocks are available")

	_, err = apis.eth.FeeHistory(ctx, retained+1, rpc.BlockNumber(head), nil)
	require.NoError(t, err, "the header series reaches past the blocks cutoff")

	res, err := apis.eth.FeeHistory(ctx, retained, rpc.BlockNumber(head), []float64{50})
	require.NoError(t, err)
	require.Equal(t, oldest, res.OldestBlock.ToInt().Uint64())
}
