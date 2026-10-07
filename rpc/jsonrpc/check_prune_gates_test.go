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
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpchelper"
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
		{"state", func(b uint64) error { return apis.eth.checkPruneState(ctx, tx, b) }, "history is available"},
		{"state_after_system_tx", func(b uint64) error { return apis.eth.checkPruneStateAfterSystemTx(ctx, tx, b) }, "history is available"},
		{"replay", func(b uint64) error { return apis.eth.checkPruneTransactionHistory(ctx, tx, b) }, "history is available"},
		{"indexed_history", func(b uint64) error { return apis.eth.checkPruneTransactionHistoryAtIndex(ctx, tx, b, 1) }, "history is available"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.gate(oldest), "the oldest retained block is served")

			err := tc.gate(oldest - 1)
			require.ErrorIs(t, err, state.ErrPruned)
			require.Contains(t, err.Error(), tc.boundary, "the error must name the boundary that rejected")
		})
	}
}

// TestCallBundleGatesTxBlockAndStateBlockSeparately pins the two blocks eth_callBundle
// reads apart: the bundle's transactions come from the bodies of the blocks that hold
// them, the state they run against comes from the block the caller names. A retention
// that took away one of the two refuses only that one.
func TestCallBundleGatesTxBlockAndStateBlockSeparately(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode},
	})
	ctx := t.Context()

	_, err := apis.eth.CallBundle(ctx, []common.Hash{chainInfo.old.txHash},
		rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.recent.num)), nil)
	require.NoError(t, err, "the body of the old block is kept and the state asked for is inside the window")

	_, err = apis.eth.CallBundle(ctx, []common.Hash{chainInfo.recent.txHash},
		rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.old.num)), nil)
	require.ErrorIs(t, err, state.ErrPruned, "the state asked for is outside the history window")
}

// TestPruneGateArchive pins that an archive node with complete on-disk history
// accepts genesis even though physical-floor checks apply to every mode.
func TestPruneGateArchive(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, 0))
	require.NoError(t, apis.eth.checkPruneHistory(ctx, tx, 0))
	require.NoError(t, apis.eth.checkPruneState(ctx, tx, 0))
	require.NoError(t, apis.eth.checkPruneStateAfterSystemTx(ctx, tx, 0))
	require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, tx, 0))
}

func TestBlocksGateIncludesGenesisWithoutSnapshots(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	oldest, err := apis.eth._blockReader.MinimumBlockAvailable(ctx, tx)
	require.NoError(t, err)
	require.Zero(t, oldest, "genesis and block 1 form a contiguous retained range")
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, 0))
}

func TestBlockFloorPreservesReaderBoundary(t *testing.T) {
	t.Parallel()
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	apis.eth._blockReader = &fixedMinimumBlockReader{FullBlockReader: apis.eth._blockReader, floor: 1}
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	floor, err := apis.eth.minimumBlockAvailable(ctx, tx, chainInfo.head)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor, "the RPC cache must not reinterpret the reader's boundary")
}

func TestPruneGatesUsePhysicalFloorsForUnboundedModes(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, 8)
	require.NoError(t, err)
	view := historyFloorTx{TemporalTx: tx, startTxNum: startTxNum}
	apis.eth._blockReader = &fixedMinimumBlockReader{FullBlockReader: apis.eth._blockReader, floor: 9}

	require.ErrorIs(t, apis.eth.checkPruneHistory(ctx, view, 7), state.ErrPruned)
	require.NoError(t, apis.eth.checkPruneState(ctx, view, 7))
	require.ErrorIs(t, apis.eth.checkPruneState(ctx, view, 6), state.ErrPruned)
	require.ErrorIs(t, apis.eth.checkPruneBlocks(ctx, view, 8), state.ErrPruned)
}

func TestBlocksGateUsesPhysicalFloorWithChainHistoryPolicy(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: prune.KeepPostMergeBlocksPruneMode},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	apis.eth._blockReader = &fixedMinimumBlockReader{FullBlockReader: apis.eth._blockReader, floor: 9}

	require.ErrorIs(t, apis.eth.checkPruneBlocks(ctx, tx, 8), state.ErrPruned)
}

func TestStateGateMatchesReaderAtHistoryStart(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const floorBlock = uint64(8)
	startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, floorBlock)
	require.NoError(t, err)
	view := historyFloorTx{TemporalTx: tx, startTxNum: startTxNum}

	reader, err := rpchelper.CreateUncachedStateReaderFromBlockNumber(ctx, view, floorBlock-1, false, -1, apis.eth._txNumReader)
	require.NoError(t, err)
	account, err := reader.ReadAccountData(accounts.InternAddress(testAddr))
	require.NoError(t, err)
	require.NotNil(t, account)
	_, err = rpchelper.CreateUncachedStateReaderFromBlockNumber(ctx, view, floorBlock-2, false, -1, apis.eth._txNumReader)
	require.ErrorIs(t, err, state.ErrPruned)

	apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, startTxNum: startTxNum}
	block := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(floorBlock - 1))
	balance, err := apis.eth.GetBalance(ctx, testAddr, &block)
	require.NoError(t, err, "the RPC must serve the oldest state accepted by its reader")
	require.Equal(t, (*hexutil.U256)(&account.Balance), balance)
	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)
	require.Equal(t, floorBlock-1, uint64(*caps.State.OldestBlock))
	require.Equal(t, floorBlock, uint64(*caps.Receipts.OldestBlock))
	require.Equal(t, floorBlock, uint64(*caps.Logs.OldestBlock))

	block = rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(floorBlock - 2))
	_, err = apis.eth.GetBalance(ctx, testAddr, &block)
	require.ErrorIs(t, err, state.ErrPruned)
	require.Contains(t, err.Error(), fmt.Sprintf("history is available from block %d", floorBlock-1))
}

func TestReplayGateMatchesReaderAtHistoryStart(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		txOffset   uint64
		firstBlock uint64
	}{
		{"first_user_transaction", 1, 8},
		{"after_first_user_transaction", 2, 9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wide := prune.Distance(pruneGatingChainLen * 3)
			apis, _ := setupPruneGating(t, pruneGatingConfig{
				mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
			})
			ctx := t.Context()
			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, 8)
			require.NoError(t, err)
			startTxNum += tc.txOffset
			view := historyFloorTx{TemporalTx: tx, startTxNum: startTxNum}

			reader, err := rpchelper.CreateHistoryStateReader(ctx, view, tc.firstBlock, 0, apis.eth._txNumReader)
			require.NoError(t, err)
			account, err := reader.ReadAccountData(accounts.InternAddress(testAddr))
			require.NoError(t, err)
			require.NotNil(t, account)
			_, err = rpchelper.CreateHistoryStateReader(ctx, view, tc.firstBlock-1, 0, apis.eth._txNumReader)
			require.ErrorIs(t, err, state.ErrPruned)

			apis.trace.kv = historyFloorDB{TemporalRoDB: apis.trace.kv, startTxNum: startTxNum}
			block := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(tc.firstBlock))
			traces, err := apis.trace.ReplayBlockTransactions(ctx, block, []string{TraceTypeTrace}, nil, &tracersConfig.TraceConfig{})
			require.NoError(t, err, "replay must accept the oldest block supported by its reader")
			require.NotEmpty(t, traces)

			err = apis.eth.checkBlockHistoryAvailable(ctx, view, tc.firstBlock-1)
			require.ErrorIs(t, err, state.ErrPruned)
			require.Contains(t, err.Error(), fmt.Sprintf("history is available from block %d", tc.firstBlock))

			apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, startTxNum: startTxNum}
			caps, err := apis.eth.Capabilities(ctx)
			require.NoError(t, err)
			require.Equal(t, uint64(8), uint64(*caps.State.OldestBlock))
			require.Equal(t, tc.firstBlock, uint64(*caps.Receipts.OldestBlock))
			require.Equal(t, tc.firstBlock, uint64(*caps.Logs.OldestBlock))
		})
	}
}

func TestIndexedHistoryGateMatchesReader(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	storageAddr := common.HexToAddress("0xcafe")
	storageKey, storageValue := common.Hash{31: 1}, common.Hash{31: 2}
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc: types.GenesisAlloc{
				testAddr:    {Balance: big.NewInt(1_000_000_000)},
				storageAddr: {Balance: big.NewInt(0), Code: []byte{0}, Storage: map[common.Hash]common.Hash{storageKey: storageValue}},
			},
			Difficulty: uint256.NewInt(1),
		}),
		execmoduletester.WithKey(testKey),
	)
	signer := types.LatestSignerForChainID(nil)
	c, err := m.GenerateChain(4, func(blockIndex int, block *blockgen.BlockGen) {
		for index := range 3 {
			txn, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), common.Address{byte(blockIndex), byte(index)}, uint256.NewInt(1), 21000, nil, nil), *signer, testKey)
			require.NoError(t, err)
			block.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))
	tx, err := m.DB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = prune.EnsureNotChanged(tx, prune.ArchiveMode)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	api := newDebugApiForTest(m)
	baseline := newDebugApiForTest(m)
	ro, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer ro.Rollback()
	block := c.Blocks[1]
	require.Len(t, block.Transactions(), 3)
	startTxNum, err := api._txNumReader.Min(ctx, ro, block.NumberU64())
	require.NoError(t, err)
	view := historyFloorTx{TemporalTx: ro, startTxNum: startTxNum + 3}
	reader, err := rpchelper.CreateHistoryStateReader(ctx, view, block.NumberU64(), 2, api._txNumReader)
	require.NoError(t, err)
	account, err := reader.ReadAccountData(accounts.InternAddress(testAddr))
	require.NoError(t, err)
	require.NotNil(t, account)
	_, err = rpchelper.CreateHistoryStateReader(ctx, view, block.NumberU64(), 1, api._txNumReader)
	require.ErrorIs(t, err, state.ErrPruned)
	err = api.checkPruneTransactionHistoryAtIndex(ctx, view, block.NumberU64()-1, 1)
	require.ErrorIs(t, err, state.ErrPruned)
	require.Contains(t, err.Error(), fmt.Sprintf("history is available from txNum %d", view.startTxNum))
	api.db = historyFloorDB{TemporalRoDB: api.db, startTxNum: view.startTxNum}

	for _, tc := range []struct {
		name string
		call func(*DebugAPIImpl, uint64) (any, error)
	}{
		{"accountAt", func(api *DebugAPIImpl, index uint64) (any, error) {
			return api.AccountAt(ctx, block.Hash(), index, testAddr)
		}},
		{"storageRangeAt", func(api *DebugAPIImpl, index uint64) (any, error) {
			return api.StorageRangeAt(ctx, block.Hash(), index, storageAddr, nil, 1)
		}},
		{"traceCall", func(api *DebugAPIImpl, index uint64) (any, error) {
			ref := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(block.NumberU64()))
			txIndex := hexutil.Uint(index)
			return streamedResult(func(stream *jsonstream.Stream) error {
				return api.TraceCall(ctx, pruneGatingCallArgs(), &ref, &tracersConfig.TraceConfig{TxIndex: &txIndex}, stream)
			})
		}},
		{"traceCallMany", func(api *DebugAPIImpl, index uint64) (any, error) {
			bundles, simulate := pruneGatingBundle(block.NumberU64())
			txIndex := int(index)
			simulate.TransactionIndex = &txIndex
			return streamedResult(func(stream *jsonstream.Stream) error {
				return api.TraceCallMany(ctx, bundles, simulate, nil, stream)
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want, err := tc.call(baseline, 2)
			require.NoError(t, err)
			got, err := tc.call(api, 2)
			require.NoError(t, err, "the requested transaction's pre-state is retained")
			require.Equal(t, want, got)
			_, err = tc.call(api, 1)
			require.ErrorIs(t, err, state.ErrPruned, "the preceding transaction's pre-state is pruned")
			_, err = tc.call(api, uint64(len(block.Transactions())))
			require.NoError(t, err, "the position after the last user transaction is valid")
			for _, index := range []uint64{uint64(len(block.Transactions()) + 1), 10_000, math.MaxUint64 - 1} {
				_, err = tc.call(api, index)
				require.ErrorIs(t, err, state.ErrPruned, "index %d must not bypass the history floor", index)
			}
		})
	}

	baselineAPIs, prunedAPIs := newPruneGatingAPIs(m), newPruneGatingAPIs(m)
	prunedAPIs.eth.db = historyFloorDB{TemporalRoDB: prunedAPIs.eth.db, startTxNum: view.startTxNum}
	prunedAPIs.debug.db = historyFloorDB{TemporalRoDB: prunedAPIs.debug.db, startTxNum: view.startTxNum}
	prunedAPIs.ots.db = historyFloorDB{TemporalRoDB: prunedAPIs.ots.db, startTxNum: view.startTxNum}
	for _, ep := range pruneGatingEndpoints {
		switch ep.name {
		case "debug_traceTransaction", "ots_traceTransaction", "ots_getInternalOperations", "ots_getTransactionError", "eth_getTransactionReceipt":
		default:
			continue
		}
		t.Run(ep.name, func(t *testing.T) {
			ref := pruneGatingRef{num: block.NumberU64(), hash: block.Hash(), txHash: block.Transactions()[2].Hash()}
			want, err := ep.call(ctx, baselineAPIs, ref)
			require.NoError(t, err)
			got, err := ep.call(ctx, prunedAPIs, ref)
			require.NoError(t, err, "the requested transaction's pre-state is retained")
			require.Equal(t, want, got)
			ref.txHash = block.Transactions()[1].Hash()
			_, err = ep.call(ctx, prunedAPIs, ref)
			require.ErrorIs(t, err, state.ErrPruned)
		})
	}
	for _, backwards := range []bool{false, true} {
		t.Run(fmt.Sprintf("ots_search_backwards=%t", backwards), func(t *testing.T) {
			search := func(api *OtterscanAPIImpl, index byte) (*TransactionsWithReceipts, error) {
				// Each recipient occurs once, so the page only needs that transaction's history.
				addr := common.Address{1, index}
				if backwards {
					return api.SearchTransactionsBefore(ctx, addr, block.NumberU64()+1, 1)
				}
				return api.SearchTransactionsAfter(ctx, addr, 0, 1)
			}
			want, err := search(baselineAPIs.ots, 2)
			require.NoError(t, err)
			require.Len(t, want.Txs, 1)
			got, err := search(prunedAPIs.ots, 2)
			require.NoError(t, err)
			require.Equal(t, want, got)
			_, err = search(prunedAPIs.ots, 1)
			require.ErrorIs(t, err, state.ErrPruned)
		})
	}
	t.Run("ots_search_after_boundary_block", func(t *testing.T) {
		want, err := baselineAPIs.ots.SearchTransactionsAfter(ctx, testAddr, block.NumberU64(), 1)
		require.NoError(t, err)
		require.NotEmpty(t, want.Txs)
		got, err := prunedAPIs.ots.SearchTransactionsAfter(ctx, testAddr, block.NumberU64(), 1)
		require.NoError(t, err, "the cursor block is excluded from the search")
		require.Equal(t, want, got)
	})
}

func TestCallGateMatchesReaderAfterSystemTransaction(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	const blockNumber = uint64(7)
	startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, blockNumber+1)
	require.NoError(t, err)
	view := historyFloorTx{TemporalTx: tx, startTxNum: startTxNum + 1}

	require.NoError(t, apis.eth.checkPruneStateAfterSystemTx(ctx, view, blockNumber))
	require.ErrorIs(t, apis.eth.checkPruneStateAfterSystemTx(ctx, view, blockNumber-1), state.ErrPruned)
	reader, err := rpchelper.CreateUncachedStateReaderFromBlockNumber(ctx, view, blockNumber, false, 0, apis.eth._txNumReader)
	require.NoError(t, err)
	account, err := reader.ReadAccountData(accounts.InternAddress(testAddr))
	require.NoError(t, err)
	require.NotNil(t, account)

	apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, startTxNum: startTxNum + 1}
	block := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumber))
	_, err = apis.eth.Call(ctx, pruneGatingCallArgs(), &block, nil, nil)
	require.NoError(t, err, "the call reader starts after the initial system transaction")
	_, err = apis.eth.GetBalance(ctx, testAddr, &block)
	require.ErrorIs(t, err, state.ErrPruned, "account queries read before the initial system transaction")
	block = rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumber - 1))
	_, err = apis.eth.Call(ctx, pruneGatingCallArgs(), &block, nil, nil)
	require.ErrorIs(t, err, state.ErrPruned, "the preceding call position is below retained history")
}

func TestExecutionWitnessNeedsInitialSystemHistory(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	const blockNumber = uint64(8)
	startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, blockNumber)
	require.NoError(t, err)
	view := historyFloorTx{TemporalTx: tx, startTxNum: startTxNum + 1}
	block := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumber))
	_, err = apis.debug.resolveWitnessBlock(ctx, view, block)
	require.ErrorIs(t, err, state.ErrPruned, "witness generation reads before the initial system transaction")
}

func TestComputedReceiptsNeedWholeBlockHistory(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode:        prune.ArchiveMode,
		chainConfig: byzantiumChainConfig(pruneGatingByzantiumHeight),
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	const blockNumber = uint64(8)
	startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, blockNumber)
	require.NoError(t, err)
	view := historyFloorTx{TemporalTx: tx, startTxNum: startTxNum + 1}

	require.NoError(t, apis.eth.checkPruneTransactionHistory(ctx, view, blockNumber))
	require.ErrorIs(t, apis.eth.checkReceiptsAvailable(ctx, view, blockNumber), state.ErrPruned,
		"post-state root calculation needs changes from the initial system transaction")
	require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, view, blockNumber+1))

	apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, startTxNum: startTxNum + 1}
	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)
	require.Equal(t, blockNumber+1, uint64(*caps.Receipts.OldestBlock))
}

func TestComputedReceiptsNeedCommitmentHistory(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name              string
		startBlock        uint64
		txOffset          uint64
		commitmentHistory bool
		oldest            uint64
	}{
		{"block_start", 8, 0, true, 8},
		{"inside_block", 8, 1, true, 9},
		{"beyond_byzantium", pruneGatingByzantiumHeight + 1, 0, true, pruneGatingByzantiumHeight},
		{"not_retired", 0, 0, true, 0},
		{"disabled", 8, 1, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mode := prune.ArchiveMode
			mode.CommitmentHistory = prune.Distance(5)
			apis, _ := setupPruneGating(t, pruneGatingConfig{
				mode:        mode,
				chainConfig: byzantiumChainConfig(pruneGatingByzantiumHeight),
			})
			ctx := t.Context()
			rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer rwTx.Rollback()
			require.NoError(t, rawdb.WriteDBCommitmentHistoryEnabled(rwTx, tc.commitmentHistory))
			require.NoError(t, rwTx.Commit())

			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			start, err := apis.eth._txNumReader.Min(ctx, tx, tc.startBlock)
			require.NoError(t, err)
			starts := map[kv.Domain]uint64{kv.CommitmentDomain: start + tc.txOffset}
			view := historyFloorTx{TemporalTx: tx, starts: starts}
			apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, starts: starts}

			t.Run("gate", func(t *testing.T) {
				if tc.oldest > 0 {
					require.NoError(t, apis.eth.checkPruneHistory(ctx, view, tc.oldest-1), "ordinary history is retained")
					require.ErrorIs(t, apis.eth.checkReceiptAvailableAtIndex(ctx, view, tc.oldest-1, 0), state.ErrPruned,
						"receipt roots need commitment history from the initial system transaction")
				}
				require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, view, tc.oldest))
				require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, view, pruneGatingByzantiumHeight))
			})
			t.Run("capabilities", func(t *testing.T) {
				caps, err := apis.eth.Capabilities(ctx)
				require.NoError(t, err)
				require.Equal(t, tc.oldest, uint64(*caps.Receipts.OldestBlock))
				require.Zero(t, uint64(*caps.Logs.OldestBlock), "logs do not need receipt post-state roots")
				if tc.commitmentHistory && tc.oldest < pruneGatingByzantiumHeight {
					require.NotNil(t, caps.Receipts.DeleteStrategy)
					require.EqualValues(t, 5, caps.Receipts.DeleteStrategy.RetentionBlocks)
				} else {
					require.Nil(t, caps.Receipts.DeleteStrategy)
				}
			})
			for _, crit := range []filters.FilterCriteria{{}, addressFilter(tc.startBlock)} {
				require.NoError(t, apis.eth.checkLogsAvailable(ctx, view, 0, pruneGatingByzantiumHeight, crit))
			}
			view.errs = map[kv.Domain]error{kv.CommitmentDomain: errors.New("commitment floor unavailable")}
			if tc.commitmentHistory {
				require.ErrorIs(t, apis.eth.checkReceiptsAvailable(ctx, view, 8), view.errs[kv.CommitmentDomain])
			} else {
				require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, view, 8))
			}
			require.NoError(t, apis.eth.checkReceiptsAvailable(ctx, view, pruneGatingByzantiumHeight))
		})
	}
}

func TestLogsGateSkipsInitialSystemHistory(t *testing.T) {
	t.Parallel()
	apis, _ := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	const block = uint64(8)
	start, err := apis.eth._txNumReader.Min(ctx, tx, block)
	require.NoError(t, err)
	apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, startTxNum: start + 1}

	for _, criteria := range []filters.FilterCriteria{
		{},
		{Addresses: []common.Address{testAddr}},
		{Topics: [][]common.Hash{{{1}}}},
	} {
		criteria.FromBlock, criteria.ToBlock = new(big.Int).SetUint64(block), new(big.Int).SetUint64(block)
		_, err := apis.eth.GetLogs(ctx, criteria)
		require.NoError(t, err, "user transactions are retained for both filtered and unfiltered queries")
		criteria.FromBlock = new(big.Int).SetUint64(block - 1)
		_, err = apis.eth.GetLogs(ctx, criteria)
		require.ErrorIs(t, err, state.ErrPruned)
	}
	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)
	require.Equal(t, block, uint64(*caps.Logs.OldestBlock))
	require.Equal(t, caps.Receipts.OldestBlock, caps.Logs.OldestBlock)
}

func TestHistoryGateUsesEarliestDomainFloor(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	starts := make(map[kv.Domain]uint64)
	for domain, block := range map[kv.Domain]uint64{
		kv.AccountsDomain: 7,
		kv.StorageDomain:  8,
		kv.CodeDomain:     9,
	} {
		starts[domain], err = apis.eth._txNumReader.Min(ctx, tx, block)
		require.NoError(t, err)
	}
	view := historyFloorTx{TemporalTx: tx, starts: starts}

	floors, err := apis.eth.readHistoryStartBlocks(ctx, view, chainInfo.head)
	require.NoError(t, err)
	require.Equal(t, uint64(7), floors.wholeBlock)
}

func TestHistoryFloorRejectsNonTemporalTransactions(t *testing.T) {
	t.Parallel()
	for name, tx := range map[string]kv.Tx{
		"nil":          nil,
		"non-temporal": struct{ kv.Tx }{},
	} {
		t.Run(name, func(t *testing.T) {
			var api BaseAPI
			_, err := api.historyStartBlocks(t.Context(), tx, 10)
			require.ErrorContains(t, err, "history availability requires a temporal transaction")
		})
	}
}

func TestHistoryGatePropagatesBackendError(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	wantErr := errors.New("history floor unavailable")
	view := historyFloorTx{TemporalTx: tx, errs: map[kv.Domain]error{kv.AccountsDomain: wantErr}}
	_, err = apis.eth.readHistoryStartBlocks(ctx, view, chainInfo.head)
	require.ErrorIs(t, err, wantErr)
}

func TestHistoryFloorRejectsMissingDebugView(t *testing.T) {
	t.Parallel()
	var api BaseAPI
	_, err := api.historyStartBlocks(t.Context(), &membatchwithdb.MemoryMutation{}, 10)
	require.ErrorContains(t, err, "state history requires a temporal debug view")
}

func TestHistoryGateKeepsLatestWithoutHistoricalState(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	view := historyFloorTx{TemporalTx: tx, startTxNum: math.MaxUint64}

	require.NoError(t, apis.eth.checkPruneHistory(ctx, view, chainInfo.head))
	require.ErrorIs(t, apis.eth.checkPruneHistory(ctx, view, chainInfo.head-1), state.ErrPruned)
	require.NoError(t, apis.eth.checkPruneState(ctx, view, chainInfo.head))
	require.ErrorIs(t, apis.eth.checkPruneState(ctx, view, chainInfo.head-1), state.ErrPruned)
	require.NoError(t, apis.eth.checkPruneStateAfterSystemTx(ctx, view, chainInfo.head))
	require.ErrorIs(t, apis.eth.checkPruneStateAfterSystemTx(ctx, view, chainInfo.head-1), state.ErrPruned)
	require.NoError(t, apis.eth.checkPruneTransactionHistory(ctx, view, chainInfo.head))
	require.ErrorIs(t, apis.eth.checkPruneTransactionHistory(ctx, view, chainInfo.head-1), state.ErrPruned)
	require.ErrorIs(t, apis.eth.checkPruneTransactionHistoryAtIndex(ctx, view, chainInfo.head-1, 1), state.ErrPruned)
}

func TestHistoryEndpointsUseOnDiskFloor(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, chainInfo.old.num+1)
	require.NoError(t, err)
	tx.Rollback()
	apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, startTxNum: startTxNum}

	_, err = apis.eth.GetLogs(ctx, addressFilter(chainInfo.old.num))
	require.ErrorIs(t, err, state.ErrPruned)
	_, err = apis.eth.GetBlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.old.num)))
	require.ErrorIs(t, err, state.ErrPruned)
}

func TestBlocksGateUsesOnDiskFloor(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	const floorBlock = uint64(9)
	dropBodies(t, apis.rwDB, 1, floorBlock)

	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	err = apis.eth.checkPruneBlocks(ctx, tx, floorBlock-1)
	require.ErrorIs(t, err, state.ErrPruned)
	require.Contains(t, err.Error(), fmt.Sprintf("blocks are available from block %d", floorBlock))
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, floorBlock))

	_, err = apis.eth.GetBlockByNumber(ctx, rpc.BlockNumber(floorBlock-1), false)
	require.ErrorIs(t, err, state.ErrPruned)
	genesis, err := apis.eth.GetBlockByNumber(ctx, rpc.BlockNumber(0), false)
	require.NoError(t, err, "retiring bodies preserves genesis")
	require.NotNil(t, genesis)
	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)
	require.Equal(t, floorBlock, uint64(*caps.Blocks.OldestBlock), "isolated genesis does not fill the missing range")
}

func TestFeeHistoryTruncatesAtPhysicalTransactionFloor(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	apis.eth._blockReader = &fixedMinimumBlockReader{FullBlockReader: apis.eth._blockReader, floor: chainInfo.old.num + 1}

	result, err := apis.eth.FeeHistory(t.Context(), 1, rpc.BlockNumber(chainInfo.old.num), []float64{50})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result.Reward)
	require.Empty(t, result.BaseFee)
	require.Empty(t, result.GasUsedRatio)
	require.Empty(t, result.BlobBaseFee)
	require.Empty(t, result.BlobGasUsedRatio)

	result, err = apis.eth.FeeHistory(t.Context(), 1, rpc.BlockNumber(chainInfo.old.num), nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result.Reward)
	require.Len(t, result.BaseFee, 2)
	require.Len(t, result.GasUsedRatio, 1)
}

func TestFeeHistoryIncludesIsolatedGenesis(t *testing.T) {
	t.Parallel()
	apis, _ := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	dropBodies(t, apis.rwDB, 1, 9)

	result, err := apis.eth.FeeHistory(t.Context(), 1, rpc.BlockNumber(0), []float64{50})
	require.NoError(t, err)
	require.Len(t, result.Reward, 1)
	require.Len(t, result.BaseFee, 2)
	require.Equal(t, []float64{0}, result.GasUsedRatio)
}

func TestGenesisRangesRequireContiguousBlocks(t *testing.T) {
	t.Parallel()
	apis, _ := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	const floorBlock = uint64(9)
	dropBodies(t, apis.rwDB, 1, floorBlock)
	ctx := t.Context()

	for _, tc := range []struct {
		name string
		call func(from, to uint64) (any, error)
	}{
		{"eth_getLogs", func(from, to uint64) (any, error) {
			return apis.eth.GetLogs(ctx, rangeFilter(from, to))
		}},
		{"eth_getLogs_filtered", func(from, to uint64) (any, error) {
			crit := rangeFilter(from, to)
			crit.Addresses = []common.Address{testAddr}
			return apis.eth.GetLogs(ctx, crit)
		}},
		{"erigon_getLogs", func(from, to uint64) (any, error) {
			return apis.erigon.GetLogs(ctx, rangeFilter(from, to))
		}},
		{"erigon_getLatestLogs", func(from, to uint64) (any, error) {
			return apis.erigon.GetLatestLogs(ctx, rangeFilter(from, to), filters.LogFilterOptions{LogCount: 10})
		}},
		{"overlay_getLogs", func(from, to uint64) (any, error) {
			return apis.overlay.GetLogs(ctx, rangeFilter(from, to), nil, nil)
		}},
		{"trace_filter", func(from, to uint64) (any, error) {
			req := TraceFilterRequest{
				FromBlock: new(rpc.BlockNumber(from)),
				ToBlock:   new(rpc.BlockNumber(to)),
			}
			return streamedResult(func(stream *jsonstream.Stream) error {
				return apis.trace.Filter(ctx, req, new(bool), nil, stream)
			})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call(0, floorBlock)
			require.ErrorIs(t, err, state.ErrPruned, "genesis does not fill the missing blocks in a range")
			_, err = tc.call(0, 0)
			require.NoError(t, err, "genesis alone remains readable")
			_, err = tc.call(floorBlock, floorBlock)
			require.NoError(t, err, "the contiguous retained range remains readable")
		})
	}
}

func TestGasPriceOracleBackendBlockAvailability(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"pruned", nil},
		{"lookup_error", errors.New("block floor unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
			reader := &countingMinimumBlockReader{FullBlockReader: &fixedMinimumBlockReader{
				FullBlockReader: apis.eth._blockReader, floor: chainInfo.old.num + 1, err: tc.err,
			}}
			apis.eth._blockReader = reader
			ctx := t.Context()
			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			backend := NewGasPriceOracleBackend(apis.eth.db, rpchelper.PinToOverlay(tx, nil), apis.eth.BaseAPI)

			for _, method := range []struct {
				name string
				call func(pruneGatingRef) (*types.Block, error)
			}{
				{"by_number", func(ref pruneGatingRef) (*types.Block, error) {
					return backend.BlockByNumber(ctx, rpc.BlockNumber(ref.num))
				}},
				{"by_hash_number", func(ref pruneGatingRef) (*types.Block, error) {
					return backend.BlockByHashNumber(ctx, ref.hash, ref.num)
				}},
			} {
				t.Run(method.name, func(t *testing.T) {
					block, err := method.call(chainInfo.old)
					require.ErrorIs(t, err, tc.err)
					require.Nil(t, block)
					if tc.err == nil {
						block, err = method.call(chainInfo.recent)
						require.NoError(t, err)
						require.NotNil(t, block)
						require.Equal(t, chainInfo.recent.num, block.NumberU64())
					}
				})
			}
			require.EqualValues(t, 1, reader.calls.Load(), "the backend resolves its floor only once")
		})
	}
}

func TestPruneGatesSkipPhysicalFloorsAtHead(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	var historyCalls atomic.Int64
	historyTx := countingHistoryFloorTx{TemporalTx: tx, calls: &historyCalls}
	blocks := &countingMinimumBlockReader{FullBlockReader: apis.eth._blockReader}
	apis.eth._blockReader = blocks

	require.NoError(t, apis.eth.checkPruneHistory(ctx, historyTx, chainInfo.head))
	require.NoError(t, apis.eth.checkPruneState(ctx, historyTx, chainInfo.head))
	require.NoError(t, apis.eth.checkPruneStateAfterSystemTx(ctx, historyTx, chainInfo.head))
	require.NoError(t, apis.eth.checkPruneTransactionHistory(ctx, historyTx, chainInfo.head))
	require.NoError(t, apis.eth.checkPruneTransactionHistoryAtIndex(ctx, historyTx, chainInfo.head, 1))
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.head))
	require.Zero(t, historyCalls.Load())
	require.Zero(t, blocks.calls.Load())
}

func TestPruneGatesSkipPhysicalFloorsBelowConfiguredCutoff(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: pruneGatingDistance},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	var historyCalls atomic.Int64
	historyTx := countingHistoryFloorTx{TemporalTx: tx, calls: &historyCalls}
	blocks := &countingMinimumBlockReader{FullBlockReader: apis.eth._blockReader}
	apis.eth._blockReader = blocks
	configuredFloor := pruneGatingDistance.PruneTo(chainInfo.head)

	require.ErrorIs(t, apis.eth.checkPruneHistory(ctx, historyTx, configuredFloor-1), state.ErrPruned)
	require.ErrorIs(t, apis.eth.checkPruneState(ctx, historyTx, configuredFloor-1), state.ErrPruned)
	require.ErrorIs(t, apis.eth.checkPruneStateAfterSystemTx(ctx, historyTx, configuredFloor-1), state.ErrPruned)
	require.ErrorIs(t, apis.eth.checkPruneTransactionHistory(ctx, historyTx, configuredFloor-1), state.ErrPruned)
	require.ErrorIs(t, apis.eth.checkPruneTransactionHistoryAtIndex(ctx, historyTx, configuredFloor-1, 1), state.ErrPruned)
	require.ErrorIs(t, apis.eth.checkPruneBlocks(ctx, tx, configuredFloor-1), state.ErrPruned)
	require.Zero(t, historyCalls.Load())
	require.Zero(t, blocks.calls.Load())
}

func TestPruneGatesReusePhysicalFloorsAtSameHead(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	apis.eth._historyPruneFloor.ttl = time.Hour
	apis.eth._blocksPruneFloor.ttl = time.Hour
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	var historyCalls atomic.Int64
	historyTx := countingHistoryFloorTx{TemporalTx: tx, calls: &historyCalls}
	blocks := &countingMinimumBlockReader{FullBlockReader: apis.eth._blockReader}
	apis.eth._blockReader = blocks

	for range 2 {
		require.NoError(t, apis.eth.checkPruneHistory(ctx, historyTx, chainInfo.head-1))
		require.NoError(t, apis.eth.checkPruneState(ctx, historyTx, chainInfo.head-1))
		require.NoError(t, apis.eth.checkPruneStateAfterSystemTx(ctx, historyTx, chainInfo.head-1))
		require.NoError(t, apis.eth.checkPruneTransactionHistory(ctx, historyTx, chainInfo.head-1))
		require.NoError(t, apis.eth.checkPruneTransactionHistoryAtIndex(ctx, historyTx, chainInfo.head-1, 1))
		require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.head-1))
	}
	require.Equal(t, int64(3), historyCalls.Load())
	require.Equal(t, int64(1), blocks.calls.Load())
}

func TestHistoryFloorCacheSeparatesPinnedFilesAtSameHead(t *testing.T) {
	t.Parallel()
	apis, chainInfo := setupPhysicallyPrunedHistory(t, prunedHistoryConfig{mode: prune.Mode{
		Initialised: true, History: prunedHistoryDistance, Blocks: prune.KeepAllBlocksPruneMode,
	}})
	apis.eth._historyPruneFloor.ttl = time.Hour
	ctx := t.Context()
	before, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer before.Rollback()
	oldFloor, err := apis.eth.historyStartBlocks(ctx, before, chainInfo.head)
	require.NoError(t, err)

	end := before.Debug().TxNumsInFiles(kv.AccountsDomain)
	retired, err := before.Debug().Retire(ctx, kv.RetireCutoffs{Default: end - prunedHistoryStepSize})
	require.NoError(t, err)
	require.Positive(t, retired)
	after, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer after.Rollback()
	require.Equal(t, before.ViewID(), after.ViewID(), "retiring files does not commit MDBX")
	newFloor, err := apis.eth.readHistoryStartBlocks(ctx, after, chainInfo.head)
	require.NoError(t, err)
	require.Greater(t, newFloor.startTxNum, oldFloor.startTxNum)
	got, err := apis.eth.historyStartBlocks(ctx, after, chainInfo.head)
	require.NoError(t, err)
	require.Equal(t, newFloor, got)
	got, err = apis.eth.historyStartBlocks(ctx, before, chainInfo.head)
	require.NoError(t, err)
	require.Equal(t, oldFloor, got, "the older transaction still pins its retained history")
}

func TestHistoryFloorCacheDoesNotShareUnidentifiedViews(t *testing.T) {
	t.Parallel()
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	apis.eth._historyPruneFloor.ttl = time.Hour
	tx, err := apis.eth.db.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for _, block := range []uint64{7, 9, 7} {
		start, err := apis.eth._txNumReader.Min(t.Context(), tx, block)
		require.NoError(t, err)
		view := historyFloorTx{TemporalTx: tx, startTxNum: start}
		want, err := apis.eth.readHistoryStartBlocks(t.Context(), view, chainInfo.head)
		require.NoError(t, err)
		got, err := apis.eth.historyStartBlocks(t.Context(), view, chainInfo.head)
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
}

func TestHistoryFloorCacheSeparatesMDBXViewsAtSameHead(t *testing.T) {
	t.Parallel()
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	apis.eth._historyPruneFloor.ttl = time.Hour
	ctx := t.Context()
	for _, block := range []uint64{7, 9} {
		t.Run(fmt.Sprintf("floor_%d", block), func(t *testing.T) {
			rwTx, err := apis.rwDB.BeginTemporalRw(ctx)
			require.NoError(t, err)
			defer rwTx.Rollback()
			start, err := apis.eth._txNumReader.Min(ctx, rwTx, block)
			require.NoError(t, err)
			require.NoError(t, writeHistoryStart(rwTx, start))
			require.NoError(t, rwTx.Commit())
			tx, err := apis.eth.db.BeginTemporalRo(ctx)
			require.NoError(t, err)
			defer tx.Rollback()
			want, err := apis.eth.readHistoryStartBlocks(ctx, tx, chainInfo.head)
			require.NoError(t, err)
			require.Equal(t, start, want.startTxNum)
			got, err := apis.eth.historyStartBlocks(ctx, tx, chainInfo.head)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

func TestBlockFloorCacheSeparatesMDBXViewsAtSameHead(t *testing.T) {
	t.Parallel()
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	apis.eth._blocksPruneFloor.ttl = time.Hour
	ctx := t.Context()
	before, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer before.Rollback()
	floor, err := apis.eth.minimumBlockAvailable(ctx, before, chainInfo.head)
	require.NoError(t, err)
	require.Zero(t, floor)
	const firstRetained = uint64(9)
	dropBodies(t, apis.rwDB, 1, firstRetained)
	after, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer after.Rollback()
	require.Equal(t, blockFilesGeneration(before), blockFilesGeneration(after))
	floor, err = apis.eth.minimumBlockAvailable(ctx, after, chainInfo.head)
	require.NoError(t, err)
	require.Equal(t, firstRetained, floor)
	floor, err = apis.eth.minimumBlockAvailable(ctx, before, chainInfo.head)
	require.NoError(t, err)
	require.Zero(t, floor)
}

func TestBlockFloorCacheSeparatesFileViewsAtSameHead(t *testing.T) {
	t.Parallel()
	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	apis.eth._blocksPruneFloor.ttl = time.Hour
	ctx := t.Context()
	reader := &countingMinimumBlockReader{FullBlockReader: apis.eth._blockReader}
	apis.eth._blockReader = reader
	before, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer before.Rollback()
	_, err = apis.eth.minimumBlockAvailable(ctx, before, chainInfo.head)
	require.NoError(t, err)
	require.EqualValues(t, 1, reader.calls.Load())
	snapshots := apis.rwDB.(freezeblocks.HasBlockFiles).DebugBlockFiles()
	require.NoError(t, snapshots.OpenFolder())
	after, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer after.Rollback()
	require.Equal(t, before.ViewID(), after.ViewID())
	require.NotEqual(t, blockFilesGeneration(before), blockFilesGeneration(after))
	_, err = apis.eth.minimumBlockAvailable(ctx, after, chainInfo.head)
	require.NoError(t, err)
	require.EqualValues(t, 2, reader.calls.Load())
	_, err = apis.eth.minimumBlockAvailable(ctx, before, chainInfo.head)
	require.NoError(t, err)
	require.EqualValues(t, 2, reader.calls.Load())
}

func TestHistoryFloorCacheSeparatesBlockFileViews(t *testing.T) {
	t.Parallel()
	const head, historyBlock = uint64(1_000), uint64(600)
	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.TestChainBerlinConfig))
	chainData, err := m.GenerateChain(int(head), func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainData))
	ctx := t.Context()
	snapshots := m.DB.(freezeblocks.HasBlockFiles).DebugBlockFiles()
	require.NoError(t, freezeblocks.DumpBlocks(ctx, 0, head, m.ChainConfig, m.Dirs.Tmp, m.Dirs.Snap,
		m.DB, 1, log.LvlDebug, log.New(), m.BlockReader, snapcfg.KnownCfgOrDevnet(m.ChainConfig.ChainName), nil))
	require.NoError(t, snapshots.OpenFolder())

	rwTx, err := m.DB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer rwTx.Rollback()
	start, err := m.BlockReader.TxnumReader().Min(ctx, rwTx, historyBlock)
	require.NoError(t, err)
	require.NoError(t, writeHistoryStart(rwTx, start))
	for block := uint64(1); block < head; block++ {
		require.NoError(t, rwTx.Delete(kv.MaxTxNum, hexutil.EncodeTs(block)))
	}
	require.NoError(t, rwTx.Commit())

	api := newBaseApiForTest(m)
	api._historyPruneFloor.ttl = time.Hour
	before, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer before.Rollback()
	oldFloor, err := api.historyStartBlocks(ctx, before, head)
	require.NoError(t, err)
	require.Equal(t, historyBlock, oldFloor.wholeBlock)

	// An incomplete block-files view can lose the txNum mapping without changing
	// the MDBX view or the state-history files. Older readers still pin the bodies.
	require.NoError(t, snapshots.OpenList(nil, false))
	after, err := m.DB.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer after.Rollback()
	require.Equal(t, before.ViewID(), after.ViewID())
	require.Equal(t, before.Debug().(interface{ HistoryFilesGeneration() uint64 }).HistoryFilesGeneration(),
		after.Debug().(interface{ HistoryFilesGeneration() uint64 }).HistoryFilesGeneration())
	require.NotEqual(t, blockFilesGeneration(before), blockFilesGeneration(after))
	want, err := api.readHistoryStartBlocks(ctx, after, head)
	require.NoError(t, err)
	require.NotEqual(t, oldFloor, want)
	got, err := api.historyStartBlocks(ctx, after, head)
	require.NoError(t, err)
	require.Equal(t, want, got)
	got, err = api.historyStartBlocks(ctx, before, head)
	require.NoError(t, err)
	require.Equal(t, oldFloor, got)
}

func TestCapabilitiesUseOnDiskFloors(t *testing.T) {
	t.Parallel()

	wide := prune.Distance(pruneGatingChainLen * 3)
	apis, _ := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: wide, Blocks: wide},
	})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	const stateFloor = uint64(8)
	startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, stateFloor+1)
	require.NoError(t, err)
	tx.Rollback()

	const blocksFloor = uint64(9)
	dropBodies(t, apis.rwDB, 1, blocksFloor)
	apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, startTxNum: startTxNum}

	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)
	require.Equal(t, stateFloor, uint64(*caps.State.OldestBlock))
	require.Equal(t, blocksFloor, uint64(*caps.Logs.OldestBlock))
	require.Equal(t, blocksFloor, uint64(*caps.Blocks.OldestBlock))
	require.Equal(t, blocksFloor, uint64(*caps.Tx.OldestBlock))
	require.Equal(t, blocksFloor, uint64(*caps.Receipts.OldestBlock))
}

func TestCapabilitiesUsePhysicalFloorsForUnboundedMode(t *testing.T) {
	t.Parallel()

	apis, _ := setupPruneGating(t, pruneGatingConfig{mode: prune.ArchiveMode})
	ctx := t.Context()
	tx, err := apis.eth.db.BeginTemporalRo(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	startTxNum, err := apis.eth._txNumReader.Min(ctx, tx, 8)
	require.NoError(t, err)
	tx.Rollback()
	apis.eth.db = historyFloorDB{TemporalRoDB: apis.eth.db, startTxNum: startTxNum}
	apis.eth._blockReader = &fixedMinimumBlockReader{FullBlockReader: apis.eth._blockReader, floor: 9}

	caps, err := apis.eth.Capabilities(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(7), uint64(*caps.State.OldestBlock))
	require.Equal(t, uint64(9), uint64(*caps.Blocks.OldestBlock))
	require.Nil(t, caps.State.DeleteStrategy)
	require.Nil(t, caps.Blocks.DeleteStrategy)
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
			require.ErrorIs(t, apis.eth.checkReceiptsAvailable(ctx, tx, tc.refused), state.ErrPruned)
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
	require.ErrorIs(t, apis.eth.checkPruneHistory(ctx, tx, 0), state.ErrPruned,
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

	require.ErrorIs(t, apis.eth.checkReceiptsAvailable(ctx, tx, 0), state.ErrPruned,
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
	require.ErrorIs(t, err, state.ErrPruned,
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
	require.ErrorIs(t, err, state.ErrPruned, "a pre-Byzantium receipt below history cannot be re-executed")
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
	require.ErrorIs(t, err, state.ErrPruned,
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
				require.ErrorIs(t, err, state.ErrPruned)
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
			err := apis.eth.checkLogsAvailable(ctx, tx, chainInfo.old.num, chainInfo.old.num, tc.crit)
			if tc.fires {
				require.ErrorIs(t, err, state.ErrPruned)
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

	require.NoError(t, apis.eth.checkLogsAvailable(ctx, tx, below, below, filters.FilterCriteria{}),
		"an unfiltered query reads the kept cache, which carries every field it needs")
	require.ErrorIs(t, apis.eth.checkLogsAvailable(ctx, tx, below, below, addressFilter(below)), state.ErrPruned,
		"an indexed filter searches LogAddrIdx, retired at the history cutoff")
	require.ErrorIs(t, apis.eth.checkReceiptsAvailable(ctx, tx, below), state.ErrPruned,
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

	require.NoError(t, apis.eth.checkLogsAvailable(ctx, tx, oldest, oldest, addressFilter(oldest)),
		"the advertised oldest block must be served")
	require.ErrorIs(t, apis.eth.checkLogsAvailable(ctx, tx, oldest-1, oldest-1, addressFilter(oldest-1)), state.ErrPruned,
		"the block below the advertised oldest must be refused")
	require.NoError(t, apis.eth.checkLogsAvailable(ctx, tx, oldest-1, oldest-1, filters.FilterCriteria{}),
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

	err = apis.eth.checkLogsAvailable(ctx, tx, chainInfo.old.num, chainInfo.old.num, filters.FilterCriteria{})
	require.ErrorIs(t, err, state.ErrPruned)
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
	require.ErrorIs(t, err, state.ErrPruned)
	require.Contains(t, err.Error(), fmt.Sprintf("blocks are available from block %d", pruneGatingMergeHeight))

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, pruneGatingMergeHeight),
		"the merge block itself is served")
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.recent.num))

	_, err = apis.eth.GetBlockByNumber(ctx, rpc.BlockNumber(chainInfo.old.num), false)
	require.ErrorIs(t, err, state.ErrPruned, "the endpoints must see the same boundary")
}

// TestLogsByBlockHashNamesThePruneBoundary pins that a filter pinned to a block
// hash, in eth_getLogs or trace_filter, reports pruning rather than a missing
// block: the range is resolved from the retained header, so the gate speaks
// before any body is read.
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
		{"trace_filter", func() (any, error) {
			return nil, apis.trace.Filter(ctx, TraceFilterRequest{BlockHash: &hash}, new(bool), nil, jsonstream.New(io.Discard))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call()
			require.ErrorIs(t, err, state.ErrPruned)
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
			require.ErrorIs(t, err, state.ErrPruned)
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
			require.ErrorIs(t, err, state.ErrPruned)
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

	require.ErrorIs(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, empty), state.ErrPruned)
	require.ErrorIs(t, apis.eth.checkLogsAvailable(ctx, tx, empty, empty, filters.FilterCriteria{}), state.ErrPruned)

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
		{"graphql_getBlockDetails", func() (any, error) { return apis.graphql.GetBlockDetails(ctx, rpc.BlockNumber(empty), nil) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call()
			require.ErrorIs(t, err, state.ErrPruned)
		})
	}

	receipts, err := apis.eth.GetBlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.recent.num)))
	require.NoError(t, err, "above the cutoff the same endpoint answers")
	require.NotNil(t, receipts)
}

// byzantiumChainConfig moves Byzantium and every later fork to at, so the blocks
// below it carry a post state the receipt cache does not store.
func byzantiumChainConfig(at uint64) *chain.Config {
	cfg := chain.TestChainBerlinConfig.Copy()
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
	cfg := chain.TestChainBerlinConfig.Copy()
	cfg.MergeHeight = &height
	return cfg
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
				{"state", caps.State, func(b uint64) error { return apis.eth.checkPruneState(ctx, tx, b) }},
				{"blocks", caps.Blocks, func(b uint64) error { return apis.eth.checkPruneBlocks(ctx, tx, b) }},
				{"tx", caps.Tx, func(b uint64) error { return apis.eth.checkPruneBlocks(ctx, tx, b) }},
				{"receipts", caps.Receipts, func(b uint64) error {
					return apis.eth.checkBlockReceiptsAvailable(ctx, tx, b)
				}},
				{"logs", caps.Logs, func(b uint64) error {
					return apis.eth.checkLogsAvailable(ctx, tx, b, b, addressFilter(b))
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
					require.ErrorIs(t, pair.gate(oldest-1), state.ErrPruned,
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
	require.ErrorIs(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, wideOldest-1), state.ErrPruned)
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
	require.ErrorIs(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, oldest-1), state.ErrPruned)
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
	require.ErrorIs(t, err, state.ErrPruned)
	require.Contains(t, err.Error(), fmt.Sprintf("blocks are available from block %d", pruneGatingMergeHeight))

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, pruneGatingMergeHeight))
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.recent.num))

	_, err = apis.eth.GetBlockByNumber(ctx, rpc.BlockNumber(chainInfo.old.num), false)
	require.ErrorIs(t, err, state.ErrPruned, "the endpoints must see the same boundary")
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
	cfg := chain.TestChainBerlinConfig.Copy()
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

	// A walk that answered nothing is held for a TTL of its own, which is what keeps a
	// datadir without block data from being walked on every request. This test is about
	// what the walk reads, so it takes one per call.
	apis.eth._preMergeUnsettledTTL = 0

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
	require.ErrorIs(t, gateOnOldBlock(), state.ErrPruned,
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
		require.Zero(t, uint64(*caps.Blocks.OldestBlock), "every block transaction is on disk")
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

	t.Run("expiry_starting_mid_chain", func(t *testing.T) {
		t.Parallel()
		apis, _ := setupPruneGating(t, cfg)
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
		require.Less(t, oldest, pruneGatingMergeHeight)

		caps, err := apis.eth.Capabilities(ctx)
		require.NoError(t, err)
		require.Equal(t, oldest, uint64(*caps.Blocks.OldestBlock),
			"what is advertised is the block the gate serves from, not the merge point above it")
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
// mid-chain is not evidence of an archive datadir, and the boundary the gate serves from
// is that oldest block, not the merge point above it.
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

	err = apis.eth.checkPruneBlocks(ctx, tx, oldest-1)
	require.ErrorIs(t, err, state.ErrPruned)
	require.Contains(t, err.Error(), fmt.Sprintf("blocks are available from block %d", oldest))

	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, oldest))
	require.NoError(t, apis.eth.checkPruneBlocks(ctx, tx, chainInfo.old.num))
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
	require.ErrorIs(t, err, state.ErrPruned)
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
	require.ErrorIs(t, gateOnOldBlock(), state.ErrPruned,
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
	require.ErrorIs(t, err, state.ErrPruned)
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
	require.ErrorIs(t, apis.eth.checkBlockReceiptsAvailable(ctx, tx, historyOldest-1), state.ErrPruned)
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
	require.NoError(t, apis.eth.checkLogsAvailable(ctx, tx, chainInfo.old.num, chainInfo.old.num, filters.FilterCriteria{}),
		"the fixture needs a mode where no gate refuses the block")

	hash := chainInfo.old.hash
	_, err = apis.eth.GetLogs(ctx, filters.FilterCriteria{BlockHash: &hash})
	require.ErrorContains(t, err, "block not found")
}

// TestCapabilitiesDropTheWindowAtTheForkBoundary checks that a fixed fork
// boundary is not reported as a rolling deletion window.
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
			require.ErrorIs(t, err, state.ErrPruned)
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
	require.ErrorIs(t, gateOnOldBlock(), state.ErrPruned, "past the window the datadir is read again")
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

type countingHistoryFloorTx struct {
	kv.TemporalTx
	calls *atomic.Int64
}

func (tx countingHistoryFloorTx) Debug() kv.TemporalDebugTx {
	return countingHistoryFloorDebugTx{TemporalDebugTx: tx.TemporalTx.Debug(), calls: tx.calls}
}

type countingHistoryFloorDebugTx struct {
	kv.TemporalDebugTx
	calls *atomic.Int64
}

func (tx countingHistoryFloorDebugTx) HistoryStartFrom(domain kv.Domain) (uint64, error) {
	tx.calls.Add(1)
	return tx.TemporalDebugTx.HistoryStartFrom(domain)
}

func (tx countingHistoryFloorDebugTx) HistoryFilesGeneration() uint64 {
	return tx.TemporalDebugTx.(interface{ HistoryFilesGeneration() uint64 }).HistoryFilesGeneration()
}

type countingMinimumBlockReader struct {
	dbservices.FullBlockReader
	calls atomic.Int64
}

type fixedMinimumBlockReader struct {
	dbservices.FullBlockReader
	floor uint64
	err   error
}

func (r *fixedMinimumBlockReader) MinimumBlockAvailable(context.Context, kv.Tx) (uint64, error) {
	return r.floor, r.err
}

func (r *countingMinimumBlockReader) MinimumBlockAvailable(ctx context.Context, tx kv.Tx) (uint64, error) {
	r.calls.Add(1)
	return r.FullBlockReader.MinimumBlockAvailable(ctx, tx)
}

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
	view := historyFloorTx{TemporalTx: tx, startTxNum: math.MaxUint64}

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
	require.ErrorIs(t, err, state.ErrPruned, "the control block must reach the unavailable history")
}

// Truncation keeps only blocks before the first unavailable one. If the oldest
// requested block is pruned, rewards are empty even when newer blocks are retained;
// a header-only request still serves the same range.
func TestFeeHistoryTruncationTakesTheOldestBlockOfTheRange(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPruneGating(t, pruneGatingConfig{
		mode: prune.Mode{Initialised: true, History: prune.KeepAllBlocksPruneMode, Blocks: pruneGatingDistance},
	})
	ctx := t.Context()
	head := chainInfo.head
	oldest := pruneGatingDistance.PruneTo(head)
	retained := rpc.DecimalOrHex(head - oldest + 1)

	res, err := apis.eth.FeeHistory(ctx, retained+1, rpc.BlockNumber(head), []float64{50})
	require.NoError(t, err)
	require.Empty(t, res.Reward)
	require.Empty(t, res.GasUsedRatio)

	_, err = apis.eth.FeeHistory(ctx, retained+1, rpc.BlockNumber(head), nil)
	require.NoError(t, err, "the header series reaches past the blocks cutoff")

	res, err = apis.eth.FeeHistory(ctx, retained, rpc.BlockNumber(head), []float64{50})
	require.NoError(t, err)
	require.Equal(t, oldest, res.OldestBlock.ToInt().Uint64())
}

// TestReceiptCacheServesBlocksWhoseHistoryIsRetired pins that a keep-all receipt retention
// is served from the cache and not by re-execution: state history is retired on disk above
// the block, so an endpoint that still answers can only be reading the cache. The shared
// fixture cannot show this — it keeps every history and stores the prune mode afterwards.
func TestReceiptCacheServesBlocksWhoseHistoryIsRetired(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPhysicallyPrunedHistory(t, prunedHistoryConfig{
		mode: prune.Mode{
			Initialised: true,
			History:     prunedHistoryDistance,
			Blocks:      prune.KeepAllBlocksPruneMode,
			Receipts:    prune.KeepAllReceiptsPruneMode,
		},
		receiptCache: true,
	})
	ctx := t.Context()

	bnh := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(chainInfo.old.num))
	_, err := apis.eth.GetBalance(ctx, testAddr, &bnh)
	require.ErrorIs(t, err, state.ErrPruned, "the history window must already refuse state for this block")

	for _, ep := range receiptGatedEndpoints() {
		t.Run(ep.name, func(t *testing.T) {
			res, err := ep.call(ctx, apis, chainInfo.old)
			require.NoError(t, err)
			require.NotNil(t, res)
		})
	}
}

// TestReceiptsWithoutCacheStopAtRetiredHistory is the control for the test above: the
// same fixture with the cache off refuses the block, which is what attributes the answers
// there to the cache. The refusal here is the history-window comparison, not a read of
// the retired files.
func TestReceiptsWithoutCacheStopAtRetiredHistory(t *testing.T) {
	t.Parallel()

	apis, chainInfo := setupPhysicallyPrunedHistory(t, prunedHistoryConfig{
		mode: prune.Mode{
			Initialised: true,
			History:     prunedHistoryDistance,
			Blocks:      prune.KeepAllBlocksPruneMode,
			Receipts:    prune.KeepAllReceiptsPruneMode,
		},
	})
	ctx := t.Context()

	for _, ep := range receiptGatedEndpoints() {
		t.Run(ep.name, func(t *testing.T) {
			_, err := ep.call(ctx, apis, chainInfo.old)
			require.ErrorIs(t, err, state.ErrPruned)
		})
	}
}
