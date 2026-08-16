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
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

const (
	pruneGatingChainLen    = 20
	pruneGatingDistance    = prune.Distance(10)
	pruneGatingOldBlockIdx = 3
)

// pruneGateBoundary declares which prune boundary an endpoint must gate its
// availability on. This is the contract the table pins: an endpoint gating on
// the wrong boundary rejects data that is physically present.
type pruneGateBoundary int

const (
	gatedByBlocks        pruneGateBoundary = iota // Mode.Blocks via checkPruneBlocks
	gatedByHistory                                // Mode.History via checkPruneHistory
	gatedByReceipts                               // Mode.Receipts-unless-following-history via checkReceiptsAvailable
	gatedByBlockReceipts                          // both of the above via checkBlockReceiptsAvailable
)

// pruneGatingAPIs holds the implementations the table drives. They share a
// chain and a prune mode; only the entry points differ.
type pruneGatingAPIs struct {
	eth     *APIImpl
	debug   *DebugAPIImpl
	erigon  *ErigonImpl
	graphql *GraphQLAPIImpl
	ots     *OtterscanAPIImpl
	overlay *OverlayAPIImpl
}

type pruneGatingRef struct {
	num    uint64
	hash   common.Hash
	txHash common.Hash
}

type pruneGatingChain struct {
	head        uint64
	old, recent pruneGatingRef
}

type pruneGatingEndpoint struct {
	name     string
	boundary pruneGateBoundary
	call     func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error)
}

var pruneGatingEndpoints = []pruneGatingEndpoint{
	{"eth_getBlockByNumber", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetBlockByNumber(ctx, rpc.BlockNumber(ref.num), false)
	}},
	{"eth_getBlockByHash", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetBlockByHash(ctx, rpc.BlockNumberOrHashWithHash(ref.hash, false), false)
	}},
	{"eth_getBlockTransactionCountByNumber", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetBlockTransactionCountByNumber(ctx, rpc.BlockNumber(ref.num))
	}},
	{"eth_getBlockTransactionCountByHash", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetBlockTransactionCountByHash(ctx, ref.hash)
	}},
	{"eth_getTransactionByHash", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetTransactionByHash(ctx, ref.txHash)
	}},
	{"eth_getRawTransactionByHash", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetRawTransactionByHash(ctx, ref.txHash)
	}},
	{"eth_getTransactionByBlockHashAndIndex", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetTransactionByBlockHashAndIndex(ctx, ref.hash, 0)
	}},
	{"eth_getRawTransactionByBlockHashAndIndex", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetRawTransactionByBlockHashAndIndex(ctx, ref.hash, 0)
	}},
	{"eth_getTransactionByBlockNumberAndIndex", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetTransactionByBlockNumberAndIndex(ctx, rpc.BlockNumber(ref.num), 0)
	}},
	{"eth_getRawTransactionByBlockNumberAndIndex", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetRawTransactionByBlockNumberAndIndex(ctx, rpc.BlockNumber(ref.num), 0)
	}},
	{"eth_getUncleByBlockNumberAndIndex", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetUncleByBlockNumberAndIndex(ctx, rpc.BlockNumber(ref.num), 0)
	}},
	{"eth_getUncleByBlockHashAndIndex", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetUncleByBlockHashAndIndex(ctx, ref.hash, 0)
	}},
	{"eth_getUncleCountByBlockNumber", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetUncleCountByBlockNumber(ctx, rpc.BlockNumber(ref.num))
	}},
	{"eth_getUncleCountByBlockHash", gatedByBlocks, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetUncleCountByBlockHash(ctx, ref.hash)
	}},
	{"eth_getBalance", gatedByHistory, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		bnh := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(ref.num))
		return apis.eth.GetBalance(ctx, testAddr, &bnh)
	}},
	{"eth_getCode", gatedByHistory, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		bnh := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(ref.num))
		return apis.eth.GetCode(ctx, testAddr, &bnh)
	}},
	// Log matching runs over standalone inverted indices retired at the history
	// cutoff, so these follow history whatever the receipt retention is.
	{"eth_getLogs", gatedByHistory, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetLogs(ctx, blockFilter(ref.num))
	}},
	{"erigon_getLogs", gatedByHistory, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.erigon.GetLogs(ctx, blockFilter(ref.num))
	}},
	{"erigon_getLatestLogs", gatedByHistory, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.erigon.GetLatestLogs(ctx, blockFilter(ref.num), filters.LogFilterOptions{LogCount: 10})
	}},
	{"overlay_getLogs", gatedByHistory, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.overlay.GetLogs(ctx, blockFilter(ref.num), nil, nil)
	}},
	// These two serve the receipts of one block and can take either path.
	{"eth_getTransactionReceipt", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetTransactionReceipt(ctx, ref.txHash)
	}},
	{"eth_getBlockReceipts", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.eth.GetBlockReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(ref.num)))
	}},
	{"debug_getRawReceipts", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.debug.GetRawReceipts(ctx, rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(ref.num)))
	}},
	{"erigon_getLogsByHash", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.erigon.GetLogsByHash(ctx, ref.hash)
	}},
	{"erigon_getBlockReceiptsByBlockHash", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.erigon.GetBlockReceiptsByBlockHash(ctx, ref.hash)
	}},
	{"ots_getBlockDetails", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.ots.GetBlockDetails(ctx, rpc.BlockNumber(ref.num))
	}},
	{"ots_getBlockDetailsByHash", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.ots.GetBlockDetailsByHash(ctx, ref.hash)
	}},
	{"ots_getBlockTransactions", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.ots.GetBlockTransactions(ctx, rpc.BlockNumber(ref.num), 0, 10)
	}},
	{"graphql_getBlockDetails", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.graphql.GetBlockDetails(ctx, rpc.BlockNumber(ref.num))
	}},
	{"graphql_getBlockDetailsByHash", gatedByBlockReceipts, func(ctx context.Context, apis pruneGatingAPIs, ref pruneGatingRef) (any, error) {
		return apis.graphql.GetBlockDetailsByHash(ctx, ref.hash)
	}},
}

// blockFilter matches every log of a single block.
func blockFilter(block uint64) filters.FilterCriteria {
	n := new(big.Int).SetUint64(block)
	return filters.FilterCriteria{FromBlock: n, ToBlock: n}
}

// pruneGatingConfigs mirrors the shapes of the named presets in
// db/kv/prune/storage_mode.go with the finite distances scaled down so the
// prune window falls inside the short test chain; full and minimal share one
// row because they only differ in distance. The legacy full shape
// (KeepPostMergeBlocksPruneMode) is kept as its own row since it is still a
// recognized production configuration.
type pruneGatingConfig struct {
	name            string
	mode            prune.Mode
	persistReceipts bool
}

var pruneGatingConfigs = []pruneGatingConfig{
	{name: "archive", mode: prune.ArchiveMode},
	{name: "blocks", mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode}},
	{name: "full_minimal", mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: pruneGatingDistance}},
	{name: "full_legacy", mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepPostMergeBlocksPruneMode}},
	{name: "blocks_receipts_follow_history", mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode}, persistReceipts: true},
	{name: "blocks_receipts_keep_all", mode: prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode, Receipts: prune.KeepAllReceiptsPruneMode}, persistReceipts: true},
}

// TestPruneModeEndpointGating pins, for every prune mode shape, that block-data
// endpoints serve old blocks whenever blocks are retained and that
// state-reading endpoints return state.PrunedError outside the history window.
// The chain is inserted without physical pruning and the prune mode is stored
// afterwards, so every cell observes only the RPC-layer gate.
func TestPruneModeEndpointGating(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	for _, cfg := range pruneGatingConfigs {
		t.Run(cfg.name, func(t *testing.T) {
			t.Parallel()
			apis, chainInfo := setupPruneGating(t, cfg)
			legs := []struct {
				name string
				ref  pruneGatingRef
			}{{"old", chainInfo.old}, {"recent", chainInfo.recent}}
			for _, ep := range pruneGatingEndpoints {
				for _, leg := range legs {
					t.Run(ep.name+"/"+leg.name, func(t *testing.T) {
						res, err := ep.call(t.Context(), apis, leg.ref)
						if pruneGateFires(ep.boundary, cfg, leg.ref.num, chainInfo.head) {
							require.ErrorIs(t, err, state.PrunedError)
						} else {
							require.NoError(t, err)
							require.NotNil(t, res)
						}
					})
				}
			}
		})
	}
}

func pruneGateFires(boundary pruneGateBoundary, cfg pruneGatingConfig, blockNum, head uint64) bool {
	var amount prune.BlockAmount
	switch boundary {
	case gatedByBlocks:
		amount = cfg.mode.Blocks
	case gatedByHistory:
		amount = cfg.mode.History
	case gatedByReceipts:
		// The cache widens availability only where it outlives history: an
		// explicit keep-all, or a window of its own.
		amount = cfg.mode.History
		if cfg.persistReceipts {
			switch r := cfg.mode.ReceiptsAmount(); {
			case r == prune.KeepAllReceiptsPruneMode:
				return false
			case !cfg.mode.ReceiptsFollowHistory():
				amount = r
			}
		}
	case gatedByBlockReceipts:
		return pruneGateFires(gatedByBlocks, cfg, blockNum, head) ||
			pruneGateFires(gatedByReceipts, cfg, blockNum, head)
	}
	return amount.Enabled() && blockNum < amount.PruneTo(head)
}

func setupPruneGating(t *testing.T, cfg pruneGatingConfig) (pruneGatingAPIs, pruneGatingChain) {
	t.Helper()
	m := execmoduletester.New(t,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc:  types.GenesisAlloc{testAddr: {Balance: big.NewInt(1_000_000_000)}},
		}),
		execmoduletester.WithKey(testKey),
	)

	signer := types.LatestSignerForChainID(nil)
	c, err := m.GenerateChain(pruneGatingChainLen, func(i int, block *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(block.TxNonce(testAddr), common.Address{}, uint256.NewInt(1), 21000, nil, nil), *signer, testKey)
		if err != nil {
			panic(err)
		}
		block.AddTx(txn)
		// Both legs need an ommer so the uncle endpoints assert on a real
		// result instead of passing vacuously on nil.
		switch i {
		case pruneGatingOldBlockIdx:
			u := block.PrevBlock(1).Header()
			u.Extra = []byte("uncle-old")
			block.AddUncle(u)
		case pruneGatingChainLen - 1:
			u := block.PrevBlock(pruneGatingChainLen - 3).Header()
			u.Extra = []byte("uncle-recent")
			block.AddUncle(u)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(c))

	ctx := t.Context()
	tx, err := m.DB.BeginTemporalRw(ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = prune.EnsureNotChanged(tx, cfg.mode)
	require.NoError(t, err)
	if cfg.persistReceipts {
		require.NoError(t, kvcfg.PersistReceipts.ForceWrite(tx, true))
	}
	require.NoError(t, tx.Commit())

	ref := func(idx int) pruneGatingRef {
		b := c.Blocks[idx]
		return pruneGatingRef{num: b.NumberU64(), hash: b.Hash(), txHash: b.Transactions()[0].Hash()}
	}
	apis := pruneGatingAPIs{
		eth:    newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil),
		erigon: NewErigonAPI(newBaseApiForTest(m), m.DB, nil),
	}
	apis.debug = NewPrivateDebugAPI(newBaseApiForTest(m), m.DB, nil, &rpccfg.DebugApiConfig{})
	apis.graphql = NewGraphQLAPI(newBaseApiForTest(m), m.DB, apis.eth, nil, &rpccfg.GraphQLApiConfig{})
	otsBase := newBaseApiForTest(m)
	apis.ots = NewOtterscanAPI(otsBase, m.DB, 25)
	apis.overlay = NewOverlayAPI(otsBase, m.DB, &rpccfg.OverlayApiConfig{GasCap: 1_000_000}, apis.ots)
	return apis, pruneGatingChain{
		head:   pruneGatingChainLen,
		old:    ref(pruneGatingOldBlockIdx),
		recent: ref(pruneGatingChainLen - 1),
	}
}
