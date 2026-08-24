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
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
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
	gatedByBlocks  pruneGateBoundary = iota // Mode.Blocks via checkPruneBlocks
	gatedByHistory                          // Mode.History via checkPruneHistory
)

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
	call     func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error)
}

var pruneGatingEndpoints = []pruneGatingEndpoint{
	{"eth_getBlockByNumber", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetBlockByNumber(ctx, rpc.BlockNumber(ref.num), false)
	}},
	{"eth_getBlockByHash", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetBlockByHash(ctx, rpc.BlockNumberOrHashWithHash(ref.hash, false), false)
	}},
	{"eth_getBlockTransactionCountByNumber", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetBlockTransactionCountByNumber(ctx, rpc.BlockNumber(ref.num))
	}},
	{"eth_getBlockTransactionCountByHash", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetBlockTransactionCountByHash(ctx, ref.hash)
	}},
	{"eth_getTransactionByHash", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetTransactionByHash(ctx, ref.txHash)
	}},
	{"eth_getRawTransactionByHash", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetRawTransactionByHash(ctx, ref.txHash)
	}},
	{"eth_getTransactionByBlockHashAndIndex", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetTransactionByBlockHashAndIndex(ctx, ref.hash, 0)
	}},
	{"eth_getRawTransactionByBlockHashAndIndex", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetRawTransactionByBlockHashAndIndex(ctx, ref.hash, 0)
	}},
	{"eth_getTransactionByBlockNumberAndIndex", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetTransactionByBlockNumberAndIndex(ctx, rpc.BlockNumber(ref.num), 0)
	}},
	{"eth_getRawTransactionByBlockNumberAndIndex", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetRawTransactionByBlockNumberAndIndex(ctx, rpc.BlockNumber(ref.num), 0)
	}},
	{"eth_getUncleByBlockNumberAndIndex", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetUncleByBlockNumberAndIndex(ctx, rpc.BlockNumber(ref.num), 0)
	}},
	{"eth_getUncleByBlockHashAndIndex", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetUncleByBlockHashAndIndex(ctx, ref.hash, 0)
	}},
	{"eth_getUncleCountByBlockNumber", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetUncleCountByBlockNumber(ctx, rpc.BlockNumber(ref.num))
	}},
	{"eth_getUncleCountByBlockHash", gatedByBlocks, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		return api.GetUncleCountByBlockHash(ctx, ref.hash)
	}},
	{"eth_getBalance", gatedByHistory, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		bnh := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(ref.num))
		return api.GetBalance(ctx, testAddr, &bnh)
	}},
	{"eth_getCode", gatedByHistory, func(ctx context.Context, api *APIImpl, ref pruneGatingRef) (any, error) {
		bnh := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(ref.num))
		return api.GetCode(ctx, testAddr, &bnh)
	}},
}

// pruneGatingConfigs mirrors the shapes of the named presets in
// db/kv/prune/storage_mode.go with the finite distances scaled down so the
// prune window falls inside the short test chain; full and minimal share one
// row because they only differ in distance. The legacy full shape
// (KeepPostMergeBlocksPruneMode) is kept as its own row since it is still a
// recognized production configuration.
var pruneGatingConfigs = []struct {
	name string
	mode prune.Mode
}{
	{"archive", prune.ArchiveMode},
	{"blocks", prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepAllBlocksPruneMode}},
	{"full_minimal", prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: pruneGatingDistance}},
	{"full_legacy", prune.Mode{Initialised: true, History: pruneGatingDistance, Blocks: prune.KeepPostMergeBlocksPruneMode}},
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
			api, chainInfo := setupPruneGating(t, cfg.mode)
			legs := []struct {
				name string
				ref  pruneGatingRef
			}{{"old", chainInfo.old}, {"recent", chainInfo.recent}}
			for _, ep := range pruneGatingEndpoints {
				for _, leg := range legs {
					t.Run(ep.name+"/"+leg.name, func(t *testing.T) {
						res, err := ep.call(t.Context(), api, leg.ref)
						if pruneGateFires(ep.boundary, cfg.mode, leg.ref.num, chainInfo.head) {
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

func pruneGateFires(boundary pruneGateBoundary, m prune.Mode, blockNum, head uint64) bool {
	var amount prune.BlockAmount
	switch boundary {
	case gatedByBlocks:
		amount = m.Blocks
	case gatedByHistory:
		amount = m.History
	}
	return amount.Enabled() && blockNum < amount.PruneTo(head)
}

func setupPruneGating(t *testing.T, pm prune.Mode) (*APIImpl, pruneGatingChain) {
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
	_, err = prune.EnsureNotChanged(tx, pm)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	ref := func(idx int) pruneGatingRef {
		b := c.Blocks[idx]
		return pruneGatingRef{num: b.NumberU64(), hash: b.Hash(), txHash: b.Transactions()[0].Hash()}
	}
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	return api, pruneGatingChain{
		head:   pruneGatingChainLen,
		old:    ref(pruneGatingOldBlockIdx),
		recent: ref(pruneGatingChainLen - 1),
	}
}
