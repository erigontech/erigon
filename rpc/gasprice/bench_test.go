// Copyright 2025 The Erigon Authors
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

package gasprice_test

import (
	"context"
	"math"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/gasprice/gaspricecfg"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

const txsPerBlock = 100

// newTestBackendN creates a test backend with n pre-generated blocks,
// each containing txsPerBlock transactions with varying gas prices (1–100 GWei),
// simulating realistic mainnet block density.
func newTestBackendN(tb testing.TB, n int) *execmoduletester.ExecModuleTester {
	tb.Helper()
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	addr := crypto.PubkeyToAddress(key.PublicKey)
	// Balance large enough for txsPerBlock × n blocks at up to 100 GWei each.
	// 100 txs × 100 GWei × 21000 gas × 200 blocks = 4.2e19 wei > MaxInt64 (9.2e18).
	// Use MaxInt64 × 100 to cover the largest benchmark (200 blocks).
	balance := new(big.Int).Mul(big.NewInt(math.MaxInt64), big.NewInt(100))
	gspec := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc:  types.GenesisAlloc{addr: {Balance: balance}},
	}
	signer := types.LatestSigner(gspec.Config)
	m := execmoduletester.New(tb, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	ch, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, n, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for j := 0; j < txsPerBlock; j++ {
			gasPrice := uint256.NewInt(uint64(j+1) * uint64(common.GWei))
			tx, txErr := types.SignTx(
				types.NewTransaction(b.TxNonce(addr), common.HexToAddress("deadbeef"),
					uint256.NewInt(100), 21000, gasPrice, nil),
				*signer, key,
			)
			if txErr != nil {
				tb.Fatalf("failed to create tx: %v", txErr)
			}
			b.AddTx(tx)
		}
	})
	require.NoError(tb, err)
	require.NoError(tb, m.InsertChain(ch))
	return m
}

// BenchmarkSuggestTipCap measures the cold-path latency of oracle.SuggestTipCap.
//
// The gas-price cache is reset before every iteration so each call exercises
// the full DB read path (block fetching + sorting), not a cache hit.
// This mirrors the real-world hot case: a new block just arrived and
// invalidated the cache.
//
// Usage:
//
//	go test -run='^$' -bench=BenchmarkSuggestTipCap -benchtime=10s ./rpc/gasprice/...
func BenchmarkSuggestTipCap(b *testing.B) {
	const numBlocks = 64
	m := newTestBackendN(b, numBlocks)
	defer m.Close()

	baseApi := jsonrpc.NewBaseApi(nil, kvcache.NewDummy(), m.BlockReader, false,
		rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0, 0)

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(b, err)
	defer tx.Rollback() //nolint:gocritic

	cases := []struct {
		name        string
		checkBlocks int
		parallel    bool // true = new code (Fork enabled), false = old code (sequential)
	}{
		// Old behaviour: nil db → Fork returns nil → single goroutine sequential.
		{"sequential/checkBlocks=2", 2, false},
		{"sequential/checkBlocks=20", 20, false},
		{"sequential/checkBlocks=40", 40, false},
		// New behaviour: real db → Fork opens per-goroutine tx → parallel fetch.
		{"parallel/checkBlocks=2", 2, true},
		{"parallel/checkBlocks=20", 20, true},
		{"parallel/checkBlocks=40", 40, true},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			cfg := gaspricecfg.Config{
				Blocks:     tc.checkBlocks,
				Percentile: 60,
			}
			dbArg := m.DB
			if !tc.parallel {
				dbArg = nil
			}
			for i := 0; i < b.N; i++ {
				// Fresh cache every iteration → cold path, no cache hits.
				cache := jsonrpc.NewGasPriceCache()
				oracle := gasprice.NewOracle(
					jsonrpc.NewGasPriceOracleBackend(dbArg, tx, baseApi),
					cfg,
					cache,
					nil,
					log.New(),
				)
				_, berr := oracle.SuggestTipCap(context.Background())
				if berr != nil {
					b.Fatal(berr)
				}
			}
		})
	}
}

// BenchmarkFeeHistory measures end-to-end latency of oracle.FeeHistory.
//
// Each sub-benchmark uses a cold LRU cache (nil historyCache) so every
// iteration exercises the full DB read path.  This highlights the
// difference between the old sequential loop and the new parallel one.
//
// Usage:
//
//	go test -run='^$' -bench=BenchmarkFeeHistory -benchtime=10s ./rpc/gasprice/...
func BenchmarkFeeHistory(b *testing.B) {
	const numBlocks = 200
	m := newTestBackendN(b, numBlocks)
	defer m.Close()

	baseApi := jsonrpc.NewBaseApi(nil, kvcache.NewDummy(), m.BlockReader, false,
		rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0, 0)

	// Single read-only tx used only by the main goroutine (GetLatestBlockNumber,
	// ChainConfig, PendingBlockAndReceipts). Worker goroutines open their own
	// transactions via Fork.
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(b, err)
	defer tx.Rollback()

	gasCache := jsonrpc.NewGasPriceCache()

	cases := []struct {
		name        string
		blocks      int
		percentiles []float64
	}{
		// Header-only path (no percentiles): reads only block headers — lighter.
		{"headers/blocks=50", 50, nil},
		{"headers/blocks=100", 100, nil},
		{"headers/blocks=200", 200, nil},
		// Full path (with percentiles): reads full blocks + receipts — heavier,
		// benefits most from parallel fetching.
		{"full/blocks=50", 50, []float64{25, 50, 75}},
		{"full/blocks=100", 100, []float64{25, 50, 75}},
		{"full/blocks=200", 200, []float64{25, 50, 75}},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				// Create a fresh oracle per iteration so the LRU history cache
				// starts cold every time (nil historyCache).  This ensures we
				// measure DB round-trips, not cache hits.
				oracle := gasprice.NewOracle(
					jsonrpc.NewGasPriceOracleBackend(m.DB, tx, baseApi),
					gaspricecfg.Config{MaxHeaderHistory: 0, MaxBlockHistory: 0},
					gasCache,
					nil, // cold: no history cache
					log.New(),
				)
				_, _, _, _, _, _, ferr := oracle.FeeHistory(
					context.Background(), tc.blocks, rpc.LatestBlockNumber, tc.percentiles,
				)
				if ferr != nil {
					b.Fatal(ferr)
				}
			}
		})
	}
}
