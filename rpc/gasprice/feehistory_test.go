// Copyright 2021 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/gasprice/gaspricecfg"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func TestFeeHistory(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	overMaxQuery := make([]float64, 101)
	for i := range 101 {
		overMaxQuery[i] = float64(1)
	}

	var cases = []struct {
		pending             bool
		maxHeader, maxBlock int
		count               int
		last                rpc.BlockNumber
		percent             []float64
		expFirst            uint64
		expCount            int
		expErr              error
	}{
		{false, 0, 0, 10, 30, nil, 21, 10, nil},
		{false, 0, 0, 10, 30, []float64{0, 10}, 21, 10, nil},
		{false, 0, 0, 10, 30, []float64{20, 10}, 0, 0, gasprice.ErrInvalidPercentile},
		{false, 0, 0, 1000000000, 30, nil, 0, 31, nil},
		{false, 0, 0, 1000000000, rpc.LatestBlockNumber, nil, 0, 33, nil},
		{false, 0, 0, 10, 40, nil, 0, 0, gasprice.ErrRequestBeyondHead},
		//{true, 0, 0, 10, 40, nil, 0, 0, gasprice.ErrRequestBeyondHead},
		{false, 20, 2, 100, rpc.LatestBlockNumber, nil, 13, 20, nil},
		{false, 20, 2, 100, rpc.LatestBlockNumber, []float64{0, 10}, 31, 2, nil},
		{false, 20, 2, 100, 32, []float64{0, 10}, 31, 2, nil},
		{false, 0, 0, 1, rpc.PendingBlockNumber, nil, 32, 1, nil},
		{true, 0, 0, 2, rpc.PendingBlockNumber, nil, 31, 2, nil},
		{true, 0, 0, 2, rpc.PendingBlockNumber, []float64{0, 10}, 31, 2, nil},
		{false, 0, 0, 10, 30, overMaxQuery, 0, 0, gasprice.ErrInvalidPercentile},
	}
	for i, c := range cases {
		config := gaspricecfg.Config{
			MaxHeaderHistory: c.maxHeader,
			MaxBlockHistory:  c.maxBlock,
		}

		func() {
			m := newTestBackend(t) //, big.NewInt(16), c.pending)
			defer m.Close()

			baseApi := jsonrpc.NewBaseApi(nil, kvcache.NewLatestBatchCache(), m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
			tx, err := m.DB.BeginTemporalRo(m.Ctx)
			require.NoError(t, err)
			defer tx.Rollback()

			cache := jsonrpc.NewGasPriceCache()
			oracle := gasprice.NewOracle(jsonrpc.NewGasPriceOracleBackend(m.DB, rpchelper.PinToOverlay(tx, nil), baseApi), config, cache, gasprice.NewFeeHistoryCache(), log.New())

			first, reward, baseFee, ratio, blobBaseFee, blobBaseFeeRatio, err := oracle.FeeHistory(context.Background(), c.count, c.last, c.percent)

			expReward := c.expCount
			if len(c.percent) == 0 {
				expReward = 0
			}
			expBaseFee := c.expCount
			if expBaseFee != 0 {
				expBaseFee++
			}

			if first.Uint64() != c.expFirst {
				t.Fatalf("Test case %d: first block mismatch, want %d, got %d", i, c.expFirst, first)
			}
			if len(reward) != expReward {
				t.Fatalf("Test case %d: reward array length mismatch, want %d, got %d", i, expReward, len(reward))
			}
			if len(baseFee) != expBaseFee {
				t.Fatalf("Test case %d: baseFee array length mismatch, want %d, got %d", i, expBaseFee, len(baseFee))
			}
			if len(ratio) != c.expCount {
				t.Fatalf("Test case %d: gasUsedRatio array length mismatch, want %d, got %d", i, c.expCount, len(ratio))
			}
			for _, r := range ratio {
				if r > 1 {
					t.Fatalf("Test case %d: gasUsedRatio greater than 1, got %f", i, r)
				}
			}
			if len(blobBaseFee) != len(baseFee) {
				t.Fatalf("Test case %d: blobBaseFee array length mismatch, want %d, got %d", i, len(baseFee), len(blobBaseFee))
			}
			if len(blobBaseFeeRatio) != c.expCount {
				t.Fatalf("Test case %d: blobBaseFeeRatio array length mismatch, want %d, got %d", i, c.expCount, len(blobBaseFeeRatio))
			}
			for _, r := range blobBaseFeeRatio {
				if r > 1 {
					t.Fatalf("Test case %d: blobGasUsedRatio greater than 1, got %f", i, r)
				}
			}
			if !errors.Is(err, c.expErr) {
				t.Fatalf("Test case %d: error mismatch, want %v, got %v", i, c.expErr, err)
			}
		}()
	}
}

// feeChain serves a fixed chain from memory; Fork returns nil, so the oracle reads it sequentially.
type feeChain struct {
	blocks   []*types.Block
	receipts map[uint64]types.Receipts
	fetches  atomic.Int32
}

func (c *feeChain) block(number uint64) *types.Block {
	if number >= uint64(len(c.blocks)) {
		return nil
	}
	return c.blocks[number]
}

func (c *feeChain) HeaderByNumber(_ context.Context, number rpc.BlockNumber) (*types.Header, error) {
	c.fetches.Add(1)
	if b := c.block(uint64(number)); b != nil {
		return b.Header(), nil
	}
	return nil, nil
}

func (c *feeChain) BlockByNumber(_ context.Context, number rpc.BlockNumber) (*types.Block, error) {
	c.fetches.Add(1)
	return c.block(uint64(number)), nil
}

func (c *feeChain) ChainConfig() *chain.Config { return chain.AllProtocolChanges }

func (c *feeChain) GetLatestBlockNumber() (uint64, error) { return uint64(len(c.blocks) - 1), nil }

func (c *feeChain) GetReceiptsGasUsed(_ context.Context, b *types.Block) (types.Receipts, error) {
	return c.receipts[b.NumberU64()], nil
}

func (c *feeChain) PendingBlockAndReceipts() (*types.Block, types.Receipts) { return nil, nil }

func (c *feeChain) CheckBlockRewardsAvailable(context.Context, uint64) error { return nil }

func (c *feeChain) CanonicalHashes(_ context.Context, from, to uint64) ([]common.Hash, error) {
	hashes := make([]common.Hash, to-from+1)
	for n := from; n <= to; n++ {
		if b := c.block(n); b != nil {
			hashes[n-from] = b.Hash()
		}
	}
	return hashes, nil
}

func (c *feeChain) FrozenBlocks() uint64 { return 0 }

func (c *feeChain) HeaderByHashNumber(ctx context.Context, hash common.Hash, number uint64) (*types.Header, error) {
	if b := c.block(number); b == nil || b.Hash() != hash {
		return nil, fmt.Errorf("hash %x is not block %d", hash, number)
	}
	return c.HeaderByNumber(ctx, rpc.BlockNumber(number))
}

func (c *feeChain) BlockByHashNumber(ctx context.Context, hash common.Hash, number uint64) (*types.Block, error) {
	if b := c.block(number); b == nil || b.Hash() != hash {
		return nil, fmt.Errorf("hash %x is not block %d", hash, number)
	}
	return c.BlockByNumber(ctx, rpc.BlockNumber(number))
}

func (c *feeChain) PrepareFork(context.Context) error { return nil }

func (c *feeChain) Fork(context.Context) (gasprice.OracleBackend, func(), error) {
	return nil, nil, nil
}

func TestFeeHistoryValues(t *testing.T) {
	gwei := func(n uint64) *uint256.Int { return uint256.NewInt(n * common.GWei) }
	u256 := func(v uint64) hexutil.U256 { return hexutil.U256(*uint256.NewInt(v)) }
	header := func(number, gasLimit, gasUsed, baseFeeGwei, blobGasUsed uint64) *types.Header {
		return &types.Header{Number: *uint256.NewInt(number), GasLimit: gasLimit, GasUsed: gasUsed,
			BaseFee: gwei(baseFeeGwei), BlobGasUsed: &blobGasUsed, ExcessBlobGas: new(uint64)}
	}
	dynamicFee := func(tipGwei, feeCapGwei uint64) types.Transaction {
		return &types.DynamicFeeTransaction{TipCap: *gwei(tipGwei), FeeCap: *gwei(feeCapGwei)}
	}
	legacy := func(gasPriceGwei uint64) types.Transaction {
		return types.NewTransaction(0, common.Address{}, uint256.NewInt(0), 21000, gwei(gasPriceGwei), nil)
	}

	backend := &feeChain{
		blocks: []*types.Block{
			types.NewBlock(header(0, 30_000_000, 15_000_000, 8, 0), nil, nil, nil, nil, nil),
			// Unsorted effective tips 3, 1 (legacy 9-8) and 2 (fee cap 10-8), weighted by gas used.
			types.NewBlock(header(1, 252_000, 126_000, 8, 0), []types.Transaction{dynamicFee(3, 100), legacy(9), dynamicFee(5, 10)}, nil, nil, nil, nil),
			types.NewBlock(header(2, 30_000_000, 0, 8, 0), nil, nil, nil, nil, nil),
			types.NewBlock(header(3, 30_000_000, 30_000_000, 7, params.GasPerBlob), []types.Transaction{legacy(11)}, nil, nil, nil, nil),
		},
		receipts: map[uint64]types.Receipts{
			1: {{GasUsed: 63_000}, {GasUsed: 21_000}, {GasUsed: 42_000}},
			3: {{GasUsed: 30_000_000}},
		},
	}
	oracle := gasprice.NewOracle(backend, gaspricecfg.Config{}, nil, gasprice.NewFeeHistoryCache(), log.New())
	maxBlobGas := chain.AllProtocolChanges.GetMaxBlobGasPerBlock(0)

	cases := []struct {
		name        string
		percentiles []float64
		wantReward  [][]uint64 // gwei
		wantFetches int32
	}{
		{"no percentiles", nil, nil, 3},
		{"header-only entries do not serve rewards", []float64{0, 25, 50, 75, 100}, [][]uint64{{1, 2, 2, 3, 3}, {0, 0, 0, 0, 0}, {4, 4, 4, 4, 4}}, 3},
		{"repeated request served from cache", []float64{0, 25, 50, 75, 100}, [][]uint64{{1, 2, 2, 3, 3}, {0, 0, 0, 0, 0}, {4, 4, 4, 4, 4}}, 0},
		{"other percentiles are served from the same entry", []float64{50}, [][]uint64{{2}, {0}, {4}}, 0},
		{"no percentiles are served from an entry with rewards", nil, nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			before := backend.fetches.Load()
			oldest, reward, baseFee, gasUsedRatio, blobBaseFee, blobGasUsedRatio, err := oracle.FeeHistory(t.Context(), 3, rpc.BlockNumber(3), c.percentiles)
			require.NoError(t, err)
			require.Equal(t, c.wantFetches, backend.fetches.Load()-before)

			require.Equal(t, uint64(1), oldest.Uint64())
			var wantReward [][]hexutil.U256
			for _, row := range c.wantReward {
				r := make([]hexutil.U256, len(row))
				for i, v := range row {
					r[i] = u256(v * common.GWei)
				}
				wantReward = append(wantReward, r)
			}
			require.Equal(t, wantReward, reward)
			// The last entry is the base fee after block 3, which used twice its gas target.
			require.Equal(t, []hexutil.U256{u256(8 * common.GWei), u256(8 * common.GWei), u256(7 * common.GWei), u256(7_875_000_000)}, baseFee)
			require.Equal(t, []float64{0.5, 0, 1}, gasUsedRatio)
			require.Equal(t, []hexutil.U256{u256(1), u256(1), u256(1), u256(1)}, blobBaseFee)
			require.Equal(t, []float64{0, 0, float64(params.GasPerBlob) / float64(maxBlobGas)}, blobGasUsedRatio)
		})
	}
}
