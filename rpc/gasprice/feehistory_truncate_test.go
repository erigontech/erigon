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

package gasprice_test

import (
	"context"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/gasprice/gaspricecfg"
)

// gapBackend serves headers for every block up to head except missingBlock,
// which returns (nil, nil) the way a reorged-away block does.
type gapBackend struct {
	head         uint64
	missingBlock uint64
	config       *chain.Config
}

func (b *gapBackend) HeaderByNumber(_ context.Context, number rpc.BlockNumber) (*types.Header, error) {
	n := uint64(number.Int64())
	if n == b.missingBlock || n > b.head {
		return nil, nil
	}
	return &types.Header{
		Number:   *uint256.NewInt(n),
		GasLimit: 30_000_000,
		GasUsed:  15_000_000,
		BaseFee:  uint256.NewInt(1_000_000_000),
	}, nil
}

func (b *gapBackend) BlockByNumber(context.Context, rpc.BlockNumber) (*types.Block, error) {
	return nil, nil
}

func (b *gapBackend) ChainConfig() *chain.Config { return b.config }

func (b *gapBackend) GetLatestBlockNumber() (uint64, error) { return b.head, nil }

func (b *gapBackend) GetReceiptsGasUsed(context.Context, *types.Block) (types.Receipts, error) {
	return nil, nil
}

func (b *gapBackend) PendingBlockAndReceipts() (*types.Block, types.Receipts) { return nil, nil }

func (b *gapBackend) Fork(context.Context) (gasprice.OracleBackend, func(), error) {
	return nil, nil, nil
}

// A block missing from the middle of the requested range truncates the response
// at that block. Every returned array must be cut to the same point, otherwise
// the blob arrays carry entries for blocks that were not returned.
func TestFeeHistoryTruncatesBlobArrays(t *testing.T) {
	backend := &gapBackend{
		head:         10,
		missingBlock: 8,
		config:       &chain.Config{ChainID: uint256.NewInt(1), LondonBlock: new(uint64)},
	}
	oracle := gasprice.NewOracle(backend, gaspricecfg.Config{}, nil, gasprice.NewFeeHistoryCache(), log.New())

	oldest, _, baseFee, gasUsedRatio, blobBaseFee, blobGasUsedRatio, err := oracle.FeeHistory(
		context.Background(), 5, rpc.BlockNumber(10), nil)
	require.NoError(t, err)

	// Range is 6..10 with block 8 missing, so blocks 6 and 7 are returned.
	require.Equal(t, uint64(6), oldest.Uint64())
	require.Len(t, gasUsedRatio, 2)
	require.Len(t, baseFee, len(gasUsedRatio)+1)
	require.Len(t, blobGasUsedRatio, len(gasUsedRatio))
	require.Len(t, blobBaseFee, len(baseFee))
}
