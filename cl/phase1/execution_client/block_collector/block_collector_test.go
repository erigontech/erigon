// Copyright 2024 The Erigon Authors
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

package block_collector_test

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/execution_client/block_collector"
	"github.com/erigontech/erigon/execution/types"
)

func TestBlockCollectorAccumulateAndFlush(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	engine := execution_client.NewMockExecutionEngine(ctrl)
	blocks, _, _ := tests.GetBellatrixRandom()

	blocksLeft := make(map[uint64]struct{})
	for _, block := range blocks {
		blocksLeft[block.Block.Body.ExecutionPayload.BlockNumber] = struct{}{}
	}

	engine.EXPECT().InsertBlocks(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes().DoAndReturn(func(ctx context.Context, blocks []*types.Block, wait bool) error {
		for _, block := range blocks {
			delete(blocksLeft, block.NumberU64())
		}
		return nil
	})
	bc := block_collector.NewBlockCollector(log.Root(), engine, &clparams.MainnetBeaconConfig, math.MaxUint64, ".")
	for _, block := range blocks {
		err := bc.AddBlock(block.Block)
		if err != nil {
			t.Fatal(err)
		}
	}
	require.NoError(t, bc.Flush(context.Background()))
	require.Empty(t, blocksLeft)
}
