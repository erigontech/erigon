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

package jsonrpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/rpc"
)

func TestErigonBlockNumber(t *testing.T) {
	if testing.Short() {
		t.Skip("long-running test")
	}

	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewErigonAPI(newBaseApiForTest(m), m.DB, nil)
	ctx := context.Background()

	// The test chain has 13 blocks, so latest executed = 13.
	const latestExecuted = 13

	t.Run("omitted parameter returns latest executed", func(t *testing.T) {
		result, err := api.BlockNumber(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, hexutil.Uint64(latestExecuted), result)
	})

	t.Run("latest returns forkchoice head", func(t *testing.T) {
		tag := rpc.LatestBlockNumber
		result, err := api.BlockNumber(ctx, &tag)
		require.NoError(t, err)
		// In the test fixture, forkchoice head == latest executed.
		require.Equal(t, hexutil.Uint64(latestExecuted), result)
	})

	t.Run("earliest returns zero", func(t *testing.T) {
		tag := rpc.EarliestBlockNumber
		result, err := api.BlockNumber(ctx, &tag)
		require.NoError(t, err)
		require.Equal(t, hexutil.Uint64(0), result)
	})

	t.Run("numeric selector returns that block number", func(t *testing.T) {
		num := rpc.BlockNumber(1)
		result, err := api.BlockNumber(ctx, &num)
		require.NoError(t, err)
		require.Equal(t, hexutil.Uint64(1), result)
	})

	t.Run("numeric selector 0x0 returns zero", func(t *testing.T) {
		num := rpc.BlockNumber(0)
		result, err := api.BlockNumber(ctx, &num)
		require.NoError(t, err)
		require.Equal(t, hexutil.Uint64(0), result)
	})

	t.Run("numeric selector mid-chain", func(t *testing.T) {
		num := rpc.BlockNumber(7)
		result, err := api.BlockNumber(ctx, &num)
		require.NoError(t, err)
		require.Equal(t, hexutil.Uint64(7), result)
	})

	t.Run("invalid negative block number returns error", func(t *testing.T) {
		num := rpc.BlockNumber(-10)
		_, err := api.BlockNumber(ctx, &num)
		require.Error(t, err)
	})
}
