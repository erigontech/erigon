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

package engineapitester

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/chain"
	enginetypes "github.com/erigontech/erigon/execution/engineapi/engine_types"
)

func TestBuildEmptyPayloadSuppliesExplicitEmptyTransactions(t *testing.T) {
	parentHash := common.HexToHash("0x01")
	cl := &MockCl{
		logger:          log.New(),
		genesis:         common.HexToHash("0x02"),
		genesisGasLimit: 30_000_000,
		state: &MockClState{
			ParentElBlock:     parentHash,
			ParentElTimestamp: 1,
		},
		chainConfig: &chain.Config{},
	}

	ctx := t.Context()
	payload, err := cl.BuildEmptyPayload(ctx, func(
		gotCtx context.Context,
		result any,
		method string,
		args ...any,
	) error {
		require.Equal(t, ctx, gotCtx)
		require.Equal(t, "testing_buildBlockV1", method)
		require.Len(t, args, 4)
		require.Equal(t, parentHash, args[0])
		attributes, ok := args[1].(*enginetypes.PayloadAttributes)
		require.True(t, ok)
		require.NotNil(t, attributes.ParentBeaconBlockRoot)
		transactions, ok := args[2].(*[]hexutil.Bytes)
		require.True(t, ok)
		require.NotNil(t, transactions)
		require.Empty(t, *transactions)
		require.Nil(t, args[3])

		response, ok := result.(*enginetypes.GetPayloadResponse)
		require.True(t, ok)
		response.ExecutionPayload = &enginetypes.ExecutionPayload{Transactions: []hexutil.Bytes{}}
		return nil
	})
	require.NoError(t, err)
	require.Empty(t, payload.ExecutionPayload.Transactions)
	require.NotNil(t, payload.ParentBeaconBlockRoot)
}
