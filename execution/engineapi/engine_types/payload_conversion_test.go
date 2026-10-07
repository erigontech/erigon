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

package engine_types

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func TestExecutionPayloadFromBlockIncludesCanonicalEmptyBAL(t *testing.T) {
	t.Parallel()

	emptyBAL := []byte{0xc0}
	sidecar, err := types.DecodeBlockAccessListSidecar(emptyBAL)
	require.NoError(t, err)
	baseFee := uint256.NewInt(1_000_000_000)
	emptyBALHash := empty.BlockAccessListHash
	header := &types.Header{
		Number:              *uint256.NewInt(101),
		Time:                1,
		BaseFee:             baseFee,
		GasLimit:            30_000_000,
		BlockAccessListHash: &emptyBALHash,
	}
	block := types.NewBlockWithHeader(header, sidecar)

	payload, err := ExecutionPayloadFromBlock(block)
	require.NoError(t, err)

	require.NotNil(t, payload.BlockAccessList)
	require.Equal(t, hexutil.Bytes(emptyBAL), *payload.BlockAccessList)
}

func TestExecutionPayloadFromBlockReturnsSidecarEncodingError(t *testing.T) {
	t.Parallel()

	balHash := common.Hash{1}
	header := &types.Header{
		Number:              *uint256.NewInt(101),
		BaseFee:             uint256.NewInt(1_000_000_000),
		GasLimit:            30_000_000,
		BlockAccessListHash: &balHash,
	}
	// A nil nested StorageChange is the encoding failure that survives an
	// account list of values.
	sidecar := types.NewBlockAccessListSidecar(types.BlockAccessList{{
		Address: common.Address{2},
		StorageChanges: []types.SlotChanges{{
			Slot:    accounts.InternKey(common.Hash{3}),
			Changes: []*types.StorageChange{nil},
		}},
	}})
	block := types.NewBlockWithHeader(header, sidecar)

	payload, err := ExecutionPayloadFromBlock(block)

	require.Nil(t, payload)
	require.ErrorContains(t, err, "encode block access list")
}
