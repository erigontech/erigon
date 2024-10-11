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

package sync

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/heimdall"
)

func TestVerifyCheckpointHeaders(t *testing.T) {
	header := &types.Header{
		TxHash: common.HexToHash("0x01"),
		Number: big.NewInt(42),
	}
	rootHash, err := bor.ComputeHeadersRootHash([]*types.Header{header})
	require.NoError(t, err)
	checkpoint := &heimdall.Milestone{
		Fields: heimdall.WaypointFields{
			RootHash: common.BytesToHash(rootHash),
		},
	}

	err = VerifyCheckpointHeaders(checkpoint, []*types.Header{header})
	require.NoError(t, err)

	diffHeader := &types.Header{
		TxHash: common.HexToHash("0x02"),
		Number: big.NewInt(42),
	}
	err = VerifyCheckpointHeaders(checkpoint, []*types.Header{diffHeader})
	require.ErrorIs(t, err, ErrBadHeadersRootHash)

	err = VerifyCheckpointHeaders(checkpoint, []*types.Header{})
	require.ErrorIs(t, err, ErrBadHeadersRootHash)
}

func TestVerifyMilestoneHeaders(t *testing.T) {
	header1 := &types.Header{
		Number:   big.NewInt(1),
		GasLimit: 123,
		Root:     common.HexToHash("0x01"),
	}
	header2 := &types.Header{
		Number:     big.NewInt(2),
		GasLimit:   456,
		Root:       common.HexToHash("0x02"),
		ParentHash: header1.Hash(),
	}
	milestone := &heimdall.Milestone{
		Fields: heimdall.WaypointFields{
			RootHash:   header2.Hash(),
			StartBlock: big.NewInt(1),
			EndBlock:   big.NewInt(2),
		},
	}

	err := VerifyMilestoneHeaders(milestone, []*types.Header{header1, header2})
	require.NoError(t, err)

	header2DiffHash := &types.Header{
		Number:     big.NewInt(2),
		GasLimit:   999,
		Root:       common.HexToHash("0x02-diff"),
		ParentHash: header1.Hash(),
	}
	err = VerifyMilestoneHeaders(milestone, []*types.Header{header1, header2DiffHash})
	require.ErrorIs(t, err, ErrBadHeadersRootHash)

	header3DisconnectedNums := &types.Header{
		Number:     big.NewInt(3),
		GasLimit:   456,
		Root:       common.HexToHash("0x02"),
		ParentHash: header1.Hash(),
	}
	err = VerifyMilestoneHeaders(milestone, []*types.Header{header1, header3DisconnectedNums})
	require.ErrorIs(t, err, ErrDisconnectedHeaders)

	header0 := types.Header{Number: big.NewInt(0)}
	header3DisconnectedHashes := &types.Header{
		Number:     big.NewInt(2),
		GasLimit:   456,
		Root:       common.HexToHash("0x02"),
		ParentHash: header0.Hash(),
	}
	err = VerifyMilestoneHeaders(milestone, []*types.Header{header1, header3DisconnectedHashes})
	require.ErrorIs(t, err, ErrDisconnectedHeaders)

	err = VerifyMilestoneHeaders(milestone, []*types.Header{header1})
	require.ErrorIs(t, err, ErrIncorrectHeadersLength)

	err = VerifyMilestoneHeaders(milestone, nil)
	require.ErrorIs(t, err, ErrIncorrectHeadersLength)
}
