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

package bor

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	common "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/bor/statefull"
	polychain "github.com/erigontech/erigon/polygon/chain"
	"github.com/erigontech/erigon/polygon/heimdall"
)

var _ bridgeReader = mockBridgeReader{}

type mockBridgeReader struct{}

func (m mockBridgeReader) Events(context.Context, common.Hash, uint64) ([]*types.Message, error) {
	panic("mock")
}

func (m mockBridgeReader) EventsWithinTime(context.Context, time.Time, time.Time) ([]*types.Message, error) {
	panic("mock")
}

var _ spanReader = mockSpanReader{}

type mockSpanReader struct{}

func (m mockSpanReader) Span(context.Context, uint64) (*heimdall.Span, bool, error) {
	panic("mock")
}

func (m mockSpanReader) Producers(context.Context, uint64) (*heimdall.ValidatorSet, error) {
	panic("mock")
}

func TestCommitStatesIndore(t *testing.T) {
	ctrl := gomock.NewController(t)
	cr := consensus.NewMockChainReader(ctrl)
	br := NewMockbridgeReader(ctrl)

	bor := New(polychain.BorDevnet.Config, nil, nil, nil, nil, br, nil)

	header := &types.Header{
		Number: big.NewInt(112),
		Time:   1744000028,
	}

	contractAddr := common.HexToAddress("a1")

	cr.EXPECT().GetHeaderByNumber(uint64(96)).Return(&types.Header{
		Number: big.NewInt(96),
		Time:   1744000000,
	})
	br.EXPECT().EventsWithinTime(gomock.Any(), time.Unix(1744000000-128, 0), time.Unix(1744000028-128, 0)).Return(
		[]*types.Message{
			types.NewMessage(
				common.HexToAddress(""),
				&contractAddr,
				0,
				uint256.NewInt(0),
				0,
				nil,
				nil,
				nil,
				nil,
				nil,
				false,
				false,
				nil,
			),
		}, nil,
	)

	called := 0

	syscall := func(contract common.Address, data []byte) ([]byte, error) {
		require.Equal(t, contract, contractAddr)
		called++

		return nil, nil
	}

	err := bor.CommitStates(header, statefull.ChainContext{Chain: cr}, syscall, true)
	require.NoError(t, err)
	require.Equal(t, 1, called)
}
