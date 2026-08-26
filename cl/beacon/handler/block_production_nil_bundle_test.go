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

package handler

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func TestProduceBeaconBodyRejectsNilBlobsBundle(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, blocks, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.ElectraVersion, log.Root(), false)

	payload := cltypes.NewEth1Block(clparams.ElectraVersion, h.beaconChainCfg)
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(h.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
	payload.Transactions = &solid.TransactionsSSZ{}

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]byte{1, 2, 3, 4, 5, 6, 7, 8}, nil).AnyTimes()
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(payload, nil, nil, big.NewInt(0), nil).AnyTimes()
	h.engine = engine

	baseBlock := blocks[len(blocks)-1].Block
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)
	targetSlot := baseBlock.Slot + 1

	_, _, err = h.produceBeaconBody(
		t.Context(), 3, baseBlock.Slot, baseBlockRoot, postState, targetSlot,
		common.Bytes96{}, common.Hash{},
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no blobs bundle")
}

func TestProduceBeaconBodyToleratesNilBlobsBundleBeforeDeneb(t *testing.T) {
	ctrl := gomock.NewController(t)
	_, blocks, _, _, postState, h, _, _, _, _ := setupTestingHandler(t, clparams.CapellaVersion, log.Root(), false)

	payload := cltypes.NewEth1Block(clparams.CapellaVersion, h.beaconChainCfg)
	payload.Withdrawals = solid.NewStaticListSSZ[*cltypes.Withdrawal](int(h.beaconChainCfg.MaxWithdrawalsPerPayload), 44)
	payload.Transactions = &solid.TransactionsSSZ{}

	engine := execution_client.NewMockExecutionEngine(ctrl)
	engine.EXPECT().ForkChoiceUpdate(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return([]byte{1, 2, 3, 4, 5, 6, 7, 8}, nil).AnyTimes()
	engine.EXPECT().GetAssembledBlock(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(payload, nil, nil, big.NewInt(0), nil).AnyTimes()
	h.engine = engine

	baseBlock := blocks[len(blocks)-1].Block
	baseBlockRoot, err := baseBlock.HashSSZ()
	require.NoError(t, err)
	targetSlot := baseBlock.Slot + 1
	require.True(t, h.beaconChainCfg.GetCurrentStateVersion(targetSlot/h.beaconChainCfg.SlotsPerEpoch).Before(clparams.DenebVersion))

	_, _, err = h.produceBeaconBody(
		t.Context(), 3, baseBlock.Slot, baseBlockRoot, postState, targetSlot,
		common.Bytes96{}, common.Hash{},
	)
	if err != nil {
		require.NotContains(t, err.Error(), "no blobs bundle")
	}
}
