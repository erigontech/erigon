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

package services

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
)

func getAggregateAndProofAndState(t *testing.T) (*cltypes.SignedAggregateAndProof, *state.CachingBeaconState) {
	_, _, s := tests.GetBellatrixRandom()
	br, _ := s.BlockRoot()
	checkpoint := s.CurrentJustifiedCheckpoint()
	a := &cltypes.SignedAggregateAndProof{
		Message: &cltypes.AggregateAndProof{
			AggregatorIndex: 141,
			Aggregate: solid.NewAttestionFromParameters([]byte{1, 2}, solid.NewAttestionDataFromParameters(
				s.Slot(),
				0,
				br,
				checkpoint,
				checkpoint,
			), common.Bytes96{}),
			SelectionProof: common.Bytes96{},
		},
	}
	a.Message.Aggregate.AttestantionData().Target().SetEpoch(s.Slot() / 32)
	return a, s

}

func setupAggregateAndProofTest(t *testing.T) (AggregateAndProofService, *synced_data.SyncedDataManager, *mock_services.ForkChoiceStorageMock) {
	ctx, cn := context.WithCancel(context.Background())
	cn()
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(true, cfg)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	p := pool.OperationsPool{}
	p.AttestationsPool = pool.NewOperationPool[libcommon.Bytes96, *solid.Attestation](100, "test")
	blockService := NewAggregateAndProofService(ctx, syncedDataManager, forkchoiceMock, cfg, p, true)
	return blockService, syncedDataManager, forkchoiceMock
}

func TestAggregateAndProofServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, _ := getAggregateAndProofAndState(t)

	aggService, _, _ := setupAggregateAndProofTest(t)
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceHighSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)
	agg.Message.Aggregate.AttestantionData().SetSlot(9998898)

	aggService, sd, _ := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceBadEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)
	agg.Message.Aggregate.AttestantionData().SetSlot(0)

	aggService, sd, _ := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceNotAncestor(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceNoHeader(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch()*32] = s.FinalizedCheckpoint().BlockRoot()
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofInvalidEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch()*32] = s.FinalizedCheckpoint().BlockRoot()
	fcu.Headers[agg.Message.Aggregate.AttestantionData().BeaconBlockRoot()] = &cltypes.BeaconBlockHeader{}
	agg.Message.Aggregate.AttestantionData().Target().SetEpoch(999999)
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofInvalidCommittee(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch()*32] = s.FinalizedCheckpoint().BlockRoot()
	fcu.Headers[agg.Message.Aggregate.AttestantionData().BeaconBlockRoot()] = &cltypes.BeaconBlockHeader{}
	agg.Message.AggregatorIndex = 12453224
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofAncestorMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch()*32] = s.FinalizedCheckpoint().BlockRoot()
	fcu.Headers[agg.Message.Aggregate.AttestantionData().BeaconBlockRoot()] = &cltypes.BeaconBlockHeader{}
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch()*32] = s.FinalizedCheckpoint().BlockRoot()
	fcu.Ancestors[agg.Message.Aggregate.AttestantionData().Slot()] = agg.Message.Aggregate.AttestantionData().Target().BlockRoot()
	fcu.Headers[agg.Message.Aggregate.AttestantionData().BeaconBlockRoot()] = &cltypes.BeaconBlockHeader{}
	require.NoError(t, aggService.ProcessMessage(context.Background(), nil, agg))
}
