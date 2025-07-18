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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
)

func getAggregateAndProofAndState(t *testing.T) (*SignedAggregateAndProofForGossip, *state.CachingBeaconState) {
	_, _, s := tests.GetBellatrixRandom()
	br, _ := s.BlockRoot()
	checkpoint := s.CurrentJustifiedCheckpoint()
	a := &SignedAggregateAndProofForGossip{
		SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{
			Message: &cltypes.AggregateAndProof{
				AggregatorIndex: 141,
				// Aggregate: solid.NewAttestionFromParameters([]byte{1, 2}, solid.NewAttestionDataFromParameters(
				// 	s.Slot(),
				// 	0,
				// 	br,
				// 	checkpoint,
				// 	checkpoint,
				// ), common.Bytes96{}),
				// SelectionProof: common.Bytes96{},
				Aggregate: &solid.Attestation{
					AggregationBits: solid.BitlistFromBytes([]byte{1, 2}, 2048),
					Data: &solid.AttestationData{
						Slot:            s.Slot(),
						BeaconBlockRoot: br,
						Source:          checkpoint,
						Target:          checkpoint,
					},
				},
			},
		},
	}

	a.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = s.Slot() / 32
	return a, s

}

func setupAggregateAndProofTest(t *testing.T) (AggregateAndProofService, *synced_data.SyncedDataManager, *mock_services.ForkChoiceStorageMock) {
	ctx, cn := context.WithCancel(context.Background())
	cn()
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	p := pool.OperationsPool{}
	p.AttestationsPool = pool.NewOperationPool[common.Bytes96, *solid.Attestation](100, "test")
	batchSignatureVerifier := NewBatchSignatureVerifier(context.TODO(), nil)
	go batchSignatureVerifier.Start()
	blockService := NewAggregateAndProofService(ctx, syncedDataManager, forkchoiceMock, cfg, p, true, batchSignatureVerifier)
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
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot = 9998898

	aggService, sd, _ := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofServiceBadEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot = 0

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
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = s.FinalizedCheckpoint().Root
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofInvalidEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = s.FinalizedCheckpoint().Root
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = 999999
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofInvalidCommittee(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = s.FinalizedCheckpoint().Root
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	agg.SignedAggregateAndProof.Message.AggregatorIndex = 12453224
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofAncestorMissing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = s.FinalizedCheckpoint().Root
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = s.FinalizedCheckpoint().Root
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{}
	require.NoError(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestSyncMapRangeDeadlock(t *testing.T) {
	var m sync.Map
	m.Store(1, 1)
	m.Store(2, 2)
	m.Store(3, 3)

	m.Range(func(key, value any) bool {
		m.Store(4, 5)
		return true
	})
}
