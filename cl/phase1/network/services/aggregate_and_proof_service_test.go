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

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
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
	verifierCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batchSignatureVerifier := NewBatchSignatureVerifier(verifierCtx, nil)
	go batchSignatureVerifier.Start()
	blockService := NewAggregateAndProofService(ctx, syncedDataManager, forkchoiceMock, cfg, p, true, batchSignatureVerifier, validator_params.NewValidatorParams())
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
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	require.Error(t, aggService.ProcessMessage(context.Background(), nil, agg))
}

func TestAggregateAndProofInvalidEpoch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	agg, s := getAggregateAndProofAndState(t)

	aggService, sd, fcu := setupAggregateAndProofTest(t)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
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
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
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
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
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
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}
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

// getAggregateAndProofAndStateForVersion creates test data for a specific version.
// For Electra+, it sets up CommitteeBits properly.
func getAggregateAndProofAndStateForVersion(t *testing.T, version clparams.StateVersion) (*SignedAggregateAndProofForGossip, *state.CachingBeaconState) {
	_, _, s := tests.GetBellatrixRandom()
	br, _ := s.BlockRoot()
	checkpoint := s.CurrentJustifiedCheckpoint()

	a := &SignedAggregateAndProofForGossip{
		SignedAggregateAndProof: &cltypes.SignedAggregateAndProof{
			Message: &cltypes.AggregateAndProof{
				AggregatorIndex: 141,
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

	// For Electra+, set up CommitteeBits with exactly one bit set
	if version >= clparams.ElectraVersion {
		committeeBits := solid.NewBitVector(64)
		committeeBits.SetBitAt(0, true) // Set committee index 0
		a.SignedAggregateAndProof.Message.Aggregate.CommitteeBits = committeeBits
	}

	a.SignedAggregateAndProof.Message.Aggregate.Data.Target.Epoch = s.Slot() / 32
	return a, s
}

// setupAggregateAndProofTestWithConfig creates a test setup with custom config
func setupAggregateAndProofTestWithConfig(t *testing.T, cfg *clparams.BeaconChainConfig) (AggregateAndProofService, *synced_data.SyncedDataManager, *mock_services.ForkChoiceStorageMock) {
	ctx, cn := context.WithCancel(context.Background())
	cn()
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	p := pool.OperationsPool{}
	p.AttestationsPool = pool.NewOperationPool[common.Bytes96, *solid.Attestation](100, "test")
	verifierCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	batchSignatureVerifier := NewBatchSignatureVerifier(verifierCtx, nil)
	go batchSignatureVerifier.Start()
	blockService := NewAggregateAndProofService(ctx, syncedDataManager, forkchoiceMock, cfg, p, true, batchSignatureVerifier, validator_params.NewValidatorParams())
	return blockService, syncedDataManager, forkchoiceMock
}

// TestAggregateAndProofGloasRejectIndexGte2 tests that GLOAS rejects aggregate.data.index >= 2
func TestAggregateAndProofGloasRejectIndexGte2(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0 so any slot is GLOAS
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 2 (invalid for GLOAS, must be < 2)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 2

	aggService, sd, _ := setupAggregateAndProofTestWithConfig(t, &cfg)
	sd.OnHeadState(s)

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be < 2")
}

// TestAggregateAndProofGloasRejectIndexNot0WhenSlotsMatch tests that GLOAS rejects
// aggregate.data.index != 0 when block.slot == aggregate.data.slot
// Per spec: [REJECT] aggregate.data.index == 0 if block.slot == aggregate.data.slot
// This means: when same slot, index MUST be 0
func TestAggregateAndProofGloasRejectIndexNot0WhenSlotsMatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 1 (will be rejected if slots match, must be 0)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}

	// Set the block header with same slot as aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot, // Same slot as aggregate
	}

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be 0 when block.slot == aggregate.data.slot")
}

// TestAggregateAndProofGloasAllowIndex0WhenSlotsDiffer tests that GLOAS allows
// aggregate.data.index == 0 when block.slot != aggregate.data.slot
func TestAggregateAndProofGloasAllowIndex0WhenSlotsDiffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 0 (allowed when slots differ - signals payload not present)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 0

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}

	// Set the block header with DIFFERENT slot than aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot - 1
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot, // Different slot from aggregate
	}

	// Should pass (index=0 is allowed when slots differ)
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.NoError(t, err)
}

// TestAggregateAndProofGloasAllowIndex1WhenSlotsDiffer tests that GLOAS allows
// aggregate.data.index == 1 when block.slot != aggregate.data.slot
func TestAggregateAndProofGloasAllowIndex1WhenSlotsDiffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set CommitteeIndex to 1 (allowed when slots differ - signals payload present)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}

	// Set the block header with DIFFERENT slot than aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot - 1
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot, // Different slot from aggregate
	}

	// Should pass (index=1 is allowed when slots differ)
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.NoError(t, err)
}

// TestAggregateAndProofGloasAllowIndex0WhenSlotsMatch tests that GLOAS allows
// aggregate.data.index == 0 when block.slot == aggregate.data.slot
// Per spec: [REJECT] aggregate.data.index == 0 if block.slot == aggregate.data.slot
// This means: when same slot, index MUST be 0 (index=0 is allowed/required)
func TestAggregateAndProofGloasAllowIndex0WhenSlotsMatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with all forks at epoch 0
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.GloasVersion)

	// Set aggregate.data.CommitteeIndex to 0 (required when slots match)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 0

	aggService, sd, fcu := setupAggregateAndProofTestWithConfig(t, &cfg)
	sd.OnHeadState(s)
	fcu.FinalizedCheckpointVal = s.FinalizedCheckpoint()
	fcu.Ancestors[s.FinalizedCheckpoint().Epoch*32] = forkchoice.ForkChoiceNode{Root: s.FinalizedCheckpoint().Root}
	fcu.Ancestors[agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot] = forkchoice.ForkChoiceNode{Root: agg.SignedAggregateAndProof.Message.Aggregate.Data.Target.Root}

	// Set the block header with SAME slot as aggregate.data.slot
	blockSlot := agg.SignedAggregateAndProof.Message.Aggregate.Data.Slot
	fcu.Headers[agg.SignedAggregateAndProof.Message.Aggregate.Data.BeaconBlockRoot] = &cltypes.BeaconBlockHeader{
		Slot: blockSlot,
	}

	// Should pass (index=0 is required when slots match)
	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.NoError(t, err)
}

// TestAggregateAndProofElectraRejectIndexNot0 tests that Electra/Fulu still rejects index != 0
func TestAggregateAndProofElectraRejectIndexNot0(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Create a config with Electra fork but GLOAS far in the future
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 999999 // Far in the future, so we stay in Fulu

	agg, s := getAggregateAndProofAndStateForVersion(t, clparams.ElectraVersion)

	// Set CommitteeIndex to 1 (invalid for Electra/Fulu, must be 0)
	agg.SignedAggregateAndProof.Message.Aggregate.Data.CommitteeIndex = 1

	aggService, sd, _ := setupAggregateAndProofTestWithConfig(t, &cfg)
	sd.OnHeadState(s)

	err := aggService.ProcessMessage(context.Background(), nil, agg)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid committee_index in aggregate and proof")
}
