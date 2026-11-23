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

package services

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

func forkChoiceMock(processing *onBlockErrorStore) *mock_services.ForkChoiceStorageMock {
	return processing.ForkChoiceStorage.(*mock_services.ForkChoiceStorageMock)
}

func TestBlockServiceGossipIgnoresChildOfInvalidatedGrandparentPayload(t *testing.T) {
	service, block, processing, _ := newFuluColumnGossipFixture(t)
	store := forkChoiceMock(processing)
	parentRoot := block.Block.ParentRoot
	grandparentRoot := common.Hash{0x61}
	parentHeader := store.Headers[parentRoot]
	store.Headers[grandparentRoot] = &cltypes.BeaconBlockHeader{Slot: parentHeader.Slot - 1}
	parentHeader.ParentRoot = grandparentRoot
	store.InvalidBlocks = map[common.Hash]struct{}{grandparentRoot: {}}

	err := service.ProcessMessage(t.Context(), nil, block)
	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, processing.calls.Load())
	require.Zero(t, scheduledBlockCount(t, service))
}

func TestBlockServiceGossipParentThatFailedValidation(t *testing.T) {
	for name, tc := range map[string]struct {
		status     *execution_client.PayloadStatus
		wantIgnore bool
	}{
		"payload not processed": {},
		"payload validated":     {status: ptr[execution_client.PayloadStatus](execution_client.PayloadStatusValidated), wantIgnore: true},
		"payload invalidated":   {status: ptr[execution_client.PayloadStatus](execution_client.PayloadStatusInvalidated), wantIgnore: true},
	} {
		t.Run(name, func(t *testing.T) {
			service, block, processing, _ := newFuluColumnGossipFixture(t)
			store := forkChoiceMock(processing)
			parentRoot := block.Block.ParentRoot
			delete(store.Headers, parentRoot)
			store.InvalidBlocks = map[common.Hash]struct{}{parentRoot: {}}
			if tc.status != nil {
				payloadHash := common.Hash{0x62}
				store.Eth1Hashes[parentRoot] = payloadHash
				store.ExecutionPayloadStatusMap[payloadHash] = *tc.status
			}

			err := service.ProcessMessage(t.Context(), nil, block)
			require.Error(t, err)
			require.Equal(t, tc.wantIgnore, errors.Is(err, ErrIgnore))
			require.Zero(t, processing.calls.Load())
			require.Zero(t, scheduledBlockCount(t, service))
		})
	}
}

func ptr[T any](v T) *T { return &v }

func TestBlockServiceGossipWrongTimestampWithInvalidatedParentPayload(t *testing.T) {
	for name, tc := range map[string]struct {
		version    clparams.StateVersion
		wantIgnore bool
	}{
		"fulu ignores":    {version: clparams.FuluVersion, wantIgnore: true},
		"electra rejects": {version: clparams.ElectraVersion},
	} {
		t.Run(name, func(t *testing.T) {
			service, block, processing, _ := newColumnGossipFixture(t, tc.version, 1)
			forkChoiceMock(processing).PayloadStatusByRootMap[block.Block.ParentRoot] = execution_client.PayloadStatusInvalidated

			err := service.ProcessMessage(t.Context(), nil, block)
			require.Error(t, err)
			require.Equal(t, tc.wantIgnore, errors.Is(err, ErrIgnore))
			require.Zero(t, processing.calls.Load())
			require.Zero(t, scheduledBlockCount(t, service))
		})
	}
}

func TestBlockServiceGossipWrongTimestampWithUnknownParentIsRetried(t *testing.T) {
	service, block, processing, _ := newColumnGossipFixture(t, clparams.FuluVersion, 1)
	delete(forkChoiceMock(processing).Headers, block.Block.ParentRoot)

	err := service.ProcessMessage(t.Context(), nil, block)
	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, processing.calls.Load())
	require.Equal(t, 1, scheduledBlockCount(t, service))
}

func newPreMergeBellatrixGossipFixture(t *testing.T, mutate func(*cltypes.SignedBeaconBlock)) (*blockService, *cltypes.SignedBeaconBlock) {
	t.Helper()
	blocks, pre, _ := tests.GetBellatrixRandom()
	source, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(source, blocks[0], nil, false))
	cfg := *source.BeaconConfig()
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = cfg.FarFutureEpoch
	cfg.DenebForkEpoch = cfg.FarFutureEpoch
	cfg.ElectraForkEpoch = cfg.FarFutureEpoch
	cfg.FuluForkEpoch = cfg.FarFutureEpoch
	cfg.GloasForkEpoch = cfg.FarFutureEpoch
	cfg.InitializeForkSchedule()
	encoded, err := source.EncodeSSZ(nil)
	require.NoError(t, err)
	parentState := state.New(&cfg)
	require.NoError(t, parentState.DecodeSSZ(encoded, int(clparams.BellatrixVersion)))
	parentState.SetLatestExecutionPayloadHeader(cltypes.NewEth1Header(clparams.BellatrixVersion))
	require.False(t, state.IsMergeTransitionComplete(parentState))

	parentSlot := parentState.Slot()
	childSlot := parentSlot + 1
	proposerState, err := parentState.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.DefaultMachine.ProcessSlots(proposerState, childSlot))
	proposerIndex, err := proposerState.GetBeaconProposerIndexForSlot(childSlot)
	require.NoError(t, err)
	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	validator, err := parentState.ValidatorForValidatorIndex(int(proposerIndex))
	require.NoError(t, err)
	var publicKey [48]byte
	copy(publicKey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	validator.SetPublicKey(publicKey)
	parentState.SetValidatorAtIndex(int(proposerIndex), validator)

	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.BellatrixVersion)
	parent.Block.Slot = parentSlot
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.BellatrixVersion)
	block.Block.Slot = childSlot
	block.Block.ProposerIndex = proposerIndex
	block.Block.ParentRoot = parentRoot
	mutate(block)
	domain, err := parentState.GetDomain(cfg.DomainBeaconProposer, childSlot/cfg.SlotsPerEpoch)
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(block.Block, domain)
	require.NoError(t, err)
	copy(block.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())

	ctrl := gomock.NewController(t)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	syncedData := synced_data.NewSyncedDataManager(&cfg, true)
	require.NoError(t, syncedData.OnHeadState(parentState))
	clock := eth_clock.NewMockEthereumClock(ctrl)
	clock.EXPECT().GetCurrentSlot().Return(childSlot).AnyTimes()
	clock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	store := mock_services.NewForkChoiceStorageMock(t)
	store.Headers[parentRoot] = parent.SignedBeaconBlockHeader().Header.Copy()
	store.Blocks[parentRoot] = parent
	store.StateAtBlockRootVal[parentRoot] = parentState
	return newBlockService(db, store, syncedData, clock, &cfg, beaconevents.NewEventEmitter()), block
}

func TestBlockServiceGossipPreMergeBellatrixTimestamp(t *testing.T) {
	t.Run("non-default payload with zero block hash and wrong timestamp is rejected", func(t *testing.T) {
		service, block := newPreMergeBellatrixGossipFixture(t, func(block *cltypes.SignedBeaconBlock) {
			block.Block.Body.ExecutionPayload.Time = 1
		})
		err := service.validateGossip(t.Context(), block, nil)
		require.ErrorContains(t, err, "invalid execution payload timestamp")
		require.NotErrorIs(t, err, ErrIgnore)
	})
	t.Run("default payload is not checked", func(t *testing.T) {
		service, block := newPreMergeBellatrixGossipFixture(t, func(*cltypes.SignedBeaconBlock) {})
		require.NoError(t, service.validateGossip(t.Context(), block, nil))
	})
}
