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
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type attesterSlashingErrorStore struct {
	forkchoice.ForkChoiceStorage
	err error
}

func (s attesterSlashingErrorStore) OnAttesterSlashing(*cltypes.AttesterSlashing, bool) error {
	return s.err
}

func setupBlockService(t *testing.T, ctrl *gomock.Controller) (BlockService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock, *mock_services.ForkChoiceStorageMock) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	blockService := NewBlockService(t.Context(), db, forkchoiceMock, syncedDataManager, ethClock, cfg, nil)
	return blockService, syncedDataManager, ethClock, forkchoiceMock
}

func TestBlockServiceUnsynced(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, _ := tests.GetBellatrixRandom()

	blockService, _, _, _ := setupBlockService(t, ctrl)
	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceIgnoreSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, _ := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceLowerThanFinalizedCheckpoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	blocks[0].Block.Slot = 0

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceUnseenParentRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceYoungerThanParent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	blocks[1].Block.Slot--

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[1]))
}

func TestBlockServiceInvalidCommitmentsPerBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	blocks[1].Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)
	// Append lots of commitments
	for range 100 {
		blocks[1].Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}
	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[1]))
}

func TestBlockServiceSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, post := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.StateAtBlockRootVal[blocks[1].Block.ParentRoot] = parentState
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}
	blocks[1].Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)

	require.NoError(t, blockService.ProcessMessage(context.Background(), nil, blocks[1]))
}

func TestBlockServiceGossipRejectsBlockOutsideFinalizedChain(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()
	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: common.Hash{0xff}}

	err := blockService.ValidateGossip(t.Context(), blocks[1])
	require.ErrorContains(t, err, "finalized checkpoint is not an ancestor")
}

func TestBlockServiceGossipRejectsUnexpectedProposer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, post := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))
	targetEpoch := blocks[1].Block.Slot / parentState.BeaconConfig().SlotsPerEpoch
	mixPosition := (targetEpoch + parentState.BeaconConfig().EpochsPerHistoricalVector - parentState.BeaconConfig().MinSeedLookahead - 1) % parentState.BeaconConfig().EpochsPerHistoricalVector
	foundUnexpectedProposer := false
	for nonce := 1; nonce <= 255; nonce++ {
		parentState.SetRandaoMixAt(int(mixPosition), common.Hash{byte(nonce)})
		expected, proposerErr := parentState.GetBeaconProposerIndexForSlot(blocks[1].Block.Slot)
		require.NoError(t, proposerErr)
		if expected != blocks[1].Block.ProposerIndex {
			foundUnexpectedProposer = true
			break
		}
	}
	require.True(t, foundUnexpectedProposer)

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.Blocks[blocks[1].Block.ParentRoot] = blocks[0]
	fcu.StateAtBlockRootVal[blocks[1].Block.ParentRoot] = parentState
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}

	err = blockService.ValidateGossip(t.Context(), blocks[1])
	require.ErrorContains(t, err, "does not match expected proposer")
}

func TestBlockServiceGossipWaitsForFullParentPayloadVerification(t *testing.T) {
	service, child, fcu, parentRoot, parentBlockHash := newGloasGossipValidationFixture(t, nil)
	fcu.ExecutionPayloadStatusMap[parentBlockHash] = execution_client.PayloadStatusValidated

	err := service.ValidateGossip(t.Context(), child)
	require.ErrorContains(t, err, "parent payload is not verified")
	require.NotContains(t, fcu.PayloadStatusByRootMap, parentRoot)
}

func TestBlockServiceGossipAcceptsVerifiedFullParentPayload(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceGossipRejectsWrongEmptyParentExecutionHead(t *testing.T) {
	service, child, _, _, _ := newGloasGossipValidationFixture(t, func(common.Hash, common.Hash) common.Hash {
		return common.Hash{0x99}
	})
	require.ErrorContains(t, service.ValidateGossip(t.Context(), child), "does not build on the parent's execution head")
}

func TestBlockServiceGossipAcceptsEmptyParentExecutionHead(t *testing.T) {
	service, child, _, _, _ := newGloasGossipValidationFixture(t, func(parentExecutionHead, _ common.Hash) common.Hash {
		return parentExecutionHead
	})
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func newGloasGossipValidationFixture(t *testing.T, childParentHash func(parentExecutionHead, parentBlockHash common.Hash) common.Hash) (BlockService, *cltypes.SignedBeaconBlock, *mock_services.ForkChoiceStorageMock, common.Hash, common.Hash) {
	t.Helper()
	ctrl := gomock.NewController(t)
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0
	parentSlot := cfg.SlotsPerEpoch
	childSlot := parentSlot + 1

	privateKey, err := bls.GenerateKey()
	require.NoError(t, err)
	validator := solid.NewValidator()
	var pubkey [48]byte
	copy(pubkey[:], bls.CompressPublicKey(privateKey.PublicKey()))
	validator.SetPublicKey(pubkey)
	validator.SetActivationEpoch(0)
	validator.SetExitEpoch(cfg.FarFutureEpoch)
	validator.SetEffectiveBalance(cfg.MaxEffectiveBalance)
	parentState := state.New(&cfg)
	parentState.SetVersion(clparams.GloasVersion)
	parentState.SetSlot(parentSlot)
	parentState.AddValidator(validator, cfg.MaxEffectiveBalance)
	parentState.SetProposerLookahead(solid.NewUint64VectorSSZ(int((cfg.MinSeedLookahead + 1) * cfg.SlotsPerEpoch)))
	parentExecutionHead := common.Hash{0x11}
	parentState.SetLatestBlockHash(parentExecutionHead)

	parentBlockHash := common.Hash{0x22}
	parent := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	parent.Block.Slot = parentSlot
	parent.Block.ProposerIndex = 0
	parent.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		ParentBlockHash: parentExecutionHead,
		BlockHash:       parentBlockHash,
	}}
	parentRoot, err := parent.Block.HashSSZ()
	require.NoError(t, err)

	child := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	child.Block.Slot = childSlot
	child.Block.ProposerIndex = 0
	child.Block.ParentRoot = parentRoot
	selectedParentHash := parentBlockHash
	if childParentHash != nil {
		selectedParentHash = childParentHash(parentExecutionHead, parentBlockHash)
	}
	child.Block.Body.SignedExecutionPayloadBid = &cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{
		ParentBlockHash: selectedParentHash,
		ParentBlockRoot: parentRoot,
	}}
	domain, err := parentState.GetDomain(cfg.DomainBeaconProposer, childSlot/cfg.SlotsPerEpoch)
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(child.Block, domain)
	require.NoError(t, err)
	copy(child.Signature[:], privateKey.Sign(signingRoot[:]).Bytes())

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	syncedDataManager := synced_data.NewSyncedDataManager(&cfg, true)
	require.NoError(t, syncedDataManager.OnHeadState(parentState))
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	ethClock.EXPECT().GetCurrentSlot().Return(childSlot).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Headers[parentRoot] = parent.SignedBeaconBlockHeader().Header.Copy()
	fcu.Blocks[parentRoot] = parent
	fcu.StateAtBlockRootVal[parentRoot] = parentState
	service := NewBlockService(t.Context(), db, fcu, syncedDataManager, ethClock, &cfg, nil)
	return service, child, fcu, parentRoot, parentBlockHash
}

func TestImportBlockOperationsAttesterSlashingLogging(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantLogged bool
	}{
		{name: "ignored", err: forkchoice.ErrIgnore},
		{name: "rejected", err: errors.New("invalid attester slashing"), wantLogged: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var output bytes.Buffer
			logger := log.Root()
			previousHandler := logger.GetHandler()
			logger.SetHandler(log.StreamHandler(&output, log.LogfmtFormat()))
			t.Cleanup(func() { logger.SetHandler(previousHandler) })

			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Body.AttesterSlashings.Append(&cltypes.AttesterSlashing{})
			service := blockService{forkchoiceStore: attesterSlashingErrorStore{err: tc.err}}

			service.importBlockOperations(block)

			require.Equal(t, tc.wantLogged, bytes.Contains(output.Bytes(), []byte("bad attester slashing received")))
		})
	}
}

// ==================== GLOAS (EIP-7732/ePBS) Tests ====================
//
// NOTE: GLOAS-specific ProcessMessage tests are currently not included because:
// 1. GLOAS-specific validation (bid checks, parent payload checks) happens AFTER
//    signature verification in the ProcessMessage flow
// 2. Signature verification requires properly signed blocks with matching validator keys
// 3. We don't have GLOAS test data with valid signatures available yet
//
// The GLOAS validation code is tested indirectly through:
// - Pre-GLOAS tests that verify the overall ProcessMessage flow
// - The validation code being structurally similar to pre-GLOAS validation
//
// Once GLOAS test vectors with proper signatures are available, these tests can be added:
// - TestBlockServiceGloasMismatchedParentBlockRoot
// - TestBlockServiceGloasParentPayloadNotSeen
// - TestBlockServiceGloasParentPayloadInvalid
// - TestBlockServiceGloasSuccess
//
// For now, the GLOAS validation code path is verified by code review and integration tests.
