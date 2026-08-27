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
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type attesterSlashingErrorStore struct {
	forkchoice.ForkChoiceStorage
	err error
}

type failFirstUpdateDB struct {
	kv.RwDB
	failed bool
}

func (db *failFirstUpdateDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	if !db.failed {
		db.failed = true
		return errors.New("database unavailable")
	}
	return db.RwDB.Update(ctx, f)
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

	service, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.StateAtBlockRootVal[blocks[1].Block.ParentRoot] = parentState
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}
	blocks[1].Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)

	require.NoError(t, service.ProcessMessage(context.Background(), nil, blocks[1]))
	key := proposerIndexAndSlot{
		proposerIndex: blocks[1].Block.ProposerIndex,
		slot:          blocks[1].Block.Slot,
	}
	seen, ok := service.(*blockService).seenBlocksCache.Get(key)
	require.True(t, ok)
	signedRoot, err := blocks[1].HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(signedRoot), seen.signedRoot)
}

func TestBlockServiceGossipRejectsBlockOutsideFinalizedChain(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()
	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	finalizedSlot := post.FinalizedCheckpoint().Epoch * post.BeaconConfig().SlotsPerEpoch
	fcu.Ancestors[finalizedSlot] = forkchoice.ForkChoiceNode{Root: common.Hash{0xff}}

	err := blockService.ValidateGossip(t.Context(), blocks[1])
	require.ErrorContains(t, err, "finalized checkpoint is not an ancestor")
}

func TestBlockServiceGossipUsesCheckpointSyncAnchorForFinalizedAncestor(t *testing.T) {
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
	fcu.AnchorSlotVal = blocks[0].Block.Slot
	fcu.Ancestors[fcu.AnchorSlotVal] = forkchoice.ForkChoiceNode{Root: post.FinalizedCheckpoint().Root}

	require.NoError(t, blockService.ValidateGossip(t.Context(), blocks[1]))
}

func TestBlockServiceGossipUsesForkChoiceFinalizedCheckpointAtGenesis(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, post := tests.GetBellatrixRandom()
	parentState, err := pre.Copy()
	require.NoError(t, err)
	require.NoError(t, transition.TransitionState(parentState, blocks[0], nil, false))
	headState, err := post.Copy()
	require.NoError(t, err)
	headState.SetFinalizedCheckpoint(solid.Checkpoint{})

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(headState))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	anchorRoot := blocks[1].Block.ParentRoot
	fcu.FinalizedCheckpointVal = solid.Checkpoint{Root: anchorRoot}
	fcu.Headers[anchorRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.StateAtBlockRootVal[anchorRoot] = parentState
	fcu.AnchorRootVal = anchorRoot
	fcu.AnchorSlotVal = blocks[0].Block.Slot
	fcu.Ancestors[fcu.AnchorSlotVal] = forkchoice.ForkChoiceNode{Root: anchorRoot}

	require.NoError(t, blockService.ValidateGossip(t.Context(), blocks[1]))
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
		require.NoError(t, parentState.SetRandaoMixAt(int(mixPosition), common.Hash{byte(nonce)}))
		expected, proposerErr := parentState.GetBeaconProposerIndexForSlot(blocks[1].Block.Slot)
		require.NoError(t, proposerErr)
		if expected != blocks[1].Block.ProposerIndex {
			foundUnexpectedProposer = true
			break
		}
	}
	require.True(t, foundUnexpectedProposer)

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	require.NoError(t, syncedData.OnHeadState(post))
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

func TestBlockServiceGossipFirstValidReservationIsAtomic(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { errs <- service.ValidateGossip(t.Context(), child) })
	}
	wg.Wait()
	close(errs)
	accepted := 0
	ignored := 0
	for err := range errs {
		if err == nil {
			accepted++
		} else if errors.Is(err, ErrIgnore) {
			ignored++
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, ignored)
}

func TestBlockServiceGossipReservationCanBeReleased(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	service.ReleaseGossipReservation(child)
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceCommittedReservationAllowsExactRESTReplayOnly(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	service.CommitGossipReservation(child)
	require.ErrorIs(t, service.ValidateGossip(t.Context(), child), ErrIgnore)
	service.ReleaseGossipReservation(child)

	child.Block.StateRoot[0] ^= 1
	err := service.ValidateGossip(t.Context(), child)
	require.ErrorIs(t, err, ErrIgnore)
	require.ErrorContains(t, err, "already seen")
	child.Block.StateRoot[0] ^= 1

	require.NoError(t, service.ValidateGossip(t.Context(), child))
	require.ErrorIs(t, service.ValidateGossip(t.Context(), child), ErrIgnore)
	child.Signature[0] ^= 1
	require.ErrorIs(t, service.ValidateGossip(t.Context(), child), ErrIgnore)
}

func TestBlockServiceExactRESTReplayClaimIsAtomic(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	service.CommitGossipReservation(child)
	service.ReleaseGossipReservation(child)

	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() { errs <- service.ValidateGossip(t.Context(), child) })
	}
	wg.Wait()
	close(errs)
	accepted := 0
	ignored := 0
	for err := range errs {
		if err == nil {
			accepted++
		} else if errors.Is(err, ErrIgnore) {
			ignored++
		}
	}
	require.Equal(t, 1, accepted)
	require.Equal(t, 1, ignored)
}

func TestBlockServiceFailedExactRESTReplayRestoresClaim(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	service.CommitGossipReservation(child)
	service.ReleaseGossipReservation(child)
	require.NoError(t, service.ValidateGossip(t.Context(), child))

	service.ReleaseGossipReservation(child)
	require.NoError(t, service.ValidateGossip(t.Context(), child))
}

func TestBlockServiceValidateGossipRejectsMissingBodyBeforeHashing(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Body = nil
	require.ErrorContains(t, service.ValidateGossip(t.Context(), block), "missing beacon block")
}

func TestBlockServiceP2PDuplicateIsIgnoredBeforeHashing(t *testing.T) {
	service, child, _, _, _ := newGloasGossipValidationFixture(t, nil)
	child.Block.Body.Eth1Data = nil
	key := blockGossipKey(child)
	service.(*blockService).seenBlocksCache.Add(key, seenBlock{})

	err := service.ProcessMessage(t.Context(), nil, child)

	require.ErrorIs(t, err, ErrIgnore)
	require.Nil(t, child.Block.Body.Eth1Data)
}

func TestScheduledBlockRepairsDatabaseWhenHeaderAlreadyExists(t *testing.T) {
	underlying := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	db := &failFirstUpdateDB{RwDB: underlying}
	fcu := mock_services.NewForkChoiceStorageMock(t)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	fcu.Headers[root] = block.SignedBeaconBlockHeader().Header.Copy()
	service := &blockService{db: db, forkchoiceStore: fcu}

	service.ScheduleBlockForLaterProcessing(block)
	jobValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	job := jobValue.(*blockJob)
	service.processScheduledBlock(t.Context(), root, job, job.creationTime)
	require.True(t, db.failed)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	service.processScheduledBlock(t.Context(), root, job, job.creationTime)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, ok)

	require.NoError(t, underlying.View(t.Context(), func(tx kv.Tx) error {
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, root))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		return nil
	}))
}

func TestPublishedBlockJobRetainsFullStoreUntilSuccess(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	service := &blockService{}
	attempts := 0
	storedSidecars := false
	imported := false
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		attempts++
		if attempts == 1 {
			return errors.New("sidecar storage unavailable")
		}
		storedSidecars = true
		imported = true
		return db.Update(t.Context(), func(tx kv.RwTx) error {
			return beacon_indicies.WriteBeaconBlockAndIndicies(t.Context(), tx, block, false)
		})
	})
	jobValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	job := jobValue.(*blockJob)
	service.processScheduledBlock(t.Context(), root, job, job.creationTime)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.False(t, storedSidecars)
	service.processScheduledBlock(t.Context(), root, job, job.creationTime)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, ok)
	require.True(t, job.terminal)
	require.NoError(t, handle.Wait(t.Context()))
	require.NoError(t, handle.Wait(t.Context()))
	require.True(t, storedSidecars)
	require.True(t, imported)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(block.Block.Slot, root))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		return nil
	}))
}

func TestPublishedBlockJobUpgradesBlockOnlyRecovery(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service := &blockService{}
	service.ScheduleBlockForLaterProcessing(block)
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	jobValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.NotNil(t, jobValue.(*blockJob).store)
}

func TestPublishedBlockJobIsNotDowngradedByBlockOnlyRecovery(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service := &blockService{}
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	fullValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	service.ScheduleBlockForLaterProcessing(block)
	currentValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, fullValue, currentValue)
}

func TestOlderPublishedBlockJobDoesNotReplaceNewerFullStore(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	newer := &blockJob{
		block:        block,
		store:        func(context.Context) error { return nil },
		creationTime: time.Now().Add(time.Minute),
	}
	service := &blockService{}
	service.blocksScheduledForLaterExecution.Store(root, newer)

	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		return errors.New("older store should not replace newer store")
	})

	currentValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, newer, currentValue)
}

func TestPublishedBlockUpgradeSurvivesStaleBlockOnlyWorker(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fcu := mock_services.NewForkChoiceStorageMock(t)
	fcu.Headers[root] = block.SignedBeaconBlockHeader().Header.Copy()
	service := &blockService{db: db, forkchoiceStore: fcu}
	service.ScheduleBlockForLaterProcessing(block)
	staleValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	staleJob := staleValue.(*blockJob)
	fullStoreCalls := 0
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		fullStoreCalls++
		return nil
	})
	service.processScheduledBlock(t.Context(), root, staleJob, staleJob.creationTime)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, ok)
	require.Equal(t, 1, fullStoreCalls)
}

func TestPublishedBlockRefreshSurvivesStaleFullStoreWorker(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service := &blockService{}
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		return errors.New("stale store should not run")
	})
	staleValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	stableJob := staleValue.(*blockJob)
	freshStoreCalls := 0
	service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		freshStoreCalls++
		return nil
	})
	freshValue, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, stableJob, freshValue)
	service.processScheduledBlock(t.Context(), root, freshValue.(*blockJob), time.Now())
	require.Equal(t, 1, freshStoreCalls)
	_, ok = service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, ok)
}

func TestBlockServicePendingGossipReservationHandsOffToP2P(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.(*blockService).validateFirstGossip(t.Context(), child, nil, true)
	}()
	select {
	case err := <-errCh:
		t.Fatalf("P2P validation returned before REST reservation resolved: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	service.ReleaseGossipReservation(child)
	require.NoError(t, <-errCh)
}

func TestBlockServiceCommittedGossipReservationRejectsWaitingP2P(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.(*blockService).validateFirstGossip(t.Context(), child, nil, true)
	}()
	select {
	case err := <-errCh:
		t.Fatalf("P2P validation returned before REST reservation resolved: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	service.CommitGossipReservation(child)
	require.ErrorIs(t, <-errCh, ErrIgnore)
}

func TestBlockServiceRevalidatesP2PAfterReservationRelease(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	type result struct {
		err       error
		scheduled bool
	}
	resultCh := make(chan result, 1)
	go func() {
		scheduled := false
		err := service.(*blockService).validateFirstGossip(t.Context(), child, func() { scheduled = true }, true)
		resultCh <- result{err: err, scheduled: scheduled}
	}()
	select {
	case got := <-resultCh:
		t.Fatalf("P2P validation returned before REST reservation resolved: %v", got.err)
	case <-time.After(20 * time.Millisecond):
	}
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusInvalidated
	service.ReleaseGossipReservation(child)
	got := <-resultCh
	require.ErrorIs(t, got.err, ErrIgnore)
	require.True(t, got.scheduled)
}

func TestBlockServiceUnrelatedReservationDoesNotRevalidateP2P(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	parentState := fcu.StateAtBlockRootVal[parentRoot]
	validationEntered := make(chan struct{})
	finishValidation := make(chan struct{})
	validationCalls := 0
	fcu.GetStateAtBlockRootFn = func(root common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
		require.Equal(t, parentRoot, root)
		require.True(t, alwaysCopy)
		validationCalls++
		if validationCalls == 1 {
			close(validationEntered)
			<-finishValidation
		}
		return parentState.Copy()
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.(*blockService).validateFirstGossip(t.Context(), child, nil, true)
	}()
	<-validationEntered
	otherKey := proposerIndexAndSlot{proposerIndex: child.Block.ProposerIndex + 1, slot: child.Block.Slot}
	require.NoError(t, service.(*blockService).reserveGossipKey(otherKey, common.Hash{1}))
	service.(*blockService).releaseGossipKey(otherKey, common.Hash{1})
	close(finishValidation)
	require.NoError(t, <-errCh)
	require.Equal(t, 1, validationCalls)
}

func TestBlockServiceCanceledHandoffDoesNotClaimSeen(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	parentState := fcu.StateAtBlockRootVal[parentRoot]
	revalidationEntered := make(chan struct{})
	finishRevalidation := make(chan struct{})
	require.NoError(t, service.ValidateGossip(t.Context(), child))
	fcu.GetStateAtBlockRootFn = func(root common.Hash, alwaysCopy bool) (*state.CachingBeaconState, error) {
		close(revalidationEntered)
		<-finishRevalidation
		return parentState.Copy()
	}
	ctx, cancel := context.WithCancel(t.Context())
	errCh := make(chan error, 1)
	go func() {
		errCh <- service.(*blockService).validateFirstGossip(ctx, child, nil, true)
	}()
	service.ReleaseGossipReservation(child)
	<-revalidationEntered
	cancel()
	close(finishRevalidation)
	require.ErrorIs(t, <-errCh, ErrIgnore)
	key := blockGossipKey(child)
	blockService := service.(*blockService)
	blockService.seenBlocksMu.Lock()
	require.False(t, blockService.seenBlocksCache.Contains(key))
	require.NotContains(t, blockService.reservations, key)
	blockService.seenBlocksMu.Unlock()
	fcu.GetStateAtBlockRootFn = func(common.Hash, bool) (*state.CachingBeaconState, error) {
		return parentState.Copy()
	}
	require.NoError(t, blockService.validateFirstGossip(t.Context(), child, nil, true))
}

func TestBlockServiceGossipIgnoresInvalidatedFullParentPayload(t *testing.T) {
	service, child, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusInvalidated
	scheduled := false
	err := service.(*blockService).validateFirstGossip(t.Context(), child, func() { scheduled = true }, false)
	require.ErrorIs(t, err, ErrIgnore)
	require.True(t, scheduled)
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
	require.NoError(t, parentState.SetSlot(parentSlot))
	require.NoError(t, parentState.AddValidator(validator, cfg.MaxEffectiveBalance))
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

func TestValidateGloasBlockBodyLimitsRejectsOversizedOperationAndRequests(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MaxProposerSlashings = 1
	cfg.MaxBuilderDepositRequestsPerPayload = 1
	body := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	body.ProposerSlashings.Append(&cltypes.ProposerSlashing{})
	body.ProposerSlashings.Append(&cltypes.ProposerSlashing{})
	require.Error(t, validateGloasBlockBodyLimits(&cfg, body))

	body = cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	body.ParentExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	body.ParentExecutionRequests.BuilderDeposits.Append(&solid.BuilderDepositRequest{})
	require.Error(t, validateGloasBlockBodyLimits(&cfg, body))
}

func TestValidateGloasBlockBodyLimitsRejectsDeposit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	body := cltypes.NewBeaconBody(&cfg, clparams.GloasVersion)
	require.NoError(t, validateGloasBlockBodyLimits(&cfg, body))
	body.Deposits.Append(&cltypes.Deposit{})
	require.ErrorContains(t, validateGloasBlockBodyLimits(&cfg, body), "deposits")
}

func TestBlockServiceDecodeGossipMessageStrict(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	service := &blockService{beaconCfg: &cfg}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	_, err = service.DecodeGossipMessage("peer", encoded, clparams.GloasVersion)
	require.NoError(t, err)

	outerGap := append([]byte(nil), encoded[:100]...)
	outerGap = append(outerGap, make([]byte, 4)...)
	outerGap = append(outerGap, encoded[100:]...)
	binary.LittleEndian.PutUint32(outerGap, 104)
	_, err = service.DecodeGossipMessage("peer", outerGap, clparams.GloasVersion)
	require.Error(t, err)

	const blockStart = 100
	const blockFixedSize = 84
	nestedGap := append([]byte(nil), encoded[:blockStart+blockFixedSize]...)
	nestedGap = append(nestedGap, make([]byte, 4)...)
	nestedGap = append(nestedGap, encoded[blockStart+blockFixedSize:]...)
	binary.LittleEndian.PutUint32(nestedGap[blockStart+80:], blockFixedSize+4)
	_, err = service.DecodeGossipMessage("peer", nestedGap, clparams.GloasVersion)
	require.Error(t, err)
}

func TestBlockServiceDecodeGossipMessageStrictPreGloasCompatibility(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	service := &blockService{beaconCfg: &cfg}
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	encoded, err := block.EncodeSSZ(nil)
	require.NoError(t, err)
	_, err = service.DecodeGossipMessage("peer", encoded, clparams.DenebVersion)
	require.NoError(t, err)
}

func TestPublishedBlockJobUpgradeKeepsWaiterOnRequiredStoreGeneration(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	service.scheduleBlockForLaterProcessing(block, func(context.Context) error {
		close(firstStarted)
		<-firstRelease
		return nil
	})
	firstDone := make(chan struct{})
	go func() {
		service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
		close(firstDone)
	}()
	<-firstStarted
	secondCalls := 0
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		secondCalls++
		return nil
	})
	waitDone := make(chan error, 1)
	go func() { waitDone <- handle.Wait(t.Context()) }()
	close(firstRelease)
	<-firstDone
	_, scheduled := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, scheduled)
	select {
	case err := <-waitDone:
		t.Fatalf("waiter completed for superseded store generation: %v", err)
	default:
	}
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	require.NoError(t, <-waitDone)
	require.Equal(t, 1, secondCalls)
}

func TestPublishedBlockJobTransientFailureReturnsToWaiterAndRemainsRetryable(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	transient := errors.New("database unavailable")
	calls := 0
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		calls++
		if calls == 1 {
			return transient
		}
		return nil
	})
	firstWaitDone := make(chan error, 1)
	go func() { firstWaitDone <- handle.Wait(t.Context()) }()
	waiter := handle.(*publishedBlockJobHandle)
	require.Eventually(t, func() bool {
		if waiter.mu.TryLock() {
			waiter.mu.Unlock()
			return false
		}
		return true
	}, time.Second, time.Millisecond)
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	require.ErrorIs(t, <-firstWaitDone, transient)
	waitDone := make(chan error, 1)
	go func() { waitDone <- handle.Wait(t.Context()) }()
	select {
	case err := <-waitDone:
		t.Fatalf("waiter replayed a previously observed transient failure: %v", err)
	default:
	}
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	require.NoError(t, <-waitDone)
	require.Equal(t, 2, calls)
}

func TestPublishedBlockJobWaitConsumesAttemptCompletedWhileWaiting(t *testing.T) {
	transient := errors.New("database unavailable")
	job := newBlockJob(cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version), func(context.Context) error {
		return transient
	})
	handle := &publishedBlockJobHandle{job: job, generation: job.storeGeneration}
	attempt := job.attempt
	attempt.err = transient
	attempt.generation = job.storeGeneration
	close(attempt.done)
	require.ErrorIs(t, handle.Wait(t.Context()), transient)

	job.mu.Lock()
	job.lastAttempt = attempt
	job.attempt = &blockJobAttempt{done: make(chan struct{})}
	job.mu.Unlock()
	waitCtx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, handle.Wait(waitCtx), context.Canceled)
}

func TestPublishedBlockJobRequestCancellationDoesNotCancelIntegration(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	started := make(chan struct{})
	release := make(chan struct{})
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(ctx context.Context) error {
		close(started)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			return nil
		}
	})
	processDone := make(chan struct{})
	go func() {
		service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
		close(processDone)
	}()
	<-started
	waitCtx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, handle.Wait(waitCtx), context.Canceled)
	close(release)
	<-processDone
	require.NoError(t, handle.Wait(t.Context()))
}

func TestPublishedBlockJobPermanentFailureIsTerminal(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	calls := 0
	permanent := fmt.Errorf("%w: execution payload is invalid", forkchoice.ErrBlockInvalid)
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		calls++
		return permanent
	})
	job := serviceJob(t, service, root)
	service.processScheduledBlock(context.Background(), root, job, time.Now())
	_, scheduled := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, scheduled)
	require.ErrorIs(t, handle.Wait(t.Context()), forkchoice.ErrBlockInvalid)
	require.ErrorIs(t, handle.Wait(t.Context()), forkchoice.ErrBlockInvalid)
	service.processScheduledBlock(context.Background(), root, job, time.Now())
	require.Equal(t, 1, calls)
}

func TestPublishedBlockJobHashFailureWaitIsReplayable(t *testing.T) {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	hashErr := errors.New("hash failure")
	service := &blockService{}
	job := newFailedBlockJob(block, func(context.Context) error { return nil }, hashErr)
	handle := &publishedBlockJobHandle{job: job, generation: job.storeGeneration}
	require.EqualError(t, handle.Wait(t.Context()), hashErr.Error())
	require.EqualError(t, handle.Wait(t.Context()), hashErr.Error())
	count := 0
	service.blocksScheduledForLaterExecution.Range(func(_, _ any) bool {
		count++
		return true
	})
	require.Zero(t, count)
}

func TestPublishedBlockJobDetachedTerminalCannotReplaceCurrentJob(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	detached := newBlockJob(block, func(context.Context) error { return nil })
	detached.terminal = true
	current := newBlockJob(block, func(context.Context) error { return nil })
	service.blocksScheduledForLaterExecution.Store(root, current)
	candidate := newBlockJob(block, func(context.Context) error { return nil })

	reused, _ := service.reuseScheduledBlockJob(root, detached, candidate, candidate.store)

	require.Nil(t, reused)
	stored, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	require.Same(t, current, stored)
}

func TestPublishedBlockJobExpiryRescheduleKeepsFreshStore(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	expiredHandle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	job := serviceJob(t, service, root)
	job.mu.Lock()
	expireStarted := make(chan struct{})
	expireDone := make(chan struct{})
	go func() {
		close(expireStarted)
		service.processScheduledBlock(context.Background(), root, job, job.creationTime.Add(blockJobExpiry+time.Second))
		close(expireDone)
	}()
	<-expireStarted
	freshStoreCalls := 0
	rescheduleStarted := make(chan struct{})
	rescheduleDone := make(chan PublishedBlockJob, 1)
	go func() {
		close(rescheduleStarted)
		rescheduleDone <- service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
			freshStoreCalls++
			return nil
		})
	}()
	<-rescheduleStarted
	job.mu.Unlock()
	<-expireDone
	require.ErrorIs(t, expiredHandle.Wait(t.Context()), ErrPublishedBlockJobExpired)
	freshHandle := <-rescheduleDone
	freshJob := serviceJob(t, service, root)
	require.NotSame(t, job, freshJob)
	service.processScheduledBlock(context.Background(), root, freshJob, time.Now())
	require.NoError(t, freshHandle.Wait(t.Context()))
	require.Equal(t, 1, freshStoreCalls)
}

func TestPublishedBlockJobShutdownClosesQueuedWaitersAndRejectsNewSchedules(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := clparams.MainnetBeaconConfig
	service := NewBlockService(ctx, nil, nil, nil, nil, &cfg, nil).(*blockService)
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	secondBlock := cltypes.NewSignedBeaconBlock(&cfg, clparams.Phase0Version)
	secondBlock.Block.Slot = 1
	handles := []PublishedBlockJob{
		service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil }),
		service.SchedulePublishedBlockForLaterProcessing(secondBlock, func(context.Context) error { return nil }),
	}
	cancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	for _, handle := range handles {
		require.ErrorIs(t, handle.Wait(waitCtx), ErrPublishedBlockJobStopped)
	}

	lateHandle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	require.ErrorIs(t, lateHandle.Wait(waitCtx), ErrPublishedBlockJobStopped)
}

func TestPublishedBlockJobShutdownOwnsRunningAttemptAndIgnoresLateResult(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	storeStarted := make(chan struct{})
	storeRelease := make(chan struct{})
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		close(storeStarted)
		<-storeRelease
		return nil
	})
	job := serviceJob(t, service, root)
	processDone := make(chan struct{})
	go func() {
		service.processScheduledBlock(context.Background(), root, job, time.Now())
		close(processDone)
	}()
	<-storeStarted
	service.stopPublishedBlockJobs()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	require.ErrorIs(t, handle.Wait(waitCtx), ErrPublishedBlockJobStopped)
	close(storeRelease)
	<-processDone
	require.ErrorIs(t, handle.Wait(waitCtx), ErrPublishedBlockJobStopped)
	_, scheduled := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, scheduled)
}

func TestPublishedBlockJobShutdownPreservesCompletedAttempt(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	require.NoError(t, handle.Wait(t.Context()))

	service.stopPublishedBlockJobs()

	require.NoError(t, handle.Wait(t.Context()))
}

func TestPublishedBlockJobConcurrentSchedulesReturnWaitableHandles(t *testing.T) {
	service := &blockService{}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	const schedules = 32
	start := make(chan struct{})
	handles := make(chan PublishedBlockJob, schedules)
	var wg sync.WaitGroup
	for range schedules {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			handles <- service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
		}()
	}
	close(start)
	wg.Wait()
	close(handles)
	service.processScheduledBlock(context.Background(), root, serviceJob(t, service, root), time.Now())
	for handle := range handles {
		require.NoError(t, handle.Wait(t.Context()))
	}
}

func serviceJob(t *testing.T, service *blockService, root [32]byte) *blockJob {
	t.Helper()
	job, ok := service.blocksScheduledForLaterExecution.Load(root)
	require.True(t, ok)
	return job.(*blockJob)
}
