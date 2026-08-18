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
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
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

type dataUnavailableStore struct {
	*mock_services.ForkChoiceStorageMock
	onBlockCalls  int
	onBlockResult error
	afterOnBlock  func()
}

func (s *dataUnavailableStore) OnBlock(context.Context, *cltypes.SignedBeaconBlock, bool, bool, bool) error {
	s.onBlockCalls++
	if s.afterOnBlock != nil {
		s.afterOnBlock()
	}
	return s.onBlockResult
}

type blockServiceTestClock struct {
	now time.Time
}

func (c *blockServiceTestClock) currentTime() time.Time {
	return c.now
}

func newDataUnavailableBlockJob(t *testing.T) (*blockService, *dataUnavailableStore, *cltypes.SignedBeaconBlock, *blockServiceTestClock) {
	t.Helper()
	store := &dataUnavailableStore{
		ForkChoiceStorageMock: mock_services.NewForkChoiceStorageMock(t),
		onBlockResult:         forkchoice.ErrEIP7594ColumnDataNotAvailable,
	}
	clock := &blockServiceTestClock{now: time.Unix(1, 0)}
	service := &blockService{
		forkchoiceStore: store,
		db:              mdbxtest.NewTestDB(t, dbcfg.ChainDB),
		now:             clock.currentTime,
	}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.FuluVersion)
	service.scheduleBlockForLaterProcessing(block)
	return service, store, block, clock
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
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(false).AnyTimes()

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
}

func TestBlockServiceLowerThanFinalizedCheckpoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockService, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
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
	syncedData.OnHeadState(post)
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
	syncedData.OnHeadState(post)
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
	syncedData.OnHeadState(post)
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

	blocks, _, post := tests.GetBellatrixRandom()

	blockServiceAPI, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	syncedData.OnHeadState(post)
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	blocks[1].Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)

	require.NoError(t, blockServiceAPI.ProcessMessage(context.Background(), nil, blocks[1]))
	service := blockServiceAPI.(*blockService)
	require.True(t, service.seenBlocksCache.Contains(proposerIndexAndSlot{
		proposerIndex: blocks[1].Block.ProposerIndex,
		slot:          blocks[1].Block.Slot,
	}))
}

func TestDataAvailabilityRetriesAreDelayed(t *testing.T) {
	service, store, _, clock := newDataUnavailableBlockJob(t)
	clock.now = clock.now.Add(blockRetryInterval)
	service.processScheduledBlocks(t.Context(), clock.now)
	clock.now = clock.now.Add(blockRetryInterval - time.Nanosecond)
	service.processScheduledBlocks(t.Context(), clock.now)

	require.Equal(t, 1, store.onBlockCalls)
}

func TestFirstScheduledBlockRetryRunsOnNextTick(t *testing.T) {
	service, store, block, clock := newDataUnavailableBlockJob(t)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service.blocksScheduledForLaterExecution.Delete(blockRoot)

	service.scheduleBlockForLaterProcessing(block)
	service.processScheduledBlocks(t.Context(), clock.now)
	require.Equal(t, 1, store.onBlockCalls)
}

func TestFirstDataAvailabilityRetryIsDelayed(t *testing.T) {
	service, store, block, clock := newDataUnavailableBlockJob(t)
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	service.blocksScheduledForLaterExecution.Delete(blockRoot)

	service.scheduleBlockForDataAvailability(block)
	clock.now = clock.now.Add(blockRetryInterval - time.Nanosecond)
	service.processScheduledBlocks(t.Context(), clock.now)
	require.Zero(t, store.onBlockCalls)

	clock.now = clock.now.Add(time.Nanosecond)
	service.processScheduledBlocks(t.Context(), clock.now)
	require.Equal(t, 1, store.onBlockCalls)
}

func TestDataAvailabilityRetryDelayStartsAfterAttempt(t *testing.T) {
	service, store, block, clock := newDataUnavailableBlockJob(t)
	startedAt := clock.now.Add(blockRetryInterval)
	finishedAt := startedAt.Add(2 * time.Second)
	clock.now = startedAt
	store.afterOnBlock = func() { clock.now = finishedAt }

	service.processScheduledBlocks(t.Context(), startedAt)

	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	value, ok := service.blocksScheduledForLaterExecution.Load(blockRoot)
	require.True(t, ok)
	require.Equal(t, finishedAt.Add(blockRetryInterval), value.(*blockJob).retryAfter)
}

func TestDataAvailabilityRetriesAreBounded(t *testing.T) {
	const retryLimit = 4
	for _, availabilityErr := range []error{
		forkchoice.ErrEIP4844DataNotAvailable,
		forkchoice.ErrEIP7594ColumnDataNotAvailable,
	} {
		t.Run(availabilityErr.Error(), func(t *testing.T) {
			service, store, block, clock := newDataUnavailableBlockJob(t)
			store.onBlockResult = availabilityErr
			blockRoot, err := block.Block.HashSSZ()
			require.NoError(t, err)

			for range retryLimit + 1 {
				clock.now = clock.now.Add(blockRetryInterval + time.Nanosecond)
				service.processScheduledBlocks(t.Context(), clock.now)
			}

			require.Equal(t, retryLimit, store.onBlockCalls)
			_, pending := service.blocksScheduledForLaterExecution.Load(blockRoot)
			require.False(t, pending)
		})
	}
}

func TestSchedulingSameBlockPreservesDataAvailabilityRetryBudget(t *testing.T) {
	service, _, block, clock := newDataUnavailableBlockJob(t)
	clock.now = clock.now.Add(blockRetryInterval)
	service.processScheduledBlocks(t.Context(), clock.now)
	service.scheduleBlockForLaterProcessing(block)

	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	value, ok := service.blocksScheduledForLaterExecution.Load(blockRoot)
	require.True(t, ok)
	require.Equal(t, uint8(1), value.(*blockJob).dataAvailabilityAttempts)
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
