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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
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

type blockProcessingErrorStore struct {
	forkchoice.ForkChoiceStorage
	err                       error
	calls                     int
	newPayloadArgs            []bool
	checkDataAvailabilityArgs []bool
	onBlock                   func()
}

type updateCountingDB struct {
	kv.RwDB
	updates int
	views   int
}

type blockDBError struct {
	kv.RwDB
	viewErr   error
	updateErr error
}

func (db *updateCountingDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	db.updates++
	return db.RwDB.Update(ctx, f)
}

func (db *updateCountingDB) View(ctx context.Context, f func(kv.Tx) error) error {
	db.views++
	return db.RwDB.View(ctx, f)
}

func (db *blockDBError) Update(ctx context.Context, f func(kv.RwTx) error) error {
	if db.updateErr != nil {
		return db.updateErr
	}
	return db.RwDB.Update(ctx, f)
}

func (db *blockDBError) View(ctx context.Context, f func(kv.Tx) error) error {
	if db.viewErr != nil {
		return db.viewErr
	}
	return db.RwDB.View(ctx, f)
}

func (s *blockProcessingErrorStore) OnBlock(
	_ context.Context,
	_ *cltypes.SignedBeaconBlock,
	newPayload bool,
	_ bool,
	checkDataAvailability bool,
) error {
	s.calls++
	s.newPayloadArgs = append(s.newPayloadArgs, newPayload)
	s.checkDataAvailabilityArgs = append(s.checkDataAvailabilityArgs, checkDataAvailability)
	if s.onBlock != nil {
		s.onBlock()
	}
	return s.err
}

func (*blockProcessingErrorStore) OnAttestation(*solid.Attestation, bool, bool) error {
	return nil
}

func (*blockProcessingErrorStore) OnAttesterSlashing(*cltypes.AttesterSlashing, bool) error {
	return nil
}

func setupBlockService(t *testing.T, ctrl *gomock.Controller) (BlockService, *synced_data.SyncedDataManager, *eth_clock.MockEthereumClock, *mock_services.ForkChoiceStorageMock) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	cfg := &clparams.MainnetBeaconConfig
	syncedDataManager := synced_data.NewSyncedDataManager(cfg, true)
	ethClock := eth_clock.NewMockEthereumClock(ctrl)
	forkchoiceMock := mock_services.NewForkChoiceStorageMock(t)
	serviceAPI := NewBlockService(canceledPendingQueueContext(t), db, forkchoiceMock, syncedDataManager, ethClock, cfg, nil)
	serviceAPI.(*blockService).blocksScheduledForLaterExecution.stopAndWait()
	return serviceAPI, syncedDataManager, ethClock, forkchoiceMock
}

func setupPendingGloasBlock(t *testing.T, ctrl *gomock.Controller) (*blockService, *cltypes.SignedBeaconBlock, *blockProcessingErrorStore, *mock_services.ForkChoiceStorageMock) {
	cfg := clparams.MainnetBeaconConfig
	cfg.AltairForkEpoch = 0
	cfg.BellatrixForkEpoch = 0
	cfg.CapellaForkEpoch = 0
	cfg.DenebForkEpoch = 0
	cfg.ElectraForkEpoch = 0
	cfg.FuluForkEpoch = 0
	cfg.GloasForkEpoch = 0

	serviceAPI, _, _, forkchoiceStore := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)
	service.beaconCfg = &cfg

	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.GloasVersion)
	block.Block.Slot = 1
	block.Block.ParentRoot = common.HexToHash("0x01")
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	bid.ParentBlockRoot = block.Block.ParentRoot
	bid.ParentBlockHash = common.HexToHash("0x02")
	forkchoiceStore.Headers[block.Block.ParentRoot] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot - 1}
	forkchoiceStore.SlotVal = block.Block.Slot

	processingStore := &blockProcessingErrorStore{ForkChoiceStorage: forkchoiceStore}
	service.forkchoiceStore = processingStore
	return service, block, processingStore, forkchoiceStore
}

func setupPendingBellatrixBlock(t *testing.T, ctrl *gomock.Controller, processingErr error) (*blockService, *cltypes.SignedBeaconBlock, *blockProcessingErrorStore) {
	blocks, _, _ := tests.GetBellatrixRandom()
	block := blocks[0]
	serviceAPI, _, _, forkchoiceStore := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)
	forkchoiceStore.Headers[block.Block.ParentRoot] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot - 1}
	forkchoiceStore.SlotVal = block.Block.Slot
	processingStore := &blockProcessingErrorStore{
		ForkChoiceStorage: forkchoiceStore,
		err:               processingErr,
	}
	service.forkchoiceStore = processingStore
	return service, block, processingStore
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

func TestBlockServiceQueuesValidBlockBeforeForkChoiceReachesSlot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, _ := tests.GetBellatrixRandom()
	block := blocks[0]
	serviceAPI, syncedData, ethClock, forkchoiceStore := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)
	require.NoError(t, syncedData.OnHeadState(pre))
	require.Less(t, syncedData.HeadSlot(), block.Block.Slot)
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(block.Block.Slot).Return(true)
	forkchoiceStore.FinalizedCheckpointVal = pre.FinalizedCheckpoint()
	forkchoiceStore.Headers[block.Block.ParentRoot] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot - 1}
	forkchoiceStore.SlotVal = block.Block.Slot - 1
	processingStore := &blockProcessingErrorStore{
		ForkChoiceStorage: forkchoiceStore,
		err:               errors.New("must not process a block before its slot"),
	}
	service.forkchoiceStore = processingStore
	emitter := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	service.emitter = emitter

	err := service.ProcessMessage(t.Context(), nil, block)

	require.ErrorIs(t, err, ErrIgnore)
	require.Zero(t, processingStore.calls)
	require.Equal(t, int32(1), service.blocksScheduledForLaterExecution.count.Load())
	select {
	case event := <-events:
		require.Equal(t, beaconevents.StateBlockGossip, event.Event)
	default:
		t.Fatal("gossip-validated queued block did not emit gossip event")
	}
}

func TestBlockServiceIgnoresBlockWhenPendingQueueIsFull(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, pre, _ := tests.GetBellatrixRandom()
	block := blocks[0]
	serviceAPI, syncedData, ethClock, forkchoiceStore := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)
	require.NoError(t, syncedData.OnHeadState(pre))
	require.Less(t, syncedData.HeadSlot(), block.Block.Slot)
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(block.Block.Slot).Return(true)
	forkchoiceStore.FinalizedCheckpointVal = pre.FinalizedCheckpoint()
	forkchoiceStore.Headers[block.Block.ParentRoot] = &cltypes.BeaconBlockHeader{Slot: block.Block.Slot - 1}
	forkchoiceStore.SlotVal = block.Block.Slot - 1
	for i := range maxPendingBlocks {
		key := common.Hash{byte(i), byte(i >> 8)}
		storePendingJob(t, service.blocksScheduledForLaterExecution, key, &pendingBlockJob{block: block}, time.Now())
	}

	err := service.ProcessMessage(t.Context(), nil, block)

	require.ErrorIs(t, err, ErrIgnore)
	require.Equal(t, int32(maxPendingBlocks), service.blocksScheduledForLaterExecution.count.Load())
}

func TestBlockServiceLowerThanFinalizedCheckpoint(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, post := tests.GetBellatrixRandom()

	blockServiceAPI, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	blockService := blockServiceAPI.(*blockService)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	blocks[0].Block.Slot = 0

	require.Error(t, blockService.ProcessMessage(context.Background(), nil, blocks[0]))
	require.Zero(t, blockService.blocksScheduledForLaterExecution.count.Load())
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

	blocks, _, post := tests.GetBellatrixRandom()

	blockServiceAPI, syncedData, ethClock, fcu := setupBlockService(t, ctrl)
	blockService := blockServiceAPI.(*blockService)
	require.NoError(t, syncedData.OnHeadState(post))
	ethClock.EXPECT().GetCurrentSlot().Return(uint64(0)).AnyTimes()
	ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
	fcu.FinalizedCheckpointVal = post.FinalizedCheckpoint()
	fcu.Headers[blocks[1].Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
	fcu.SlotVal = blocks[1].Block.Slot
	blocks[1].Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)
	emitter := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 1)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	blockService.emitter = emitter
	eventPublishedBeforeProcessing := false
	processingStore := &blockProcessingErrorStore{
		ForkChoiceStorage: fcu,
		onBlock: func() {
			select {
			case event := <-events:
				eventPublishedBeforeProcessing = event.Event == beaconevents.StateBlockGossip
			default:
			}
		},
	}
	blockService.forkchoiceStore = processingStore

	require.NoError(t, blockService.ProcessMessage(context.Background(), nil, blocks[1]))
	require.Equal(t, 1, processingStore.calls)
	require.True(t, eventPublishedBeforeProcessing)
	select {
	case event := <-events:
		t.Fatalf("block emitted duplicate gossip event: %v", event)
	default:
	}
}

func TestBlockServiceInitialProcessingQueuesRetryableDependencyFailure(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{name: "parent state", err: forkchoice.ErrMissingSegment},
		{name: "execution status", err: forkchoice.ErrNewPayloadNoStatus},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			blocks, _, post := tests.GetBellatrixRandom()
			block := blocks[1]
			serviceAPI, syncedData, ethClock, forkchoiceStore := setupBlockService(t, ctrl)
			service := serviceAPI.(*blockService)
			require.NoError(t, syncedData.OnHeadState(post))
			ethClock.EXPECT().IsSlotCurrentSlotWithMaximumClockDisparity(gomock.Any()).Return(true).AnyTimes()
			forkchoiceStore.FinalizedCheckpointVal = post.FinalizedCheckpoint()
			forkchoiceStore.Headers[block.Block.ParentRoot] = blocks[0].SignedBeaconBlockHeader().Header.Copy()
			forkchoiceStore.SlotVal = block.Block.Slot
			block.Block.Body.BlobKzgCommitments = solid.NewStaticListSSZ[*cltypes.KZGCommitment](100, 48)
			processingStore := &blockProcessingErrorStore{
				ForkChoiceStorage: forkchoiceStore,
				err:               fmt.Errorf("dependency unavailable: %w", tc.err),
			}
			service.forkchoiceStore = processingStore

			err := service.ProcessMessage(t.Context(), nil, block)

			require.ErrorIs(t, err, ErrIgnore)
			require.NotErrorIs(t, err, tc.err)
			require.Equal(t, 1, processingStore.calls)
			require.Equal(t, int32(1), service.blocksScheduledForLaterExecution.count.Load())
		})
	}
}

func TestBlockServicePendingQueueCap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, _ := tests.GetBellatrixRandom()
	serviceAPI, _, _, _ := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)
	require.Equal(t, int32(maxPendingBlocks), service.blocksScheduledForLaterExecution.capacity)
	service.blocksScheduledForLaterExecution.count.Store(maxPendingBlocks)
	output := captureServiceLogs(t)

	err := service.scheduleBlockForLaterProcessing(blocks[0])

	require.ErrorIs(t, err, ErrIgnore)
	require.Equal(t, int32(maxPendingBlocks), service.blocksScheduledForLaterExecution.count.Load())
	root, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	_, exists := service.blocksScheduledForLaterExecution.jobs.Load(common.Hash(root))
	require.False(t, exists)
	require.Contains(t, output.String(), "Pending block queue full; block not scheduled")
	require.NotContains(t, output.String(), "Block scheduled for later processing")
}

func TestBlockServicePendingQueueDeduplicates(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, _ := tests.GetBellatrixRandom()
	serviceAPI, _, _, _ := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)

	require.NoError(t, service.scheduleBlockForLaterProcessing(blocks[0]))
	root, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	first, exists := service.blocksScheduledForLaterExecution.jobs.Load(common.Hash(root))
	require.True(t, exists)
	require.NoError(t, service.scheduleBlockForLaterProcessing(blocks[0]))

	require.Equal(t, int32(1), service.blocksScheduledForLaterExecution.count.Load())
	stored, exists := service.blocksScheduledForLaterExecution.jobs.Load(common.Hash(root))
	require.True(t, exists)
	require.Same(t, first, stored)
}

func TestBlockServicePendingQueueRemovesPermanentProcessingFailure(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{name: "not finalized descendant", err: forkchoice.ErrNotFinalizedDescendant},
		{name: "fork schema mismatch", err: forkchoice.ErrForkSchemaSlotMismatch},
		{name: "unknown validation error", err: errors.New("block is invalid")},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			blocks, _, _ := tests.GetBellatrixRandom()
			serviceAPI, _, _, forkchoiceStore := setupBlockService(t, ctrl)
			service := serviceAPI.(*blockService)
			forkchoiceStore.Headers[blocks[0].Block.ParentRoot] = &cltypes.BeaconBlockHeader{}
			forkchoiceStore.SlotVal = blocks[0].Block.Slot
			processingStore := &blockProcessingErrorStore{
				ForkChoiceStorage: forkchoiceStore,
				err:               tc.err,
			}
			service.forkchoiceStore = processingStore

			decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: blocks[0]})

			require.Equal(t, pendingJobRemove, decision)
			require.Equal(t, 1, processingStore.calls)
		})
	}
}

func TestBlockServicePendingQueueRetainsRetryableProcessingFailure(t *testing.T) {
	testCases := []struct {
		name string
		err  error
	}{
		{name: "blob data", err: forkchoice.ErrEIP4844DataNotAvailable},
		{name: "column data", err: forkchoice.ErrEIP7594ColumnDataNotAvailable},
		{name: "parent envelope", err: forkchoice.ErrParentEnvelopePending},
		{name: "parent state", err: forkchoice.ErrMissingSegment},
		{name: "execution status", err: forkchoice.ErrNewPayloadNoStatus},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			blocks, _, _ := tests.GetBellatrixRandom()
			serviceAPI, _, _, forkchoiceStore := setupBlockService(t, ctrl)
			service := serviceAPI.(*blockService)
			forkchoiceStore.Headers[blocks[0].Block.ParentRoot] = &cltypes.BeaconBlockHeader{}
			forkchoiceStore.SlotVal = blocks[0].Block.Slot
			processingStore := &blockProcessingErrorStore{
				ForkChoiceStorage: forkchoiceStore,
				err:               fmt.Errorf("dependency unavailable: %w", tc.err),
			}
			service.forkchoiceStore = processingStore

			decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: blocks[0]})

			require.Equal(t, pendingJobKeep, decision)
			require.Equal(t, 1, processingStore.calls)
		})
	}
}

func TestPendingBlockJobBacksOffExecutionStatusFailures(t *testing.T) {
	now := time.Unix(1_000, 0)
	job := &pendingBlockJob{}

	job.recordProcessingFailure(now, fmt.Errorf("execution unavailable: %w", forkchoice.ErrNewPayloadNoStatus))

	require.Equal(t, blockELRetryInitialDelay, job.retryDelay)
	require.False(t, job.readyToRetry(now.Add(blockELRetryInitialDelay-time.Nanosecond)))
	require.True(t, job.readyToRetry(now.Add(blockELRetryInitialDelay)))

	now = job.retryAfter
	job.recordProcessingFailure(now, forkchoice.ErrNewPayloadNoStatus)
	require.Equal(t, 2*blockELRetryInitialDelay, job.retryDelay)

	for range 10 {
		now = job.retryAfter
		job.recordProcessingFailure(now, forkchoice.ErrNewPayloadNoStatus)
	}
	require.Equal(t, blockELRetryMaxDelay, job.retryDelay)
}

func TestPendingBlockJobPreservesExecutionBackoffAcrossOtherFailures(t *testing.T) {
	job := &pendingBlockJob{
		retryAfter: time.Unix(2_000, 0),
		retryDelay: blockELRetryInitialDelay,
	}

	job.recordProcessingFailure(time.Unix(1_000, 0), forkchoice.ErrMissingSegment)

	require.Equal(t, blockELRetryInitialDelay, job.retryDelay)
	require.True(t, job.retryAfter.IsZero())

	now := time.Unix(3_000, 0)
	job.recordProcessingFailure(now, forkchoice.ErrNewPayloadNoStatus)
	require.Equal(t, 2*blockELRetryInitialDelay, job.retryDelay)
	require.Equal(t, now.Add(2*blockELRetryInitialDelay), job.retryAfter)
}

func TestBlockServicePendingQueueBacksOffExecutionStatusRetries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, block, processingStore := setupPendingBellatrixBlock(t, ctrl, forkchoice.ErrNewPayloadNoStatus)
	require.NoError(t, service.scheduleBlockForLaterProcessing(block))

	service.blocksScheduledForLaterExecution.processPending(t.Context())
	blockRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)
	queued, ok := service.blocksScheduledForLaterExecution.jobs.Load(common.Hash(blockRoot))
	require.True(t, ok)
	queued.(*pendingJob[*pendingBlockJob]).msg.retryAfter = time.Now().Add(time.Hour)
	service.blocksScheduledForLaterExecution.processPending(t.Context())

	require.Equal(t, 1, processingStore.calls)
}

func TestBlockServicePendingQueueDoesNotRewriteStoredBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, block, processingStore := setupPendingBellatrixBlock(t, ctrl, forkchoice.ErrMissingSegment)
	countingDB := &updateCountingDB{RwDB: service.db}
	service.db = countingDB
	require.NoError(t, service.scheduleBlockForLaterProcessing(block))

	service.blocksScheduledForLaterExecution.processPending(t.Context())
	service.blocksScheduledForLaterExecution.processPending(t.Context())

	require.Equal(t, 2, processingStore.calls)
	require.Equal(t, []bool{true, false}, processingStore.newPayloadArgs)
	require.Equal(t, []bool{true, false}, processingStore.checkDataAvailabilityArgs)
	require.Equal(t, 1, countingDB.updates)
	require.Equal(t, 1, countingDB.views)
}

func TestBlockServicePendingQueuePublishesGossipEventOnceAfterValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, block, processingStore := setupPendingBellatrixBlock(t, ctrl, forkchoice.ErrMissingSegment)
	emitter := beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 2)
	subscription := emitter.State().Subscribe(events)
	defer subscription.Unsubscribe()
	service.emitter = emitter
	job := &pendingBlockJob{block: block}

	require.Equal(t, pendingJobKeep, service.tryProcessPendingBlock(t.Context(), common.Hash{}, job))
	require.Equal(t, pendingJobKeep, service.tryProcessPendingBlock(t.Context(), common.Hash{}, job))
	require.Equal(t, 2, processingStore.calls)
	select {
	case event := <-events:
		require.Equal(t, beaconevents.StateBlockGossip, event.Event)
	default:
		t.Fatal("deferred block did not emit gossip event after validation")
	}
	select {
	case event := <-events:
		t.Fatalf("deferred block emitted duplicate gossip event: %v", event)
	default:
	}
}

func TestBlockServicePendingQueueKeepsBlockAfterDatabaseFailure(t *testing.T) {
	testCases := []struct {
		name      string
		configure func(*blockDBError, error)
	}{
		{
			name: "storage probe",
			configure: func(db *blockDBError, err error) {
				db.viewErr = err
			},
		},
		{
			name: "block store",
			configure: func(db *blockDBError, err error) {
				db.updateErr = err
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			service, block, processingStore := setupPendingBellatrixBlock(t, ctrl, nil)
			db := &blockDBError{RwDB: service.db}
			tc.configure(db, errors.New("temporary database failure"))
			service.db = db

			decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: block})

			require.Equal(t, pendingJobKeep, decision)
			require.Zero(t, processingStore.calls)
		})
	}
}

func TestBlockServicePendingQueueWaitsForGloasParentPayloadStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, block, processingStore, forkchoiceStore := setupPendingGloasBlock(t, ctrl)
	parentBlockHash := block.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash

	decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: block})

	require.Equal(t, pendingJobKeep, decision)
	require.Zero(t, processingStore.calls)

	forkchoiceStore.ExecutionPayloadStatusMap[parentBlockHash] = execution_client.PayloadStatusValidated
	processingStore.err = forkchoice.ErrNewPayloadNoStatus
	decision = service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: block})

	require.Equal(t, pendingJobKeep, decision)
	require.Equal(t, 1, processingStore.calls)
}

func TestBlockServicePendingQueueRemovesGloasBlockAfterSuccessfulProcessing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, block, processingStore, forkchoiceStore := setupPendingGloasBlock(t, ctrl)
	parentBlockHash := block.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash
	forkchoiceStore.ExecutionPayloadStatusMap[parentBlockHash] = execution_client.PayloadStatusValidated

	decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: block})

	require.Equal(t, pendingJobRemove, decision)
	require.Equal(t, 1, processingStore.calls)
}

func TestBlockServicePendingQueueRemovesGloasBlockWithInvalidParentPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, block, processingStore, forkchoiceStore := setupPendingGloasBlock(t, ctrl)
	parentBlockHash := block.Block.Body.GetSignedExecutionPayloadBid().Message.ParentBlockHash
	forkchoiceStore.ExecutionPayloadStatusMap[parentBlockHash] = execution_client.PayloadStatusInvalidated

	decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: block})

	require.Equal(t, pendingJobRemove, decision)
	require.Zero(t, processingStore.calls)
}

func TestBlockServicePendingQueueRemovesGloasBlockWithMismatchedParentRoot(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	service, block, processingStore, forkchoiceStore := setupPendingGloasBlock(t, ctrl)
	bid := block.Block.Body.GetSignedExecutionPayloadBid().Message
	forkchoiceStore.ExecutionPayloadStatusMap[bid.ParentBlockHash] = execution_client.PayloadStatusValidated
	bid.ParentBlockRoot = common.HexToHash("0x03")

	decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: block})

	require.Equal(t, pendingJobRemove, decision)
	require.Zero(t, processingStore.calls)
}

func TestBlockServicePendingQueueWaitsForParentBeforeProcessing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, _ := tests.GetBellatrixRandom()
	serviceAPI, _, _, forkchoiceStore := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)
	processingStore := &blockProcessingErrorStore{
		ForkChoiceStorage: forkchoiceStore,
		err:               forkchoice.ErrNotFinalizedDescendant,
	}
	service.forkchoiceStore = processingStore

	decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: blocks[0]})

	require.Equal(t, pendingJobKeep, decision)
	require.Zero(t, processingStore.calls)
}

func TestBlockServicePendingQueueWaitsForBlockSlotBeforeProcessing(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, _ := tests.GetBellatrixRandom()
	block := blocks[0]
	block.Block.Slot = 2
	serviceAPI, _, _, forkchoiceStore := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)
	forkchoiceStore.Headers[block.Block.ParentRoot] = &cltypes.BeaconBlockHeader{}
	forkchoiceStore.SlotVal = block.Block.Slot - 1
	processingStore := &blockProcessingErrorStore{
		ForkChoiceStorage: forkchoiceStore,
		err:               errors.New("must not process a future block"),
	}
	service.forkchoiceStore = processingStore

	decision := service.tryProcessPendingBlock(t.Context(), common.Hash{}, &pendingBlockJob{block: block})

	require.Equal(t, pendingJobKeep, decision)
	require.Zero(t, processingStore.calls)
}

func TestBlockServicePendingQueueRemovesAlreadyProcessedBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	blocks, _, _ := tests.GetBellatrixRandom()
	blockRoot, err := blocks[0].Block.HashSSZ()
	require.NoError(t, err)
	serviceAPI, _, _, forkchoiceStore := setupBlockService(t, ctrl)
	service := serviceAPI.(*blockService)
	forkchoiceStore.Headers[blockRoot] = blocks[0].SignedBeaconBlockHeader().Header
	processingStore := &blockProcessingErrorStore{
		ForkChoiceStorage: forkchoiceStore,
		err:               errors.New("must not process an imported block"),
	}
	service.forkchoiceStore = processingStore

	decision := service.tryProcessPendingBlock(t.Context(), common.Hash(blockRoot), &pendingBlockJob{
		block:      blocks[0],
		retryAfter: time.Now().Add(time.Hour),
	})

	require.Equal(t, pendingJobRemove, decision)
	require.Zero(t, processingStore.calls)
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
			output := captureServiceLogs(t)

			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Body.AttesterSlashings.Append(&cltypes.AttesterSlashing{})
			service := blockService{forkchoiceStore: attesterSlashingErrorStore{err: tc.err}}

			service.importBlockOperations(block)

			require.Equal(t, tc.wantLogged, bytes.Contains(output.Bytes(), []byte("bad attester slashing received")))
		})
	}
}
