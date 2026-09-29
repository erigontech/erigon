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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
)

func pendingGossipFixture(t *testing.T) (*blockService, *cltypes.SignedBeaconBlock, *mock_services.ForkChoiceStorageMock) {
	t.Helper()
	service, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
	impl := service.(*blockService)
	impl.blocksScheduledForLaterExecution.stopAndWait()
	fcu.SlotVal = block.Block.Slot
	impl.forkchoiceStore = &blockProcessingErrorStore{ForkChoiceStorage: fcu}
	return impl, block, fcu
}

func TestPendingGossipRetainsInitialDatabaseFailure(t *testing.T) {
	service, block, _ := pendingGossipFixture(t)
	service.db = &failFirstUpdateDB{RwDB: service.db}
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	require.Zero(t, service.forkchoiceStore.(*blockProcessingErrorStore).calls)
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Equal(t, 1, service.forkchoiceStore.(*blockProcessingErrorStore).calls)
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingGossipWaitsForForkChoiceSlot(t *testing.T) {
	service, block, fcu := pendingGossipFixture(t)
	fcu.SlotVal = block.Block.Slot - 1
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	require.Zero(t, service.forkchoiceStore.(*blockProcessingErrorStore).calls)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Zero(t, service.forkchoiceStore.(*blockProcessingErrorStore).calls)
	fcu.SlotVal = block.Block.Slot
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Equal(t, 1, service.forkchoiceStore.(*blockProcessingErrorStore).calls)
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingGossipReservationSurvivesHistoryEviction(t *testing.T) {
	service, block, fcu := pendingGossipFixture(t)
	fcu.SlotVal = block.Block.Slot - 1
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	for i := range seenBlockCacheSize {
		service.seenBlocksCache.Add(proposerIndexAndSlot{slot: uint64(i + 1000)}, seenBlock{})
	}
	require.False(t, service.seenBlocksCache.Contains(blockGossipKey(block)))
	duplicate := *block
	duplicate.Signature[0] ^= 1
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, &duplicate), ErrIgnore)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())

	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	entry, ok := service.blocksScheduledForLaterExecution.jobs.Load(root)
	require.True(t, ok)
	entry.(*pendingJob[*blockJob]).creationTime = time.Now().Add(-2 * blockJobExpiry)
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
}

type blockProcessingErrorStore struct {
	forkchoice.ForkChoiceStorage
	err                       error
	calls                     int
	newPayloadArgs            []bool
	checkDataAvailabilityArgs []bool
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

type failNthUpdateDB struct {
	kv.RwDB
	failAt  int
	updates int
	err     error
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

func (db *failNthUpdateDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	db.updates++
	if db.updates == db.failAt {
		return db.err
	}
	return db.RwDB.Update(ctx, f)
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
	return s.err
}

func (*blockProcessingErrorStore) OnAttestation(*solid.Attestation, bool, bool) error {
	return nil
}

func (*blockProcessingErrorStore) OnAttesterSlashing(*cltypes.AttesterSlashing, bool) error {
	return nil
}

func TestPendingBlockJobBacksOffExecutionStatusFailures(t *testing.T) {
	now := time.Unix(1_000, 0)
	job := &blockJob{}

	job.recordProcessingFailureLocked(now, fmt.Errorf("execution unavailable: %w", forkchoice.ErrNewPayloadNoStatus))

	require.Equal(t, blockELRetryInitialDelay, job.retryDelay)
	require.False(t, job.readyToRetryLocked(now.Add(blockELRetryInitialDelay-time.Nanosecond)))
	require.True(t, job.readyToRetryLocked(now.Add(blockELRetryInitialDelay)))

	now = job.retryAfter
	job.recordProcessingFailureLocked(now, forkchoice.ErrNewPayloadNoStatus)
	require.Equal(t, 2*blockELRetryInitialDelay, job.retryDelay)

	for range 10 {
		now = job.retryAfter
		job.recordProcessingFailureLocked(now, forkchoice.ErrNewPayloadNoStatus)
	}
	require.Equal(t, blockELRetryMaxDelay, job.retryDelay)
}

func TestPendingBlockJobPreservesExecutionBackoffAcrossOtherFailures(t *testing.T) {
	job := &blockJob{
		retryAfter: time.Unix(2_000, 0),
		retryDelay: blockELRetryInitialDelay,
	}

	job.recordProcessingFailureLocked(time.Unix(1_000, 0), forkchoice.ErrMissingSegment)

	require.Equal(t, blockELRetryInitialDelay, job.retryDelay)
	require.True(t, job.retryAfter.IsZero())

	now := time.Unix(3_000, 0)
	job.recordProcessingFailureLocked(now, forkchoice.ErrNewPayloadNoStatus)
	require.Equal(t, 2*blockELRetryInitialDelay, job.retryDelay)
	require.Equal(t, now.Add(2*blockELRetryInitialDelay), job.retryAfter)
}

func TestMergePendingBlockJobsAdvancesExecutionBackoff(t *testing.T) {
	firstFailure := time.Unix(1_000, 0)
	latestFailure := time.Unix(2_000, 0)
	existing := &blockJob{}
	existing.recordProcessingFailureLocked(firstFailure, forkchoice.ErrNewPayloadNoStatus)
	incoming := &blockJob{}
	incoming.recordProcessingFailureLocked(latestFailure, forkchoice.ErrNewPayloadNoStatus)

	mergeBlockProcessingState(existing, incoming)

	require.Equal(t, 2*blockELRetryInitialDelay, existing.retryDelay)
	require.Equal(t, latestFailure.Add(2*blockELRetryInitialDelay), existing.retryAfter)
}

func TestPendingGossipExecutionBackoffAndStoredBlock(t *testing.T) {
	service, block, _ := pendingGossipFixture(t)
	processing := service.forkchoiceStore.(*blockProcessingErrorStore)
	processing.err = forkchoice.ErrNewPayloadNoStatus
	db := &updateCountingDB{RwDB: service.db}
	service.db = db
	started := time.Now()
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	entry, exists := service.blocksScheduledForLaterExecution.jobs.Load(root)
	require.True(t, exists)
	job := entry.(*pendingJob[*blockJob]).msg
	require.True(t, job.persisted)
	require.Equal(t, blockELRetryInitialDelay, job.retryDelay)
	require.False(t, job.processingFailureAt.Before(started))
	require.Equal(t, job.processingFailureAt.Add(blockELRetryInitialDelay), job.retryAfter)
	require.Equal(t, 1, db.updates)
	require.Equal(t, 1, db.views)
	service.processScheduledBlock(t.Context(), root, job, job.retryAfter.Add(-time.Nanosecond))
	require.Equal(t, 1, processing.calls)
	service.processScheduledBlock(t.Context(), root, job, job.retryAfter)
	require.Equal(t, 2, processing.calls)
	require.Equal(t, 2*blockELRetryInitialDelay, job.retryDelay)
	require.Equal(t, 1, db.updates)
	require.Equal(t, 1, db.views)
}

func TestPendingGossipMissingSegmentSkipsCompletedExecutionChecks(t *testing.T) {
	service, block, _ := pendingGossipFixture(t)
	processing := service.forkchoiceStore.(*blockProcessingErrorStore)
	processing.err = forkchoice.ErrMissingSegment
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Equal(t, []bool{true, false}, processing.newPayloadArgs)
	require.Equal(t, []bool{true, false}, processing.checkDataAvailabilityArgs)
	processing.err = nil
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingBlockDuplicatePreservesProgressAndAdmissionTime(t *testing.T) {
	service, block, _ := pendingGossipFixture(t)
	service.blocksScheduledForLaterExecution.capacity = 1
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	original := newBlockJob(block, nil)
	service.scheduleBlockJob(root, original)
	entry, ok := service.blocksScheduledForLaterExecution.jobs.Load(root)
	require.True(t, ok)
	incoming := newBlockJob(block, nil)
	incoming.persisted = true
	incoming.executionAndDataChecked = true
	incoming.recordProcessingFailureLocked(time.Now(), forkchoice.ErrNewPayloadNoStatus)
	retained, _ := service.scheduleBlockJob(root, incoming)
	require.Same(t, original, retained)
	after, ok := service.blocksScheduledForLaterExecution.jobs.Load(root)
	require.True(t, ok)
	require.Same(t, entry, after)
	require.True(t, retained.persisted)
	require.True(t, retained.executionAndDataChecked)
	require.Equal(t, blockELRetryInitialDelay, retained.retryDelay)
	require.Equal(t, incoming.retryAfter, retained.retryAfter)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingGossipRetriesDependenciesAndRemovesPermanentFailure(t *testing.T) {
	for _, processingErr := range []error{
		forkchoice.ErrEIP4844DataNotAvailable, forkchoice.ErrEIP7594ColumnDataNotAvailable,
		forkchoice.ErrParentEnvelopePending, forkchoice.ErrMissingSegment,
	} {
		t.Run(processingErr.Error(), func(t *testing.T) {
			service, block, _ := pendingGossipFixture(t)
			processing := service.forkchoiceStore.(*blockProcessingErrorStore)
			processing.err = processingErr
			err := service.ProcessMessage(t.Context(), nil, block)
			require.ErrorIs(t, err, ErrIgnore)
			require.NotErrorIs(t, err, processingErr)
			require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
			service.blocksScheduledForLaterExecution.processPending(t.Context())
			require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
			processing.err = errors.New("invalid block")
			service.blocksScheduledForLaterExecution.processPending(t.Context())
			require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
		})
	}
}

func TestPendingGossipQueueFullAllowsRedelivery(t *testing.T) {
	service, block, fcu := pendingGossipFixture(t)
	service.blocksScheduledForLaterExecution.capacity = 1
	filler := newBlockJob(block, nil)
	retained, err := service.blocksScheduledForLaterExecution.enqueueKey([32]byte{}, filler)
	require.NoError(t, err)
	require.Same(t, filler, retained)
	fcu.SlotVal = block.Block.Slot - 1
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	require.False(t, service.seenBlocksCache.Contains(blockGossipKey(block)))
	service.removeScheduledBlockLocked([32]byte{}, filler)
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, exists := service.blocksScheduledForLaterExecution.jobs.Load(root)
	require.True(t, exists)
}

func TestPublishedBlockRefreshReplacesExpiryEntry(t *testing.T) {
	service, block, _ := pendingGossipFixture(t)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	store := func(context.Context) error { return nil }
	first := service.SchedulePublishedBlockForLaterProcessing(block, store)
	entry, exists := service.blocksScheduledForLaterExecution.jobs.Load(root)
	require.True(t, exists)
	second := service.SchedulePublishedBlockForLaterProcessing(block, store)
	require.False(t, service.blocksScheduledForLaterExecution.remove(root, entry.(*pendingJob[*blockJob])))
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.NoError(t, first.Wait(t.Context()))
	require.NoError(t, second.Wait(t.Context()))
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPublishedBlockJobBacksOffExecutionFailures(t *testing.T) {
	service, block, _ := pendingGossipFixture(t)
	calls := 0
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		calls++
		if calls == 1 {
			return forkchoice.ErrNewPayloadNoStatus
		}
		return nil
	})
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	job := serviceJob(t, service, root)
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.Equal(t, blockELRetryInitialDelay, job.retryDelay)
	require.Equal(t, job.processingFailureAt.Add(blockELRetryInitialDelay), job.retryAfter)
	service.processScheduledBlock(t.Context(), root, job, job.retryAfter.Add(-time.Nanosecond))
	require.Equal(t, 1, calls)
	service.processScheduledBlock(t.Context(), root, job, job.retryAfter)
	require.Equal(t, 2, calls)
	require.NoError(t, handle.Wait(t.Context()))
	require.False(t, job.executionAndDataChecked)
}

func TestPendingGossipPublicationUpgradeEmitsOnce(t *testing.T) {
	service, block, fcu := pendingGossipFixture(t)
	service.emitter = beaconevents.NewEventEmitter()
	events := make(chan *beaconevents.EventStream, 2)
	sub := service.emitter.State().Subscribe(events)
	defer sub.Unsubscribe()
	fcu.SlotVal = block.Block.Slot - 1
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	require.Empty(t, events)
	published := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error { return nil })
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.NoError(t, published.Wait(t.Context()))
	require.Len(t, events, 1)
	require.Equal(t, beaconevents.StateBlockGossip, (<-events).Event)
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Empty(t, events)
}

func TestPendingGossipWaitsForRootScopedParentPayload(t *testing.T) {
	api, block, fcu, parentRoot, _ := newGloasGossipValidationFixture(t, nil)
	service := api.(*blockService)
	fcu.SlotVal = block.Block.Slot
	processing := &blockProcessingErrorStore{ForkChoiceStorage: fcu}
	service.forkchoiceStore = processing
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Zero(t, processing.calls)
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusInvalidated
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Zero(t, processing.calls)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	fcu.PayloadStatusByRootMap[parentRoot] = execution_client.PayloadStatusValidated
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Equal(t, 1, processing.calls)
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingGossipRepeatsValidationBeforeWriting(t *testing.T) {
	service, block, fcu := pendingGossipFixture(t)
	fcu.SlotVal = block.Block.Slot - 1
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	db := &updateCountingDB{RwDB: service.db}
	service.db = db
	fcu.Ancestors[0] = forkchoice.ForkChoiceNode{Root: common.Hash{0xff}}
	fcu.SlotVal = block.Block.Slot
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Zero(t, service.forkchoiceStore.(*blockProcessingErrorStore).calls)
	require.Zero(t, db.updates)
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingGossipRetainsDeferredDatabaseFailure(t *testing.T) {
	service, block, fcu := pendingGossipFixture(t)
	fcu.SlotVal = block.Block.Slot - 1
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	fcu.SlotVal = block.Block.Slot
	db := &blockDBError{RwDB: service.db, viewErr: errors.New("read unavailable")}
	service.db = db
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	db.viewErr, db.updateErr = nil, errors.New("write unavailable")
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.EqualValues(t, 1, service.blocksScheduledForLaterExecution.count.Load())
	db.updateErr = nil
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingGossipFinalizedIndexFailureDoesNotReject(t *testing.T) {
	service, block, _ := pendingGossipFixture(t)
	service.db = &failNthUpdateDB{RwDB: service.db, failAt: 2, err: errors.New("index unavailable")}
	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingGossipDoesNotReplaceCommittedRESTReservation(t *testing.T) {
	service, block, fcu := pendingGossipFixture(t)
	fcu.GetStateAtBlockRootFn = func(common.Hash, bool) (*state.CachingBeaconState, error) {
		key := blockGossipKey(block)
		require.NoError(t, service.reserveGossipKey(key, common.Hash{0xfe}))
		service.commitGossipKey(key)
		return nil, nil
	}
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
}

func TestPendingGossipCompletionPreservesRESTReplay(t *testing.T) {
	service, block, fcu := pendingGossipFixture(t)
	fcu.SlotVal = block.Block.Slot - 1
	require.ErrorIs(t, service.ProcessMessage(t.Context(), nil, block), ErrIgnore)
	service.ReleaseGossipReservation(block)
	fcu.SlotVal = block.Block.Slot
	service.blocksScheduledForLaterExecution.processPending(t.Context())
	require.Zero(t, service.blocksScheduledForLaterExecution.count.Load())
	require.NoError(t, service.ValidateGossip(t.Context(), block))
}
