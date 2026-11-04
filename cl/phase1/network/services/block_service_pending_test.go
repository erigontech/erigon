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
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

type blockDBError struct {
	kv.RwDB
	viewErr      error
	updateErr    error
	failUpdateAt int
	views        int
	updates      int
	beforeView   func()
	beforeUpdate func()
}

func (db *blockDBError) View(ctx context.Context, f func(kv.Tx) error) error {
	db.views++
	if db.beforeView != nil {
		db.beforeView()
	}
	if db.viewErr != nil {
		return db.viewErr
	}
	return db.RwDB.View(ctx, f)
}

func (db *blockDBError) Update(ctx context.Context, f func(kv.RwTx) error) error {
	db.updates++
	if db.beforeUpdate != nil {
		db.beforeUpdate()
	}
	if db.updateErr != nil && (db.failUpdateAt == 0 || db.updates == db.failUpdateAt) {
		return db.updateErr
	}
	return db.RwDB.Update(ctx, f)
}

func TestPendingGossipRetainsCanceledDatabaseAttempt(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
			service := api.(*blockService)
			processing := &onBlockErrorStore{ForkChoiceStorage: fcu}
			service.forkchoiceStore = processing
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			db := &blockDBError{RwDB: service.db}
			if operation == "read" {
				db.beforeView = cancel
			} else {
				db.beforeUpdate = cancel
			}
			service.db = db

			err := service.ProcessMessage(ctx, nil, block)
			if operation == "write" {
				require.NoError(t, err, "an accepted block can propagate while its write is retried")
			} else {
				require.ErrorIs(t, err, ErrIgnore)
				require.ErrorIs(t, err, context.Canceled)
				require.NotErrorIs(t, err, errBlockStorage)
			}
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			job := serviceJob(t, service, root)
			if operation == "write" {
				require.Equal(t, int32(1), processing.calls.Load())
			} else {
				require.Zero(t, processing.calls.Load())
			}

			service.processScheduledBlock(ctx, root, job, time.Now())
			require.ErrorIs(t, job.lastAttempt.err, context.Canceled)
			require.NotErrorIs(t, job.lastAttempt.err, errBlockStorage)
			require.True(t, job.retryAfter.IsZero())
			require.Zero(t, job.retryDelay)
			require.False(t, job.terminal)

			db.beforeView, db.beforeUpdate = nil, nil
			service.processScheduledBlock(t.Context(), root, job, time.Now())
			require.Equal(t, int32(1), processing.calls.Load())
			_, queued := service.blocksScheduledForLaterExecution.Load(root)
			require.False(t, queued)
		})
	}
}

func TestPendingGossipRetainsInitialDatabaseFailure(t *testing.T) {
	for _, tc := range []struct {
		name             string
		writeFailure     bool
		interveningError error
	}{
		{name: "read"},
		{name: "write", writeFailure: true},
		{name: "read with missing data", interveningError: forkchoice.ErrEIP4844DataNotAvailable},
		{name: "read with missing segment", interveningError: forkchoice.ErrMissingSegment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
			service := api.(*blockService)
			processing := &onBlockErrorStore{ForkChoiceStorage: fcu}
			service.forkchoiceStore = processing
			failure := errors.New("database unavailable")
			service.emitter = beaconevents.NewEventEmitter()
			indicesChanged := service.emitter.BlockIndicesChanged()
			db := &blockDBError{RwDB: service.db}
			if tc.writeFailure {
				db.updateErr = failure
			} else {
				db.viewErr = failure
			}
			service.db = db

			err := service.ProcessMessage(t.Context(), nil, block)
			if tc.writeFailure {
				require.NoError(t, err, "an accepted block can propagate while its write is retried")
			} else {
				require.ErrorIs(t, err, ErrIgnore)
				require.ErrorIs(t, err, failure)
			}
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			job := serviceJob(t, service, root)
			if tc.writeFailure {
				require.Equal(t, int32(1), processing.calls.Load())
			} else {
				require.Zero(t, processing.calls.Load())
			}

			now := time.Now()
			service.processScheduledBlock(t.Context(), root, job, now)
			require.ErrorIs(t, job.lastAttempt.err, failure)
			require.ErrorIs(t, job.lastAttempt.err, errBlockStorage)
			if tc.interveningError != nil {
				db.viewErr = nil
				processing.err = tc.interveningError
				service.processScheduledBlock(t.Context(), root, job, now)
				require.ErrorIs(t, job.lastAttempt.err, tc.interveningError)
				processing.err = nil
			}
			select {
			case <-indicesChanged:
				t.Fatal("failed writes must not notify index readers")
			default:
			}
			db.viewErr, db.updateErr = nil, nil
			callsBefore := processing.calls.Load()
			service.processScheduledBlock(t.Context(), root, job, now)
			require.NoError(t, job.lastAttempt.err)
			select {
			case <-indicesChanged:
			default:
				t.Fatal("the successful retry must notify index readers")
			}
			if tc.writeFailure {
				require.Equal(t, callsBefore, processing.calls.Load())
			} else {
				require.Equal(t, callsBefore+1, processing.calls.Load())
			}
			_, queued := service.blocksScheduledForLaterExecution.Load(root)
			require.False(t, queued)
		})
	}
}

func TestPendingGossipFinalizedIndexFailureDoesNotReject(t *testing.T) {
	api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
	service := api.(*blockService)
	processing := &onBlockErrorStore{ForkChoiceStorage: fcu}
	service.forkchoiceStore = processing
	db := &blockDBError{RwDB: service.db, failUpdateAt: 2, updateErr: errors.New("index unavailable")}
	service.db = db
	service.emitter = beaconevents.NewEventEmitter()
	indicesChanged := service.emitter.BlockIndicesChanged()

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
	select {
	case <-indicesChanged:
	default:
		t.Fatal("the committed block indices must notify readers even if the finalized index write fails")
	}
	require.Equal(t, int32(1), processing.calls.Load())
	require.Equal(t, 2, db.updates)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, queued := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, queued)
}

func TestPendingGossipRetriesAcceptedScheduledBlockAfterExpiry(t *testing.T) {
	api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
	service := api.(*blockService)
	processing := &onBlockErrorStore{ForkChoiceStorage: fcu}
	service.forkchoiceStore = processing
	failure := errors.New("database unavailable")
	db := &blockDBError{RwDB: service.db, updateErr: failure}
	service.db = db

	service.ScheduleBlockForLaterProcessing(block)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	job := serviceJob(t, service, root)
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, errBlockStorage)
	require.ErrorIs(t, job.lastAttempt.err, failure)
	_, accepted := processing.GetHeader(root)
	require.True(t, accepted)

	processing.headers.Delete(common.Hash(root))
	_, accepted = processing.GetHeader(root)
	require.False(t, accepted, "the retry must run without the accepted header")

	db.updateErr = nil
	service.processScheduledBlock(t.Context(), root, job, job.creationTime.Add(blockJobExpiry+time.Second))

	require.NoError(t, job.lastAttempt.err)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot, "an accepted block must still be persisted after the validation retry window")
		require.Equal(t, block.Block.Slot, *slot)
		storedRoot, err := beacon_indicies.ReadBlockRootByStateRoot(tx, block.Block.StateRoot)
		require.NoError(t, err)
		require.Equal(t, common.Hash(root), storedRoot)
		return nil
	}))
	require.Equal(t, int32(1), processing.calls.Load(), "storage recovery must not reimport the block")
	_, queued := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, queued)
}

func TestPendingGossipWriteKeepsAcceptedBlockOnJobReuse(t *testing.T) {
	api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
	service := api.(*blockService)
	service.forkchoiceStore = &onBlockErrorStore{ForkChoiceStorage: fcu}
	db := &blockDBError{RwDB: service.db, updateErr: errors.New("database unavailable")}
	service.db = db
	deferred := *block
	deferred.Signature[0] ^= 1
	job, _ := service.scheduleBlockForLaterProcessing(&deferred, nil, false, false)

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.Same(t, job, serviceJob(t, service, root))
	db.updateErr = nil
	service.processScheduledBlock(t.Context(), root, job, job.creationTime.Add(blockJobExpiry+time.Second))

	require.NoError(t, job.lastAttempt.err)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		header, _, err := beacon_indicies.ReadSignedHeaderByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, header)
		require.Equal(t, block.Signature, header.Signature, "retry the accepted copy, not an earlier unvalidated copy with the same block root")
		return nil
	}))
}

func TestPendingGossipWriteSurvivesStoreCompletion(t *testing.T) {
	for _, tc := range []struct {
		name     string
		storeErr error
	}{
		{name: "callback succeeds"},
		{name: "callback rejects block", storeErr: forkchoice.ErrBlockInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
			service := api.(*blockService)
			processing := &onBlockErrorStore{ForkChoiceStorage: fcu}
			service.forkchoiceStore = processing
			db := &blockDBError{RwDB: service.db, updateErr: errors.New("database unavailable")}
			service.db = db
			calls := 0
			service.SchedulePublishedBlockForLaterProcessing(block, func(ctx context.Context) error {
				calls++
				if calls == 1 {
					require.NoError(t, service.ProcessMessage(ctx, nil, block))
				}
				return tc.storeErr
			})
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			job := serviceJob(t, service, root)
			service.processScheduledBlock(t.Context(), root, job, time.Now())
			require.Same(t, job, serviceJob(t, service, root), "a running callback must not discard a new storage retry")

			db.updateErr = nil
			service.processScheduledBlock(t.Context(), root, job, job.creationTime.Add(blockJobExpiry+time.Second))
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
				require.NoError(t, err)
				require.NotNil(t, slot)
				return nil
			}))
			require.Equal(t, int32(1), processing.calls.Load())
			_, queued := service.blocksScheduledForLaterExecution.Load(root)
			require.False(t, queued)
		})
	}
}

func TestPublishedBlockRetainsAcceptedWriteAfterExpiry(t *testing.T) {
	api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
	service := api.(*blockService)
	processing := &onBlockErrorStore{ForkChoiceStorage: fcu}
	service.forkchoiceStore = processing
	failure := errors.New("database unavailable")
	db := &blockDBError{RwDB: service.db, updateErr: failure}
	service.db = db
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(ctx context.Context) error {
		if _, known := processing.GetHeader(root); !known {
			if err := processing.OnBlock(ctx, block, true, true, false); err != nil {
				return err
			}
		}
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			if err := beacon_indicies.WriteHighestFinalized(tx, processing.FinalizedSlot()); err != nil {
				return err
			}
			return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, false)
		}); err != nil {
			return &AcceptedBlockWriteError{Block: block, Err: err}
		}
		return nil
	})
	job := serviceJob(t, service, root)
	service.processScheduledBlock(t.Context(), root, job, time.Now())
	require.ErrorIs(t, job.lastAttempt.err, failure)
	require.ErrorIs(t, job.lastAttempt.err, errBlockStorage)
	_, accepted := processing.GetHeader(root)
	require.True(t, accepted)

	now := job.creationTime.Add(blockJobExpiry + time.Second)
	service.processScheduledBlock(t.Context(), root, job, now)
	require.False(t, job.terminal, "an accepted block's write must survive job expiry")
	require.ErrorIs(t, job.lastAttempt.err, failure)

	db.updateErr = nil
	service.processScheduledBlock(t.Context(), root, job, now.Add(time.Second))
	require.True(t, job.terminal)
	require.NoError(t, handle.Wait(t.Context()))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		require.NotNil(t, slot)
		require.Equal(t, block.Block.Slot, *slot)
		return nil
	}))
	require.Equal(t, int32(1), processing.calls.Load())
	_, queued := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, queued)
}

type blockWriteOutageDB struct {
	kv.RwDB
	failing  atomic.Bool
	attempts atomic.Int32
}

func (db *blockWriteOutageDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	db.attempts.Add(1)
	if db.failing.Load() {
		return errors.New("database unavailable")
	}
	return db.RwDB.Update(ctx, f)
}

func TestBlockServiceSharesStorageBackoffAcrossPendingWrites(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := &blockWriteOutageDB{RwDB: mdbxtest.NewTestDB(t, dbcfg.ChainDB)}
		db.failing.Store(true)
		service := &blockService{db: db, forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
		var roots [][32]byte
		for slot := uint64(1); slot <= 8; slot++ {
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Slot = slot
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			roots = append(roots, root)
			service.scheduleBlockForLaterProcessing(block, nil, false, true)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go service.loop(ctx)
		synctest.Wait()
		time.Sleep(blockJobsIntervalTick)
		synctest.Wait()
		require.Equal(t, int32(1), db.attempts.Load(), "one failed write must delay all queued storage retries")

		outageEnd := time.Now().Add(2 * blockJobExpiry)
		for _, delay := range []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 2 * time.Second} {
			attempts := db.attempts.Load()
			time.Sleep(delay - blockJobsIntervalTick)
			synctest.Wait()
			require.Equal(t, attempts, db.attempts.Load(), "all jobs must share the retry deadline")
			time.Sleep(blockJobsIntervalTick)
			synctest.Wait()
			require.Equal(t, attempts+1, db.attempts.Load())
		}
		time.Sleep(time.Until(outageEnd))
		synctest.Wait()
		require.LessOrEqual(t, db.attempts.Load(), int32(35), "retry work must not grow with the number of retained blocks")
		for _, root := range roots {
			job := serviceJob(t, service, root)
			job.mu.Lock()
			pending := job.pendingBlockWrite
			job.mu.Unlock()
			require.NotNil(t, pending)
		}

		db.failing.Store(false)
		time.Sleep(blockRetryMaxDelay + blockJobsIntervalTick)
		synctest.Wait()
		for _, root := range roots {
			_, queued := service.blocksScheduledForLaterExecution.Load(root)
			require.False(t, queued)
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
				require.NoError(t, err)
				require.NotNil(t, slot)
				return nil
			}))
		}
	})
}

func TestBlockServiceExpiresJobsDuringStorageFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		db := &blockWriteOutageDB{RwDB: mdbxtest.NewTestDB(t, dbcfg.ChainDB)}
		db.failing.Store(true)
		service := &blockService{db: db, forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
		handles := make(map[[32]byte]PublishedBlockJob)
		for slot := uint64(1); slot <= 8; slot++ {
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			block.Block.Slot = slot
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			handles[root] = service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
				return forkchoice.ErrNewPayloadNoStatus
			})
		}
		var pendingRoot [32]byte
		// Expiry must not depend on a job's position in the scan.
		service.blocksScheduledForLaterExecution.Range(func(key, value any) bool {
			pendingRoot = key.([32]byte)
			job := value.(*blockJob)
			job.pendingBlockWrite = job.block
			return false
		})
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go service.loop(ctx)
		synctest.Wait()
		time.Sleep(blockJobExpiry + blockRetryMaxDelay + blockJobsIntervalTick)
		synctest.Wait()
		waitCtx, stop := context.WithTimeout(t.Context(), blockJobsIntervalTick)
		defer stop()
		for root, handle := range handles {
			if root == pendingRoot {
				continue
			}
			require.ErrorIs(t, handle.Wait(waitCtx), ErrPublishedBlockJobExpired,
				"a pending write must not keep unrelated jobs past their expiry")
			_, queued := service.blocksScheduledForLaterExecution.Load(root)
			require.False(t, queued)
		}
		_, retained := service.blocksScheduledForLaterExecution.Load(pendingRoot)
		require.True(t, retained, "the accepted block must still wait for storage recovery")
	})
}

func TestPublishedBlockJobBacksOffExecutionFailures(t *testing.T) {
	for _, tc := range []struct {
		name             string
		interveningError error
	}{
		{name: "consecutive failures"},
		{name: "interleaved with missing data", interveningError: forkchoice.ErrEIP4844DataNotAvailable},
		{name: "interleaved with missing segment", interveningError: forkchoice.ErrMissingSegment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			calls := 0
			retryError := fmt.Errorf("execution failure: %w", forkchoice.ErrNewPayloadNoStatus)
			failure := retryError
			handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
				calls++
				return failure
			})
			job := serviceJob(t, service, root)
			now := time.Now()
			for _, delay := range []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 2 * time.Second} {
				callsBefore := calls
				started := time.Now()
				service.processScheduledBlock(t.Context(), root, job, now)
				require.Equal(t, callsBefore+1, calls)
				require.False(t, job.retryAfter.Before(started.Add(delay)), "backoff must start after the failed attempt")
				require.False(t, job.retryAfter.After(time.Now().Add(delay)))
				service.processScheduledBlock(t.Context(), root, job, job.retryAfter.Add(-time.Nanosecond))
				require.Equal(t, callsBefore+1, calls, "a local failure must delay the next retry")
				now = job.retryAfter
				if tc.interveningError != nil {
					failure = tc.interveningError
					service.processScheduledBlock(t.Context(), root, job, now)
					require.Equal(t, callsBefore+2, calls)
					failure = retryError
				}
			}
			callsBefore := calls
			failure = nil
			service.processScheduledBlock(t.Context(), root, job, now)
			require.Equal(t, callsBefore+1, calls)
			require.True(t, job.terminal)
			require.NoError(t, handle.Wait(t.Context()))
			_, queued := service.blocksScheduledForLaterExecution.Load(root)
			require.False(t, queued)
		})
	}
}

func TestPublishedBlockJobUpgradeResetsExecutionBackoff(t *testing.T) {
	for _, previousStore := range []string{"block-only", "published"} {
		t.Run(previousStore, func(t *testing.T) {
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			fcu := mock_services.NewForkChoiceStorageMock(t)
			fcu.OnBlockErr = forkchoice.ErrNewPayloadNoStatus
			service := &blockService{db: mdbxtest.NewTestDB(t, dbcfg.ChainDB), forkchoiceStore: fcu}
			if previousStore == "block-only" {
				service.ScheduleBlockForLaterProcessing(block)
			} else {
				service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
					return forkchoice.ErrNewPayloadNoStatus
				})
			}
			job := serviceJob(t, service, root)
			beforeBackoff := time.Now()
			service.processScheduledBlock(t.Context(), root, job, beforeBackoff)
			require.ErrorIs(t, job.lastAttempt.err, forkchoice.ErrNewPayloadNoStatus)
			require.True(t, job.retryAfter.After(beforeBackoff))
			require.Equal(t, blockRetryInitialDelay, job.retryDelay)

			calls := 0
			failure := forkchoice.ErrNewPayloadNoStatus
			handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
				calls++
				return failure
			})
			service.processScheduledBlock(t.Context(), root, job, beforeBackoff)
			require.Equal(t, 1, calls, "a new store generation must not inherit the old retry deadline")
			require.Equal(t, blockRetryInitialDelay, job.retryDelay, "a new store generation must start a fresh backoff sequence")

			failure = nil
			service.processScheduledBlock(t.Context(), root, job, job.retryAfter)
			require.Equal(t, 2, calls)
			require.True(t, job.terminal)
			require.NoError(t, handle.Wait(t.Context()))
		})
	}
}

func TestPublishedBlockJobBackoffDoesNotExtendExpiry(t *testing.T) {
	service := &blockService{forkchoiceStore: mock_services.NewForkChoiceStorageMock(t)}
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	calls := 0
	handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
		calls++
		return forkchoice.ErrNewPayloadNoStatus
	})
	job := serviceJob(t, service, root)
	expiresAt := time.Now()
	job.creationTime = expiresAt.Add(-blockJobExpiry)
	service.processScheduledBlock(t.Context(), root, job, expiresAt)
	require.Equal(t, 1, calls)
	require.True(t, job.retryAfter.After(expiresAt))

	service.processScheduledBlock(t.Context(), root, job, expiresAt.Add(time.Nanosecond))
	require.Equal(t, 1, calls, "expiry must not trigger an extra attempt or wait for backoff")
	require.True(t, job.terminal)
	require.ErrorIs(t, handle.Wait(t.Context()), ErrPublishedBlockJobExpired)
	_, queued := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, queued)
}
