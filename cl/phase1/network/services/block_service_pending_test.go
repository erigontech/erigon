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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
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
			require.ErrorIs(t, err, ErrIgnore)
			require.ErrorIs(t, err, context.Canceled)
			require.NotErrorIs(t, err, errBlockStorage)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			job := serviceJob(t, service, root)
			require.Zero(t, processing.calls.Load())

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
			db := &blockDBError{RwDB: service.db}
			if tc.writeFailure {
				db.updateErr = failure
			} else {
				db.viewErr = failure
			}
			service.db = db

			err := service.ProcessMessage(t.Context(), nil, block)
			require.ErrorIs(t, err, ErrIgnore)
			require.ErrorIs(t, err, failure)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			job := serviceJob(t, service, root)
			require.Zero(t, processing.calls.Load())

			now := time.Now()
			for _, delay := range []time.Duration{250 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 2 * time.Second} {
				viewsBefore := db.views
				started := time.Now()
				service.processScheduledBlock(t.Context(), root, job, now)
				require.ErrorIs(t, job.lastAttempt.err, failure)
				require.ErrorIs(t, job.lastAttempt.err, errBlockStorage)
				require.Equal(t, viewsBefore+1, db.views)
				require.Equal(t, delay, job.retryDelay)
				require.False(t, job.retryAfter.Before(started.Add(delay)), "backoff must start after the failed attempt")
				require.False(t, job.retryAfter.After(time.Now().Add(delay)))
				service.processScheduledBlock(t.Context(), root, job, job.retryAfter.Add(-time.Nanosecond))
				require.Equal(t, viewsBefore+1, db.views, "storage retries must respect the backoff")
				now = job.retryAfter
				if tc.interveningError != nil {
					db.viewErr = nil
					processing.err = tc.interveningError
					service.processScheduledBlock(t.Context(), root, job, now)
					require.ErrorIs(t, job.lastAttempt.err, tc.interveningError)
					db.viewErr = failure
					processing.err = nil
				}
			}
			db.viewErr, db.updateErr = nil, nil
			callsBefore := processing.calls.Load()
			service.processScheduledBlock(t.Context(), root, job, now)
			require.NoError(t, job.lastAttempt.err)
			require.Equal(t, callsBefore+1, processing.calls.Load())
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

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
	require.Equal(t, int32(1), processing.calls.Load())
	require.Equal(t, 2, db.updates)
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	_, queued := service.blocksScheduledForLaterExecution.Load(root)
	require.False(t, queued)
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
			service := &blockService{}
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
	service := &blockService{}
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
