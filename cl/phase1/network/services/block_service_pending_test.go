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
	viewErr   error
	updateErr error
}

type countingBlockStore struct {
	forkchoice.ForkChoiceStorage
	calls int
}

func (s *countingBlockStore) OnBlock(context.Context, *cltypes.SignedBeaconBlock, bool, bool, bool) error {
	s.calls++
	return nil
}

func (db *blockDBError) View(ctx context.Context, f func(kv.Tx) error) error {
	if db.viewErr != nil {
		return db.viewErr
	}
	return db.RwDB.View(ctx, f)
}

func (db *blockDBError) Update(ctx context.Context, f func(kv.RwTx) error) error {
	if db.updateErr != nil {
		return db.updateErr
	}
	return db.RwDB.Update(ctx, f)
}

type failNthUpdateDB struct {
	kv.RwDB
	failAt  int
	updates int
	err     error
}

func (db *failNthUpdateDB) Update(ctx context.Context, f func(kv.RwTx) error) error {
	db.updates++
	if db.updates == db.failAt {
		return db.err
	}
	return db.RwDB.Update(ctx, f)
}

func TestPendingGossipRetainsInitialDatabaseFailure(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
			service := api.(*blockService)
			processing := &countingBlockStore{ForkChoiceStorage: fcu}
			service.forkchoiceStore = processing
			failure := errors.New("database unavailable")
			db := &blockDBError{RwDB: service.db}
			if operation == "read" {
				db.viewErr = failure
			} else {
				db.updateErr = failure
			}
			service.db = db

			err := service.ProcessMessage(t.Context(), nil, block)
			require.ErrorIs(t, err, ErrIgnore)
			require.ErrorIs(t, err, failure)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			job := serviceJob(t, service, root)
			require.Zero(t, processing.calls)

			db.viewErr, db.updateErr = nil, nil
			service.processScheduledBlock(t.Context(), root, job, time.Now())
			require.Equal(t, 1, processing.calls)
			_, queued := service.blocksScheduledForLaterExecution.Load(root)
			require.False(t, queued)
		})
	}
}

func TestPendingGossipFinalizedIndexFailureDoesNotReject(t *testing.T) {
	api, block, fcu, _, _ := newGloasGossipValidationFixture(t, func(head, _ common.Hash) common.Hash { return head })
	service := api.(*blockService)
	processing := &countingBlockStore{ForkChoiceStorage: fcu}
	service.forkchoiceStore = processing
	db := &failNthUpdateDB{RwDB: service.db, failAt: 2, err: errors.New("index unavailable")}
	service.db = db

	require.NoError(t, service.ProcessMessage(t.Context(), nil, block))
	require.Equal(t, 1, processing.calls)
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
		{name: "consecutive EL failures"},
		{name: "interleaved missing data", interveningError: forkchoice.ErrEIP4844DataNotAvailable},
		{name: "interleaved missing segment", interveningError: forkchoice.ErrMissingSegment},
	} {
		t.Run(tc.name, func(t *testing.T) {
			service := &blockService{}
			block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
			root, err := block.Block.HashSSZ()
			require.NoError(t, err)
			calls := 0
			executionFailure := fmt.Errorf("execution unavailable: %w", forkchoice.ErrNewPayloadNoStatus)
			failure := executionFailure
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
				require.Equal(t, callsBefore+1, calls, "an EL failure must delay the next retry")
				now = job.retryAfter
				if tc.interveningError != nil {
					failure = tc.interveningError
					service.processScheduledBlock(t.Context(), root, job, now)
					require.Equal(t, callsBefore+2, calls)
					failure = executionFailure
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
			require.Equal(t, blockELRetryInitialDelay, job.retryDelay)

			calls := 0
			failure := forkchoice.ErrNewPayloadNoStatus
			handle := service.SchedulePublishedBlockForLaterProcessing(block, func(context.Context) error {
				calls++
				return failure
			})
			service.processScheduledBlock(t.Context(), root, job, beforeBackoff)
			require.Equal(t, 1, calls, "a new store generation must not inherit the old retry deadline")
			require.Equal(t, blockELRetryInitialDelay, job.retryDelay, "a new store generation must start a fresh backoff sequence")

			failure = nil
			service.processScheduledBlock(t.Context(), root, job, job.retryAfter)
			require.Equal(t, 2, calls)
			require.True(t, job.terminal)
			require.NoError(t, handle.Wait(t.Context()))
		})
	}
}
