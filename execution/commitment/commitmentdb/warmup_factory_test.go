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

package commitmentdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/commitment"
)

type stubSharedDomains struct{ sd }

func (stubSharedDomains) StepSize() uint64 { return 1 }

type beginRoRecordingDB struct {
	kv.TemporalRoDB
	sawNonBlocking bool
}

func (db *beginRoRecordingDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	db.sawNonBlocking = kv.IsNonBlockingAcquire(ctx)
	return nil, kv.ErrReadTxLimitExceeded
}

// Warmup is best-effort: its read txs must never queue on the read-tx
// semaphore, or warmup workers can wedge commitment shutdown and starve
// execution workers of semaphore slots.
func TestWarmupTrieContextFactoryUsesNonBlockingReadTxAcquire(t *testing.T) {
	t.Parallel()
	db := &beginRoRecordingDB{}
	sdc := &SharedDomainsCommitmentContext{sharedDomains: stubSharedDomains{}}

	_, cleanup := sdc.warmupTrieContextFactory(db, 0)(t.Context())
	defer cleanup()

	require.True(t, db.sawNonBlocking, "warmup BeginTemporalRo must use non-blocking semaphore acquire")
}

type blockingBeginDB struct {
	kv.TemporalRoDB
}

func (db *blockingBeginDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A factory blocked opening its read tx (e.g. parked on the read-tx semaphore)
// must unblock when the warmuper shuts down, or CloseAndWait hangs.
func TestWarmupFactoriesUnblockBeginOnWarmuperClose(t *testing.T) {
	t.Parallel()
	sdc := &SharedDomainsCommitmentContext{sharedDomains: stubSharedDomains{}}
	concurrent, _ := sdc.concurrentTrieContextFactory(&blockingBeginDB{}, nil, 0)
	factories := map[string]commitment.TrieContextFactory{
		"warmup":     sdc.warmupTrieContextFactory(&blockingBeginDB{}, 0),
		"concurrent": concurrent,
	}
	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			errCh := make(chan error, 1)
			go func() {
				trieCtx, cleanup := factory(ctx)
				defer cleanup()
				_, err := trieCtx.Account(nil)
				errCh <- err
			}()
			cancel()
			select {
			case err := <-errCh:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				t.Fatal("factory did not honor warmuper ctx cancellation")
			}
		})
	}
}
