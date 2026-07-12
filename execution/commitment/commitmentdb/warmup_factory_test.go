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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
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

	_, cleanup := sdc.warmupTrieContextFactory(context.Background(), db, 0)()
	defer cleanup()

	require.True(t, db.sawNonBlocking, "warmup BeginTemporalRo must use non-blocking semaphore acquire")
}
