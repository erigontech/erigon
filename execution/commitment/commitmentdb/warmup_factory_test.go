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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx/execctxapi"
	"github.com/erigontech/erigon/db/state/kvmetrics"
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
	concurrent, _ := sdc.concurrentTrieContextFactory(&blockingBeginDB{}, nil, nil, 0)
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

// snapshotTx answers every domain read with val, from a scratch buffer it
// rewrites on each read — the aliasing a real getter has.
type snapshotTx struct {
	kv.TemporalTx
	viewID  uint64
	val     []byte
	scratch []byte
}

func (t *snapshotTx) ViewID() uint64 { return t.viewID }
func (t *snapshotTx) Rollback()      {}

type snapshotGetter struct {
	execctxapi.StateGetter
	tx *snapshotTx
}

func (g *snapshotGetter) GetLatest(kv.Domain, []byte, kv.GetLatestOptions) ([]byte, kv.Step, error) {
	g.tx.scratch = append(g.tx.scratch[:0], g.tx.val...)
	return g.tx.scratch, 0, nil
}

type snapshotSD struct{ sd }

func (snapshotSD) StepSize() uint64 { return 1 }
func (snapshotSD) AsStateGetter(tx kv.TemporalTx, _ execctxapi.StateGetterOptions) execctxapi.StateGetter {
	return &snapshotGetter{tx: tx.(*snapshotTx)}
}
func (snapshotSD) AsPutDel(kv.TemporalTx) kv.TemporalPutDel                { return nil }
func (snapshotSD) MergeMetrics(kvmetrics.Source, *kvmetrics.DomainMetrics) {}

// snapshotDB hands every worker its own read view, as a real backend does.
type snapshotDB struct {
	kv.TemporalRoDB
	viewID uint64
	val    []byte
}

func (db *snapshotDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) {
	return &snapshotTx{viewID: db.viewID, val: db.val}, nil
}

func newSnapshotSdc(t *testing.T) *SharedDomainsCommitmentContext {
	t.Helper()
	return &SharedDomainsCommitmentContext{sharedDomains: snapshotSD{}, tmpDir: t.TempDir()}
}

// A fold outside the exec-module semaphore can overlap a commit, so a worker
// read view opened at fold time sits past the caller's snapshot. Reads have to
// resolve on the caller's snapshot, or the root mixes two states.
func TestConcurrentTrieContextReadsCallerSnapshot(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 8, val: []byte("committed-head")}
	sdc := newSnapshotSdc(t)

	factory, drain := sdc.concurrentTrieContextFactory(db, nil, caller, 0)
	trieCtx, cleanup := factory(t.Context())
	defer func() {
		cleanup()
		for _, c := range drain() {
			c.Close()
		}
	}()

	enc, _, err := trieCtx.Branch([]byte("prefix"))
	require.NoError(t, err)
	require.Equal(t, []byte("caller-snapshot"), enc)
}

// Under the semaphore no commit can land, the worker view is the caller's
// snapshot, and workers must keep their own read views.
func TestConcurrentTrieContextKeepsWorkerTxOnSameSnapshot(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 7, val: []byte("worker-view")}
	sdc := newSnapshotSdc(t)

	factory, drain := sdc.concurrentTrieContextFactory(db, nil, caller, 0)
	trieCtx, cleanup := factory(t.Context())
	defer func() {
		cleanup()
		for _, c := range drain() {
			c.Close()
		}
	}()

	enc, _, err := trieCtx.Branch([]byte("prefix"))
	require.NoError(t, err)
	require.Equal(t, []byte("worker-view"), enc)
}

// Workers sharing the caller's tx must not touch it concurrently, and the bytes
// one worker gets back must survive another worker's next read.
func TestConcurrentTrieContextsShareCallerTxSerially(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 8, val: []byte("committed-head")}
	sdc := newSnapshotSdc(t)

	factory, drain := sdc.concurrentTrieContextFactory(db, nil, caller, 0)
	defer func() {
		for _, c := range drain() {
			c.Close()
		}
	}()

	const workers = 8
	var wg sync.WaitGroup
	errs := make([]error, workers)
	vals := make([][]byte, workers)
	start := make(chan struct{})
	for i := range workers {
		wg.Go(func() {
			trieCtx, cleanup := factory(t.Context())
			defer cleanup()
			<-start
			for range 64 {
				enc, _, err := trieCtx.Branch([]byte{byte(i)})
				if err != nil {
					errs[i] = err
					return
				}
				vals[i] = append(vals[i][:0], enc...)
			}
		})
	}
	close(start)
	wg.Wait()

	for i := range workers {
		require.NoError(t, errs[i])
		require.Equal(t, []byte("caller-snapshot"), vals[i])
	}
}
