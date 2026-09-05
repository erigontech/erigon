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
	"runtime"
	"sync"
	"sync/atomic"
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
	concurrent, _ := sdc.concurrentTrieContextFactory(t.Context(), &blockingBeginDB{}, nil, nil, 0)
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

	// libmdbx's Txn.ID memoizes on first call and takes no lock, so two
	// goroutines asking one txn for its view is a write race.
	viewMemo  uint64
	viewCalls atomic.Int64
}

func (t *snapshotTx) ViewID() uint64 {
	t.viewCalls.Add(1)
	if t.viewMemo == 0 {
		t.viewMemo = t.viewID
	}
	return t.viewMemo
}
func (t *snapshotTx) Rollback() {}

type snapshotGetter struct {
	execctxapi.StateGetter
	tx *snapshotTx
}

func (g *snapshotGetter) GetLatest(kv.Domain, []byte, kv.GetLatestOptions) ([]byte, kv.Step, error) {
	g.tx.scratch = append(g.tx.scratch[:0], g.tx.val...)
	return g.tx.scratch, 0, nil
}

type snapshotSD struct {
	sd
	getters atomic.Int64
	putDels atomic.Int64
}

func (*snapshotSD) StepSize() uint64 { return 1 }
func (s *snapshotSD) AsStateGetter(tx kv.TemporalTx, _ execctxapi.StateGetterOptions) execctxapi.StateGetter {
	s.getters.Add(1)
	return &snapshotGetter{tx: tx.(*snapshotTx)}
}
func (s *snapshotSD) AsPutDel(kv.TemporalTx) kv.TemporalPutDel {
	s.putDels.Add(1)
	return nil
}
func (*snapshotSD) MergeMetrics(kvmetrics.Source, *kvmetrics.DomainMetrics) {}

// snapshotDB hands every worker its own read view, as a real backend does.
type snapshotDB struct {
	kv.TemporalRoDB
	viewID uint64
	val    []byte
	opens  atomic.Int64
}

func (db *snapshotDB) BeginTemporalRo(context.Context) (kv.TemporalTx, error) {
	db.opens.Add(1)
	return &snapshotTx{viewID: db.viewID, val: db.val}, nil
}

func newSnapshotSdc(t *testing.T) (*SharedDomainsCommitmentContext, *snapshotSD) {
	t.Helper()
	shared := &snapshotSD{}
	return &SharedDomainsCommitmentContext{sharedDomains: shared, tmpDir: t.TempDir()}, shared
}

// A fold outside the exec-module semaphore can overlap a commit, so a worker
// read view opened at fold time sits past the caller's snapshot. Reads have to
// resolve on the caller's snapshot, or the root mixes two states.
func TestConcurrentTrieContextReadsCallerSnapshot(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 8, val: []byte("committed-head")}
	sdc, _ := newSnapshotSdc(t)

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
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
	sdc, _ := newSnapshotSdc(t)

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
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
	sdc, _ := newSnapshotSdc(t)

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
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

// The factory runs inside each worker goroutine, so building the caller-tx
// reader there would touch the caller's tx from every worker at once.
func TestConcurrentTrieContextBuildsCallerReaderOnce(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 8, val: []byte("committed-head")}
	sdc, shared := newSnapshotSdc(t)

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
	defer func() {
		for _, c := range drain() {
			c.Close()
		}
	}()
	require.Equal(t, int64(1), shared.getters.Load(), "caller reader must be built before any worker exists")

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			_, cleanup := factory(t.Context())
			cleanup()
		})
	}
	wg.Wait()

	require.Equal(t, int64(1), shared.getters.Load(), "workers must not each build a getter on the caller's tx")
}

// sharedSourceReader stands in for a custom reader installed by SetStateReader.
// With binds=false its clones keep the source tx, the way LatestStateReader and
// the replay readers do; it reports overlapping reads so a test can tell whether
// the fold serialized them.
type sharedSourceReader struct {
	tx       *snapshotTx
	binds    bool
	reading  atomic.Bool
	overlaps atomic.Bool
}

func (r *sharedSourceReader) WithHistory() bool                           { return false }
func (r *sharedSourceReader) CheckDataAvailable(kv.Domain, kv.Step) error { return nil }

func (r *sharedSourceReader) Read(kv.Domain, []byte, uint64) ([]byte, kv.Step, error) {
	if !r.reading.CompareAndSwap(false, true) {
		r.overlaps.Store(true)
	}
	runtime.Gosched()
	r.tx.scratch = append(r.tx.scratch[:0], r.tx.val...)
	r.reading.Store(false)
	return r.tx.scratch, 0, nil
}

func (r *sharedSourceReader) Clone(kv.TemporalTx) StateReader { return r }

func (r *sharedSourceReader) CloneForWorker(_ context.Context, tx kv.TemporalTx) StateReader {
	if !r.binds {
		return r
	}
	return &sharedSourceReader{tx: tx.(*snapshotTx), binds: true}
}

func (r *sharedSourceReader) BindsWorkerTx() bool { return r.binds }

// A custom reader whose clones keep one tx has to be serialized even when the
// worker views have not drifted: the workers would otherwise share it unlocked.
func TestConcurrentTrieContextSerializesSharedCustomReader(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 7, val: []byte("worker-view")}
	sdc, _ := newSnapshotSdc(t)
	reader := &sharedSourceReader{tx: caller}
	sdc.SetStateReader(reader)

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
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

	require.False(t, reader.overlaps.Load(), "workers must not read the shared custom reader concurrently")
	for i := range workers {
		require.NoError(t, errs[i])
		require.Equal(t, []byte("caller-snapshot"), vals[i])
	}
}

func TestConcurrentTrieContextSkipsWorkerTxForSharedReader(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 7, val: []byte("worker-view")}
	sdc, _ := newSnapshotSdc(t)
	sdc.SetStateReader(&sharedSourceReader{tx: caller})

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
	defer func() {
		for _, c := range drain() {
			c.Close()
		}
	}()

	for range 8 {
		trieCtx, cleanup := factory(t.Context())
		enc, _, err := trieCtx.Branch([]byte("prefix"))
		require.NoError(t, err)
		require.Equal(t, []byte("caller-snapshot"), enc)
		cleanup()
	}

	require.Zero(t, db.opens.Load(), "a shared-source worker must not open a read view it never reads through")
}

func TestConcurrentTrieContextSkipsPinnedReaderForWriteCaller(t *testing.T) {
	t.Parallel()
	caller := &rwCallerTx{viewID: 7}
	db := &snapshotDB{viewID: 8, val: []byte("committed-head")}
	sdc, shared := newSnapshotSdc(t)

	_, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
	defer func() {
		for _, c := range drain() {
			c.Close()
		}
	}()

	require.Zero(t, shared.getters.Load(), "the pinned reader must not be built when no worker can select it")
}

type rwCallerTx struct {
	kv.TemporalRwTx
	viewID uint64
}

func (t *rwCallerTx) ViewID() uint64 { return t.viewID }

// A custom reader that rebinds to the worker's tx keeps the parallel path: this
// is what exec3's as-of reader does, and serializing it would cost the fold its
// concurrency for nothing.
func TestConcurrentTrieContextKeepsBindingCustomReaderPerWorker(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 7, val: []byte("worker-view")}
	sdc, _ := newSnapshotSdc(t)
	sdc.SetStateReader(&sharedSourceReader{tx: caller, binds: true})

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
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

// Every worker asking the shared caller tx whether its own view drifted is the
// same txn-level race the pinned reader exists to avoid: the caller's view is
// resolved once, before any worker runs.
func TestConcurrentTrieContextResolvesCallerViewOnce(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 8, val: []byte("committed-head")}
	sdc, _ := newSnapshotSdc(t)

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
	require.Equal(t, int64(1), caller.viewCalls.Load(), "caller view must be resolved before any worker runs")

	const workers = 8
	var wg sync.WaitGroup
	cleanups := make([]func(), workers)
	encs := make([][]byte, workers)
	errs := make([]error, workers)
	for i := range workers {
		wg.Go(func() {
			trieCtx, cleanup := factory(t.Context())
			cleanups[i] = cleanup
			encs[i], _, errs[i] = trieCtx.Branch([]byte("prefix"))
		})
	}
	wg.Wait()
	for _, cleanup := range cleanups {
		cleanup()
	}
	for _, c := range drain() {
		c.Close()
	}

	for i := range workers {
		require.NoError(t, errs[i])
		require.Equal(t, []byte("caller-snapshot"), encs[i])
	}
	require.Equal(t, int64(1), caller.viewCalls.Load(), "workers must not ask the caller tx for its view")
}

func TestConcurrentTrieContextSerializesSharedReaderWithoutCallerTx(t *testing.T) {
	t.Parallel()
	src := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 8, val: []byte("committed-head")}
	sdc, _ := newSnapshotSdc(t)
	reader := &sharedSourceReader{tx: src}
	sdc.SetStateReader(reader)

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, nil, 0)
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

	require.False(t, reader.overlaps.Load(),
		"a shared-source reader is serialized by a lock that needs no caller tx, so a nil caller must not drop it")
	for i := range workers {
		require.NoError(t, errs[i])
		require.Equal(t, []byte("caller-snapshot"), vals[i])
	}
	require.Zero(t, db.opens.Load(), "a shared-source worker must not open a read view it never reads through")
}

func TestConcurrentTrieContextSkipsPutterWithoutWorkerTx(t *testing.T) {
	t.Parallel()
	caller := &snapshotTx{viewID: 7, val: []byte("caller-snapshot")}
	db := &snapshotDB{viewID: 7, val: []byte("worker-view")}
	sdc, shared := newSnapshotSdc(t)
	sdc.SetStateReader(&sharedSourceReader{tx: caller})

	factory, drain := sdc.concurrentTrieContextFactory(t.Context(), db, nil, caller, 0)
	trieCtx, cleanup := factory(t.Context())
	defer func() {
		cleanup()
		for _, c := range drain() {
			c.Close()
		}
	}()

	require.Zero(t, shared.putDels.Load(), "a worker with no read tx must not bind a putter to a nil one")
	require.NoError(t, trieCtx.PutBranch([]byte("prefix"), []byte("data"), nil))
}
