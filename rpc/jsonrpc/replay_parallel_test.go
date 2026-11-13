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

package jsonrpc

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// readTxLimitDB opens the first n read txs; past them it acts as a full read-tx semaphore:
// it fails under kv.WithNonBlockingAcquire and blocks until ctx is done otherwise.
type readTxLimitDB struct {
	kv.TemporalRoDB
	left atomic.Int32
}

func newReadTxLimitDB(db kv.TemporalRoDB, n int32) *readTxLimitDB {
	limited := &readTxLimitDB{TemporalRoDB: db}
	limited.left.Store(n)
	return limited
}

func (db *readTxLimitDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	if db.left.Add(-1) < 0 {
		if kv.IsNonBlockingAcquire(ctx) {
			return nil, kv.ErrReadTxLimitExceeded
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return db.TemporalRoDB.BeginTemporalRo(ctx)
}

// readTxLimitCtx is a request ctx without kv.WithNonBlockingAcquire, as on IPC, bounded so
// that a request blocked on the read-tx limit fails instead of hanging.
func readTxLimitCtx(t *testing.T, ctx context.Context) context.Context {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// TestTraceBlockUnderReadTxLimit pins that debug_traceBlock answers with the read txs it
// gets: the request's own one only, or that one and one worker, also when the request
// itself would wait for a read tx.
func TestTraceBlockUnderReadTxLimit(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	trace := func(t *testing.T, api *DebugAPIImpl) string {
		var buf bytes.Buffer
		stream := jsonstream.New(&buf)
		require.NoError(t, api.TraceBlockByNumber(readTxLimitCtx(t, m.Ctx), rpc.BlockNumber(1), withLogCallTracer(), stream))
		require.NoError(t, stream.Flush())
		return buf.String()
	}
	withReplayCPUs(t, 1)
	want := trace(t, newDebugApiForTest(m))

	withReplayCPUs(t, 16)
	for name, readTxs := range map[string]int32{"no worker tx": 1, "one worker tx": 2} {
		t.Run(name, func(t *testing.T) {
			api := newDebugApiForTest(m)
			api.db = newReadTxLimitDB(m.DB, readTxs)
			require.Equal(t, want, trace(t, api))
		})
	}
}

// TestTraceBlockParityUnderReadTxLimit pins the same for trace_block.
func TestTraceBlockParityUnderReadTxLimit(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	want, err := newTraceApiForTest(m).Block(m.Ctx, 1, nil, nil)
	require.NoError(t, err)

	withReplayCPUs(t, 16)
	for name, readTxs := range map[string]int32{"no worker tx": 1, "one worker tx": 2} {
		t.Run(name, func(t *testing.T) {
			api := newTraceApiForTest(m)
			api.kv = newReadTxLimitDB(m.DB, readTxs)
			got, err := api.Block(readTxLimitCtx(t, m.Ctx), 1, nil, nil)
			require.NoError(t, err)
			require.Equal(t, want, got)
		})
	}
}

// TestReplayTxnsParallelRejectsReorgedBlock pins that the workers fail, instead of replaying
// another block's txNums, when a reorg after the caller's read tx reaches their own view.
func TestReplayTxnsParallelRejectsReorgedBlock(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, m.DB.Update(m.Ctx, func(rwTx kv.RwTx) error {
		return rawdb.WriteCanonicalHash(rwTx, common.Hash{0xff}, 1)
	}))

	var replayed atomic.Bool
	r, err := replayTxnsParallel(m.Ctx, m.DB, tx, 1, 0, 4, 4, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(int) error {
				replayed.Store(true)
				return nil
			}, nil
		})
	require.NoError(t, err)
	require.ErrorIs(t, r.Wait(), errBlockNotCanonical)
	require.False(t, replayed.Load())
}

// TestReplayTxnsParallelKeepsBlockAfterReorgAboveIt pins that a reorg above the traced block,
// which leaves the block and its txNums unchanged, does not fail the workers.
func TestReplayTxnsParallelKeepsBlockAfterReorgAboveIt(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	// a canonical marker above block 1, so that the caller's tip is not the traced block
	require.NoError(t, m.DB.Update(m.Ctx, func(rwTx kv.RwTx) error {
		return rawdb.WriteCanonicalHash(rwTx, common.Hash{0xaa}, 2)
	}))
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, m.DB.Update(m.Ctx, func(rwTx kv.RwTx) error {
		return rawdb.WriteCanonicalHash(rwTx, common.Hash{0xbb}, 2)
	}))

	var replayed atomic.Int32
	r, err := replayTxnsParallel(m.Ctx, m.DB, tx, 1, 0, 4, 4, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(int) error {
				replayed.Add(1)
				return nil
			}, nil
		})
	require.NoError(t, err)
	for range 4 {
		r.written()
	}
	require.NoError(t, r.Wait())
	require.EqualValues(t, 4, replayed.Load())
}

// TestReplayTxnsParallelKeepsBlockAfterMarkerPrune pins that a frozen block whose canonical
// marker is pruned after the caller's read tx does not fail the workers: the prune deletes the
// lowest markers only, while a reorg deletes the markers from the block up.
func TestReplayTxnsParallelKeepsBlockAfterMarkerPrune(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	require.NoError(t, m.DB.Update(m.Ctx, func(rwTx kv.RwTx) error {
		return rawdb.WriteCanonicalHash(rwTx, common.Hash{0xaa}, 2)
	}))
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, m.DB.Update(m.Ctx, func(rwTx kv.RwTx) error {
		return rwTx.Delete(kv.HeaderCanonical, hexutil.EncodeTs(1))
	}))

	r, err := replayTxnsParallel(m.Ctx, m.DB, tx, 1, 0, 4, 4, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(int) error { return nil }, nil
		})
	require.NoError(t, err)
	for range 4 {
		r.written()
	}
	require.NoError(t, r.Wait())
}

// TestReplayTxnsParallelRejectsTruncatedBlock pins that the workers fail when a reorg after the
// caller's read tx deletes the block's canonical marker and every marker above it.
func TestReplayTxnsParallelRejectsTruncatedBlock(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, m.DB.Update(m.Ctx, func(rwTx kv.RwTx) error {
		return rawdb.TruncateCanonicalChain(rwTx, 1)
	}))

	r, err := replayTxnsParallel(m.Ctx, m.DB, tx, 1, 0, 4, 4, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(int) error { return nil }, nil
		})
	require.NoError(t, err)
	require.ErrorIs(t, r.Wait(), errBlockNotCanonical)
}

// TestReplayTxnsParallelRejectsReorgBelowBlock pins that the workers fail when a reorg after
// the caller's read tx replaces a block below the traced one, which changes the traced block's hash.
func TestReplayTxnsParallelRejectsReorgBelowBlock(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	writeMarkers := func(markers map[uint64]common.Hash) {
		require.NoError(t, m.DB.Update(m.Ctx, func(rwTx kv.RwTx) error {
			for num, hash := range markers {
				if err := rawdb.WriteCanonicalHash(rwTx, hash, num); err != nil {
					return err
				}
			}
			return nil
		}))
	}
	writeMarkers(map[uint64]common.Hash{2: {0xa2}, 3: {0xa3}})
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, m.DB.Update(m.Ctx, func(rwTx kv.RwTx) error {
		return rawdb.TruncateCanonicalChain(rwTx, 1)
	}))
	writeMarkers(map[uint64]common.Hash{1: {0xb1}, 2: {0xb2}, 3: {0xb3}})

	r, err := replayTxnsParallel(m.Ctx, m.DB, tx, 2, 0, 4, 4, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(int) error { return nil }, nil
		})
	require.NoError(t, err)
	require.ErrorIs(t, r.Wait(), errBlockNotCanonical)
}

// TestReplayTxnsParallelRejectsPrunedHistory pins that the workers fail with ErrPruned when the
// state history in their view starts after the block, as when pruning moves on after the caller's read tx.
func TestReplayTxnsParallelRejectsPrunedHistory(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	var replayed atomic.Bool
	db := historyFloorDB{TemporalRoDB: m.DB, startTxNum: 100}
	r, err := replayTxnsParallel(m.Ctx, db, tx, 1, 5, 4, 4, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(int) error {
				replayed.Store(true)
				return nil
			}, nil
		})
	require.NoError(t, err)
	require.ErrorIs(t, r.Wait(), state.ErrPruned)
	require.False(t, replayed.Load())
}

// TestTraceBlockOnRemoteBlockReaderIsSequential pins that debug_traceBlock and trace_block open
// no worker read tx when block lookups go to a remote backend: a worker's BLOCKHASH would open
// one more backend read tx while it holds one, and could wait for it until the request times out.
func TestTraceBlockOnRemoteBlockReaderIsSequential(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	remoteBase := func() *BaseAPI {
		return NewBaseApi(nil, m.StateCache, rpcdaemonBlockReader(t, m, true), m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	}
	const readTxs = 100
	opened := func(db *readTxLimitDB) int32 { return readTxs - db.left.Load() }

	t.Run("debug_traceBlock", func(t *testing.T) {
		trace := func(t *testing.T, api *DebugAPIImpl) string {
			var buf bytes.Buffer
			stream := jsonstream.New(&buf)
			require.NoError(t, api.TraceBlockByNumber(m.Ctx, rpc.BlockNumber(1), withLogCallTracer(), stream))
			require.NoError(t, stream.Flush())
			return buf.String()
		}
		withReplayCPUs(t, 1)
		want := trace(t, newDebugApiForTest(m))

		withReplayCPUs(t, 16)
		db := newReadTxLimitDB(m.DB, readTxs)
		api := NewPrivateDebugAPI(remoteBase(), db, nil, &rpccfg.DebugApiConfig{})
		require.Equal(t, want, trace(t, api))
		require.EqualValues(t, 1, opened(db))
	})

	t.Run("trace_block", func(t *testing.T) {
		withReplayCPUs(t, 1)
		want, err := newTraceApiForTest(m).Block(m.Ctx, 1, nil, nil)
		require.NoError(t, err)

		withReplayCPUs(t, 16)
		db := newReadTxLimitDB(m.DB, readTxs)
		api := NewTraceAPI(remoteBase(), db, &rpccfg.TraceApiConfig{})
		got, err := api.Block(m.Ctx, 1, nil, nil)
		require.NoError(t, err)
		require.Equal(t, want, got)
		require.EqualValues(t, 1, opened(db))
	})
}

// rpcdaemonBlockReader builds the block reader as a separate rpcdaemon does: its own reader
// with --datadir, a remote one without it, wrapped in a RemoteBackend either way.
func rpcdaemonBlockReader(t *testing.T, m *execmoduletester.ExecModuleTester, remote bool) *rpcservices.RemoteBackend {
	_, conn := rpcdaemontest.CreateTestGrpcConn(t, m)
	client := remoteproto.NewETHBACKENDClient(conn)
	if remote {
		return rpcservices.NewRemoteBackend(client, m.DB, freezeblocks.NewRemoteBlockReader(client))
	}
	return rpcservices.NewRemoteBackend(client, m.DB, m.BlockReader)
}

// TestNewBaseApiDetectsRemoteBlocks pins that only an rpcdaemon without --datadir counts as a
// remote block reader, also behind the RemoteBackend that wraps it.
func TestNewBaseApiDetectsRemoteBlocks(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	newBase := func(reader dbservices.FullBlockReader) *BaseAPI {
		return NewBaseApi(nil, m.StateCache, reader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	}
	require.False(t, newBase(m.BlockReader).remoteBlocks, "embedded")
	require.False(t, newBase(rpcdaemonBlockReader(t, m, false)).remoteBlocks, "rpcdaemon with --datadir")
	require.True(t, newBase(rpcdaemonBlockReader(t, m, true)).remoteBlocks, "rpcdaemon without --datadir")
}

// TestReplayTxnsParallelBoundsBufferedTxns pins that a slow txn holds back every txn a window
// or more after it, so at most a window of results waits to be written.
func TestReplayTxnsParallelBoundsBufferedTxns(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const txnCount, workers, window = 16, 4, 4
	var written atomic.Int64
	var ahead atomic.Bool
	done := make([]chan struct{}, txnCount)
	for i := range done {
		done[i] = make(chan struct{})
	}
	r, err := replayTxnsParallel(m.Ctx, m.DB, tx, 1, 0, txnCount, workers, window,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(txnIndex int) error {
				if int64(txnIndex) >= written.Load()+window {
					ahead.Store(true)
				}
				if txnIndex == 0 {
					time.Sleep(50 * time.Millisecond)
				}
				close(done[txnIndex])
				return nil
			}, nil
		})
	require.NoError(t, err)
	for i := range txnCount {
		<-done[i]
		written.Add(1)
		r.written()
	}
	require.NoError(t, r.Wait())
	require.False(t, ahead.Load())
}

// TestReplayTxnsParallelStopsWaitingWorkersOnError pins that workers held back by the window
// stop when a txn before them fails, instead of waiting for a write that never comes.
func TestReplayTxnsParallelStopsWaitingWorkersOnError(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	const txnCount, workers, window = 16, 4, 4
	errSlowTxn := errors.New("slow txn failed")
	r, err := replayTxnsParallel(m.Ctx, m.DB, tx, 1, 0, txnCount, workers, window,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(txnIndex int) error {
				if txnIndex == 0 {
					time.Sleep(50 * time.Millisecond)
					return errSlowTxn
				}
				return nil
			}, nil
		})
	require.NoError(t, err)
	waited := make(chan error, 1)
	go func() { waited <- r.Wait() }()
	select {
	case err := <-waited:
		require.ErrorIs(t, err, errSlowTxn)
	case <-time.After(10 * time.Second):
		t.Fatal("workers held back by the window did not stop")
	}
}

// TestReplayTxnsParallelReturnsWorkerPanic pins that a panic in a job fails the request
// with an error instead of killing the process, and that the panic value stays out of it.
func TestReplayTxnsParallelReturnsWorkerPanic(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	r, err := replayTxnsParallel(m.Ctx, m.DB, tx, 1, 0, 4, 2, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			return func(txnIndex int) error {
				if txnIndex == 1 {
					panic("tracer bug")
				}
				return nil
			}, nil
		})
	require.NoError(t, err)
	err = r.Wait()
	require.ErrorIs(t, err, errReplayWorkerPanic)
	require.NotContains(t, err.Error(), "tracer bug")
}

// openTxCountingDB counts the read txs it opened that are not rolled back yet, and fails
// every open after the first n with failErr.
type openTxCountingDB struct {
	kv.TemporalRoDB
	left    atomic.Int32
	open    atomic.Int32
	failErr error
	mu      sync.Mutex
	txs     []*countedTx
}

// rollbackLeaked rolls back the txs left open, so a failing test can still close the db.
func (db *openTxCountingDB) rollbackLeaked() {
	db.mu.Lock()
	defer db.mu.Unlock()
	for _, tx := range db.txs {
		tx.Rollback()
	}
}

type countedTx struct {
	kv.TemporalTx
	db   *openTxCountingDB
	once sync.Once
}

func (tx *countedTx) Rollback() {
	tx.once.Do(func() {
		tx.db.open.Add(-1)
		tx.TemporalTx.Rollback()
	})
}

func (db *openTxCountingDB) BeginTemporalRo(ctx context.Context) (kv.TemporalTx, error) {
	if db.left.Add(-1) < 0 {
		return nil, db.failErr
	}
	tx, err := db.TemporalRoDB.BeginTemporalRo(ctx) //nolint:gocritic // the caller owns and rolls back the tx
	if err != nil {
		return nil, err
	}
	db.open.Add(1)
	counted := &countedTx{TemporalTx: tx, db: db}
	db.mu.Lock()
	db.txs = append(db.txs, counted)
	db.mu.Unlock()
	return counted, nil
}

// TestReplayTxnsParallelRollsBackOnOpenError pins that an open error other than the read-tx
// limit fails the replay and rolls back the read txs already opened.
func TestReplayTxnsParallelRollsBackOnOpenError(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	errOpen := errors.New("open failed")
	db := &openTxCountingDB{TemporalRoDB: m.DB, failErr: errOpen}
	db.left.Store(2)
	t.Cleanup(db.rollbackLeaked)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	_, err = replayTxnsParallel(m.Ctx, db, tx, 1, 0, 4, 4, 4,
		func(context.Context, kv.TemporalTx) (func(int) error, error) {
			t.Fatal("no worker may start")
			return nil, nil
		})
	require.ErrorIs(t, err, errOpen)
	require.Zero(t, db.open.Load())
}
