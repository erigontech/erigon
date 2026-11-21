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

package txpool

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/txnprovider"
)

func TestBestReleasesTheLockWhenTheCallerGivesUpWaitingForABlock(t *testing.T) {
	p := &TxPool{logger: log.New()}

	// The requested block has not been seen, so the wait loop is entered, and the caller is already
	// gone by the time it checks.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := p.PeekBest(ctx, 1, &TxnsRlp{}, 1)
	require.ErrorIs(t, err, context.Canceled)

	// Returning with the lock still held blocks every later pool operation, including the block
	// updates that would have let this caller through, so nothing recovers on its own.
	require.True(t, p.lock.TryLock(), "best returned holding the pool lock")
	p.lock.Unlock()
}

type provideTxnsResult struct {
	txns []types.Transaction
	err  error
}

func provideTxnsAsync(ctx context.Context, pool *TxPool, opts ...txnprovider.ProvideOption) <-chan provideTxnsResult {
	done := make(chan provideTxnsResult, 1)
	go func() {
		txns, err := pool.ProvideTxns(ctx, opts...)
		done <- provideTxnsResult{txns, err}
	}()
	return done
}

func TestProvideTxnsInterruptsParentBlockWaitWhilePoolIsLocked(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wantErr error
	}{
		{"cancel caller", context.Canceled},
		{"stop parent wait", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// A waiter stuck on a mutex cannot advance synctest's clock.
			timeout := time.After(5 * time.Second)
			synctest.Test(t, func(t *testing.T) {
				_, p, _, _ := newTestPool(t)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				stop := make(chan struct{})
				done := provideTxnsAsync(ctx, p, txnprovider.WithParentBlockNum(1), txnprovider.WithParentBlockWaitStop(stop))
				synctest.Wait()
				p.lock.Lock()
				defer p.lock.Unlock()
				if tc.wantErr != nil {
					cancel()
				} else {
					close(stop)
				}
				select {
				case got := <-done:
					require.Empty(t, got.txns)
					require.ErrorIs(t, got.err, tc.wantErr)
				case <-timeout:
					t.Fatal("interrupted parent block wait needed the pool lock to return")
				}
			})
		})
	}
}

func TestPeekBestWakesAtRequestedBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, pool, _, _ := newTestPool(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		type result struct {
			height uint64
			onTime bool
			err    error
		}
		done := make(chan result, 2)
		for _, height := range []uint64{1, 2} {
			go func() {
				onTime, err := pool.PeekBest(ctx, 0, &TxnsRlp{}, height)
				done <- result{height, onTime, err}
			}()
		}
		synctest.Wait()
		for _, height := range []uint64{1, 2} {
			change := newBlockChangeForAccounts(common.Hash{}, 0)
			change.ChangeBatch[0].BlockHeight = height
			require.NoError(t, pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{}))
			synctest.Wait()
			require.Len(t, done, 1)
			require.Equal(t, result{height: height, onTime: true}, <-done)
		}
	})
}

func TestProvideTxnsRechecksParentAfterUnwind(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, pool, _, _ := newTestPool(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		readers := semaphore.NewWeighted(1)
		opts := mdbx.New(dbcfg.TxPoolDB, log.New()).RoTxsLimiter(readers)
		pool.poolDB = mdbxtest.InMem(t, opts, t.TempDir()).MustOpen()
		t.Cleanup(pool.poolDB.Close)

		sender := common.Address{1}
		update := func(height uint64, direction remoteproto.Direction) error {
			change := newBlockChangeForAccounts(common.Hash{byte(height)}, 0, sender)
			change.ChangeBatch[0].BlockHeight = height
			change.ChangeBatch[0].Direction = direction
			return pool.OnNewBlock(ctx, change, TxnSlots{}, TxnSlots{}, TxnSlots{})
		}
		require.NoError(t, update(1, remoteproto.Direction_FORWARD))
		var slots TxnSlots
		slots.Append(newTestSenderTxnSlot(sender, 0), sender[:], true)
		_, err := pool.AddLocalTxns(ctx, slots)
		require.NoError(t, err)

		// Queue the unwind first so it takes effect after both callers see the parent
		// and before either caller can open its database snapshot.
		require.NoError(t, readers.Acquire(ctx, 1))
		unwound := make(chan error, 1)
		go func() { unwound <- update(0, remoteproto.Direction_UNWIND) }()
		synctest.Wait()
		stop := make(chan struct{})
		stopped := provideTxnsAsync(ctx, pool, txnprovider.WithParentBlockNum(1), txnprovider.WithParentBlockWaitStop(stop))
		waiting := provideTxnsAsync(ctx, pool, txnprovider.WithParentBlockNum(1))
		synctest.Wait()
		readers.Release(1)
		require.NoError(t, <-unwound)
		synctest.Wait()
		close(stop)
		got := <-stopped
		require.NoError(t, got.err)
		require.Empty(t, got.txns, "stopped builder selected transactions below its requested parent")
		require.Empty(t, waiting, "selection returned before the pool reached the parent again")
		require.True(t, readers.TryAcquire(1), "waiting for the parent held a database reader")
		readers.Release(1)

		require.NoError(t, update(1, remoteproto.Direction_FORWARD))
		got = <-waiting
		require.NoError(t, got.err)
		require.Len(t, got.txns, 1)
	})
}

func TestProvideTxnsReturnsWhileTheStartupReloadHoldsTheLock(t *testing.T) {
	p := new(TxPool)
	require.False(t, p.Started())

	p.lock.Lock()
	t.Cleanup(p.lock.Unlock)

	returned := provideTxnsAsync(t.Context(), p, txnprovider.WithParentBlockNum(1))

	select {
	case got := <-returned:
		require.NoError(t, got.err)
		require.Empty(t, got.txns)
	case <-time.After(5 * time.Second):
		t.Fatal("ProvideTxns waited for the lock that the startup reload holds")
	}
}

func TestProvideTxnsReportsACancelledCallerBeforeStartup(t *testing.T) {
	p := new(TxPool)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err := p.ProvideTxns(ctx, txnprovider.WithParentBlockNum(1))
	require.ErrorIs(t, err, context.Canceled)
}

func TestProvideTxnsReportsCancellationWhenTheParentBlockWaitIsAlsoStopped(t *testing.T) {
	p := &TxPool{logger: log.New()}
	p.started.Store(true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	stop := make(chan struct{})
	close(stop)

	for range 100 {
		_, err := p.ProvideTxns(ctx, txnprovider.WithParentBlockNum(1), txnprovider.WithParentBlockWaitStop(stop))
		require.ErrorIs(t, err, context.Canceled)
	}
}
