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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
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
				type result struct {
					txns []types.Transaction
					err  error
				}
				done := make(chan result, 1)
				go func() {
					txns, err := p.ProvideTxns(ctx, txnprovider.WithParentBlockNum(1), txnprovider.WithParentBlockWaitStop(stop))
					done <- result{txns, err}
				}()
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

func TestProvideTxnsReturnsWhileTheStartupReloadHoldsTheLock(t *testing.T) {
	p := new(TxPool)
	require.False(t, p.Started())

	p.lock.Lock()
	t.Cleanup(p.lock.Unlock)

	type provided struct {
		txns []types.Transaction
		err  error
	}
	returned := make(chan provided, 1)
	go func() {
		txns, err := p.ProvideTxns(t.Context(), txnprovider.WithParentBlockNum(1))
		returned <- provided{txns, err}
	}()

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
