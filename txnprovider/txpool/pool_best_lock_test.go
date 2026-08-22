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
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	mdgas "github.com/erigontech/erigon/execution/protocol/mdgas"
)

func TestBestReleasesTheLockWhenTheCallerGivesUpWaitingForABlock(t *testing.T) {
	lock := &sync.Mutex{}
	p := &TxPool{lock: lock, lastSeenCond: sync.NewCond(lock)}

	// The requested block has not been seen, so the wait loop is entered, and the caller is already
	// gone by the time it checks.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, _, err := p.best(ctx, 1, &TxnsRlp{}, 1, mdgas.FullMdGas{}, mapset.NewSet[[32]byte](), 0)
	require.ErrorIs(t, err, context.Canceled)

	// Returning with the lock still held blocks every later pool operation, including the block
	// updates that would have let this caller through, so nothing recovers on its own.
	require.True(t, p.lock.TryLock(), "best returned holding the pool lock")
	p.lock.Unlock()
}

// waitingBuffer records what the pool logged, so a test can tell when a caller has reached the
// "Waiting for block" trace and is therefore about to park in the condition.
type waitingBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *waitingBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *waitingBuffer) contains(s string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return strings.Contains(w.buf.String(), s)
}

func TestBestReturnsWhenItsCallerGoesAwayWhileWaitingForABlock(t *testing.T) {
	logs := &waitingBuffer{}
	logger := log.New()
	logger.SetHandler(log.StreamHandler(logs, log.LogfmtFormat()))

	lock := &sync.Mutex{}
	p := &TxPool{
		lock:         lock,
		lastSeenCond: sync.NewCond(lock),
		logger:       logger,
		pending:      NewPendingSubPool(PendingSubPool, 1),
		baseFee:      NewSubPool(BaseFeeSubPool, 1),
		queued:       NewSubPool(QueuedSubPool, 1),
	}
	ctx, cancel := context.WithCancel(t.Context())

	returned := make(chan error, 1)
	go func() {
		_, _, err := p.best(ctx, 1, &TxnsRlp{}, 1, mdgas.FullMdGas{}, mapset.NewSet[[32]byte](), 0)
		returned <- err
	}()

	// Cancel only once it is parked, so this exercises the wait rather than the check before it.
	require.Eventually(t, func() bool { return logs.contains("Waiting for block") }, 5*time.Second, time.Millisecond)
	cancel()

	// Nothing else wakes it: no block arrives and the pool is not shutting down. A wait that cannot
	// observe its caller leaves the builder that made this request holding a read view until one of
	// those happens, which on a stalled chain is never.
	select {
	case err := <-returned:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("best never returned; only a new block would have woken it")
	}
}
