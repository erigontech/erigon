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
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

func TestPendingPoolRevisionTracksMembershipChanges(t *testing.T) {
	var revision atomic.Uint64
	pool := NewPendingSubPool(PendingSubPool, 2)
	pool.revision = &revision
	txnSlot := newTestTxnSlot(0, 0, 300_000, 300_000, 100_000)
	txnSlot.IDHash[0] = 1
	txn := newMetaTxn(txnSlot, true, 0)

	finish := pool.trackChanges()
	pool.Add(txn, log.New())
	finish()
	require.Equal(t, uint64(1), revision.Load())

	transientSlot := newTestTxnSlot(1, 0, 300_000, 300_000, 100_000)
	transientSlot.IDHash[0] = 2
	transient := newMetaTxn(transientSlot, true, 0)
	finish = pool.trackChanges()
	pool.Add(transient, log.New())
	pool.Remove(transient, "test", log.New())
	finish()
	require.Equal(t, uint64(1), revision.Load())

	finish = pool.trackChanges()
	pool.Remove(txn, "test", log.New())
	finish()
	require.Equal(t, uint64(2), revision.Load())

	finish = pool.trackChanges()
	pool.Remove(txn, "test", log.New())
	finish()
	require.Equal(t, uint64(2), revision.Load())

	var overflowRevision atomic.Uint64
	overflowPool := NewPendingSubPool(PendingSubPool, 1)
	overflowPool.revision = &overflowRevision
	finish = overflowPool.trackChanges()
	overflowPool.Add(transient, log.New())
	require.Same(t, transient, overflowPool.PopWorst())
	finish()
	require.Zero(t, overflowRevision.Load())
}
