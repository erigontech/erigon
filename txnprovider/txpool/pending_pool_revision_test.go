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
	txn := newMetaTxn(newTestTxnSlot(0, 0, 300_000, 300_000, 100_000), true, 0)

	pool.Add(txn, log.New())
	require.Equal(t, uint64(1), revision.Load())

	pool.Remove(txn, "test", log.New())
	require.Equal(t, uint64(2), revision.Load())

	pool.Remove(txn, "test", log.New())
	require.Equal(t, uint64(2), revision.Load())
}
