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
	"sync"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/require"

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
