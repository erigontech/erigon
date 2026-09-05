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

package execmodule

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/db/state/execctx"
)

// TestEnsureQuiescent_ClearsIdleCachedContext pins that mode-B SetHead
// does not depend on new chain activity to proceed.
//
// InsertBlocks caches its SharedDomains in e.currentContext and reuses it
// across calls, so the field stays set while the module is idle — it is
// not a signal that a stage is running. Only the FCU path clears it. That
// made SetHead's precondition "wait for another block to arrive and be
// forkchoice'd", which a quiet chain never satisfies: cycle 28 sat
// through its whole 5-minute window with exactly one head update, logged
// at the instant the wait began, and failed with "currentContext is still
// set".
//
// Holding the semaphore is what makes clearing it safe: InsertBlocks
// acquires it and the FCU path releases it, so neither can be in flight
// while SetHead holds it, and an idle cached context has no user.
func TestEnsureQuiescent_ClearsIdleCachedContext(t *testing.T) {
	t.Parallel()
	e := &ExecModule{semaphore: semaphore.NewWeighted(1)}

	// A cached context with no stage running — the steady state between
	// blocks, and what every failure sat on.
	e.currentContext = &execctx.SharedDomains{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.NoError(t, e.ensureQuiescent(ctx),
		"an idle cached context must be cleared, not waited on — nothing else will clear it on a quiet chain")

	e.lock.RLock()
	got := e.currentContext
	e.lock.RUnlock()
	require.Nil(t, got, "currentContext must be cleared so the DB reset can proceed")
}

// TestEnsureQuiescent_NoOpWhenAlreadyQuiescent pins the common path: no
// cached context means nothing to do.
func TestEnsureQuiescent_NoOpWhenAlreadyQuiescent(t *testing.T) {
	t.Parallel()
	e := &ExecModule{semaphore: semaphore.NewWeighted(1)}

	require.NoError(t, e.ensureQuiescent(context.Background()))

	e.lock.RLock()
	defer e.lock.RUnlock()
	require.Nil(t, e.currentContext)
}
