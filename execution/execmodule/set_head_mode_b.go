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
	"fmt"
	"time"

	"github.com/erigontech/erigon/db/kv"
)

// modeBQuiescenceTimeout bounds how long SetHead's mode B path will
// wait for the ExecModule to become quiescent (no SharedDomains in
// flight). Long enough that any in-flight forward-execution stage
// finishes flushing; short enough that a wedged pipeline surfaces
// rather than hanging forever. See
// docs/plans/20260525-admin-sethead-unwind-design.md.
const modeBQuiescenceTimeout = 120 * time.Second

// modeBQuiescencePoll is the polling interval for the quiescence
// wait. Small enough that the wait usually terminates almost as soon
// as the in-flight SharedDomains is released.
const modeBQuiescencePoll = 50 * time.Millisecond

// setHeadModeB runs the past-diffset admin unwind path. Entered when
// targetBlock < minUnwindableBlock AND the chain is aligned-mode AND
// an Unwinder is wired. Delegates the storage-layer + DB-reset
// sub-ops to the Unwinder, which in production is the
// *storage.Provider adapter.
//
// Caller has already begun the temporal RW tx; this method does not
// commit it (the Unwinder owns the commit for now). The caller
// (SetHead) returns the Unwinder's error verbatim.
func (e *ExecModule) setHeadModeB(ctx context.Context, tx kv.TemporalRwTx, targetBlock, currentHead uint64) error {
	// Tell engine-API consumers (Caplin / external CL) we're SYNCING
	// for the duration of the unwind so they hold off pushing fresh
	// FCU events. The flag is cleared on every exit path — success
	// (after tx.Commit) and every error before it.
	e.adminUnwindInProgress.Store(true)
	defer e.adminUnwindInProgress.Store(false)

	if err := e.waitForQuiescence(ctx); err != nil {
		// Quiescence failure: Unwind never ran, nothing was staged,
		// AbortUnwind is a no-op but called for symmetry.
		e.unwinder.AbortUnwind()
		return fmt.Errorf("SetHead mode B: %w", err)
	}

	// Engine is threaded in so Provider.Unwind can run the partial-block
	// re-exec path internally when a mid-block step cut is detected.
	args := UnwindArgs{Tx: tx, Engine: e.engine}

	if err := e.unwinder.Unwind(ctx, targetBlock, args); err != nil {
		// Unwind staged FS / inventory / network ops up to the point
		// of failure (snapshot-trim runs first, so a failure in
		// ensureCommitmentAtBlock / WipeWritableShadowPast leaves
		// trim staged). Abort drops them — no FS or network mutations.
		e.unwinder.AbortUnwind()
		return fmt.Errorf("SetHead mode B (unwind %d → %d): %w", currentHead, targetBlock, err)
	}

	// Commit the unwind. Without this the outer SetHead's deferred
	// tx.Rollback reverts every DB change Provider.Unwind made —
	// head pointers, TxNums truncation, canonical-hash cleanup,
	// writable-shadow wipe. Mode A's analog commit lives at the end
	// of SetHead (set_head.go:164) but mode B returns from a branch
	// above that, so the commit has to land here.
	if err := tx.Commit(); err != nil {
		// Commit failed: tx fully rolled back. Drop the staged
		// trim ops so the datadir is unchanged and retriable.
		e.unwinder.AbortUnwind()
		return fmt.Errorf("SetHead mode B (unwind %d → %d): commit: %w", currentHead, targetBlock, err)
	}

	// Tx is durable. Execute the deferred FS / inventory / network
	// ops. Errors here are best-effort (chain head has already moved;
	// a stale torrent or undeleted .seg leftover is recoverable, not
	// corrupting). The Unwinder logs internally on partial failure.
	if err := e.unwinder.FinalizeUnwind(); err != nil {
		e.logger.Warn("SetHead mode B: FinalizeUnwind returned an error (tx already committed)", "err", err, "targetBlock", targetBlock)
	}
	return nil
}

// waitForQuiescence blocks until no SharedDomains is in flight or the
// bounded timeout expires. Mode B's precondition: no execution stage
// holds a live SharedDomains pointer that would clash with the DB
// reset.
//
// The caller already holds e.semaphore (acquired at SetHead entry),
// which serializes new SharedDomains creation by InsertBlocks /
// UpdateForkChoice. Any currentContext that survives the semaphore
// acquire is a stage that hasn't yet released; this method polls
// e.currentContext until it clears.
func (e *ExecModule) waitForQuiescence(ctx context.Context) error {
	waitCtx, cancel := context.WithTimeout(ctx, modeBQuiescenceTimeout)
	defer cancel()

	for {
		e.lock.RLock()
		quiescent := e.currentContext == nil
		e.lock.RUnlock()
		if quiescent {
			return nil
		}

		select {
		case <-time.After(modeBQuiescencePoll):
			// poll again
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("execution did not become quiescent within %s (currentContext is still set)", modeBQuiescenceTimeout)
		}
	}
}
