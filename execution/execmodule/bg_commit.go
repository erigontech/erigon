// Copyright 2024 The Erigon Authors
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
	"errors"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/state/execctx"
)

// Background commit / foreground-priority coordination (gate item 2).
//
// Base intent: foreground execution (FCU / ValidateChain / InsertBlocks /
// AssembleBlock / SetHead) is NEVER blocked on the DB. The post-FCU
// flush+commit+prune runs on a single background goroutine that yields to
// foreground: a commit starts only in a foreground-free window, so the
// commit RwTx never overlaps a foreground roTx and never pins MDBX pages
// (which would grow the freelist / DB file). An in-flight commit is never
// aborted — a foreground op that arrives mid-commit simply makes the *next*
// commit wait. See project_fcu_semaphore_decouple_plan.

// commitGen is one in-flight background-commit unit: the SharedDomains of a
// completed FCU whose domain state must be flushed + committed off the
// foreground path.
type commitGen struct {
	sd                   *execctx.SharedDomains
	roTx                 kv.TemporalTx
	finishProgressBefore uint64
	isSynced             bool
	initialCycle         bool
	committed            bool // guarded by ExecModule.fgMu
}

// fgTryAcquire acquires the foreground semaphore non-blocking and, on
// success, registers the caller as an active foreground op. Pair with
// fgRelease. Replaces direct e.semaphore.TryAcquire so the foreground
// counter and the semaphore can never drift.
func (e *ExecModule) fgTryAcquire() bool {
	if !e.semaphore.TryAcquire(1) {
		return false
	}
	e.enterForeground()
	return true
}

// fgAcquire is the blocking counterpart of fgTryAcquire.
func (e *ExecModule) fgAcquire(ctx context.Context) error {
	if err := e.semaphore.Acquire(ctx, 1); err != nil {
		return err
	}
	e.enterForeground()
	return nil
}

// fgRelease releases the foreground semaphore and deregisters the foreground
// op, waking the commit worker if this was the last one.
func (e *ExecModule) fgRelease() {
	e.leaveForeground()
	e.semaphore.Release(1)
}

// enterForeground marks a foreground operation as active. The background
// commit worker will not start a commit while any foreground op is active.
func (e *ExecModule) enterForeground() {
	e.fgMu.Lock()
	e.fgCount++
	e.fgMu.Unlock()
}

// leaveForeground marks a foreground operation as finished, waking the
// commit worker when the last one leaves.
func (e *ExecModule) leaveForeground() {
	e.fgMu.Lock()
	e.fgCount--
	if e.fgCount == 0 {
		e.fgIdle.Broadcast()
	}
	e.fgMu.Unlock()
}

// waitForegroundIdle blocks until no foreground operation is active. The
// commit worker calls it before each commit so the commit RwTx runs in a
// foreground-free window — no foreground roTx open to pin MDBX pages.
func (e *ExecModule) waitForegroundIdle() {
	e.fgMu.Lock()
	for e.fgCount > 0 {
		e.fgIdle.Wait()
	}
	e.fgMu.Unlock()
}

// enqueueCommit hands a completed generation to the background commit
// worker. Non-blocking by design (buffered channel) so the foreground FCU
// is never blocked handing off its commit.
func (e *ExecModule) enqueueCommit(gen *commitGen) {
	e.commitCh <- gen
}

// commitWorker is the single FIFO background-commit goroutine. It pulls
// completed generations and commits each one's domain delta — but only once
// foreground execution is idle, so the commit never overlaps a foreground
// roTx. An in-flight commit is never aborted; a foreground op that arrives
// mid-commit makes the next iteration wait.
func (e *ExecModule) commitWorker() {
	for gen := range e.commitCh {
		e.waitForegroundIdle()
		err := e.runPostForkchoice(gen.sd, gen.roTx, gen.finishProgressBefore, gen.isSynced, gen.initialCycle)
		if err != nil && !errors.Is(err, context.Canceled) {
			e.logger.Error("background commit failed", "err", err)
		}
		// roTx is rolled back inside runForkchoiceFlushCommit between Flush
		// and Commit; this is a safety net (Rollback is idempotent).
		gen.roTx.Rollback()
		e.markGenCommitted(gen)
		if dispatcher := e.pipelineExecutor.Dispatcher(); dispatcher != nil {
			dispatcher.PublishOverlay(nil)
		}
	}
}

// markGenCommitted records that a generation's commit has landed. The
// generation's SharedDomains is NOT closed here — a foreground op may still
// hold it via the parent chain. It is closed as a unit by drainCommittedGens
// once the whole chain is committed.
func (e *ExecModule) markGenCommitted(gen *commitGen) {
	e.fgMu.Lock()
	gen.committed = true
	e.fgMu.Unlock()
}

// latestGen returns the newest in-flight generation — the parent a new
// foreground SharedDomains chains to so its reads see not-yet-committed
// domain state. nil when the chain is empty.
func (e *ExecModule) latestGen() *execctx.SharedDomains {
	e.fgMu.Lock()
	defer e.fgMu.Unlock()
	if len(e.gens) == 0 {
		return nil
	}
	return e.gens[len(e.gens)-1].sd
}

// addGen appends a new in-flight generation to the chain.
func (e *ExecModule) addGen(gen *commitGen) {
	e.fgMu.Lock()
	e.gens = append(e.gens, gen)
	e.fgMu.Unlock()
}

// drainCommittedGens closes and clears the generation chain when every
// generation in it has committed. Called by a foreground op while it holds
// the semaphore — no concurrent reader exists, so closing the SharedDomains
// is race-free. If any generation is still uncommitted the chain is left
// intact. Returns true when the chain was drained.
func (e *ExecModule) drainCommittedGens() bool {
	e.fgMu.Lock()
	defer e.fgMu.Unlock()
	if len(e.gens) == 0 {
		return true
	}
	for _, g := range e.gens {
		if !g.committed {
			return false
		}
	}
	gens := e.gens
	e.gens = nil
	// Detach + close oldest→newest; each generation's data is in the DB,
	// so a reader that still walked the chain would get the same bytes.
	for _, g := range gens {
		g.sd.SetParent(nil)
		g.sd.Close()
	}
	return true
}
