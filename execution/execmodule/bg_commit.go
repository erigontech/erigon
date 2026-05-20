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
//
// On commitWorkerStop it drains any still-queued generations (so a shutdown
// never drops a not-yet-landed commit) and exits; commitWg lets WaitIdle
// block until the worker — and its in-flight commit tx — are done before
// DB-close.
func (e *ExecModule) commitWorker() {
	defer e.commitWg.Done()
	for {
		select {
		case gen := <-e.commitCh:
			e.runCommit(gen)
		case <-e.commitWorkerStop:
			for {
				select {
				case gen := <-e.commitCh:
					e.runCommit(gen)
				default:
					return
				}
			}
		}
	}
}

// runCommit flushes + commits one generation's delta in a foreground-free
// window. Called only by commitWorker.
func (e *ExecModule) runCommit(gen *commitGen) {
	e.waitForegroundIdle()
	err := e.runPostForkchoice(gen.sd, gen.roTx, gen.finishProgressBefore, gen.isSynced, gen.initialCycle)
	if err != nil && !errors.Is(err, context.Canceled) {
		e.logger.Error("background commit failed", "err", err)
	}
	// roTx is rolled back inside runForkchoiceFlushCommit between Flush and
	// Commit; this is a safety net (Rollback is idempotent).
	gen.roTx.Rollback()
	e.markGenCommitted(gen)
	// NOTE: the worker does NOT touch the published overlay (Events.LatestSD).
	// Publishing the latest executed block's SharedDomains is a foreground
	// concern, owned by updateForkChoice (dispatchNotificationsFromOverlay).
	// A background commit landing does not change "what the latest block is",
	// so it must not republish — doing so previously nil'd LatestSD whenever
	// the worker caught up, leaving the block builder / RPC caches blind.
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

// drainCommittedGens retires fully-committed generations, but ALWAYS keeps
// the newest one alive. The newest generation's SharedDomains is what the
// foreground published as Events.LatestSD — the latest executed block's
// state — and the block builder + RPC caches read through it until a newer
// block supersedes it. Retiring it here would make LatestSD point at a
// closed SD (the root cause of the builder reading a stale DB).
//
// Older generations are detached + closed only once EVERY generation has
// committed: each closed generation's delta is durably in the DB, and the
// kept newest generation reads its own committed delta via the stateCache /
// files, so detaching the parent chain loses nothing. Called by a foreground
// op holding the semaphore, so closing is race-free.
func (e *ExecModule) drainCommittedGens() {
	e.fgMu.Lock()
	defer e.fgMu.Unlock()
	if len(e.gens) <= 1 {
		return // nothing to retire — keep the (at most one) newest generation
	}
	for _, g := range e.gens {
		if !g.committed {
			return // a commit is still in flight — leave the chain intact
		}
	}
	newest := e.gens[len(e.gens)-1]
	old := e.gens[:len(e.gens)-1]
	e.gens = []*commitGen{newest}
	newest.sd.SetParent(nil)
	for _, g := range old {
		g.sd.SetParent(nil)
		g.sd.Close()
	}
}

// closeAllGens detaches + closes every remaining generation. Called only at
// shutdown (after the commit worker has stopped) to release the newest
// generation that drainCommittedGens deliberately keeps alive.
func (e *ExecModule) closeAllGens() {
	e.fgMu.Lock()
	defer e.fgMu.Unlock()
	for _, g := range e.gens {
		g.sd.SetParent(nil)
		g.sd.Close()
	}
	e.gens = nil
}
