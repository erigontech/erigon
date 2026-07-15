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

	"github.com/erigontech/erigon/common"
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
	blockHash            common.Hash // the FCU head this generation's state is for
	blockNum             uint64
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
	// Hold the foreground semaphore for the whole flush+commit+prune, not just
	// wait for idle: prune physically collates/removes domain history, and a
	// foreground FCU acquiring mid-prune could run a reorg unwind against
	// half-pruned state and re-execute to a wrong (empty) root.
	if err := e.fgAcquire(e.bacgroundCtx); err != nil {
		e.markGenCommitted(gen)
		return
	}
	defer e.fgRelease()
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
	e.uncommittedGens--
	if e.uncommittedGens == 0 {
		e.fgIdle.Broadcast()
	}
	e.fgMu.Unlock()
}

// WaitCommitsDrained blocks until every enqueued background commit has landed;
// unlike WaitIdle it leaves the commit worker running, so it is repeatable.
func (e *ExecModule) WaitCommitsDrained() {
	e.fgMu.Lock()
	for e.uncommittedGens > 0 {
		e.fgIdle.Wait()
	}
	e.fgMu.Unlock()
}

// maxInFlightCommits bounds the not-yet-committed generation chain. Each
// in-flight generation adds a level to the block-overlay parent chain that
// foreground reads walk, so an unbounded chain degrades every read; the bound
// also forces a foreground-idle window (via commitBacklogFull → Busy) so the
// commit worker can never be starved by back-to-back FCUs.
const maxInFlightCommits = 4

// commitBacklogFull reports whether the not-yet-committed generation chain has
// reached maxInFlightCommits.
func (e *ExecModule) commitBacklogFull() bool {
	e.fgMu.Lock()
	defer e.fgMu.Unlock()
	return e.uncommittedGens >= maxInFlightCommits
}

// latestGen returns the newest not-yet-committed generation to chain a new
// SharedDomains onto — a committed generation's state is already in the raw DB,
// so chaining to it adds nothing and masks direct-DB reads. The single FIFO
// commit worker commits in enqueue order, so a committed newest generation
// means all are committed: read straight from the DB.
func (e *ExecModule) latestGen() *execctx.SharedDomains {
	e.fgMu.Lock()
	defer e.fgMu.Unlock()
	if len(e.gens) == 0 {
		return nil
	}
	last := e.gens[len(e.gens)-1]
	if last.committed {
		return nil
	}
	return last.sd
}

// beginCoordinatedRo opens a base RO tx under fgMu so its committed snapshot
// reflects every generation markGenCommitted has recorded — markGenCommitted runs
// under fgMu strictly after the DB commit, so a datum dropped from the parent
// chain as committed is guaranteed visible in this tx. Handed to SharedDomains as
// their read coordinator; exec/validation readers open through it instead of an
// ad-hoc BeginTemporalRo that could straddle a background commit.
func (e *ExecModule) beginCoordinatedRo(ctx context.Context) (kv.TemporalTx, error) {
	e.fgMu.Lock()
	defer e.fgMu.Unlock()
	return e.db.BeginTemporalRo(ctx)
}

// addGen appends a new in-flight generation to the chain.
func (e *ExecModule) addGen(gen *commitGen) {
	e.fgMu.Lock()
	e.gens = append(e.gens, gen)
	e.uncommittedGens++
	e.fgMu.Unlock()
}

// drainCommittedGens is intentionally a no-op for now (model A).
//
// Each generation's SharedDomains.mem is kept after commit (FlushKeepMem) so
// the generation stays a stable, self-contained snapshot — it may still be
// Events.LatestSD, or a parent in some reader's chain. Freeing one mid-run
// would need to prove no consumer (block builder, RPC cache, txpool) is still
// reading it; LatestSD hands out a bare pointer, so that proof needs a
// refcount / scoped-read primitive we have not built yet. Until then,
// generations are released as a unit at shutdown by closeAllGens.
//
// Precise per-generation freeing — a refcounted/scoped-read lifetime, or a
// shallow COW snapshot decoupled from the generation — is the explicit
// memory-coordination follow-up (next optimization phase). The call site is
// kept as the documented hook for that work.
func (e *ExecModule) drainCommittedGens() {
	// no-op — see closeAllGens and the memory-coordination follow-up
}

// closeAllGens detaches + closes every remaining generation. Called only at
// shutdown (after the commit worker has stopped) to release the generations
// that drainCommittedGens deliberately keeps alive for the lifetime of the run.
func (e *ExecModule) closeAllGens() {
	e.fgMu.Lock()
	defer e.fgMu.Unlock()
	for _, g := range e.gens {
		g.sd.SetParent(nil)
		g.sd.Close()
		// Roll back the generation's read tx: committed gens already had it
		// rolled back in runCommit (Rollback is idempotent), but a never-committed
		// gen would otherwise leak an open MDBX reader and wedge DB.Close.
		if g.roTx != nil {
			g.roTx.Rollback()
		}
	}
	e.gens = nil
	e.uncommittedGens = 0
}
