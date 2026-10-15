package stagedsync

import (
	"context"
	"fmt"
)

// A round the CALLER stopped is not a round that failed.
//
// The block builder runs pre-execution in ROUNDS, each one a batch of transactions fed into the block it
// maintains, and it ends a round for reasons that have nothing to do with the transactions in it: the slot
// is closing, or the seal has been asked for. Both arrived here as a cancelled context, which execution
// could only read as "abort" — so every transaction the round had already executed was thrown away with
// the ones it had not, and the work was done again in the next block.
//
// Stopping is the other half of that. The round schedules nothing more, lets what is already executing
// finish, and then ends AS IF THE BLOCK ENDED AT THE LAST TRANSACTION THAT COMPLETED. The apply loop and
// the commitment calculator see an ordinary complete block and drain normally — which is the part a cancel
// can never do, because it skips the calculator and leaves sd.mem inconsistent with the commitment
// boundary.
//
// Gas is NOT this. A block may never exceed its gas limit, so gas is checked BEFORE a transaction goes in.
// The slot is a schedule, and a schedule is cut.

// stopping reports that the caller has stopped this round, so no further task may be dispatched.
func (be *blockExecutor) stopping(pe *parallelExecutor) bool {
	if be.cut {
		return true
	}
	if pe.roundStop == nil {
		return false
	}
	select {
	case <-pe.roundStop:
	default:
		return false
	}
	// Only an ACCUMULATION round may be cut. Its block-end does not run, so what it leaves is the same
	// shape as what any other round leaves — a prefix the next round extends. A round that has to finalize
	// cannot stop part way: the block it would leave claims a body it did not execute.
	return pe.isForkValidation && pe.doms != nil && pe.doms.FlashblockAccumulating()
}

// cutIfStopped shortens a stopped round to the transactions that completed, and reports whether it did.
//
// It only acts once everything dispatched has come back (nothing in progress), so no result can arrive for
// a task that has just been truncated away. The cut is at the last PUBLISHED transaction, which at this
// point is also the last validated one — the publish loop has already drained everything validation
// finished — so the block keeps exactly the prefix whose state reached sd.mem.
func (be *blockExecutor) cutIfStopped(pe *parallelExecutor) bool {
	if be.cut {
		return true
	}
	if !be.stopping(pe) || be.execTasks.inProgressCount() > 0 {
		return false
	}
	k := be.publishTasks.maxComplete()
	// Nothing finished, or everything but the block-end did: neither is a cut worth making. The first has
	// no prefix to keep and the second is a round about to complete on its own.
	if k < 0 || k >= len(be.tasks)-2 {
		return false
	}
	be.tasks = be.tasks[:k+1]
	be.results = be.results[:k+1]
	be.txIncarnations = be.txIncarnations[:k+1]
	be.execFailed = be.execFailed[:k+1]
	be.execAborted = be.execAborted[:k+1]
	be.execTasks.truncate(k + 1)
	be.validateTasks.truncate(k + 1)
	be.publishTasks.truncate(k + 1)
	be.cut = true
	return true
}

// nothingInFlight reports that no block this executor holds has a transaction out with a worker, so no
// result is coming and nothing will drive the cut.
func (pe *parallelExecutor) nothingInFlight() bool {
	pe.RLock()
	defer pe.RUnlock()
	for _, be := range pe.blockExecutors {
		if be.execTasks.inProgressCount() > 0 {
			return false
		}
	}
	return true
}

// errRoundStopped ends the exec loop for a round the caller stopped before it executed anything. It is a
// cancellation, not a failure: the caller named the reason and reports it.
var errRoundStopped = fmt.Errorf("pre-exec round stopped with nothing executed: %w", context.Canceled)
