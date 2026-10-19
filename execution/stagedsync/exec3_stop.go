package stagedsync

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
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

// stoppableRound reports that this round may END AT A TRANSACTION rather than at its body.
//
// Only an ACCUMULATION round may: its block-end does not run, so what it leaves is the same shape as
// what any other round leaves — a prefix the next round extends. A round that has to finalize cannot
// stop part way, because the block it would leave claims a body it did not execute.
func (be *blockExecutor) stoppableRound(pe *parallelExecutor) bool {
	return pe.isForkValidation && pe.doms != nil && pe.doms.FlashblockAccumulating()
}

// stopping reports that this round takes no further transaction, so none may be dispatched.
//
// Two reasons, and they end the round the same way while meaning opposite things about the
// transaction at the boundary. The CALLER stopping it (the slot closing, the seal asked for) is a
// schedule running out: the last transaction that completed is good and is kept. GAS-FULL is the
// block running out: the transaction that did not fit is excluded, and the round ends at the one
// before it. Either way nothing new goes out and what is already running is finished.
func (be *blockExecutor) stopping(pe *parallelExecutor) bool {
	if be.cut {
		return true
	}
	if !be.stoppableRound(pe) {
		return false
	}
	if be.gasFull {
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
	return true
}

// cutIfStopped shortens a stopped round to the transactions that completed, and reports whether it did.
//
// The cut is at the last PUBLISHED transaction, which at this point is also the last validated one — the publish loop has already drained everything validation
// finished — so the block keeps exactly the prefix whose state reached sd.mem.
func (be *blockExecutor) cutIfStopped(pe *parallelExecutor) bool {
	if be.cut {
		return true
	}
	if !be.stopping(pe) {
		return false
	}
	// THE CUT DOES NOT WAIT FOR WHAT IS STILL RUNNING. A round ends at the last transaction executed
	// IN ORDER; anything dispatched beyond that is work the block will not carry, so there is nothing
	// to wait for — its result is discarded as it arrives (nextResult).
	//
	// This used to require inProgressCount() == 0, and that wait WAS the stop's cost: measured on
	// live328, a round given 13ms reported back 117ms later, and 14 of 50 stops kept NOTHING while
	// still burning 61-104ms of the slot. The seal then lands past its deadline, the CL retries, the
	// duplicate-build short circuit hands back a PayloadID for a build that never runs, and block
	// production stalls for minutes. A stop must cost at most the transaction already running.
	if be.stopSeen.IsZero() {
		be.stopSeen = time.Now()
		be.stopInFlight = be.execTasks.inProgressCount()
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
	if !be.stopSeen.IsZero() {
		log.Warn("[round-stop] drained",
			"inFlightAtStop", be.stopInFlight,
			"drainMs", time.Since(be.stopSeen).Milliseconds(),
			"kept", k+1, "ofTasks", len(be.tasks))
	}
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

// errRoundGasFull ends a round whose FIRST transaction did not fit the block's remaining gas. There is
// no prefix to keep, so there is nothing to cut to; the block is full and the round keeps nothing.
var errRoundGasFull = fmt.Errorf("pre-exec round found the block gas-full with nothing executed: %w", context.Canceled)
