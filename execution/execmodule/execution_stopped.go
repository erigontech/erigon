package execmodule

import (
	"context"
	"errors"
	"fmt"
)

// StopReason says WHY a pre-execution round was stopped. It is the caller's reason, not execution's:
// nothing here is a failure of the block or of the transactions in it.
type StopReason string

const (
	// StopSlotEnd is the ordinary way a loaded block ends. The slot is a schedule, not an invariant —
	// unlike the gas limit, which a block may never exceed and which is therefore checked BEFORE a
	// transaction goes in, never discovered afterwards.
	StopSlotEnd StopReason = "slot end"
	// StopSealWanted is the seal request arriving while a round is still running. The block is wanted
	// now; what this round would have added waits for the next one.
	StopSealWanted StopReason = "seal requested"
	// StopLostCommit is a round that lost the commit claim — its caller already stopped waiting and
	// requeued its transactions, so committing now would put them in the block twice.
	StopLostCommit StopReason = "lost the commit claim"
	// StopShutdown is the node going down under a round.
	StopShutdown StopReason = "shutdown"
)

// ExecutionStopped reports a pre-execution round that the CALLER stopped, and why. The block is left as
// it was before the round began — the round's transactions are out of the maintained body again and its
// staged state was closed, not merged — so the caller may seal immediately. The transactions are the
// caller's to dispose of: they were not executed here.
//
// It replaces a single "abandoned on deadline" error that could only express one reason. There were
// already two — the slot ending and the seal arriving — and the second borrowed the first's error and
// carried its real reason alongside in a bool. A third would have meant a third bool.
//
// Applied is how many of the round's transactions completed before the stop. It is 0 while a stopped
// round is discarded whole; when the prefix is committed instead it is what survived, and the caller
// requeues only the remainder.
type ExecutionStopped struct {
	Reason  StopReason
	Num     uint64 // the block whose round was stopped
	Txs     int    // transactions the round was given
	Applied int    // of those, how many completed and were kept
}

func (e ExecutionStopped) Error() string {
	return fmt.Sprintf("pre-exec round stopped (%s): num=%d txs=%d applied=%d", e.Reason, e.Num, e.Txs, e.Applied)
}

// Is reports that any ExecutionStopped matches a bare ExecutionStopped{} target, so callers that do not
// care which reason can write errors.Is(err, execmodule.ExecutionStopped{}).
func (e ExecutionStopped) Is(target error) bool {
	other, ok := target.(ExecutionStopped)
	return ok && other == ExecutionStopped{}
}

// StoppedBecause reports the reason a round was stopped, and whether it was stopped at all.
func StoppedBecause(err error) (StopReason, bool) {
	var s ExecutionStopped
	if errors.As(err, &s) {
		return s.Reason, true
	}
	return "", false
}

// stopReasonOf reads the reason the caller cancelled a round's context. The driver owns the clock, so a
// round ends by CANCELLATION carrying a cause rather than by a deadline execution has to interpret —
// which is what lets a reason other than time stop a round without inventing a new channel for it.
func stopReasonOf(ctx context.Context) StopReason {
	var s ExecutionStopped
	if errors.As(context.Cause(ctx), &s) {
		return s.Reason
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return StopSlotEnd
	}
	return StopShutdown
}
