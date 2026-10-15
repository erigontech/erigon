package execmodule

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A round stops for reasons that are not all about time. The slot ending and the seal arriving are
// different events, and before this they shared one error with the real reason carried alongside in a
// bool — so a third reason would have needed a third bool.
func TestExecutionStopped_CarriesItsReason(t *testing.T) {
	for _, reason := range []StopReason{StopSlotEnd, StopSealWanted, StopLostCommit, StopShutdown} {
		err := error(ExecutionStopped{Reason: reason, Num: 7, Txs: 128})

		got, stopped := StoppedBecause(err)
		require.True(t, stopped)
		require.Equal(t, reason, got)

		// Still found once wrapped, which is how it travels back up through the round.
		wrapped := fmt.Errorf("round failed: %w", err)
		got, stopped = StoppedBecause(wrapped)
		require.True(t, stopped)
		require.Equal(t, reason, got, "the reason must survive wrapping")
	}
}

// A caller that only needs "was it stopped" should not have to enumerate the reasons.
func TestExecutionStopped_MatchesAnyReason(t *testing.T) {
	require.True(t, errors.Is(ExecutionStopped{Reason: StopSealWanted}, ExecutionStopped{}))
	require.True(t, errors.Is(fmt.Errorf("x: %w", ExecutionStopped{Reason: StopSlotEnd}), ExecutionStopped{}))

	// And an unrelated error is not a stop.
	_, stopped := StoppedBecause(errors.New("boom"))
	require.False(t, stopped)
	require.False(t, errors.Is(context.Canceled, ExecutionStopped{}))
}

// The driver owns the clock and cancels with a cause. Execution reads the reason off the context rather
// than interpreting a deadline it does not own.
func TestStopReasonOf_ReadsTheCancelCause(t *testing.T) {
	ctx, stop := context.WithCancelCause(context.Background())
	stop(ExecutionStopped{Reason: StopSealWanted})
	require.Equal(t, StopSealWanted, stopReasonOf(ctx))

	// A plain deadline — no cause — still reads as the slot ending, so a caller that has not been
	// converted does not report every stop as a shutdown.
	dctx, dcancel := context.WithDeadline(context.Background(), timeInThePast())
	defer dcancel()
	<-dctx.Done()
	require.Equal(t, StopSlotEnd, stopReasonOf(dctx))

	// Cancellation with no cause at all is the node going down under the round.
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	require.Equal(t, StopShutdown, stopReasonOf(cctx))
}

// Applied is what survived the stop. It is 0 while a stopped round is discarded whole; it exists so the
// caller can requeue only the remainder once the prefix is committed instead.
func TestExecutionStopped_ReportsWhatSurvived(t *testing.T) {
	s := ExecutionStopped{Reason: StopSlotEnd, Num: 12, Txs: 128, Applied: 93}
	require.Contains(t, s.Error(), "slot end")
	require.Contains(t, s.Error(), "applied=93")
}

func timeInThePast() time.Time { return time.Now().Add(-time.Second) }
