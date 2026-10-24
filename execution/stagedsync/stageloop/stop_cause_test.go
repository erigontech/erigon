package stageloop

import (
	"context"
	"errors"
	"testing"
)

// A round the caller STOPPED is not a block that would not validate.
//
// The state step reports "more work" both when it genuinely could not finish and when the caller
// cancelled it mid-way. Only the first is a fault. The pre-exec valve cancels with its own reason
// the moment a seal is wanted — not a deadline — so matching DeadlineExceeded alone missed every
// stop the driver actually makes: 150 drain pre-exec and 130 pre-seal failures in one soak pass,
// all reported as "unexpected state step has more work", with nothing mined behind them.
//
// This pins the rule the fix turns on: context.Cause is nil until the context is done, so a
// non-nil cause means someone asked for the stop, whatever the reason was.
func TestOnlyAnUncancelledContextIsASurprise(t *testing.T) {
	stopped := errors.New("pre-exec round stopped (seal wanted)")

	for _, c := range []struct {
		name     string
		ctx      func() (context.Context, func())
		wantStop bool
	}{
		{
			name:     "a round nobody stopped",
			ctx:      func() (context.Context, func()) { return context.Background(), func() {} },
			wantStop: false,
		},
		{
			name: "the valve's own reason, which is not a deadline",
			ctx: func() (context.Context, func()) {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(stopped)
				return ctx, func() {}
			},
			wantStop: true,
		},
		{
			name: "a deadline, which is what used to be the only case recognised",
			ctx: func() (context.Context, func()) {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				return ctx, cancel
			},
			wantStop: true,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, done := c.ctx()
			defer done()
			if got := context.Cause(ctx) != nil; got != c.wantStop {
				t.Errorf("treated as a deliberate stop = %v, want %v (cause %v)", got, c.wantStop, context.Cause(ctx))
			}
			// The old rule, kept here to show what it missed.
			if c.wantStop && !errors.Is(context.Cause(ctx), context.DeadlineExceeded) && c.name[0] == 't' {
				t.Logf("the DeadlineExceeded-only rule would have called this unexpected: %v", context.Cause(ctx))
			}
		})
	}
}
