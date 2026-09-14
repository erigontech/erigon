package execmodule

import (
	"context"
	"sync"
	"time"
)

// RoundContext carries a per-round cancellation into the validation stage pipeline.
//
// The stages are built ONCE — StateStages bakes the node's context into every stage's Forward closure —
// and Sync.RunNoInterrupt takes no context at all. So a context handed to ValidateBlock reaches
// StateStep's own bookkeeping and stops there: execution never sees it. A pre-exec round given a deadline
// therefore ran to completion regardless, and since the seal waits behind the round, the budget was missed
// anyway. The deadline was advice, and the valve that depends on it was decoration.
//
// This is the one thing the already-built pipeline holds that can be re-pointed. Execution selects on the
// Done channel it gets from here, and each round points it at that round's deadline for the round's
// duration, then back at the node's context. Values still resolve against the base context so anything
// scoped to the node (loggers, tracers) is unaffected by what a round happens to be doing.
//
// Only one round runs at a time — the exec semaphore is held across the whole of it — so the swap has no
// window in which two rounds could disagree about which deadline is current.
type RoundContext struct {
	base context.Context
	mu   sync.RWMutex
	cur  context.Context
}

func NewRoundContext(base context.Context) *RoundContext {
	return &RoundContext{base: base, cur: base}
}

// Enter points the pipeline at ctx and returns the function that points it back. Call it around the work
// that must observe ctx — the restore has to run before the next round enters.
func (r *RoundContext) Enter(ctx context.Context) func() {
	if r == nil || ctx == nil {
		return func() {}
	}
	r.mu.Lock()
	r.cur = ctx
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		r.cur = r.base
		r.mu.Unlock()
	}
}

func (r *RoundContext) current() context.Context {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.cur
}

func (r *RoundContext) Deadline() (time.Time, bool) { return r.current().Deadline() }
func (r *RoundContext) Done() <-chan struct{}       { return r.current().Done() }
func (r *RoundContext) Err() error                  { return r.current().Err() }

// Value answers from the BASE context: values are node-scoped, and a round's deadline context carries none
// of its own. Reading them through the swap would make an unrelated lookup depend on round timing.
func (r *RoundContext) Value(key any) any { return r.base.Value(key) }
