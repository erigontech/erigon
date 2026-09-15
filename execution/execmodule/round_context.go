package execmodule

import (
	"context"
	"sync"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/exec"
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

// Current is the context the round in progress runs under. Anything derived for the length of an execution must
// derive from THIS, not from the RoundContext: the context package watches a custom parent's Done channel and then
// asks the parent for its Err, and by then the RoundContext may already point back at the node's context — Err nil
// on a closed Done, which it treats as an internal error and panics. An execution cut at its deadline returns
// before its shutdown has finished, so that swap-back does come first.
func (r *RoundContext) Current() context.Context { return r.current() }

func (r *RoundContext) Deadline() (time.Time, bool) { return r.current().Deadline() }
func (r *RoundContext) Done() <-chan struct{}       { return r.current().Done() }
func (r *RoundContext) Err() error                  { return r.current().Err() }

// Value answers from the BASE context: values are node-scoped, and a round's deadline context carries none
// of its own. Reading them through the swap would make an unrelated lookup depend on round timing.
func (r *RoundContext) Value(key any) any { return r.base.Value(key) }

type roundCommitKey struct{}

// WithRoundCommit attaches a one-shot claim a pre-exec round must win before it commits.
//
// The caller runs the round on its own goroutine and may stop waiting for it at a deadline, requeueing
// its transactions. Without a single decision the round could still merge after that — its transactions
// committed to the block AND back in the backlog. So commit and abandon go through one claim: the round
// takes it immediately before merging, the caller takes it when it gives up, and whoever is first
// decides. A round that loses closes its staged state and reports ErrRoundAbandoned.
func WithRoundCommit(ctx context.Context, claim func() bool) context.Context {
	return context.WithValue(ctx, roundCommitKey{}, claim)
}

// WithTxExecuted attaches a callback a pre-exec round calls as each of its transactions finishes executing. See
// exec.WithTxExecuted.
func WithTxExecuted(ctx context.Context, done func(common.Hash)) context.Context {
	return exec.WithTxExecuted(ctx, done)
}

func roundCommitClaim(ctx context.Context) func() bool {
	claim, _ := ctx.Value(roundCommitKey{}).(func() bool)
	return claim
}
