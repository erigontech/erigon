package exec

import (
	"context"
	"time"
)

type outerKey struct{}

// WithOuter marks ctx as a ROUND context — one whose cancellation means STOP, not ABORT — and names the
// context whose cancellation is the abort.
//
// A pre-execution round has two different endings and they need opposite handling. Its caller ending it
// (the slot closing, the seal being asked for) is a SCHEDULING event: what has executed is good, and the
// right response is to schedule nothing more and finish. The node going down is an ABORT: nothing may be
// kept. Both arrived as a cancelled context, so execution could only treat the first as the second and
// throw the round away.
func WithOuter(ctx, outer context.Context) context.Context {
	return context.WithValue(ctx, outerKey{}, outer)
}

// Outer is the context whose cancellation ABORTS. A context that never declared a round is its own outer,
// so a caller that knows nothing of rounds sees no change in behaviour.
func Outer(ctx context.Context) context.Context {
	if o, ok := ctx.Value(outerKey{}).(context.Context); ok && o != nil {
		return o
	}
	return ctx
}

// IsOuterKey reports that key is the one WithOuter attaches under, so a custom context that wants to
// answer it from somewhere other than its usual parent can recognise it. See execmodule.RoundContext.
func IsOuterKey(key any) bool {
	_, ok := key.(outerKey)
	return ok
}

// UntilAbort is ctx's VALUES with the ABORT's lifetime: everything the round attached — its commit claim,
// its per-transaction progress hook — still resolves, but the context ends only when the round is
// abandoned, not when it is merely stopped.
//
// This is what the workers, the apply loop and the commitment calculator run under. They must not be torn
// down by a stop, because a stopped round keeps the prefix it executed and the prefix has to be committed
// like any other block; but they still need to see what the round attached, and a context derived from the
// abort alone carries none of it.
func UntilAbort(ctx context.Context) context.Context {
	outer := Outer(ctx)
	if outer == ctx {
		return ctx
	}
	return roundValues{values: ctx, life: outer}
}

type roundValues struct {
	values context.Context
	life   context.Context
}

func (c roundValues) Deadline() (time.Time, bool) { return c.life.Deadline() }
func (c roundValues) Done() <-chan struct{}       { return c.life.Done() }
func (c roundValues) Err() error                  { return c.life.Err() }
func (c roundValues) Value(key any) any           { return c.values.Value(key) }
