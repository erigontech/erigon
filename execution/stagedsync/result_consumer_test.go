package stagedsync

import (
	"context"
	"testing"
)

// The resultStream is the exec loop's fan-out registry: the single owner of
// result delivery to every registered consumer and of the ordered shutdown of
// their channels. These tests pin the fan-out, backpressure, mustDeliver and
// ordered-close semantics lifted out of the old sendResult/closeApplyChannels
// pair so a new consumer inherits them without re-deriving the delicate parts.

func TestResultStream_PublishFansOutToAll(t *testing.T) {
	t.Parallel()
	apply := make(chan applyResult, 1)
	commit := make(chan applyResult, 1)
	s := newResultStream()
	s.register("applyResults", apply)
	s.register("commitResults", commit)

	if err := s.publish(context.Background(), "r", false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := <-apply; got != "r" {
		t.Fatalf("apply sink got %v", got)
	}
	if got := <-commit; got != "r" {
		t.Fatalf("commit sink got %v", got)
	}
}

func TestResultStream_PublishSkipsNilSink(t *testing.T) {
	t.Parallel()
	apply := make(chan applyResult, 1)
	s := newResultStream()
	s.register("applyResults", apply)
	s.register("commitResults", nil) // DiscardCommitment path

	if err := s.publish(context.Background(), "r", false); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := <-apply; got != "r" {
		t.Fatalf("apply sink got %v", got)
	}
}

func TestResultStream_MustDeliverBlocksUntilDrained(t *testing.T) {
	t.Parallel()
	apply := make(chan applyResult) // unbuffered
	commit := make(chan applyResult)
	s := newResultStream()
	s.register("applyResults", apply)
	s.register("commitResults", commit)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { done <- s.publish(ctx, "r", true) }()

	// Both consumers must receive even though the coordination ctx is cancelled:
	// mustDeliver ignores ctx.Done. A cancelled ctx must not short-circuit.
	if got := <-apply; got != "r" {
		t.Fatalf("apply sink got %v", got)
	}
	if got := <-commit; got != "r" {
		t.Fatalf("commit sink got %v", got)
	}
	if err := <-done; err != nil {
		t.Fatalf("mustDeliver publish: %v", err)
	}
}

func TestResultStream_PublishHonoursCtxDoneWhenFull(t *testing.T) {
	t.Parallel()
	apply := make(chan applyResult) // unbuffered, no receiver → full
	s := newResultStream()
	s.register("applyResults", apply)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.publish(ctx, "r", false); err == nil {
		t.Fatal("publish must return ctx.Err() when a sink is full and ctx is done")
	}
}

func TestResultStream_CloseOrderCommitBeforeApply(t *testing.T) {
	t.Parallel()
	apply := make(chan applyResult)
	commit := make(chan applyResult)
	s := newResultStream()
	s.register("applyResults", apply)
	s.register("commitResults", commit)

	order := s.close()
	if len(order) != 2 || order[0] != "commitResults" || order[1] != "applyResults" {
		t.Fatalf("close order must be [commitResults, applyResults]; got %v", order)
	}
	if _, ok := <-commit; ok {
		t.Error("commit channel not closed")
	}
	if _, ok := <-apply; ok {
		t.Error("apply channel not closed")
	}
}

func TestResultStream_SendControlTargetsOneSink(t *testing.T) {
	t.Parallel()
	apply := make(chan applyResult, 1)
	commit := make(chan applyResult, 1)
	s := newResultStream()
	s.register("applyResults", apply)
	s.register("commitResults", commit)

	s.sendControl("commitResults", "ctrl")
	if got := <-commit; got != "ctrl" {
		t.Fatalf("commit sink got %v", got)
	}
	select {
	case got := <-apply:
		t.Fatalf("control message must not reach the apply sink; got %v", got)
	default:
	}
}
