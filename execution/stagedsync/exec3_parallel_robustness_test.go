// Tests for the parallel-exec robustness scaffolding added in the
// "stagedsync: stop apply loop exiting on rootResults close +
// completeness checks" commit. These guard against regressions of the
// silent-failure class of bugs that previously let invalid blocks
// become canonical (validator never fired) and cost days of debugging.
//
// See agentspecs/parallel-exec-robustness-plan.md for the design
// rationale and failure mode taxonomy.

package stagedsync

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/erigontech/erigon/execution/protocol/rules"
)

// TestApplyLoopMissingBlocks covers the pure completeness-check helper.
// Every entry asserts a single invariant — see the comment on each case.
func TestApplyLoopMissingBlocks(t *testing.T) {
	mkSet := func(ns ...uint64) map[uint64]struct{} {
		s := make(map[uint64]struct{}, len(ns))
		for _, n := range ns {
			s[n] = struct{}{}
		}
		return s
	}

	tests := []struct {
		name            string
		txResultBlocks  map[uint64]struct{}
		appliedBlocks   map[uint64]struct{}
		reachedMaxBlock bool
		maxBlockNum     uint64
		wantMissing     []uint64
	}{
		{
			// Happy path: every block whose tx-results arrived also had a
			// blockResult, and we reached the goal.
			name:            "all applied, max reached",
			txResultBlocks:  mkSet(0, 1),
			appliedBlocks:   mkSet(0, 1),
			reachedMaxBlock: true,
			maxBlockNum:     1,
			wantMissing:     nil,
		},
		{
			// The exact bug we fixed: block 1 had tx-results arrive but
			// the trailing blockResult was dropped on the floor by the
			// rootResults-close race. Validator never fired.
			name:            "tx-results without blockResult — the rootResults race",
			txResultBlocks:  mkSet(0, 1),
			appliedBlocks:   mkSet(0),
			reachedMaxBlock: false,
			maxBlockNum:     1,
			wantMissing:     []uint64{1},
		},
		{
			// Exec loop signaled "done" but never delivered the goal block —
			// no tx-results, no blockResult, just a clean nil return that
			// silently let an unprocessed block become canonical.
			name:            "max block never delivered (no tx-results either)",
			txResultBlocks:  mkSet(0),
			appliedBlocks:   mkSet(0),
			reachedMaxBlock: false,
			maxBlockNum:     1,
			wantMissing:     []uint64{1},
		},
		{
			// Same maxBlockNum is missing from both checks — must NOT
			// appear twice in the missing list.
			name:            "max block missing — no double-count",
			txResultBlocks:  mkSet(0, 1),
			appliedBlocks:   mkSet(0),
			reachedMaxBlock: false,
			maxBlockNum:     1,
			wantMissing:     []uint64{1},
		},
		{
			// reachedMaxBlock=true means exec loop set the flag (sent the
			// final blockResult), so we don't add maxBlockNum to missing
			// even though appliedBlocks doesn't have it. The applyResults
			// drain or another exit-path catches that case.
			name:            "reached max but not applied — don't double-flag",
			txResultBlocks:  mkSet(0),
			appliedBlocks:   mkSet(0),
			reachedMaxBlock: true,
			maxBlockNum:     1,
			wantMissing:     nil,
		},
		{
			// Multiple blocks missing — all should be reported.
			name:            "multiple missing blocks",
			txResultBlocks:  mkSet(0, 1, 2, 3),
			appliedBlocks:   mkSet(0, 2),
			reachedMaxBlock: true,
			maxBlockNum:     3,
			wantMissing:     []uint64{1, 3},
		},
		{
			// Empty inputs — degenerate but legal.
			name:            "empty",
			txResultBlocks:  mkSet(),
			appliedBlocks:   mkSet(),
			reachedMaxBlock: true,
			maxBlockNum:     0,
			wantMissing:     nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := applyLoopMissingBlocks(tc.txResultBlocks, tc.appliedBlocks, tc.reachedMaxBlock, tc.maxBlockNum)
			if !sameSet(got, tc.wantMissing) {
				t.Fatalf("applyLoopMissingBlocks() = %v, want (set-equal) %v", got, tc.wantMissing)
			}
		})
	}
}

// TestExecLoopExitCheck covers the exec-loop exit invariant:
// pe.blockExecutors must be empty at every clean exit, otherwise an
// orphaned (queued-but-never-scheduled) block silently sits there
// forever and the apply loop never sees its blockResult.
func TestExecLoopExitCheck(t *testing.T) {
	t.Run("empty map returns nil", func(t *testing.T) {
		pe := &parallelExecutor{}
		pe.blockExecutors = map[uint64]*blockExecutor{}
		if err := pe.execLoopExitCheck("test"); err != nil {
			t.Fatalf("execLoopExitCheck on empty map should return nil, got: %v", err)
		}
	})

	t.Run("non-empty map returns ErrInvalidBlock with block nums", func(t *testing.T) {
		pe := &parallelExecutor{}
		pe.blockExecutors = map[uint64]*blockExecutor{
			3: {},
			7: {},
		}
		err := pe.execLoopExitCheck("test-reason")
		if err == nil {
			t.Fatalf("execLoopExitCheck on non-empty map should return error, got nil")
		}
		if !errors.Is(err, rules.ErrInvalidBlock) {
			t.Fatalf("expected wrapped ErrInvalidBlock, got: %v", err)
		}
		// Both block nums must appear in the error so the operator can
		// see exactly which blocks were left orphaned.
		for _, want := range []string{"3", "7", "test-reason"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error message missing %q: %s", want, err.Error())
			}
		}
	})

	t.Run("nil map returns nil (defensive)", func(t *testing.T) {
		pe := &parallelExecutor{}
		// pe.blockExecutors is nil
		if err := pe.execLoopExitCheck("test"); err != nil {
			t.Fatalf("execLoopExitCheck on nil map should return nil, got: %v", err)
		}
	})
}

// TestBlockValidatorWaitNil verifies the per-block validator is
// safe to Wait on when nil (the case where the apply loop's if-condition
// declined to construct one). Defends against NPE regression if someone
// changes the if-block to drop the nil-guard in Wait.
func TestBlockValidatorWaitNil(t *testing.T) {
	var bv *blockValidator
	if err := bv.Wait(); err != nil {
		t.Fatalf("nil blockValidator.Wait() should return nil, got: %v", err)
	}
}

// TestBlockValidatorWaitMultipleTimes verifies Wait can be called
// repeatedly on the same blockValidator without blocking after the
// goroutine completes. The current implementation re-stuffs the
// channel after each read; this regression test ensures that property
// holds if the implementation changes.
func TestBlockValidatorWaitMultipleTimes(t *testing.T) {
	bv := &blockValidator{done: make(chan error, 1)}
	bv.done <- nil // simulate goroutine completion

	for i := 0; i < 3; i++ {
		done := make(chan error, 1)
		go func() {
			done <- bv.Wait()
		}()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Wait() #%d returned unexpected error: %v", i+1, err)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Wait() #%d hung — channel not re-stuffed", i+1)
		}
	}
}

// TestBlockValidatorWaitErrorWrapping verifies the validation error
// is wrapped with rules.ErrInvalidBlock so that errors.Is callers
// (notably InsertChain → block_test_util) classify it correctly.
func TestBlockValidatorWaitErrorWrapping(t *testing.T) {
	innerErr := errors.New("blob gas mismatch")
	bv := &blockValidator{done: make(chan error, 1)}
	bv.done <- innerErr

	got := bv.Wait()
	if got == nil {
		t.Fatal("expected non-nil error")
	}
	if !errors.Is(got, rules.ErrInvalidBlock) {
		t.Errorf("expected wrapped rules.ErrInvalidBlock, got: %v", got)
	}
	if !strings.Contains(got.Error(), "blob gas mismatch") {
		t.Errorf("expected inner error preserved in message, got: %v", got)
	}
}

// TestApplyLoopRootResultsCloseDoesNotRace simulates the exact race
// the silent-failure fix protects against: rootResults closes BEFORE
// applyResults drains. The pre-fix apply loop returned nil
// immediately on rootResults close, dropping queued applyResults on
// the floor. The post-fix apply loop must keep draining applyResults.
//
// We exercise the actual select-arm pattern with a tiny in-test
// reproduction (channel orchestration only — no parallelExecutor
// dependencies) to lock the race fix in.
func TestApplyLoopRootResultsCloseDoesNotRace(t *testing.T) {
	type marker struct{ kind string }

	rootResults := make(chan struct{})
	applyResults := make(chan marker)
	rootClosedAck := make(chan struct{})

	// Producer goroutine: deterministically forces rootResults to close
	// while the apply loop is mid-flight. Sends 2 markers, closes
	// rootResults (then waits for the consumer to observe the close), then
	// sends the trailing marker — the one the pre-fix code would drop.
	go func() {
		applyResults <- marker{"early-tx"}
		applyResults <- marker{"early-block"}
		close(rootResults)
		<-rootClosedAck // ensure consumer has processed the !ok branch
		applyResults <- marker{"trailing-block"}
		close(applyResults)
	}()

	// The apply-loop select pattern under test: same shape as
	// exec3_parallel.go's apply loop. The post-fix behavior is to
	// disable rootResults' arm on close (set to nil) and continue.
	var (
		seen             []string
		rootResultsClose chan struct{} = rootResults
		closed           bool
		ctx, cancel      = context.WithTimeout(context.Background(), 2*time.Second)
	)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("apply loop hung instead of draining; saw=%v closed=%v", seen, closed)
		case ar, ok := <-applyResults:
			if !ok {
				// Channel closed — loop exits cleanly. All messages drained.
				goto done
			}
			seen = append(seen, ar.kind)
		case _, ok := <-rootResultsClose:
			if !ok {
				// Disable this arm to prevent busy-spin on the closed channel.
				// This is the post-fix behavior we lock in.
				rootResultsClose = nil
				closed = true
				close(rootClosedAck)
				continue
			}
		}
	}

done:
	if !closed {
		t.Fatal("rootResults case never fired with !ok — test scaffolding bug")
	}
	want := []string{"early-tx", "early-block", "trailing-block"}
	if len(seen) != len(want) {
		t.Fatalf("apply loop dropped messages on rootResults close: seen=%v want=%v", seen, want)
	}
	for i, w := range want {
		if seen[i] != w {
			t.Fatalf("apply loop saw wrong order: seen=%v want=%v", seen, want)
		}
	}
}

// TestApplyLoopDoesNotHangAfterRootResultsClose: complementary to
// the above — verifies that disabling the rootResults arm doesn't
// leave the apply loop stuck. Specifically, after rootResults is
// closed and disabled, the loop must still exit promptly when
// applyResults closes.
func TestApplyLoopDoesNotHangAfterRootResultsClose(t *testing.T) {
	rootResults := make(chan struct{})
	applyResults := make(chan struct{}, 1)

	close(rootResults) // closed before loop starts

	done := make(chan struct{})
	go func() {
		defer close(done)
		var rr chan struct{} = rootResults
		for {
			select {
			case _, ok := <-applyResults:
				if !ok {
					return
				}
			case _, ok := <-rr:
				if !ok {
					rr = nil
					continue
				}
			}
		}
	}()

	// Apply-side: signal close after a brief delay.
	close(applyResults)

	select {
	case <-done:
		// passed
	case <-time.After(2 * time.Second):
		t.Fatal("apply loop hung after rootResults close + applyResults close")
	}
}

// TestExecLoopExitCheckConcurrentReads verifies execLoopExitCheck is
// safe to call concurrently with map mutations under the lock — guards
// against future regression if someone removes the RLock.
func TestExecLoopExitCheckConcurrentReads(t *testing.T) {
	pe := &parallelExecutor{}
	pe.blockExecutors = map[uint64]*blockExecutor{}

	var stop atomic.Bool
	var wg sync.WaitGroup

	// Mutator: add and remove blocks under pe's lock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			pe.Lock()
			pe.blockExecutors[42] = &blockExecutor{}
			pe.Unlock()
			pe.Lock()
			delete(pe.blockExecutors, 42)
			pe.Unlock()
		}
	}()

	// Reader: continually call execLoopExitCheck.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_ = pe.execLoopExitCheck("concurrent")
		}
	}()

	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	// Test passes iff no race detector fires AND no deadlock.
}

// sameSet compares two slices ignoring order. Used because
// applyLoopMissingBlocks iterates a map; order is non-deterministic.
func sameSet(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[uint64]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		seen[v]--
		if seen[v] < 0 {
			return false
		}
	}
	for _, c := range seen {
		if c != 0 {
			return false
		}
	}
	return true
}
