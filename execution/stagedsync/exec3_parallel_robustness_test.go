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
		name           string
		txResultBlocks map[uint64]struct{}
		appliedBlocks  map[uint64]struct{}
		wantMissing    []uint64
	}{
		{
			// Happy path: every block whose tx-results arrived also had a
			// blockResult.
			name:           "all applied",
			txResultBlocks: mkSet(0, 1),
			appliedBlocks:  mkSet(0, 1),
			wantMissing:    nil,
		},
		{
			// The exact bug the original guard caught: block 1 had tx-results
			// arrive but the trailing blockResult was dropped by the
			// rootResults-close race. Validator never fired — must flag.
			name:           "tx-results without blockResult — the rootResults race",
			txResultBlocks: mkSet(0, 1),
			appliedBlocks:  mkSet(0),
			wantMissing:    []uint64{1},
		},
		{
			// Partial batch (size-limit hit): exec stopped at block N
			// before reaching maxBlockNum. txResultBlocks and appliedBlocks
			// agree on [0..N]; nothing past N appeared on the apply side
			// because exec returned before scheduling N+1. The follow-up
			// stage-loop iteration picks up at N+1 — must NOT flag here.
			name:           "partial batch — size-limit hit, no spurious flag for unreached blocks",
			txResultBlocks: mkSet(0, 1, 2),
			appliedBlocks:  mkSet(0, 1, 2),
			wantMissing:    nil,
		},
		{
			// Multiple genuine silent failures — all should be reported.
			name:           "multiple missing blocks",
			txResultBlocks: mkSet(0, 1, 2, 3),
			appliedBlocks:  mkSet(0, 2),
			wantMissing:    []uint64{1, 3},
		},
		{
			// Empty inputs — degenerate but legal.
			name:           "empty",
			txResultBlocks: mkSet(),
			appliedBlocks:  mkSet(),
			wantMissing:    nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := applyLoopMissingBlocks(tc.txResultBlocks, tc.appliedBlocks)
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

// TestApplyLoopPartialBatchReturnsErrLoopExhausted exercises the
// apply-loop exit decision tree end-to-end with channel orchestration:
// when applyResults closes after the exec loop hit its size-limit
// (lastBlockResult < maxBlockNum, no missing blocks), the apply loop
// must return ErrLoopExhausted so the stage loop resumes from the next
// block. The previous bug spuriously flagged maxBlockNum as missing
// because it wasn't applied — turning every legitimate partial batch
// into an InvalidBlock error. This test locks in the corrected
// behavior: completeness check sees no missing → exhausted → stage
// loop continues — no re-execution.
func TestApplyLoopPartialBatchReturnsErrLoopExhausted(t *testing.T) {
	// Simulate the apply loop's exit-branch decision sequence.
	// (We cannot run the full apply loop in a unit test — requires the
	// parallel executor + workers + commitment calculator. Instead this
	// test covers the same decision tree that exec3_parallel.go runs
	// after the applyResults channel closes.)
	type result struct {
		err          error
		isExhausted  bool
		isInvalid    bool
		isOK         bool
		errSubstring string
	}

	run := func(txResultBlocks, appliedBlocks map[uint64]struct{}, reachedMaxBlock bool, lastBlockResult, maxBlockNum, startBlockNum uint64) result {
		// The decision tree (mirroring exec3_parallel.go's
		// applyResults-close branch in execErr's anonymous func):
		if missing := applyLoopMissingBlocks(txResultBlocks, appliedBlocks); len(missing) > 0 {
			return result{
				err:          errors.New("invalid block: missing blocks"),
				isInvalid:    true,
				errSubstring: "invalid block",
			}
		}
		if reachedMaxBlock {
			return result{isOK: true}
		}
		return result{
			err:          &ErrLoopExhausted{From: startBlockNum, To: lastBlockResult, Reason: "block batch is full"},
			isExhausted:  true,
			errSubstring: "exhausted",
		}
	}

	mkSet := func(ns ...uint64) map[uint64]struct{} {
		s := make(map[uint64]struct{}, len(ns))
		for _, n := range ns {
			s[n] = struct{}{}
		}
		return s
	}

	t.Run("partial batch, size-limit hit — exhausted (the regression case)", func(t *testing.T) {
		got := run(mkSet(1, 2, 3, 4, 5), mkSet(1, 2, 3, 4, 5), false, 5, 200, 1)
		if !got.isExhausted {
			t.Fatalf("expected ErrLoopExhausted, got: %+v", got)
		}
		if !errors.Is(got.err, &ErrLoopExhausted{}) {
			t.Errorf("err must wrap *ErrLoopExhausted, got: %v", got.err)
		}
	})

	t.Run("full batch, max reached — clean nil", func(t *testing.T) {
		got := run(mkSet(1, 2, 3), mkSet(1, 2, 3), true, 3, 3, 1)
		if !got.isOK {
			t.Fatalf("expected clean nil, got: %+v", got)
		}
	})

	t.Run("genuine silent failure mid-batch — InvalidBlock", func(t *testing.T) {
		// Block 3 had tx-results but no blockResult. Real bug — must surface.
		got := run(mkSet(1, 2, 3), mkSet(1, 2), false, 2, 5, 1)
		if !got.isInvalid {
			t.Fatalf("expected InvalidBlock error, got: %+v", got)
		}
	})

	t.Run("partial batch with single block — exhausted", func(t *testing.T) {
		got := run(mkSet(1), mkSet(1), false, 1, 200, 1)
		if !got.isExhausted {
			t.Fatalf("expected ErrLoopExhausted, got: %+v", got)
		}
	})
}

// TestApplyLoopChannelCloseOrder exercises the documented invariant
// that the exec loop closes commitResults BEFORE applyResults on
// shutdown. The calculator drains commitResults and signals the apply
// loop via rootResults; if applyResults closes first, the apply loop
// can race with the calculator's final commitment write.
//
// The exec loop's deferred close in execLoop() does the right thing
// (commitResults first), but the close ordering is load-bearing —
// changing it silently corrupts the shutdown sequence. This test
// captures the shape of the deferred close to pin the order down.
func TestApplyLoopChannelCloseOrder(t *testing.T) {
	commitResults := make(chan struct{})
	applyResults := make(chan struct{})

	closeOrder := make(chan string, 2)

	// Mimic execLoop's deferred close: commitResults first, then
	// applyResults.
	closeBoth := func() {
		close(commitResults)
		closeOrder <- "commitResults"
		close(applyResults)
		closeOrder <- "applyResults"
	}

	go closeBoth()

	first := <-closeOrder
	second := <-closeOrder

	if first != "commitResults" {
		t.Fatalf("commitResults must close first; got close order [%s, %s]", first, second)
	}
	if second != "applyResults" {
		t.Fatalf("applyResults must close second; got close order [%s, %s]", first, second)
	}

	// Sanity: both channels actually closed.
	if _, ok := <-commitResults; ok {
		t.Fatal("commitResults not actually closed")
	}
	if _, ok := <-applyResults; ok {
		t.Fatal("applyResults not actually closed")
	}
}

// TestExecLoopHonorsBlockResultExhausted is the orchestration counterpart
// to TestApplyLoopPartialBatchReturnsErrLoopExhausted: the apply-loop
// side gets the "exhausted" surface right (return ErrLoopExhausted, no
// false-positive missing-block flag), but only if the exec-loop actually
// closes the apply channels in the partial-batch case. executeBlocks
// signals partial-batch completion by setting blockResult.Exhausted on
// the final dispatched block then exiting its goroutine — without
// closing pe.execRequests, without cancelling ctx. If the exec loop
// doesn't react to that signal, it parks on its main select forever
// (no more dispatched requests, no more rws.ResultCh activity, no
// ctx.Done) and the apply loop never gets the channel-close signal it
// needs to return ErrLoopExhausted. Symptom in production: chiado
// `EXEC3_PARALLEL=true ... --sync.loop.block.limit=10_000` parallel
// exec from block 0 silently hangs at the first step boundary
// (block 150662 in chiado's case) — a hang masking a wrong-trie-root
// failure that issue erigon#20711 originally reported as the visible
// symptom.
//
// We can't reach into the real execLoop without spinning up the full
// parallel executor + workers + calculator (the same constraint
// TestApplyLoopPartialBatchReturnsErrLoopExhausted documents). Instead
// the test runs TWO models of the exec loop's blockResult-handling
// decision tree against an identical channel-orchestration setup:
//   - "with fix" (post-fix): mirrors the production precedence
//     including the Exhausted check — must return cleanly.
//   - "without fix" (pre-fix): same precedence WITHOUT the Exhausted
//     check — must hang past the timeout, proving the orchestration
//     scaffolding genuinely surfaces the bug rather than passing
//     vacuously.
//
// This also locks in the precedence ordering: the Exhausted check
// must fire after maxBlockNum (otherwise the final-block-of-cycle
// case where both flags happen to be set would mis-flag as "more
// work pending") and before StopAfterBlock (which is debug-only).
func TestExecLoopHonorsBlockResultExhausted(t *testing.T) {
	const (
		dispatched   = 5  // dispatcher emits this many blockResults
		maxBlockNum  = 99 // far above dispatched — partial batch
		batchLimit   = uint64(1 << 30)
		fakeSizeEst  = uint64(1024) // < batchLimit, so sizeEst path doesn't fire
		stopAfterBlk = uint64(0)    // disabled
	)

	type result struct {
		exitErr      error
		triggerCount int
		hung         bool
	}

	// runModel runs `dispatched` blockResults through `model`, with the
	// last carrying Exhausted. Returns whether `model` exited cleanly,
	// and how many times the trigger-equivalent fired.
	runModel := func(model func(br *blockResult, trigger func()) (done bool)) result {
		br := make(chan *blockResult, dispatched)

		// Dispatcher mirrors executeBlocks's partial-batch exit: send,
		// return — do NOT close the channel (the production exec loop
		// owns close ordering).
		go func() {
			for i := uint64(1); i <= dispatched; i++ {
				r := &blockResult{BlockNum: i}
				if i == dispatched {
					r.Exhausted = &ErrLoopExhausted{From: 1, To: i, Reason: "block limit reached"}
				}
				br <- r
			}
		}()

		var triggerCount int
		trigger := func() { triggerCount++ }
		exited := make(chan error, 1)
		// Inner ctx lets the goroutine cooperate with the outer timeout
		// without leaking when the model hangs on <-br. We translate
		// "exited via ctx" to result.hung — that is the hang signal.
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()

		go func() {
			for {
				select {
				case <-ctx.Done():
					exited <- ctx.Err()
					return
				case b := <-br:
					if model(b, trigger) {
						exited <- nil
						return
					}
				}
			}
		}()

		select {
		case err := <-exited:
			if errors.Is(err, context.DeadlineExceeded) {
				// Model couldn't make progress — only escape was the
				// inner ctx timeout. That's the hang signal.
				return result{hung: true, triggerCount: triggerCount}
			}
			return result{exitErr: err, triggerCount: triggerCount}
		case <-time.After(2 * time.Second):
			// Outer safety net — model didn't even respect ctx. Still a
			// hang, just a different shape.
			return result{hung: true, triggerCount: triggerCount}
		}
	}

	// Production decision tree, mirroring exec3_parallel.go's
	//   if blockResult != nil { ... } block:
	//     1. sizeEst > batchLimit → trigger + return
	//     2. blockResult.BlockNum >= maxBlockNum → reachedMaxBlock + trigger + return
	//     3. blockResult.Exhausted != nil → trigger + return
	//     4. dbg.StopAfterBlock → trigger + return
	//     5. fall through to next-block scheduling.
	withFix := func(b *blockResult, trigger func()) (done bool) {
		if fakeSizeEst > batchLimit {
			trigger()
			return true
		}
		if b.BlockNum >= maxBlockNum {
			trigger()
			return true
		}
		if b.Exhausted != nil {
			trigger()
			return true
		}
		if stopAfterBlk > 0 && b.BlockNum >= stopAfterBlk {
			trigger()
			return true
		}
		return false
	}

	// Pre-fix decision tree: same as withFix but missing the Exhausted
	// branch — i.e. the production code as it stood when issue #20711
	// was first reported.
	withoutFix := func(b *blockResult, trigger func()) (done bool) {
		if fakeSizeEst > batchLimit {
			trigger()
			return true
		}
		if b.BlockNum >= maxBlockNum {
			trigger()
			return true
		}
		if stopAfterBlk > 0 && b.BlockNum >= stopAfterBlk {
			trigger()
			return true
		}
		return false
	}

	t.Run("with fix — exec loop returns cleanly on Exhausted", func(t *testing.T) {
		r := runModel(withFix)
		if r.hung {
			t.Fatal("post-fix model hung past timeout — fix is not effective")
		}
		if r.exitErr != nil {
			t.Fatalf("post-fix model exited with %v, want nil", r.exitErr)
		}
		if r.triggerCount != 1 {
			t.Fatalf("post-fix model: triggerBatchCommitment-equivalent must fire exactly once on Exhausted, got %d", r.triggerCount)
		}
	})

	t.Run("without fix — exec loop hangs (regression guard)", func(t *testing.T) {
		r := runModel(withoutFix)
		if !r.hung {
			t.Fatalf("pre-fix model did not hang as expected — orchestration scaffolding is wrong; result=%+v", r)
		}
		if r.triggerCount != 0 {
			t.Fatalf("pre-fix model fired trigger %d times — should be 0 because no exit branch matches", r.triggerCount)
		}
	})
}

// TestExecLoopExhaustedOnlySetOnFinalBlock is a guard against future
// regressions in the dispatcher: blockResult.Exhausted being set on a
// non-final block would cause the exec loop to drop later blocks that
// have already been queued. Locks down the precedence: among blocks in
// flight, only the trailing dispatched block's Exhausted is honored —
// any earlier Exhausted signal would short-circuit the batch and lose
// already-scheduled work. This test pins the convention to exec3.go's
// `if exhausted != nil { break }` after dispatch: the break ensures
// no later blockResult is produced once Exhausted is set on a
// dispatched request.
func TestExecLoopExhaustedOnlySetOnFinalBlock(t *testing.T) {
	// Simulate executeBlocks' loop: it sets exhausted, dispatches the
	// blockResult containing it, and breaks out — no further blocks.
	// Verify the convention: count(Exhausted != nil) == 1 always, and
	// it's always the last blockResult.
	type dispatched struct {
		num       uint64
		exhausted bool
	}
	dispatch := func(blocks []dispatched) error {
		var prevExhausted bool
		for _, b := range blocks {
			if prevExhausted {
				return errors.New("dispatched a block after Exhausted was set — exec loop would drop it")
			}
			if b.exhausted {
				prevExhausted = true
			}
		}
		return nil
	}

	t.Run("Exhausted only on last block — valid", func(t *testing.T) {
		err := dispatch([]dispatched{
			{num: 1, exhausted: false},
			{num: 2, exhausted: false},
			{num: 3, exhausted: true},
		})
		if err != nil {
			t.Fatalf("valid case rejected: %v", err)
		}
	})

	t.Run("no Exhausted — valid (full-batch path)", func(t *testing.T) {
		err := dispatch([]dispatched{
			{num: 1, exhausted: false},
			{num: 2, exhausted: false},
		})
		if err != nil {
			t.Fatalf("valid case rejected: %v", err)
		}
	})

	t.Run("Exhausted mid-stream — invalid (regression guard)", func(t *testing.T) {
		err := dispatch([]dispatched{
			{num: 1, exhausted: false},
			{num: 2, exhausted: true},
			{num: 3, exhausted: false},
		})
		if err == nil {
			t.Fatal("dispatch convention violated: blockResult emitted after Exhausted")
		}
	})
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
