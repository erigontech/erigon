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
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// TestChangesetWindowStart covers the pure helper that decides where the
// changeset window of a batch begins. Evaluating the window once at
// startBlockNum (instead of per block) would leave any batch longer than
// MaxReorgDepth without changesets, making the node unable to reorg
// afterwards.
func TestChangesetWindowStart(t *testing.T) {
	cases := []struct {
		name                     string
		alwaysGenerateChangesets bool
		maxReorgDepth            uint64
		frozenBlocks             uint64
		startBlockNum            uint64
		maxBlockNum              uint64
		want                     uint64
	}{
		{
			name:          "big batch: window covers the last maxReorgDepth blocks",
			maxReorgDepth: 96,
			startBlockNum: 1,
			maxBlockNum:   1000,
			want:          904,
		},
		{
			name:          "small batch: whole batch in window",
			maxReorgDepth: 96,
			startBlockNum: 950,
			maxBlockNum:   1000,
			want:          950,
		},
		{
			name:          "batch end below maxReorgDepth: window from batch start",
			maxReorgDepth: 96,
			startBlockNum: 1,
			maxBlockNum:   96,
			want:          1,
		},
		{
			name:                     "alwaysGenerateChangesets overrides depth and frozen gates",
			alwaysGenerateChangesets: true,
			maxReorgDepth:            96,
			frozenBlocks:             2000,
			startBlockNum:            1,
			maxBlockNum:              1000,
			want:                     1,
		},
		{
			name:          "frozen blocks push the window up",
			maxReorgDepth: 96,
			frozenBlocks:  950,
			startBlockNum: 1,
			maxBlockNum:   1000,
			want:          950,
		},
		{
			name:          "fully frozen batch has no window",
			maxReorgDepth: 96,
			frozenBlocks:  2000,
			startBlockNum: 1,
			maxBlockNum:   1000,
			want:          math.MaxUint64,
		},
		{
			name:          "long catch-up batch keeps a shallow reorg below its tip unwindable",
			maxReorgDepth: 96,
			startBlockNum: 5138,
			maxBlockNum:   6137,
			want:          6041,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := changesetWindowStart(tc.alwaysGenerateChangesets, tc.maxReorgDepth, tc.frozenBlocks, tc.startBlockNum, tc.maxBlockNum)
			if got != tc.want {
				t.Fatalf("changesetWindowStart got %d, want %d", got, tc.want)
			}
		})
	}
}

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
//
// IMPORTANT: this test models the apply-loop's exit-branch decision
// rather than driving the production apply loop end-to-end (which would
// require full parallel-executor + workers + calculator setup). The
// `run` closure is a hand-coded mirror of the production sequence in
// exec3_parallel.go around line 355: applyLoopMissingBlocks → reachedMaxBlock
// check → ErrLoopExhausted. If those production lines change, this
// closure must be updated in lock-step or the test will pass vacuously.
func TestApplyLoopPartialBatchReturnsErrLoopExhausted(t *testing.T) {
	// Simulate the apply loop's exit-branch decision sequence.
	// (We cannot run the full apply loop in a unit test — requires the
	// parallel executor + workers + commitment calculator. Instead this
	// test covers the same decision tree that exec3_parallel.go runs
	// after the applyResults channel closes.)
	type result struct {
		err         error
		isExhausted bool
		isInvalid   bool
		isOK        bool
	}

	run := func(txResultBlocks, appliedBlocks map[uint64]struct{}, reachedMaxBlock bool, lastBlockResult, maxBlockNum, startBlockNum uint64) result {
		// The decision tree (mirroring exec3_parallel.go's
		// applyResults-close branch in execErr's anonymous func around
		// line 355 — keep these branches in sync with the production
		// sequence):
		if missing := applyLoopMissingBlocks(txResultBlocks, appliedBlocks); len(missing) > 0 {
			return result{
				err:       errors.New("invalid block: missing blocks"),
				isInvalid: true,
			}
		}
		if reachedMaxBlock {
			return result{isOK: true}
		}
		return result{
			err:         &ErrLoopExhausted{From: startBlockNum, To: lastBlockResult, Reason: "block batch is full"},
			isExhausted: true,
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

// TestApplyLoopChannelCloseOrder exercises the production
// closeApplyChannels helper to pin the documented close-order
// invariant: commitResults BEFORE applyResults. The calculator drains
// commitResults and signals the apply loop via rootResults; if
// applyResults closes first, the apply loop can race with the
// calculator's final commitment write.
//
// The test creates a parallelExecutor with mock channels, calls the
// production helper, and asserts the order via close-detection on each
// channel. Reordering the closes inside closeApplyChannels — or
// dropping the helper and inlining a wrong order back into execLoop —
// surfaces here.
func TestApplyLoopChannelCloseOrder(t *testing.T) {
	commit := make(chan applyResult)
	apply := make(chan applyResult)
	pe := &parallelExecutor{
		commitResultsCh: commit,
		applyResultsCh:  apply,
	}

	// closeApplyChannels' return value records the close sequence
	// inline as each close() succeeds — no goroutine wakeup races.
	order := pe.closeApplyChannels()
	if len(order) != 2 {
		t.Fatalf("closeApplyChannels must close 2 channels, got order=%v", order)
	}
	if order[0] != "commitResults" || order[1] != "applyResults" {
		t.Fatalf("close order must be [commitResults, applyResults]; got %v", order)
	}

	// Sanity: both channels are actually closed (read returns ok=false).
	if _, ok := <-commit; ok {
		t.Error("commit channel not actually closed")
	}
	if _, ok := <-apply; ok {
		t.Error("apply channel not actually closed")
	}

	// pe.commitResultsCh and pe.applyResultsCh must be nil-ed by the
	// helper so subsequent calls are no-ops rather than double-closes.
	if pe.commitResultsCh != nil {
		t.Error("closeApplyChannels must nil commitResultsCh after closing")
	}
	if pe.applyResultsCh != nil {
		t.Error("closeApplyChannels must nil applyResultsCh after closing")
	}

	// Calling the helper again with already-nil fields must be a no-op,
	// not a panic, and the returned order must be empty.
	if order := pe.closeApplyChannels(); len(order) != 0 {
		t.Errorf("closeApplyChannels on already-nil channels must return empty order, got %v", order)
	}
}

// TestCloseApplyChannelsDoubleCloseRecovers ensures the safety-net
// recover in closeApplyChannels actually catches the
// "close of closed channel" panic when, e.g., a parallel shutdown path
// closes the channels before the deferred close fires. After the
// recover, the closed-order slice should NOT include the channel name
// (since the close() didn't succeed) but the field should still be
// nilled so subsequent calls are clean no-ops.
func TestCloseApplyChannelsDoubleCloseRecovers(t *testing.T) {
	pe := &parallelExecutor{
		commitResultsCh: make(chan applyResult),
		applyResultsCh:  make(chan applyResult),
	}
	close(pe.commitResultsCh) // pre-closed by the racing path
	close(pe.applyResultsCh)

	// Must not panic — the helper's recover catches "close of closed channel".
	order := pe.closeApplyChannels()
	if len(order) != 0 {
		t.Errorf("closeApplyChannels on already-closed channels must NOT count them as freshly closed; got order=%v", order)
	}
	if pe.commitResultsCh != nil || pe.applyResultsCh != nil {
		t.Fatal("closeApplyChannels must nil the fields even on double-close")
	}
}

// TestExecLoopShouldExitPriority exercises the production
// execLoopShouldExit helper directly. Each case pins one branch's
// precedence; reordering the production helper or dropping a branch
// makes the corresponding case fail. This replaces the earlier
// model-based decision-tree test that could drift from production.
//
// Production background: executeBlocks marks the final dispatched
// block with Exhausted when the per-cycle block limit is reached, then
// exits — without closing pe.execRequests, without cancelling ctx. If
// execLoopShouldExit doesn't honor that signal the exec loop parks on
// its main select forever waiting for work the dispatcher will never
// produce. Symptom in production: chiado
// `EXEC3_PARALLEL=true ... --sync.loop.block.limit=10_000` parallel
// exec from block 0 silently hangs at the first step boundary
// (block 150662 in chiado's case) — a hang masking the wrong-trie-root
// failure that issue erigon#20711 reported as the visible symptom.
func TestExecLoopShouldExitPriority(t *testing.T) {
	const (
		batchLimit   = uint64(1 << 30)
		smallSizeEst = uint64(1024) // < batchLimit
		bigSizeEst   = uint64(1<<30) + 1
	)

	exhaustedSignal := &ErrLoopExhausted{From: 1, To: 5, Reason: "test"}

	cases := []struct {
		name           string
		blockNum       uint64
		exhausted      *ErrLoopExhausted
		sizeEst        uint64
		maxBlockNum    uint64
		stopAfterBlock uint64
		want           execLoopExitDecision
	}{
		{
			// No exit condition met — keep processing.
			name: "continue", blockNum: 5, sizeEst: smallSizeEst, maxBlockNum: 99,
			want: execLoopContinue,
		},
		{
			// Size limit alone — fires regardless of other state.
			name:     "size limit fires before maxBlockNum",
			blockNum: 5, sizeEst: bigSizeEst, maxBlockNum: 99,
			want: execLoopExitSizeLimit,
		},
		{
			// maxBlockNum alone — flag for clean batch end.
			name:     "maxBlockNum reached",
			blockNum: 99, sizeEst: smallSizeEst, maxBlockNum: 99,
			want: execLoopExitMaxReached,
		},
		{
			// Final dispatched block carries Exhausted — partial batch.
			name:     "Exhausted on partial batch",
			blockNum: 5, exhausted: exhaustedSignal, sizeEst: smallSizeEst, maxBlockNum: 99,
			want: execLoopExitExhausted,
		},
		{
			// dbg.StopAfterBlock crossed — debug halt.
			name:     "stopAfterBlock crossed",
			blockNum: 7, sizeEst: smallSizeEst, maxBlockNum: 99, stopAfterBlock: 5,
			want: execLoopExitStopAfter,
		},
		{
			// stopAfterBlock=0 means disabled — must NOT fire.
			name:     "stopAfterBlock=0 disabled",
			blockNum: 7, sizeEst: smallSizeEst, maxBlockNum: 99, stopAfterBlock: 0,
			want: execLoopContinue,
		},
		// Precedence: when MULTIPLE conditions overlap, only the
		// highest-priority one wins. Reordering the helper would flip
		// these cases.
		{
			// Final block of a cycle that ALSO reaches maxBlockNum.
			// maxBlockNum must win (clean nil return) — Exhausted
			// would mis-flag the batch as "more work pending".
			name:     "precedence: maxBlockNum beats Exhausted",
			blockNum: 99, exhausted: exhaustedSignal, sizeEst: smallSizeEst, maxBlockNum: 99,
			want: execLoopExitMaxReached,
		},
		{
			// Size limit at the same block that also reached max.
			// Size limit wins — most urgent (sd.mem is over budget).
			name:     "precedence: sizeLimit beats maxBlockNum",
			blockNum: 99, sizeEst: bigSizeEst, maxBlockNum: 99,
			want: execLoopExitSizeLimit,
		},
		{
			// Exhausted set together with stopAfterBlock crossed.
			// Exhausted wins — production stops the partial batch
			// rather than masking it with the debug halt.
			name:     "precedence: Exhausted beats stopAfterBlock",
			blockNum: 7, exhausted: exhaustedSignal, sizeEst: smallSizeEst,
			maxBlockNum: 99, stopAfterBlock: 5,
			want: execLoopExitExhausted,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			br := &blockResult{BlockNum: tc.blockNum, Exhausted: tc.exhausted}
			got := execLoopShouldExit(br, tc.sizeEst, batchLimit, tc.maxBlockNum, tc.stopAfterBlock)
			if got != tc.want {
				t.Fatalf("execLoopShouldExit got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestShouldMarkExhaustedAtBlock exercises the production
// shouldMarkExhaustedAtBlock helper directly. The helper is the gate
// that decides whether executeBlocks stamps a dispatched block with
// Exhausted; misjudging this either causes the exec loop to park
// forever (Exhausted not set when it should be) or trims a batch
// short (set when it shouldn't be).
//
// The "only-set-on-final-block" structural property — that
// executeBlocks runs `if exhausted != nil { break }` immediately
// after stamping, so no later block is dispatched in the same cycle —
// is enforced by the explicit `break` at exec3.go's call site rather
// than by the helper itself; that's a single line of code that can be
// reasoned about by inspection. This test focuses on the helper's
// own decision matrix.
func TestShouldMarkExhaustedAtBlock(t *testing.T) {
	cases := []struct {
		name                                             string
		initialCycle                                     bool
		lastExecutedStep, lastFrozenStep                 kv.Step
		discardCommitment                                bool
		blockLimit, blockNum, startBlockNum, maxBlockNum uint64
		want                                             bool
	}{
		{
			// Later cycle, blockLimit reached mid-batch — must mark.
			name:          "later cycle, limit reached",
			blockLimit:    10,
			blockNum:      100,
			startBlockNum: 91, // span = 10 == limit
			maxBlockNum:   200,
			want:          true,
		},
		{
			// Later cycle, but landed exactly on maxBlockNum — must NOT
			// mark (the goal block triggers reachedMaxBlock instead).
			name:          "later cycle, hit maxBlockNum exactly",
			blockLimit:    10,
			blockNum:      100,
			startBlockNum: 91,
			maxBlockNum:   100,
			want:          false,
		},
		{
			// blockLimit == 0 means "no per-cycle limit" — must NOT mark
			// regardless of how far we've progressed.
			name:          "blockLimit=0 disabled",
			blockLimit:    0,
			blockNum:      100,
			startBlockNum: 1,
			maxBlockNum:   200,
			want:          false,
		},
		{
			// Later cycle, span < limit — keep going.
			name:          "later cycle, span below limit",
			blockLimit:    10,
			blockNum:      95,
			startBlockNum: 91, // span = 5 < 10
			maxBlockNum:   200,
			want:          false,
		},
		{
			// Initial cycle, no frozen progress yet — gate closed.
			name:             "initial cycle, no step progress",
			initialCycle:     true,
			lastExecutedStep: 0,
			lastFrozenStep:   0,
			blockLimit:       10,
			blockNum:         100,
			startBlockNum:    91,
			maxBlockNum:      200,
			want:             false,
		},
		{
			// Initial cycle, lastExecutedStep > lastFrozenStep AND not
			// DiscardCommitment — gate open, limit reached.
			name:             "initial cycle, step progressed, limit reached",
			initialCycle:     true,
			lastExecutedStep: 5,
			lastFrozenStep:   3,
			blockLimit:       10,
			blockNum:         100,
			startBlockNum:    91,
			maxBlockNum:      200,
			want:             true,
		},
		{
			// Initial cycle, step progressed but DiscardCommitment is
			// on — gate stays closed (a partial-batch flush would lose
			// the in-memory commitment).
			name:              "initial cycle, DiscardCommitment masks step progress",
			initialCycle:      true,
			lastExecutedStep:  5,
			lastFrozenStep:    3,
			discardCommitment: true,
			blockLimit:        10,
			blockNum:          100,
			startBlockNum:     91,
			maxBlockNum:       200,
			want:              false,
		},
		{
			// Initial cycle, lastExecutedStep == lastFrozenStep — no
			// new committable step yet, gate closed.
			name:             "initial cycle, step not advanced past frozen",
			initialCycle:     true,
			lastExecutedStep: 3,
			lastFrozenStep:   3,
			blockLimit:       10,
			blockNum:         100,
			startBlockNum:    91,
			maxBlockNum:      200,
			want:             false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := shouldMarkExhaustedAtBlock(
				tc.initialCycle, tc.lastExecutedStep, tc.lastFrozenStep,
				tc.discardCommitment,
				tc.blockLimit, tc.blockNum, tc.startBlockNum, tc.maxBlockNum,
			)
			if got != tc.want {
				t.Fatalf("shouldMarkExhaustedAtBlock got %v, want %v", got, tc.want)
			}
		})
	}
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

// TestApplyLoopFlushAsComplete covers the helper that decides the `complete`
// flag the apply loop passes to versionMap.FlushVersionedWrites. The `valid`
// term in this helper is the regression guard for the gnosis-block-18,483,405
// phantom-write bug: a current tx with a VersionInvalid verdict must NOT
// flush its writes as Done, otherwise downstream OCC readers see them as
// committed.
func TestApplyLoopFlushAsComplete(t *testing.T) {
	tests := []struct {
		name       string
		valid      bool
		cntInvalid int
		want       bool
	}{
		{
			name:       "valid current tx, no prior invalids → Done",
			valid:      true,
			cntInvalid: 0,
			want:       true,
		},
		{
			// Regression guard for the gnosis-18,483,405 phantom-write bug:
			// before this fix the apply loop only checked cntInvalid (which
			// counts *prior* invalids), so an invalid current tx fell through
			// as `complete=true → Done` and produced phantom committed entries.
			name:       "INVALID current tx → must NOT be Done (phantom-write guard)",
			valid:      false,
			cntInvalid: 0,
			want:       false,
		},
		{
			name:       "valid current but prior invalid in iteration → Estimate",
			valid:      true,
			cntInvalid: 1,
			want:       false,
		},
		{
			name:       "INVALID current and prior invalid → Estimate",
			valid:      false,
			cntInvalid: 1,
			want:       false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, applyLoopFlushAsComplete(tc.valid, tc.cntInvalid),
				"applyLoopFlushAsComplete(valid=%v, cntInvalid=%d)", tc.valid, tc.cntInvalid)
		})
	}
}

// TestApplyLoopFlush_InvalidTxWritesAreEstimate reproduces the bug-scenario
// at the VersionMap layer using the production flush-decision helper.
//
// Repro recipe from gnosis block 18,483,405:
//
//  1. tx[3] inc=0 executed, EVM did NOT revert, and emitted 28 storage writes
//     (one of them: contract 0x18b2b767… slot 0x08 = `aabS…0b886…5981`).
//  2. Apply loop's ValidateVersionBlock returned VersionInvalid (some read no
//     longer matched versionMap).
//  3. Apply loop then called FlushVersionedWrites with `cntInvalid == 0` as
//     the `complete` flag — true, because cntInvalid counts only *prior*
//     invalid txs in the current iteration. The 28 writes were stored as
//     flag=Done.
//  4. tx[16] subsequently read slot 8, got `aabS…` via MapRead, recorded
//     readVersion=tx[3]:inc0. Version-only validation passed.
//  5. Downstream gas-mismatch ~80K blocks later from the phantom-derived state
//     cascading through the tx queue.
//
// The fix at exec3_parallel.go:applyLoopFlushAsComplete folds `valid` into
// the gating so an invalidated tx's writes are flushed as Estimate. This test
// asserts the downstream effect: a tx that later reads the slot must see
// MVReadResultDependency (the validator treats this as VersionInvalid and
// forces re-execution), not MVReadResultDone.
func TestApplyLoopFlush_InvalidTxWritesAreEstimate(t *testing.T) {
	addr := accounts.InternAddress(common.HexToAddress("0x18b2b7673c6d661923e9460d592699617828b293"))
	slot := accounts.InternKey(common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000008"))

	vm := state.NewVersionMap(nil)

	// Simulate the apply loop processing tx=3 with validity=VersionInvalid in
	// the first iteration of toValidate (cntInvalid starts at 0).
	const invalidTxIdx = 3
	const invalidTxInc = 0
	phantomVal := *uint256.NewInt(0xaabb)

	invalidTxWrites := state.VersionedWrites{
		{
			Address: addr,
			Path:    state.StoragePath,
			Key:     slot,
			Version: state.Version{TxIndex: invalidTxIdx, Incarnation: invalidTxInc},
			Val:     phantomVal,
		},
	}

	// Drive the production flush-decision helper end-to-end.
	valid := false  // validity == VersionInvalid
	cntInvalid := 0 // no prior invalids in this iteration
	complete := applyLoopFlushAsComplete(valid, cntInvalid)
	require.False(t, complete,
		"invalidated tx must flush as Estimate (not Done) — see "+
			"TestApplyLoopFlushAsComplete for the unit-level guard")

	vm.FlushVersionedWrites(invalidTxWrites, complete, "")

	// Downstream tx=16 reads the slot — this is the read that committed
	// phantom state in the bug.
	const downstreamTxIdx = 16
	res := vm.Read(addr, state.StoragePath, slot, downstreamTxIdx)

	// MVReadResultDependency: the validator will treat any read of this cell
	// as VersionInvalid, forcing the reader to re-execute. This is correct
	// OCC behavior when an invalidated tx is awaiting retry.
	require.Equal(t, state.MVReadResultDependency, res.Status(),
		"invalid tx's writes must be flushed as Estimate so downstream reads "+
			"return MVReadResultDependency. Pre-fix, this returned "+
			"MVReadResultDone and downstream txs committed phantom state "+
			"(gnosis block 18,483,405 repro).")

	// Sanity: the entry IS recorded against tx[3] in versionMap (Estimate, not
	// absent), so downstream readers' OCC dependency tracking still works.
	require.Equal(t, invalidTxIdx, res.DepIdx(),
		"phantom write is recorded as Estimate, not deleted — downstream "+
			"OCC must still see it as a dependency")
}
