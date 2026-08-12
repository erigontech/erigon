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
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon/common"
	commonerrors "github.com/erigontech/erigon/common/errors"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/protocol"
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
			require.Equal(t, tc.wantMissing, got)
		})
	}

	t.Run("missing blocks are sorted", func(t *testing.T) {
		txResultBlocks := mkSet(17, 1, 31, 9, 25, 5, 21, 13, 29, 3, 19, 7, 23, 11, 27, 15)
		want := []uint64{1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31}
		for range 32 {
			require.Equal(t, want, applyLoopMissingBlocks(txResultBlocks, nil))
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

	for i := range 3 {
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

// Keep the pending-block snapshot safe while the executor map is being updated.
func TestCheckBlocksDrainedConcurrentReads(t *testing.T) {
	pe := &parallelExecutor{}
	pe.blockExecutors = map[uint64]*blockExecutor{}

	var stop atomic.Bool
	var wg sync.WaitGroup

	wg.Go(func() {
		for !stop.Load() {
			pe.Lock()
			pe.blockExecutors[42] = &blockExecutor{}
			pe.Unlock()
			pe.Lock()
			delete(pe.blockExecutors, 42)
			pe.Unlock()
		}
	})

	wg.Go(func() {
		for !stop.Load() {
			_ = pe.checkBlocksDrained(context.Background(), context.Background(), nil)
		}
	})

	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()
	// Test passes iff no race detector fires AND no deadlock.
}

// TestApplyLoopCloseClassification covers the apply-loop close decisions via
// the production helpers: a partial batch with every terminal result is a
// resumable boundary (a result, not an error), a fully applied batch is clean,
// and a missing terminal result is an operational executor error rather than
// an invalid-block verdict.
func TestApplyLoopCloseClassification(t *testing.T) {
	run := func(txResultBlocks, appliedBlocks map[uint64]struct{}, sc *stopCause, lastBlockResult, maxBlockNum, startBlockNum uint64) (*ErrLoopExhausted, error) {
		pe := &parallelExecutor{maxBlockNum: maxBlockNum}
		err := pe.resolveApplyLoopClose(context.Background(), nil, failCandidate{}, sc, startBlockNum, lastBlockResult, txResultBlocks, appliedBlocks)
		return pe.exhausted, err
	}

	mkSet := func(ns ...uint64) map[uint64]struct{} {
		s := make(map[uint64]struct{}, len(ns))
		for _, n := range ns {
			s[n] = struct{}{}
		}
		return s
	}

	t.Run("partial batch, size-limit hit — exhausted (the regression case)", func(t *testing.T) {
		exhausted, err := run(mkSet(1, 2, 3, 4, 5), mkSet(1, 2, 3, 4, 5), &stopCause{kind: stopMoreWork}, 5, 200, 1)
		require.NoError(t, err)
		require.NotNil(t, exhausted)
		require.Equal(t, uint64(5), exhausted.To)
	})

	t.Run("full batch, max reached — clean", func(t *testing.T) {
		exhausted, err := run(mkSet(1, 2, 3), mkSet(1, 2, 3), &stopCause{kind: stopReachedMax}, 3, 3, 1)
		require.NoError(t, err)
		require.Nil(t, exhausted)
	})

	t.Run("missing terminal result is an operational error", func(t *testing.T) {
		exhausted, err := run(mkSet(1, 2, 3), mkSet(1, 2), nil, 2, 5, 1)
		require.Error(t, err)
		require.Nil(t, exhausted)
		require.NotErrorIs(t, err, rules.ErrInvalidBlock)
		require.Contains(t, err.Error(), "without a blockResult")
	})

	t.Run("partial batch with single block — exhausted", func(t *testing.T) {
		exhausted, err := run(mkSet(1), mkSet(1), &stopCause{kind: stopMoreWork}, 1, 200, 1)
		require.NoError(t, err)
		require.NotNil(t, exhausted)
	})

	t.Run("no stop cause below max — exhausted", func(t *testing.T) {
		exhausted, err := run(mkSet(1, 2), mkSet(1, 2), nil, 2, 200, 1)
		require.NoError(t, err)
		require.NotNil(t, exhausted)
	})

	t.Run("no stop cause, empty stream — provisionally clean", func(t *testing.T) {
		exhausted, err := run(mkSet(), mkSet(), nil, 0, 200, 1)
		require.NoError(t, err)
		require.Nil(t, exhausted, "executor-side failures are checked after teardown")
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
			got := execLoopShouldExit(execLoopExitInput{
				blockNum:       tc.blockNum,
				exhausted:      tc.exhausted,
				sizeEst:        tc.sizeEst,
				batchLimit:     batchLimit,
				maxBlockNum:    tc.maxBlockNum,
				stopAfterBlock: tc.stopAfterBlock,
			})
			if got != tc.want {
				t.Fatalf("execLoopShouldExit got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestApplyLoopCloseIsClean covers close classification when no stop cause was
// published. Completed and empty streams are clean at the apply-loop layer;
// partial progress is resumable. An empty stream is only provisionally clean
// because executor errors and scheduled blocks left pending are checked after
// teardown.
func TestApplyLoopCloseIsClean(t *testing.T) {
	cases := []struct {
		name         string
		lastBlockNum uint64
		maxBlockNum  uint64
		txResults    int
		want         bool
	}{
		{name: "fully applied", lastBlockNum: 5, maxBlockNum: 5, txResults: 3, want: true},
		{name: "past target", lastBlockNum: 6, maxBlockNum: 5, txResults: 3, want: true},
		{name: "partial batch is not clean", lastBlockNum: 3, maxBlockNum: 5, txResults: 2, want: false},
		{name: "empty loop, nothing executed", lastBlockNum: 0, maxBlockNum: 21, txResults: 0, want: true},
		{name: "tx-results without blockResult is not clean", lastBlockNum: 0, maxBlockNum: 21, txResults: 4, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := applyLoopCloseIsClean(tc.lastBlockNum, tc.maxBlockNum, tc.txResults)
			if got != tc.want {
				t.Fatalf("applyLoopCloseIsClean(%d,%d,%d) = %v, want %v", tc.lastBlockNum, tc.maxBlockNum, tc.txResults, got, tc.want)
			}
		})
	}
}

func TestAppliedBlockProgress(t *testing.T) {
	var progress appliedBlockProgress

	if progress.advance(0, 1) {
		t.Fatal("genesis must not advance applied block progress")
	}
	if progress.blockNum != 0 || progress.lastTxNum != 0 {
		t.Fatalf("genesis changed progress to block=%d txNum=%d", progress.blockNum, progress.lastTxNum)
	}

	if !progress.advance(1, 4) {
		t.Fatal("block 1 must advance applied block progress")
	}
	if progress.blockNum != 1 || progress.lastTxNum != 4 {
		t.Fatalf("unexpected progress block=%d txNum=%d", progress.blockNum, progress.lastTxNum)
	}

	if progress.advance(1, 5) {
		t.Fatal("duplicate block must not advance applied block progress")
	}
	if progress.blockNum != 1 || progress.lastTxNum != 4 {
		t.Fatalf("duplicate block changed progress to block=%d txNum=%d", progress.blockNum, progress.lastTxNum)
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

	invalidTxWrites := newWS().
		stor(addr, slot, state.Version{TxIndex: invalidTxIdx, Incarnation: invalidTxInc}, phantomVal).
		build()

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
	_, res, _ := vm.ReadStorage(addr, slot, downstreamTxIdx)

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

// Real concurrent errors surface and are joined with a real apply result.
// Cancellation-only values never mask or displace a real error on either side.
func TestReconcileExecErrors(t *testing.T) {
	waitFail := errors.New("snapshot step misalignment: snapshot files need rebuilding")

	surfacesLoudly := func(err error) bool {
		return err != nil && !commonerrors.IsOnlyCanceled(err)
	}

	t.Run("both nil", func(t *testing.T) {
		require.NoError(t, reconcileExecErrors(nil, nil))
	})

	t.Run("wait error with no apply error surfaces", func(t *testing.T) {
		got := reconcileExecErrors(nil, waitFail)
		require.Same(t, waitFail, got)
		require.True(t, surfacesLoudly(got))
	})

	t.Run("specific apply error is kept alongside the wait error", func(t *testing.T) {
		applyFail := errors.New("apply loop: open roTx: boom")
		got := reconcileExecErrors(applyFail, waitFail)
		require.ErrorIs(t, got, applyFail)
		require.ErrorIs(t, got, waitFail)
		require.True(t, surfacesLoudly(got))
	})

	t.Run("canceled wait after a clean batch stays clean", func(t *testing.T) {
		require.NoError(t, reconcileExecErrors(nil, context.Canceled))
	})

	t.Run("canceled wait does not contaminate a specific apply error", func(t *testing.T) {
		applyFail := errors.New("apply loop: boom")
		got := reconcileExecErrors(applyFail, fmt.Errorf("worker: %w", context.Canceled))
		require.Same(t, applyFail, got)
		require.NotErrorIs(t, got, context.Canceled,
			"joining a cancellation would flip execImpl's quiet-exit gate and skip the failure handling")
	})

	t.Run("wait error supersedes a canceled apply exit", func(t *testing.T) {
		got := reconcileExecErrors(context.Canceled, waitFail)
		require.Same(t, waitFail, got)
		require.True(t, surfacesLoudly(got),
			"a Canceled-classified aggregate is dropped by execImpl's gate and ExecModule.Start")
	})

	t.Run("wait error supersedes a wrapped canceled apply exit", func(t *testing.T) {
		got := reconcileExecErrors(fmt.Errorf("apply loop: open roTx: %w", context.Canceled), waitFail)
		require.Same(t, waitFail, got)
		require.True(t, surfacesLoudly(got))
	})

	t.Run("mixed canceled apply error keeps its real branch", func(t *testing.T) {
		applyFail := errors.New("apply loop: boom")
		got := reconcileExecErrors(errors.Join(context.Canceled, applyFail), waitFail)
		require.ErrorIs(t, got, applyFail)
		require.ErrorIs(t, got, waitFail)
		require.True(t, surfacesLoudly(got))
	})

	t.Run("mixed canceled wait error surfaces", func(t *testing.T) {
		got := reconcileExecErrors(nil, errors.Join(context.Canceled, waitFail))
		require.ErrorIs(t, got, waitFail)
		require.False(t, commonerrors.IsOnlyCanceled(got))
		require.True(t, surfacesLoudly(got))
	})
}

func TestIsOnlyLoopExhausted(t *testing.T) {
	exhausted := &ErrLoopExhausted{From: 1, To: 2, Reason: "block batch is full"}
	boom := errors.New("boom")

	require.False(t, isOnlyLoopExhausted(nil))
	require.True(t, isOnlyLoopExhausted(exhausted))
	require.True(t, isOnlyLoopExhausted(fmt.Errorf("apply loop: %w", exhausted)))
	require.False(t, isOnlyLoopExhausted(errors.Join(exhausted, context.Canceled)))
	require.False(t, isOnlyLoopExhausted(errors.Join(exhausted, boom)))
}

func TestRunApplyLoopPanicDrainsChannels(t *testing.T) {
	applyResults := make(chan applyResult, 1)
	commitResults := make(chan applyResult, 1)
	rootResults := make(chan commitmentResult, 1)
	applyResults <- &txResult{}
	rootResults <- commitmentResult{}

	executorCtx, cancelExecLoop := context.WithCancelCause(context.Background())
	pe := &parallelExecutor{
		txExecutor:     txExecutor{logger: log.New()},
		cancelExecLoop: cancelExecLoop,
	}
	blockExecutor := &blockExecutor{
		applyResults:  applyResults,
		commitResults: commitResults,
	}

	sendDone := make(chan error, 1)
	go func() {
		sendDone <- blockExecutor.sendResult(executorCtx, &blockResult{}, true)
		close(commitResults)
		close(applyResults)
	}()

	calculatorDone := make(chan struct{})
	go func() {
		defer close(calculatorDone)
		for range commitResults {
			rootResults <- commitmentResult{}
		}
		close(rootResults)
	}()

	emergencyDrain := func() {
		done := make(chan struct{})
		go func() {
			defer close(done)
			applyCh, rootCh := applyResults, rootResults
			for applyCh != nil || rootCh != nil {
				select {
				case _, ok := <-applyCh:
					if !ok {
						applyCh = nil
					}
				case _, ok := <-rootCh:
					if !ok {
						rootCh = nil
					}
				}
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("emergency channel drain hung")
		}
	}

	panicErr := errors.New("boom")
	handlerDone := make(chan error, 1)
	go func() {
		handlerDone <- pe.runApplyLoop("test", applyResults, rootResults, func() error {
			panic(panicErr)
		})
	}()

	var recoveredErr error
	select {
	case recoveredErr = <-handlerDone:
	case <-time.After(5 * time.Second):
		emergencyDrain()
		<-handlerDone
		t.Fatal("panic handler hung")
	}

	select {
	case err := <-sendDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		emergencyDrain()
		require.NoError(t, <-sendDone)
		<-calculatorDone
		t.Fatal("panic handler returned before the terminal send completed")
	}

	<-calculatorDone
	require.EqualError(t, recoveredErr, "apply loop panic: boom")
	require.NotErrorIs(t, recoveredErr, panicErr,
		"a recovered panic keeps its message but not its sentinel identity")
	require.Same(t, recoveredErr, context.Cause(executorCtx))
}

// A recovered panic is always an operational failure: the panic value keeps
// its message but never a sentinel identity, so it cannot classify as a block
// verdict, a resumable boundary, or routine cancellation anywhere upstream.
func TestRecoveredPanicError(t *testing.T) {
	cause := errors.New("boom")
	recoveredErr := recoveredPanicError("apply loop", cause)
	require.EqualError(t, recoveredErr, "apply loop panic: boom")
	require.NotErrorIs(t, recoveredErr, cause)

	require.EqualError(t, recoveredPanicError("exec loop", "boom"), "exec loop panic: boom")

	verdictPanic := recoveredPanicError("apply loop", fmt.Errorf("%w, block=5", ErrWrongTrieRoot))
	require.NotErrorIs(t, verdictPanic, rules.ErrInvalidBlock,
		"a panic must not carry a block verdict — verdicts come only from the fail-candidate")

	exhaustedPanic := recoveredPanicError("exec loop", &ErrLoopExhausted{From: 1, To: 2})
	require.NotErrorIs(t, exhaustedPanic, &ErrLoopExhausted{},
		"a panic must not read as a resumable boundary at the sync loop")
}

func TestRecoveredCancellationPanicIsFailure(t *testing.T) {
	recoveredErr := recoveredPanicError("exec loop", context.Canceled)

	require.EqualError(t, recoveredErr, "exec loop panic: context canceled")
	require.NotErrorIs(t, recoveredErr, context.Canceled)
	require.False(t, commonerrors.IsOnlyCanceled(recoveredErr))
	require.Same(t, recoveredErr, commonerrors.NilIfCanceled(recoveredErr))
}

func TestRunApplyLoopErrorDrainsChannels(t *testing.T) {
	applyResults := make(chan applyResult, 1)
	rootResults := make(chan commitmentResult)
	applyResults <- &txResult{}
	close(rootResults)

	executorCtx, cancelExecLoop := context.WithCancelCause(context.Background())
	pe := &parallelExecutor{
		txExecutor:     txExecutor{logger: log.New()},
		cancelExecLoop: cancelExecLoop,
	}

	sendStarted := make(chan struct{})
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		close(sendStarted)
		applyResults <- &blockResult{}
		close(applyResults)
	}()
	<-sendStarted

	boom := errors.New("apply loop: open roTx: boom")
	got := pe.runApplyLoop("test", applyResults, rootResults, func() error {
		return boom
	})

	select {
	case <-sendDone:
	case <-time.After(5 * time.Second):
		<-applyResults
		<-sendDone
		for range applyResults {
		}
		t.Fatal("ordinary apply-loop error returned before the terminal send completed")
	}

	require.Same(t, boom, got)
	require.Same(t, boom, context.Cause(executorCtx))
}

func TestRunApplyLoopExhaustionDoesNotCancelExecutor(t *testing.T) {
	applyResults := make(chan applyResult)
	rootResults := make(chan commitmentResult)
	close(applyResults)
	close(rootResults)

	executorCtx, cancelExecLoop := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancelExecLoop(nil) })
	pe := &parallelExecutor{
		txExecutor:     txExecutor{logger: log.New()},
		cancelExecLoop: cancelExecLoop,
	}
	exhausted := &ErrLoopExhausted{From: 1, To: 2, Reason: "block batch is full"}

	// A resumable boundary is recorded as a result and returns nil, so the
	// apply-loop exit must not publish any executor cancellation cause.
	got := pe.runApplyLoop("test", applyResults, rootResults, func() error {
		pe.exhausted = exhausted
		return nil
	})

	require.NoError(t, got)
	require.Same(t, exhausted, pe.exhausted)
	require.NoError(t, context.Cause(executorCtx))
}

func TestRunApplyLoopErrorAfterRecordedBoundaryDrainsChannels(t *testing.T) {
	applyResults := make(chan applyResult, 1)
	rootResults := make(chan commitmentResult)
	applyResults <- &txResult{}
	close(rootResults)

	executorCtx, cancelExecLoop := context.WithCancelCause(context.Background())
	pe := &parallelExecutor{
		txExecutor:     txExecutor{logger: log.New()},
		cancelExecLoop: cancelExecLoop,
	}

	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		applyResults <- &blockResult{}
		close(applyResults)
	}()

	// A real error after a boundary was already recorded must still cancel and
	// drain; the boundary itself never suppresses failure handling.
	boom := errors.New("apply loop: boom")
	exhausted := &ErrLoopExhausted{From: 1, To: 2, Reason: "block batch is full"}
	got := pe.runApplyLoop("test", applyResults, rootResults, func() error {
		pe.exhausted = exhausted
		return boom
	})

	select {
	case <-sendDone:
	case <-time.After(5 * time.Second):
		t.Fatal("apply-loop error exit did not drain the blocked terminal send")
	}

	require.Same(t, boom, got)
	require.Same(t, got, context.Cause(executorCtx))
}

type recordChannelHandler chan *log.Record

func (h recordChannelHandler) Log(record *log.Record) error {
	h <- record
	return nil
}

func (h recordChannelHandler) Enabled(context.Context, log.Lvl) bool { return true }

func TestWaitForTeardownPhase(t *testing.T) {
	newExecutor := func() (*parallelExecutor, <-chan *log.Record) {
		records := make(chan *log.Record, 1)
		logger := log.New()
		logger.SetHandler(recordChannelHandler(records))
		return &parallelExecutor{txExecutor: txExecutor{logger: logger, logPrefix: "test"}}, records
	}

	t.Run("apply loop drain warns without abandoning drain", func(t *testing.T) {
		const warnAfter = 10 * time.Millisecond
		pe, records := newExecutor()
		pe.cancelExecLoop = func(error) {}
		applyResults := make(chan applyResult)
		rootResults := make(chan commitmentResult)
		var releaseOnce sync.Once
		release := func() {
			releaseOnce.Do(func() {
				close(applyResults)
				close(rootResults)
			})
		}
		t.Cleanup(release)
		finished := make(chan struct{})

		go func() {
			pe.cancelAndDrainApplyLoop(warnAfter, errors.New("apply loop failed"), applyResults, rootResults)
			close(finished)
		}()

		select {
		case record := <-records:
			require.Equal(t, log.LvlWarn, record.Lvl)
			require.Contains(t, record.Msg, "executor teardown is still running")
			require.Equal(t, []any{"phase", "apply loop drain", "elapsed", warnAfter}, record.Ctx)
		case <-time.After(time.Second):
			t.Fatal("blocked apply-loop drain did not emit a warning")
		}

		select {
		case <-finished:
			t.Fatal("warning deadline abandoned the apply-loop drain")
		default:
		}

		release()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Fatal("apply-loop drain did not return after both channels closed")
		}
	})

	t.Run("slow phase warns without returning", func(t *testing.T) {
		const warnAfter = 10 * time.Millisecond
		pe, records := newExecutor()
		gate := make(chan struct{})
		var releaseOnce sync.Once
		release := func() { releaseOnce.Do(func() { close(gate) }) }
		t.Cleanup(release)
		started := make(chan struct{})
		finished := make(chan struct{})

		go func() {
			pe.waitForTeardownPhase(warnAfter, "worker pool", func() {
				close(started)
				<-gate
			})
			close(finished)
		}()
		<-started

		select {
		case record := <-records:
			require.Equal(t, log.LvlWarn, record.Lvl)
			require.Contains(t, record.Msg, "executor teardown is still running")
			require.Equal(t, []any{"phase", "worker pool", "elapsed", warnAfter}, record.Ctx)
		case <-time.After(5 * time.Second):
			t.Fatal("slow teardown phase did not emit a warning")
		}

		select {
		case <-finished:
			t.Fatal("warning deadline abandoned the teardown wait")
		default:
		}

		release()
		select {
		case <-finished:
		case <-time.After(5 * time.Second):
			t.Fatal("teardown phase did not return after its work completed")
		}
	})

	t.Run("completed phase stays silent", func(t *testing.T) {
		pe, records := newExecutor()
		pe.waitForTeardownPhase(time.Hour, "executor group", func() {})

		select {
		case record := <-records:
			t.Fatalf("completed teardown phase emitted a warning: %s", record.Msg)
		default:
		}
	})
}

// wait suppresses cancellation-only results and joins every group member.
func TestParallelExecWait(t *testing.T) {
	newPE := func(group func() error) *parallelExecutor {
		pe := &parallelExecutor{}
		pe.execLoopGroup, _ = commonerrors.NewGroup(context.Background())
		pe.execLoopGroup.Go(group)
		return pe
	}

	t.Run("real group error surfaces", func(t *testing.T) {
		boom := errors.New("exec blocks error: boom")
		pe := newPE(func() error { return boom })
		require.Same(t, boom, pe.wait())
	})

	t.Run("canceled group is routine teardown", func(t *testing.T) {
		pe := newPE(func() error { return fmt.Errorf("drain: %w", context.Canceled) })
		require.NoError(t, pe.wait())
	})

	t.Run("nil group is a no-op", func(t *testing.T) {
		require.NoError(t, (&parallelExecutor{}).wait())
	})

	t.Run("wait joins every member, error or not", func(t *testing.T) {
		boom := errors.New("exec blocks error: boom")
		var joined atomic.Bool
		pe := newPE(func() error { return boom })
		pe.execLoopGroup.Go(func() error {
			time.Sleep(20 * time.Millisecond)
			joined.Store(true)
			return nil
		})
		require.Same(t, boom, pe.wait())
		require.True(t, joined.Load(),
			"every group member must be joined before wait returns — execImpl reads shared state next")
	})

	t.Run("worker-pool member error surfaces through wait", func(t *testing.T) {
		boom := errors.New("exec.Worker panic: boom")
		pe := newPE(func() error { return nil })
		pe.execLoopGroup.Go(func() error {
			return joinWorkers(func() error { return boom })
		})
		require.ErrorIs(t, pe.wait(), boom)
	})

	t.Run("independent real member errors are both preserved", func(t *testing.T) {
		first := errors.New("exec blocks error: first")
		second := errors.New("worker pool: second")
		pe := newPE(func() error { return first })
		pe.execLoopGroup.Go(func() error {
			time.Sleep(20 * time.Millisecond)
			return second
		})
		got := pe.wait()
		require.ErrorIs(t, got, first)
		require.ErrorIs(t, got, second,
			"a second independent failure must not be lost to the first-error slot")
	})
}

// A send on a closed result channel is a channel-ownership bug: it must panic
// loudly instead of being classified as routine cancellation, which the group
// join would filter into silent result loss.
func TestSendResultOnClosedChannelPanics(t *testing.T) {
	applyResults := make(chan applyResult, 1)
	close(applyResults)
	be := &blockExecutor{applyResults: applyResults}
	require.Panics(t, func() {
		_ = be.sendResult(context.Background(), &blockResult{}, true)
	})
}

// classifyApplyExit decides what the recorded fail-candidate means for the
// executor outcome: block verdicts travel as data, infrastructure faults stay
// operational errors.
func TestClassifyApplyExit(t *testing.T) {
	t.Parallel()

	t.Run("unset candidate is clean", func(t *testing.T) {
		verdict, err := classifyApplyExit(failCandidate{})
		require.Nil(t, verdict)
		require.NoError(t, err)
	})

	t.Run("exec verdict becomes the block's verdict", func(t *testing.T) {
		var fail failCandidate
		fail.consider(7, common.HexToHash("0xbad7"), true,
			fmt.Errorf("%w: could not apply tx, block=7", rules.ErrInvalidBlock))
		verdict, err := classifyApplyExit(fail)
		require.NoError(t, err)
		require.NotNil(t, verdict)
		require.Equal(t, uint64(7), verdict.blockNum)
		require.Equal(t, common.HexToHash("0xbad7"), verdict.blockHash)
		require.ErrorIs(t, verdict.err, rules.ErrInvalidBlock)
	})

	t.Run("wrong root becomes the block's verdict", func(t *testing.T) {
		var fail failCandidate
		fail.consider(9, common.HexToHash("0xbad9"), false, fmt.Errorf("%w, block=9", ErrWrongTrieRoot))
		verdict, err := classifyApplyExit(fail)
		require.NoError(t, err)
		require.NotNil(t, verdict)
		require.ErrorIs(t, verdict.err, ErrWrongTrieRoot)
		require.ErrorIs(t, verdict.err, rules.ErrInvalidBlock)
	})

	t.Run("infrastructure fault stays an operational error", func(t *testing.T) {
		var fail failCandidate
		boom := errors.New("marshal transactions for accumulator, block 3: boom")
		fail.consider(3, common.HexToHash("0x03"), true, boom)
		verdict, err := classifyApplyExit(fail)
		require.Nil(t, verdict, "an infrastructure fault is not a statement about the block")
		require.ErrorIs(t, err, boom)
	})
}

// joinWorkers labels pool failures; cancellation filtering is the group Wait's
// job, pinned by TestParallelExecWait.
func TestJoinWorkers(t *testing.T) {
	require.NoError(t, joinWorkers(func() error { return nil }))

	boom := errors.New("exec.Worker panic: boom")
	got := joinWorkers(func() error { return boom })
	require.ErrorIs(t, got, boom)
	require.EqualError(t, got, "worker pool: exec.Worker panic: boom")
}

// errgroup keeps its first non-nil error, so members must filter cancellation.
func TestCanceledMemberCannotMaskRealError(t *testing.T) {
	boom := errors.New("exec.Worker panic: boom")

	t.Run("raw cancellation occupies the first-error slot", func(t *testing.T) {
		g, groupCtx := errgroup.WithContext(context.Background())
		g.Go(func() error { return context.Canceled })
		g.Go(func() error {
			<-groupCtx.Done()
			return boom
		})
		got := g.Wait()
		require.ErrorIs(t, got, context.Canceled)
		require.NotErrorIs(t, got, boom,
			"errgroup keeps the first non-nil return — the raw Canceled masks the real error")
	})

	t.Run("a raw canceled member cannot mask a late real worker error", func(t *testing.T) {
		canceledReturned := make(chan struct{})
		pe := &parallelExecutor{}
		pe.execLoopGroup, _ = commonerrors.NewGroup(context.Background())
		pe.execLoopGroup.Go(func() error {
			defer close(canceledReturned)
			return context.Canceled
		})
		pe.execLoopGroup.Go(func() error {
			<-canceledReturned
			return joinWorkers(func() error { return boom })
		})
		require.ErrorIs(t, pe.wait(), boom,
			"members must not need to self-filter cancellation for real failures to survive")
	})
}

// A recorded failure must take precedence over missing terminal results because
// cancellation for that failure can prevent later block results from arriving.
func TestResolveApplyLoopClosePrecedence(t *testing.T) {
	run := func(ctx context.Context, infraErr error, fail failCandidate, txResultBlocks, appliedBlocks map[uint64]struct{}) (*parallelExecutor, error) {
		pe := &parallelExecutor{maxBlockNum: 10}
		err := pe.resolveApplyLoopClose(ctx, infraErr, fail, nil, 1, 5, txResultBlocks, appliedBlocks)
		return pe, err
	}

	mkSet := func(ns ...uint64) map[uint64]struct{} {
		s := make(map[uint64]struct{}, len(ns))
		for _, n := range ns {
			s[n] = struct{}{}
		}
		return s
	}

	t.Run("deferred root + missing block — root verdict wins", func(t *testing.T) {
		rootErr := fmt.Errorf("%w, block=5", ErrWrongTrieRoot)
		var fail failCandidate
		fail.consider(5, common.HexToHash("0x05"), false, rootErr)

		pe, err := run(context.Background(), nil, fail, mkSet(5, 6), mkSet(5))
		require.NoError(t, err)
		require.NotNil(t, pe.verdict)
		require.ErrorIs(t, pe.verdict.err, ErrWrongTrieRoot)
		require.Equal(t, rootErr.Error(), pe.verdict.err.Error())
		require.Nil(t, pe.exhausted)
	})

	t.Run("infrastructure fault + missing block — infrastructure fault wins", func(t *testing.T) {
		infraErr := errors.New("worker pool failed")
		pe, err := run(context.Background(), infraErr, failCandidate{}, mkSet(5, 6), mkSet(5))
		require.ErrorIs(t, err, infraErr)
		require.NotContains(t, err.Error(), "without a blockResult")
		require.Nil(t, pe.verdict)
		require.Nil(t, pe.exhausted)
	})

	t.Run("missing block only — operational error stands", func(t *testing.T) {
		pe, err := run(context.Background(), nil, failCandidate{}, mkSet(5, 6), mkSet(5))
		require.Error(t, err)
		require.NotErrorIs(t, err, rules.ErrInvalidBlock)
		require.NotErrorIs(t, err, ErrWrongTrieRoot)
		require.Contains(t, err.Error(), "without a blockResult")
		require.Nil(t, pe.verdict)
		require.Nil(t, pe.exhausted)
	})

	t.Run("missing block during routine cancellation returns cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := run(ctx, nil, failCandidate{}, mkSet(5, 6), mkSet(5))
		require.ErrorIs(t, err, context.Canceled)
		require.True(t, commonerrors.IsOnlyCanceled(err))
		require.ErrorContains(t, err, "without a blockResult: [6]")
	})

	t.Run("missing block during cancellation preserves a real cause", func(t *testing.T) {
		cause := errors.New("worker failed")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)

		_, err := run(ctx, nil, failCandidate{}, mkSet(5, 6), mkSet(5))
		require.ErrorIs(t, err, cause)
		require.False(t, commonerrors.IsOnlyCanceled(err))
		require.ErrorContains(t, err, "without a blockResult: [6]")
	})

	t.Run("deferred root + no missing block — root verdict surfaces", func(t *testing.T) {
		rootErr := fmt.Errorf("%w, block=5", ErrWrongTrieRoot)
		var fail failCandidate
		fail.consider(5, common.HexToHash("0x05"), false, rootErr)

		pe, err := run(context.Background(), nil, fail, mkSet(5), mkSet(5))
		require.NoError(t, err)
		require.NotNil(t, pe.verdict)
		require.ErrorIs(t, pe.verdict.err, ErrWrongTrieRoot)
		require.Nil(t, pe.exhausted)
	})

	t.Run("complete observed stream falls through to resumable boundary", func(t *testing.T) {
		pe, err := run(context.Background(), nil, failCandidate{}, mkSet(5), mkSet(5))
		require.NoError(t, err)
		require.Nil(t, pe.verdict)
		require.NotNil(t, pe.exhausted)
	})
}

// Undrained work is an executor failure, not proof that a block is invalid.
func TestCheckBlocksDrained(t *testing.T) {
	withPending := func() *parallelExecutor {
		pe := &parallelExecutor{}
		pe.blockExecutors = map[uint64]*blockExecutor{3: {}}
		return pe
	}

	t.Run("undrained block is an operational error", func(t *testing.T) {
		err := withPending().checkBlocksDrained(context.Background(), context.Background(), nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, rules.ErrInvalidBlock)
	})

	t.Run("undrained block numbers appear in the error", func(t *testing.T) {
		pe := &parallelExecutor{}
		pe.blockExecutors = map[uint64]*blockExecutor{3: {}, 7: {}}
		err := pe.checkBlocksDrained(context.Background(), context.Background(), nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, rules.ErrInvalidBlock)
		require.Contains(t, err.Error(), "3")
		require.Contains(t, err.Error(), "7")
	})

	t.Run("clean exit with everything drained is fine", func(t *testing.T) {
		pe := &parallelExecutor{}
		pe.blockExecutors = map[uint64]*blockExecutor{}
		require.NoError(t, pe.checkBlocksDrained(context.Background(), context.Background(), nil))
	})

	t.Run("nil map is fine", func(t *testing.T) {
		require.NoError(t, (&parallelExecutor{}).checkBlocksDrained(context.Background(), context.Background(), nil))
	})

	t.Run("canceled batch leaves undrained blocks alone", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		require.NoError(t, withPending().checkBlocksDrained(ctx, context.Background(), nil))
	})

	t.Run("real parent cancellation cause does not hide an undrained block", func(t *testing.T) {
		cause := errors.New("parent execution failed")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)

		err := reconcileParentCause(ctx, withPending().checkBlocksDrained(ctx, context.Background(), nil))
		require.ErrorIs(t, err, cause)
		require.ErrorContains(t, err, "never reached apply-loop validation: [3]")
	})

	t.Run("real parent cause preserves an existing error", func(t *testing.T) {
		execErr := errors.New("apply loop failed")
		cause := errors.New("parent execution failed")
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(cause)

		err := reconcileParentCause(ctx, withPending().checkBlocksDrained(ctx, context.Background(), execErr))
		require.ErrorIs(t, err, execErr)
		require.ErrorIs(t, err, cause)
	})

	t.Run("plain parent cancellation stays routine through the pair", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := reconcileParentCause(ctx, withPending().checkBlocksDrained(ctx, context.Background(), nil))
		require.NoError(t, err)
	})

	t.Run("bad-block stop without a recorded verdict does not exempt", func(t *testing.T) {
		// A bad-block stop always records a verdict or an operational failure
		// first (deliberateCancel closes over the fail-candidate), so a bare
		// stopBadBlock cause with pending blocks means the failure was lost —
		// report the lost work instead of trusting the cause alone.
		ectx, cancel := context.WithCancelCause(context.Background())
		cancel(&stopCause{block: 5, kind: stopBadBlock, err: errors.New("wrong root")})
		err := withPending().checkBlocksDrained(context.Background(), ectx, nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, rules.ErrInvalidBlock)
	})

	t.Run("reached-max stop with an undrained block still flags", func(t *testing.T) {
		ectx, cancel := context.WithCancelCause(context.Background())
		cancel(&stopCause{block: 5, kind: stopReachedMax})
		err := withPending().checkBlocksDrained(context.Background(), ectx, nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, rules.ErrInvalidBlock)
	})

	t.Run("routine boundary executorCancel does not exempt", func(t *testing.T) {
		// A nil cancel cause becomes context.Canceled, not a deliberate stop.
		ectx, cancel := context.WithCancelCause(context.Background())
		cancel(nil)
		err := withPending().checkBlocksDrained(context.Background(), ectx, nil)
		require.Error(t, err)
		require.NotErrorIs(t, err, rules.ErrInvalidBlock)
	})

	t.Run("recorded verdict leaves undrained follow-on blocks alone", func(t *testing.T) {
		// A verdict deliberately abandons queued work even when no stop cause
		// was published (the exec loop self-exits after an errored block).
		pe := withPending()
		pe.verdict = &blockVerdict{blockNum: 2, blockHash: common.HexToHash("0x02"), err: rules.ErrInvalidBlock}
		require.NoError(t, pe.checkBlocksDrained(context.Background(), context.Background(), nil))
	})

	t.Run("cancellation-only exec error does not hide an undrained block", func(t *testing.T) {
		execErr := fmt.Errorf("apply loop: %w", context.Canceled)
		err := withPending().checkBlocksDrained(context.Background(), context.Background(), execErr)
		require.ErrorContains(t, err, "never reached apply-loop validation: [3]")
		require.False(t, commonerrors.IsOnlyCanceled(err))
	})

	t.Run("more-work stop leaves undrained follow-on blocks alone", func(t *testing.T) {
		// A batch boundary deliberately cancels queued follow-on blocks.
		ectx, cancel := context.WithCancelCause(context.Background())
		cancel(&stopCause{block: 5, kind: stopMoreWork})
		require.NoError(t, withPending().checkBlocksDrained(context.Background(), ectx, nil))
	})

	t.Run("existing error is not masked", func(t *testing.T) {
		boom := errors.New("snapshot step misalignment")
		got := withPending().checkBlocksDrained(context.Background(), context.Background(), boom)
		require.Same(t, boom, got)
	})
}

// TestStopCausePropagation pins the mechanism the unified shutdown rests on: a
// stopCause published on a context is readable via stopCauseOf, survives a child
// context (as coordCtx does through errgroup.WithContext), and is distinguished
// from an unrelated cancel cause.
func TestStopCausePropagation(t *testing.T) {
	t.Run("round-trips through a child context", func(t *testing.T) {
		parent, cancel := context.WithCancelCause(context.Background())
		child, childCancel := context.WithCancel(parent)
		defer childCancel()
		cancel(&stopCause{block: 42, kind: stopReachedMax})

		sc, ok := stopCauseOf(child)
		require.True(t, ok, "stopCause must be visible through the child context")
		require.Equal(t, uint64(42), sc.block)
		require.Equal(t, stopReachedMax, sc.kind)
	})

	t.Run("no cause before cancel", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		defer cancel(nil)
		_, ok := stopCauseOf(ctx)
		require.False(t, ok, "an un-cancelled context carries no stopCause")
	})

	t.Run("unrelated cause is not a stopCause", func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(context.Background())
		cancel(errors.New("shutdown"))
		_, ok := stopCauseOf(ctx)
		require.False(t, ok, "a plain cancel cause must not read as a stopCause")
	})
}

// Pins wrapAsExecAbort: a real underlying err must survive as OriginError
// (or remain a true nil interface), never be substituted by a zero
// ErrExecAbortError whose Error() reads "execution aborted due to dependency 0".
func TestWrapAsExecAbort_PreservesOriginError(t *testing.T) {
	realErr := errors.New("engine.Initialize: validator set call reverted")
	tests := []struct {
		name       string
		origErr    error
		depTxIndex int
		check      func(t *testing.T, got error)
	}{
		{
			name:       "nil err is wrapped with nil OriginError (no bogus dep-0 string)",
			origErr:    nil,
			depTxIndex: 5,
			check: func(t *testing.T, got error) {
				abort, ok := got.(protocol.ErrExecAbortError)
				require.True(t, ok)
				require.Equal(t, 5, abort.DependencyTxIndex)
				require.Nil(t, abort.OriginError,
					"OriginError must be a true nil interface so IsError() reports false")
				require.False(t, abort.IsError(),
					"a wrapped nil err must NOT classify as a genuine execution error")
			},
		},
		{
			name:       "non-abort err survives as OriginError",
			origErr:    realErr,
			depTxIndex: 0,
			check: func(t *testing.T, got error) {
				abort, ok := got.(protocol.ErrExecAbortError)
				require.True(t, ok)
				require.Equal(t, 0, abort.DependencyTxIndex)
				require.True(t, abort.IsError())
				require.Equal(t, realErr.Error(), abort.OriginError.Error(),
					"real err must reach OriginError verbatim, not be replaced by "+
						"a zero ErrExecAbortError whose Error() reads as "+
						"\"execution aborted due to dependency 0\"")
			},
		},
		{
			name:       "already-wrapped err is returned unchanged",
			origErr:    protocol.ErrExecAbortError{DependencyTxIndex: 7, OriginError: nil},
			depTxIndex: 99,
			check: func(t *testing.T, got error) {
				abort, ok := got.(protocol.ErrExecAbortError)
				require.True(t, ok)
				require.Equal(t, 7, abort.DependencyTxIndex,
					"depTxIndex of the passed-through err must not be overwritten")
				require.Nil(t, abort.OriginError)
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.check(t, wrapAsExecAbort(tc.origErr, tc.depTxIndex))
		})
	}
}

// Operational faults surface unconditionally; a coincident verdict is still
// recorded for the withholding log. The block-ranked candidate holds verdicts
// only, so an infrastructure fault can never be displaced by one.
func TestClassifyApplyFailures(t *testing.T) {
	t.Parallel()
	verdictErr := fmt.Errorf("%w: bad receipts, block=3", rules.ErrInvalidBlock)
	infra := errors.New("commitment: lazy load failed")

	t.Run("infra fault survives a coincident verdict", func(t *testing.T) {
		var fail failCandidate
		fail.consider(3, common.HexToHash("0x03"), true, verdictErr)
		verdict, opErr := classifyApplyFailures(infra, fail)
		require.NotNil(t, verdict, "the verdict stays recorded for the withholding log")
		require.ErrorIs(t, opErr, infra,
			"an unhealthy run must fail operationally, not report INVALID")
	})

	t.Run("verdict alone stays a clean exit", func(t *testing.T) {
		var fail failCandidate
		fail.consider(3, common.HexToHash("0x03"), true, verdictErr)
		verdict, opErr := classifyApplyFailures(nil, fail)
		require.NotNil(t, verdict)
		require.NoError(t, opErr)
	})

	t.Run("infra fault alone is operational", func(t *testing.T) {
		verdict, opErr := classifyApplyFailures(infra, failCandidate{})
		require.Nil(t, verdict)
		require.ErrorIs(t, opErr, infra)
	})

	t.Run("nothing recorded is clean", func(t *testing.T) {
		verdict, opErr := classifyApplyFailures(nil, failCandidate{})
		require.Nil(t, verdict)
		require.NoError(t, opErr)
	})
}
