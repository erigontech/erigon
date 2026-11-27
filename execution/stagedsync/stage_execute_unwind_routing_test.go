package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/protocol/rules"
)

type recordingUnwinder struct {
	calls  []recordedUnwind
	retErr error
}

type recordedUnwind struct {
	point  uint64
	reason UnwindReason
}

func (r *recordingUnwinder) UnwindTo(point uint64, reason UnwindReason, tx kv.Tx) error {
	r.calls = append(r.calls, recordedUnwind{point, reason})
	return r.retErr
}
func (r *recordingUnwinder) HasUnwindPoint() bool { return len(r.calls) > 0 }
func (r *recordingUnwinder) LogPrefix() string    { return "test" }

func TestFinalizeExecV3Outcome(t *testing.T) {
	t.Parallel()

	exhausted := &ErrLoopExhausted{From: 1, To: 2}
	boom := errors.New("finalization failed")

	t.Run("failure clears the resumable boundary", func(t *testing.T) {
		out, err := finalizeExecV3Outcome(execV3Outcome{exhausted: exhausted}, boom)
		require.ErrorIs(t, err, boom)
		require.Nil(t, out.exhausted)
	})

	t.Run("success preserves the resumable boundary", func(t *testing.T) {
		out, err := finalizeExecV3Outcome(execV3Outcome{exhausted: exhausted}, nil)
		require.NoError(t, err)
		require.Same(t, exhausted, out.exhausted)
	})
}

// TestRenderExecOutcome pins the stage-boundary routing of the parallel executor's
// typed outcome: operational error outranks a verdict outranks exhaustion, and
// which unwind action a given (execErr, outcome, cycle) triggers. The wrong-root
// binary-search keys off the implicated block (out.verdict.blockNum).
func TestRenderExecOutcome(t *testing.T) {
	t.Parallel()
	logger := log.New()

	plainInvalid := fmt.Errorf("%w: gas mismatch", rules.ErrInvalidBlock)
	wrongRoot := fmt.Errorf("%w, block=5", ErrWrongTrieRoot)

	t.Run("clean outcome unwinds nothing", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		got := renderExecOutcome(nil, execV3Outcome{}, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Empty(t, u.calls)
	})

	t.Run("typed exhaustion surfaces as ErrLoopExhausted, unwinds nothing", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		exhausted := &ErrLoopExhausted{From: 1, To: 2}
		got := renderExecOutcome(nil, execV3Outcome{exhausted: exhausted}, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, exhausted)
		require.Empty(t, u.calls)
	})

	t.Run("operational error wins over a resumable boundary", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		boom := errors.New("worker failed")
		out := execV3Outcome{exhausted: &ErrLoopExhausted{From: 1, To: 2}}
		got := renderExecOutcome(boom, out, ExecuteBlockCfg{}, s, u, logger)
		require.Same(t, boom, got,
			"a real failure must fail the stage, or the sync loop retries it as a clean partial batch forever")
		require.Empty(t, u.calls)
	})

	t.Run("verdict wins over a resumable boundary", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		out := execV3Outcome{
			verdict:   &blockVerdict{blockNum: 12, blockHash: common.HexToHash("0xbad12"), err: plainInvalid},
			exhausted: &ErrLoopExhausted{From: 1, To: 2},
		}
		got := renderExecOutcome(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.NotErrorIs(t, got, &ErrLoopExhausted{})
		require.Empty(t, u.calls)
	})

	t.Run("operational error outranks a recorded verdict", func(t *testing.T) {
		// The fix: a joined operational+invalid can no longer read as a verdict. An
		// operational failure present alongside a recorded verdict surfaces the
		// operational error (retryable, no unwind), not the bad-block verdict.
		u := &recordingUnwinder{}
		s := &StageState{}
		boom := fmt.Errorf("worker reopen roTx: boom")
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 5, err: plainInvalid}}
		got := renderExecOutcome(boom, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, boom)
		require.NotErrorIs(t, got, rules.ErrInvalidBlock, "operational failure must not surface as an invalid block")
		require.Empty(t, u.calls)
	})

	t.Run("plain invalid verdict propagates without setting an unwind point", func(t *testing.T) {
		// A plain invalid block propagates the error and lets the caller own the
		// unwind: setting a stage unwind point here would leave a stale bad-block
		// verdict that blocks a fresh canonical block at the same height from
		// re-executing on the next fork-choice.
		u := &recordingUnwinder{}
		s := &StageState{}
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 5, err: plainInvalid}}
		got := renderExecOutcome(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("badBlockHalt suppresses unwind and propagates", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		cfg := ExecuteBlockCfg{badBlockHalt: true}
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 5, err: plainInvalid}}
		got := renderExecOutcome(nil, out, cfg, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("nil unwinder suppresses unwind and propagates", func(t *testing.T) {
		s := &StageState{}
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 5, err: plainInvalid}}
		got := renderExecOutcome(nil, out, ExecuteBlockCfg{}, s, nil, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
	})

	t.Run("badBlockHalt suppresses the wrong-root unwind and propagates the verdict", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		cfg := ExecuteBlockCfg{badBlockHalt: true}
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 20, blockHash: common.HexToHash("0xbad20"), err: wrongRoot}}
		got := renderExecOutcome(nil, out, cfg, s, u, logger)
		require.Same(t, wrongRoot, got)
		require.Empty(t, u.calls)
	})

	t.Run("nil unwinder suppresses the wrong-root unwind and propagates the verdict", func(t *testing.T) {
		s := &StageState{}
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 20, blockHash: common.HexToHash("0xbad20"), err: wrongRoot}}
		got := renderExecOutcome(nil, out, ExecuteBlockCfg{}, s, nil, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.ErrorIs(t, got, ErrWrongTrieRoot)
	})

	t.Run("wrong root on non-initial cycle takes the binary-search path from the implicated block", func(t *testing.T) {
		u := &recordingUnwinder{}
		// blockNum (5) <= s.BlockNumber (10) makes handleIncorrectRootHashError
		// return nil at its guard, before touching applyTx. If the routing wrongly
		// fell through to the plain branch it would UnwindTo(lastHeader-1)=19.
		s := &StageState{BlockNumber: 10}
		s.CurrentSyncCycle.IsInitialCycle = false
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 5, blockHash: common.HexToHash("0xdead"), err: wrongRoot}}
		got := renderExecOutcome(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Empty(t, u.calls, "must not take the plain lastHeader-1 unwind branch")
	})

	t.Run("wrong root on initial cycle is fatal and not routed through the recovery handler", func(t *testing.T) {
		// On the initial cycle there is no fork to recover from, and
		// handleIncorrectRootHashError would return nil for blockNum (5) <=
		// s.BlockNumber (10), hiding the mismatch. The wrong trie root must
		// propagate as a fatal error instead, scheduling no unwind.
		u := &recordingUnwinder{}
		s := &StageState{BlockNumber: 10}
		s.CurrentSyncCycle.IsInitialCycle = true
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 5, blockHash: common.HexToHash("0xdead"), err: wrongRoot}}
		got := renderExecOutcome(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, ErrWrongTrieRoot)
		require.Empty(t, u.calls, "must not schedule an unwind on the initial cycle")
	})

	t.Run("wrong root on non-initial cycle schedules the binary-search unwind", func(t *testing.T) {
		// Implicated block 15 > s.BlockNumber 5 takes the scheduled-unwind branch:
		// UnwindTo(15 - (15-5)/2). The ChangeSets3 key at block 4 keeps the target
		// unwindable, so it is not clamped.
		dirs := datadir.New(t.TempDir())
		db := temporaltest.NewTestDB(t, dirs, temporaltest.WithStepSize(10_000))
		tx, err := db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		require.NoError(t, tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(4, common.Hash{0x01}), []byte{0x01}))

		u := &recordingUnwinder{}
		s := &StageState{BlockNumber: 5}
		s.CurrentSyncCycle.IsInitialCycle = false
		out := execV3Outcome{verdict: &blockVerdict{blockNum: 15, blockHash: common.HexToHash("0xf00d"), err: wrongRoot}, applyTx: tx}
		got := renderExecOutcome(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Len(t, u.calls, 1)
		require.Equal(t, uint64(10), u.calls[0].point, "unwind target = implicated - (implicated-s.BlockNumber)/2")
	})
}
