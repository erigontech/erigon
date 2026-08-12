package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/types"
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

func headerAt(n uint64) *types.Header {
	return &types.Header{Number: *uint256.NewInt(n)}
}

// TestUnwindOnExecError pins the stage-boundary rendering of the executor
// outcome: an operational error passes through and is never a block verdict; a
// verdict propagates as its rules.ErrInvalidBlock-wrapping error, with only a
// non-initial-cycle wrong root consumed by handleIncorrectRootHashError's
// binary-search unwind keyed off the implicated block.
func TestUnwindOnExecError(t *testing.T) {
	t.Parallel()
	logger := log.New()

	plainInvalidVerdict := func(block uint64, hash common.Hash) *blockVerdict {
		return &blockVerdict{blockNum: block, blockHash: hash, err: fmt.Errorf("%w: gas mismatch, block=%d", rules.ErrInvalidBlock, block)}
	}
	wrongRootVerdict := func(block uint64, hash common.Hash) *blockVerdict {
		return &blockVerdict{blockNum: block, blockHash: hash, err: fmt.Errorf("%w, block=%d", ErrWrongTrieRoot, block)}
	}

	t.Run("clean outcome unwinds nothing", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		got := unwindOnExecError(nil, execV3Outcome{lastHeader: headerAt(20)}, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Empty(t, u.calls)
	})

	t.Run("ErrLoopExhausted passes through and unwinds nothing", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		err := &ErrLoopExhausted{From: 1, To: 2}
		got := unwindOnExecError(err, execV3Outcome{lastHeader: headerAt(20)}, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, err)
		require.Empty(t, u.calls)
	})

	t.Run("operational error passes through and is not a bad-block verdict", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		boom := errors.New("worker pool: exec.Worker panic: boom")
		got := unwindOnExecError(boom, execV3Outcome{}, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, boom)
		require.NotErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("operational error wins over a stale verdict in the outcome", func(t *testing.T) {
		// execV3 never returns both (the executor withholds the verdict on a
		// failed run); the renderer still must not turn a failure into a verdict.
		u := &recordingUnwinder{}
		s := &StageState{}
		boom := errors.New("worker failed")
		out := execV3Outcome{verdict: wrongRootVerdict(5, common.HexToHash("0xdead"))}
		got := unwindOnExecError(boom, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, boom)
		require.NotErrorIs(t, got, rules.ErrInvalidBlock,
			"an operational failure must not be reported as a bad block")
		require.Empty(t, u.calls)
	})

	t.Run("plain invalid verdict propagates without setting a stage unwind point", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		// The caller owns the unwind for a plain invalid block: a stage unwind
		// point here would leave a stale bad-block verdict that blocks a fresh
		// canonical block at the same height on the next fork-choice.
		out := execV3Outcome{lastHeader: headerAt(19), verdict: plainInvalidVerdict(20, common.HexToHash("0xbad20"))}
		got := unwindOnExecError(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("plain invalid verdict on the first block of the batch propagates without unwinding", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		out := execV3Outcome{verdict: plainInvalidVerdict(12, common.HexToHash("0xbad12"))}
		got := unwindOnExecError(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("badBlockHalt suppresses unwind and propagates the verdict", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		cfg := ExecuteBlockCfg{badBlockHalt: true}
		out := execV3Outcome{lastHeader: headerAt(20), verdict: wrongRootVerdict(20, common.HexToHash("0xbad20"))}
		got := unwindOnExecError(nil, out, cfg, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.ErrorIs(t, got, ErrWrongTrieRoot)
		require.Empty(t, u.calls)
	})

	t.Run("nil unwinder suppresses unwind and propagates the verdict", func(t *testing.T) {
		s := &StageState{}
		out := execV3Outcome{lastHeader: headerAt(20), verdict: wrongRootVerdict(20, common.HexToHash("0xbad20"))}
		got := unwindOnExecError(nil, out, ExecuteBlockCfg{}, s, nil, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
	})

	t.Run("wrong root on non-initial cycle takes the binary-search path from the implicated block", func(t *testing.T) {
		u := &recordingUnwinder{}
		// implicated block (5) <= s.BlockNumber (10) makes handleIncorrectRootHashError
		// return nil at its guard, before touching applyTx. If the routing wrongly
		// fell through to the plain branch it would propagate the verdict error.
		s := &StageState{BlockNumber: 10}
		s.CurrentSyncCycle.IsInitialCycle = false
		out := execV3Outcome{lastHeader: headerAt(20), verdict: wrongRootVerdict(5, common.HexToHash("0xdead"))}
		got := unwindOnExecError(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Empty(t, u.calls, "must not take any other unwind branch")
	})

	t.Run("wrong root on initial cycle is fatal (no fork to recover from)", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{BlockNumber: 10}
		s.CurrentSyncCycle.IsInitialCycle = true
		// Initial sync has no competing fork: the wrong root must propagate as a
		// fatal error, not route to handleIncorrectRootHashError (which would return
		// nil here — implicated block (5) <= s.BlockNumber (10) — and swallow it).
		out := execV3Outcome{lastHeader: headerAt(20), verdict: wrongRootVerdict(5, common.HexToHash("0xbad5"))}
		got := unwindOnExecError(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, ErrWrongTrieRoot, "initial-cycle wrong root must stay fatal")
		require.Empty(t, u.calls, "must not schedule any unwind")
	})

	t.Run("wrong root on non-initial cycle schedules the binary-search unwind", func(t *testing.T) {
		// implicated block (15) > s.BlockNumber (5) takes handleIncorrectRootHashError's
		// scheduled-unwind branch: jump = (15-5)/2 = 5 -> UnwindTo(10). Needs a real
		// temporal tx: one 40-byte ChangeSets3 key pins the lowest unwindable block
		// to 4, so CanUnwindToBlockNum = 3 <= 10 and the target is not clamped.
		dirs := datadir.New(t.TempDir())
		db := temporaltest.NewTestDBWithStepSize(t, dirs, 10_000)
		tx, err := db.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		require.NoError(t, tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(4, common.Hash{0x01}), []byte{0x01}))

		u := &recordingUnwinder{}
		s := &StageState{BlockNumber: 5}
		s.CurrentSyncCycle.IsInitialCycle = false
		out := execV3Outcome{lastHeader: headerAt(20), verdict: wrongRootVerdict(15, common.HexToHash("0xf00d")), applyTx: tx}
		got := unwindOnExecError(nil, out, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Len(t, u.calls, 1)
		require.Equal(t, uint64(10), u.calls[0].point, "unwind target = implicated - (implicated-s.BlockNumber)/2")
	})
}
