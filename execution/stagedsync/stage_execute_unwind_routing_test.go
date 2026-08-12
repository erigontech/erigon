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

// TestUnwindOnExecError pins the stage-boundary error routing hoisted out of
// ExecV3: a plain invalid block propagates without setting a stage unwind point
// (the caller owns the unwind, so a fresh block at the same height can be
// re-executed), while a wrong trie root routes to handleIncorrectRootHashError's
// binary search keyed off the implicated block.
func TestUnwindOnExecError(t *testing.T) {
	t.Parallel()
	logger := log.New()

	plainInvalid := fmt.Errorf("%w: gas mismatch", rules.ErrInvalidBlock)
	wrongRoot := fmt.Errorf("%w, block=5", ErrWrongTrieRoot)

	t.Run("nil error unwinds nothing", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		got := unwindOnExecError(nil, execV3Outcome{lastHeader: headerAt(20)}, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Empty(t, u.calls)
	})

	t.Run("ErrLoopExhausted unwinds nothing", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		err := &ErrLoopExhausted{From: 1, To: 2}
		got := unwindOnExecError(err, execV3Outcome{lastHeader: headerAt(20)}, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, err)
		require.Empty(t, u.calls)
	})

	t.Run("plain invalid propagates without setting a stage unwind point (recorded failed block)", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		// A plain invalid block must not set a stage unwind point even with a
		// recorded failed block: the caller owns the unwind, so a fresh canonical
		// block at the same height can be re-executed on the next fork-choice.
		out := execV3Outcome{lastHeader: headerAt(19), failedBlock: 20, failedHash: common.HexToHash("0xbad20")}
		got := unwindOnExecError(plainInvalid, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("plain invalid propagates without unwinding when only lastHeader is set", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		got := unwindOnExecError(plainInvalid, execV3Outcome{lastHeader: headerAt(20)}, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("plain invalid propagates without unwinding when the first block in the batch fails", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		out := execV3Outcome{failedBlock: 12, failedHash: common.HexToHash("0xbad12")}
		got := unwindOnExecError(plainInvalid, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("plain invalid with neither failedBlock nor lastHeader unwinds nothing but propagates", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		got := unwindOnExecError(plainInvalid, execV3Outcome{}, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("badBlockHalt suppresses unwind and propagates", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		cfg := ExecuteBlockCfg{badBlockHalt: true}
		got := unwindOnExecError(plainInvalid, execV3Outcome{lastHeader: headerAt(20)}, cfg, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Empty(t, u.calls)
	})

	t.Run("nil unwinder suppresses unwind and propagates", func(t *testing.T) {
		s := &StageState{}
		got := unwindOnExecError(plainInvalid, execV3Outcome{lastHeader: headerAt(20)}, ExecuteBlockCfg{}, s, nil, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
	})

	t.Run("wrong root on non-initial cycle takes the binary-search path from the implicated block", func(t *testing.T) {
		u := &recordingUnwinder{}
		// failedBlock (5) <= s.BlockNumber (10) makes handleIncorrectRootHashError
		// return nil at its guard, before touching applyTx. If the routing wrongly
		// fell through to the plain branch it would UnwindTo(lastHeader-1)=19.
		s := &StageState{BlockNumber: 10}
		s.CurrentSyncCycle.IsInitialCycle = false
		out := execV3Outcome{lastHeader: headerAt(20), failedBlock: 5, failedHash: common.HexToHash("0xdead")}
		got := unwindOnExecError(wrongRoot, out, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Empty(t, u.calls, "must not take the plain lastHeader-1 unwind branch")
	})

	t.Run("wrong root joined with executor failure propagates both", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{BlockNumber: 10}
		s.CurrentSyncCycle.IsInitialCycle = false
		executorErr := errors.New("worker failed")
		execErr := errors.Join(wrongRoot, executorErr)
		out := execV3Outcome{failedBlock: 5, failedHash: common.HexToHash("0xdead")}

		got := unwindOnExecError(execErr, out, ExecuteBlockCfg{}, s, u, logger)

		require.ErrorIs(t, got, ErrWrongTrieRoot)
		require.ErrorIs(t, got, executorErr)
		require.Empty(t, u.calls)
	})

	t.Run("wrong root on initial cycle is fatal (no fork to recover from)", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{BlockNumber: 10}
		s.CurrentSyncCycle.IsInitialCycle = true
		// Initial sync has no competing fork: the wrong root must propagate as a
		// fatal error, not route to handleIncorrectRootHashError (which would return
		// nil here — failedBlock (5) <= s.BlockNumber (10) — and swallow it).
		out := execV3Outcome{lastHeader: headerAt(20), failedBlock: 5, failedHash: common.HexToHash("0xbad5")}
		got := unwindOnExecError(wrongRoot, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, ErrWrongTrieRoot, "initial-cycle wrong root must stay fatal")
		require.Empty(t, u.calls, "must not schedule any unwind")
	})

	t.Run("wrong root on non-initial cycle schedules the binary-search unwind", func(t *testing.T) {
		// failedBlock (15) > s.BlockNumber (5) takes handleIncorrectRootHashError's
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
		out := execV3Outcome{lastHeader: headerAt(20), failedBlock: 15, failedHash: common.HexToHash("0xf00d"), applyTx: tx}
		got := unwindOnExecError(wrongRoot, out, ExecuteBlockCfg{}, s, u, logger)
		require.NoError(t, got)
		require.Len(t, u.calls, 1)
		require.Equal(t, uint64(10), u.calls[0].point, "unwind target = failedBlock - (failedBlock-s.BlockNumber)/2")
	})
}
