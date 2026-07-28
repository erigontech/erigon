package stagedsync

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
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

// TestUnwindOnExecError pins the stage-boundary unwind routing hoisted out of
// ExecV3: which unwind action a given (execErr, outcome, cycle) combination
// triggers, and with which block. The wrong-root binary-search must key off the
// implicated block (out.failedBlock), not the last executed header.
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

	t.Run("plain invalid block unwinds to failedBlock-1 and marks the failed block bad", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		failedHash := common.HexToHash("0xbad20")
		// Block 20 failed; last good block is 19. Unwind must target failedBlock-1=19
		// and mark block 20 bad — not lastHeader-1=18, which would unwind one block
		// too far and mark the valid block 19 bad.
		out := execV3Outcome{lastHeader: headerAt(19), failedBlock: 20, failedHash: failedHash}
		got := unwindOnExecError(plainInvalid, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Len(t, u.calls, 1)
		require.Equal(t, uint64(19), u.calls[0].point)
		require.True(t, u.calls[0].reason.IsBadBlock())
	})

	t.Run("plain invalid without a recorded failed block falls back to lastHeader-1", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		got := unwindOnExecError(plainInvalid, execV3Outcome{lastHeader: headerAt(20)}, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Len(t, u.calls, 1)
		require.Equal(t, uint64(19), u.calls[0].point)
		require.True(t, u.calls[0].reason.IsBadBlock())
	})

	t.Run("plain invalid recording the first block in the batch still unwinds", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{}
		// The batch's first block fails: lastHeader is nil, but failedBlock is
		// recorded, so an unwind must still be scheduled (to failedBlock-1).
		out := execV3Outcome{failedBlock: 12, failedHash: common.HexToHash("0xbad12")}
		got := unwindOnExecError(plainInvalid, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, rules.ErrInvalidBlock)
		require.Len(t, u.calls, 1)
		require.Equal(t, uint64(11), u.calls[0].point)
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

	t.Run("wrong root on initial cycle falls through to the recorded-failure unwind", func(t *testing.T) {
		u := &recordingUnwinder{}
		s := &StageState{BlockNumber: 10}
		s.CurrentSyncCycle.IsInitialCycle = true
		// Initial cycle skips the binary search and falls through to the plain
		// unwind, which targets the recorded failed block (5) minus one, not
		// lastHeader-1.
		out := execV3Outcome{lastHeader: headerAt(20), failedBlock: 5, failedHash: common.HexToHash("0xbad5")}
		got := unwindOnExecError(wrongRoot, out, ExecuteBlockCfg{}, s, u, logger)
		require.ErrorIs(t, got, ErrWrongTrieRoot)
		require.Len(t, u.calls, 1)
		require.Equal(t, uint64(4), u.calls[0].point)
	})
}
