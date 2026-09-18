package stagedsync

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/rawdbv3"
)

func TestIsOnlyLoopExhausted(t *testing.T) {
	t.Parallel()
	exhausted := &ErrLoopExhausted{From: 1, To: 2, Reason: "block batch is full"}

	require.True(t, IsOnlyLoopExhausted(exhausted))
	// A joined error carrying a real failure alongside exhaustion is NOT purely
	// exhausted: runStage must fall through and surface the failure rather than
	// save progress and drop it.
	require.False(t, IsOnlyLoopExhausted(errors.Join(exhausted, errors.New("boom"))))
	require.False(t, IsOnlyLoopExhausted(errors.New("boom")))
	require.False(t, IsOnlyLoopExhausted(nil))
}

// TestSavesExecProgress pins the stage-progress gate: only a clean run or a purely
// loop-exhausted one advances Execution progress. A loop-exhausted error joined with a
// real failure (parallel exec joins the drain's waitErr) must NOT save progress, or the
// stage advances past the failed block and the real error is silently dropped.
func TestSavesExecProgress(t *testing.T) {
	t.Parallel()
	exhausted := &ErrLoopExhausted{From: 1, To: 2, Reason: "block batch is full"}

	require.True(t, savesExecProgress(nil))
	require.True(t, savesExecProgress(exhausted))
	require.False(t, savesExecProgress(errors.Join(exhausted, errors.New("boom"))),
		"loop-exhausted joined with a real failure must not advance stage progress")
	require.False(t, savesExecProgress(errors.New("boom")))
}

func TestResolveExecResumePoint(t *testing.T) {
	t.Parallel()
	// The reader is only consulted on the exec-only advanced branch; the branches
	// below never touch it, so a zero-value reader and nil tx are safe.
	var noReader rawdbv3.TxNumsReader

	// Normal mode: the commitment boundary is authoritative regardless of how far
	// Execution progress has moved.
	txNum, blockNum, err := resolveExecResumePoint(context.Background(), noReader, nil, false, 100, 42, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(42), txNum)
	require.Equal(t, uint64(10), blockNum)

	// Exec-only, but Execution progress has not moved past the commitment boundary:
	// still resume from the boundary.
	txNum, blockNum, err = resolveExecResumePoint(context.Background(), noReader, nil, true, 10, 42, 10)
	require.NoError(t, err)
	require.Equal(t, uint64(42), txNum)
	require.Equal(t, uint64(10), blockNum)
}
