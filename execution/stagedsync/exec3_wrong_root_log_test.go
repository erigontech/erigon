package stagedsync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

type levelRecorder struct{ levels []log.Lvl }

func (h *levelRecorder) Log(r *log.Record) error               { h.levels = append(h.levels, r.Lvl); return nil }
func (h *levelRecorder) Enabled(context.Context, log.Lvl) bool { return true }

func newRecordingExecutor(isForkValidation bool) (*txExecutor, *levelRecorder) {
	rec := &levelRecorder{}
	logger := log.New()
	logger.SetHandler(rec)
	return &txExecutor{logger: logger, logPrefix: "5/5 Execution", isForkValidation: isForkValidation}, rec
}

// A payload rejected during fork validation is an expected outcome — the CL asked
// "is this block valid?" and got an answer. Reporting it at Error makes a normal
// gossip rejection indistinguishable from a node fault, and aborts QA sync runs
// that treat any [EROR] line as a failure.
func TestWrongTrieRootIsNotAnErrorDuringForkValidation(t *testing.T) {
	te, rec := newRecordingExecutor(true)

	te.logWrongTrieRoot("[5/5 Execution] Wrong trie root of block 25661334")

	require.Len(t, rec.levels, 1)
	require.Greater(t, rec.levels[0], log.LvlError, "fork-validation wrong root must log below Error")
}

func TestWrongTrieRootStaysAnErrorDuringNormalExecution(t *testing.T) {
	te, rec := newRecordingExecutor(false)

	te.logWrongTrieRoot("[5/5 Execution] Wrong trie root of block 25661334")

	require.Len(t, rec.levels, 1)
	require.Equal(t, log.LvlError, rec.levels[0], "a wrong root while applying blocks is a real node error")
}
