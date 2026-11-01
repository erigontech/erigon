// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package execmodule_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
)

// newExecLagTester leaves block 1 executed and blocks 2..1+lag canonical but
// unexecuted, the state a node restarts into after its execution stage fell
// behind its headers. It returns the next block, a payload whose parent is the
// canonical head.
func newExecLagTester(t *testing.T, lag int, opts ...execmoduletester.Option) (*execmoduletester.ExecModuleTester, *types.Block) {
	t.Helper()
	m, chainPack := execmoduletester.NewWithExecLag(t, lag, 1, opts...)
	require.Equal(t, uint64(1), execProgress(t, m))

	payload := chainPack.Blocks[1+lag]
	_, err := m.InsertBlocks(t.Context(), []*types.Block{payload})
	require.NoError(t, err)
	return m, payload
}

func execProgress(t *testing.T, m *execmoduletester.ExecModuleTester) uint64 {
	t.Helper()
	var progress uint64
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) (err error) {
		progress, err = stages.GetStageProgress(tx, stages.Execution)
		return err
	}))
	return progress
}

func TestValidateChainAcceptsPayloadBeyondLoopBlockLimit(t *testing.T) {
	m, payload := newExecLagTester(t, 12, execmoduletester.WithLoopBlockLimit(5))

	for range 2 {
		result, err := m.ValidateChain(t.Context(), payload.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusMissingSegment, result.ValidationStatus)
	}
	require.Equal(t, uint64(1), execProgress(t, m))

	fcu, err := m.UpdateForkChoice(t.Context(), payload.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	require.Equal(t, payload.NumberU64(), execProgress(t, m))
}

func TestValidateChainExecutesLagWithinLoopBlockLimit(t *testing.T) {
	m, payload := newExecLagTester(t, 3, execmoduletester.WithLoopBlockLimit(5))

	result, err := m.ValidateChain(t.Context(), payload.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
}

func TestValidateChainAcceptsPayloadBeyondBatchSize(t *testing.T) {
	prevParallel := dbg.Exec3Parallel
	dbg.Exec3Parallel = true
	t.Cleanup(func() { dbg.Exec3Parallel = prevParallel })

	m, payload := newExecLagTester(t, 12, execmoduletester.WithBatchSize(1))

	result, err := m.ValidateChain(t.Context(), payload.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusMissingSegment, result.ValidationStatus)
	require.Equal(t, uint64(1), execProgress(t, m))

	fcu, err := m.UpdateForkChoice(t.Context(), payload.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	require.Equal(t, payload.NumberU64(), execProgress(t, m))
}

type validationWarnRecorder struct {
	next   log.Handler
	warned atomic.Bool
}

func (w *validationWarnRecorder) Log(r *log.Record) error {
	if r.Lvl <= log.LvlWarn && r.Msg == "Could not validate block" {
		w.warned.Store(true)
	}
	return w.next.Log(r)
}

func (w *validationWarnRecorder) Enabled(context.Context, log.Lvl) bool { return true }

func TestValidateChainBeyondLoopBlockLimitDoesNotWarn(t *testing.T) {
	m, payload := newExecLagTester(t, 12, execmoduletester.WithLoopBlockLimit(5))
	prev := log.Root().GetHandler()
	recorder := &validationWarnRecorder{next: prev}
	log.Root().SetHandler(recorder)
	t.Cleanup(func() { log.Root().SetHandler(prev) })

	result, err := m.ValidateChain(t.Context(), payload.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusMissingSegment, result.ValidationStatus)
	require.False(t, recorder.warned.Load())
}
