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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func newSnapshotBoundaryChain(t *testing.T, opts ...execmoduletester.Option) (*execmoduletester.ExecModuleTester, *blockgen.ChainPack, uint64) {
	t.Helper()
	const stepSize = 16
	ctx := t.Context()
	m, chainPack := newExecutedTransferChain(t, append([]execmoduletester.Option{
		execmoduletester.WithStepSize(stepSize),
		execmoduletester.WithMaxReorgDepth(96),
		execmoduletester.WithAlwaysGenerateChangesets(false),
	}, opts...)...)
	m.ExecModule.Drain()
	m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator).WaitForFiles()
	var lastFileBlock uint64
	require.NoError(t, m.DB.View(ctx, func(tx kv.Tx) error {
		block, found, err := m.BlockReader.TxnumReader().FindBlockNum(ctx, tx, stepSize-1)
		require.NoError(t, err)
		require.True(t, found)
		require.Positive(t, block)
		lastFileBlock = block
		return nil
	}))
	return m, chainPack, lastFileBlock
}

// holdStateFilePublication finalizes the block that holds the first step's last transaction and
// starts a prune. If that prune builds the step, publication waits until the returned func runs.
func holdStateFilePublication(t *testing.T) (m *execmoduletester.ExecModuleTester, chainPack *blockgen.ChainPack, lastFileBlock uint64, publish func()) {
	t.Helper()
	m, chainPack, lastFileBlock = newSnapshotBoundaryChain(t)
	ctx := t.Context()
	agg := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	built, release := make(chan struct{}), make(chan struct{})
	allowPublication := sync.OnceFunc(func() {
		agg.SetMaintenanceObserver(nil)
		close(release)
	})
	agg.SetMaintenanceObserver(func(ctx context.Context, event dbstate.MaintenanceEvent) {
		if event.Point == dbstate.MaintenanceFilesBuilt {
			close(built)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	})
	t.Cleanup(allowPublication)

	finalizedHash := chainPack.Blocks[lastFileBlock-1].Hash()
	fcu, err := m.UpdateForkChoice(ctx, chainPack.TopBlock.Header(),
		execmoduletester.WithSafeHash(finalizedHash),
		execmoduletester.WithFinalisedHash(finalizedHash),
	)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, fcu.Status)
	finished, err := m.ExecModule.Prune(ctx)
	require.NoError(t, err)
	select {
	case <-built:
	case <-finished:
	}
	return m, chainPack, lastFileBlock, func() {
		allowPublication()
		<-finished
		agg.WaitForFiles()
	}
}

// requireFilesOnCanonicalChain checks that state files cover only canonical blocks of chainPack at or below the head.
func requireFilesOnCanonicalChain(t *testing.T, m *execmoduletester.ExecModuleTester, chainPack *blockgen.ChainPack) {
	t.Helper()
	endTxNum := m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator).EndTxNumMinimax()
	if endTxNum == 0 {
		return
	}
	var lastFileBlock uint64
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		var found bool
		var err error
		lastFileBlock, found, err = m.BlockReader.TxnumReader().FindBlockNum(t.Context(), tx, endTxNum-1)
		require.NoError(t, err)
		require.True(t, found, "state files end at txNum %d, above the last block in the database", endTxNum-1)
		head := rawdb.ReadHeaderNumber(tx, rawdb.ReadHeadBlockHash(tx))
		require.NotNil(t, head)
		require.LessOrEqual(t, lastFileBlock, *head)
		return nil
	}))
	require.Equal(t, chainPack.Blocks[lastFileBlock-1].Hash(), canonicalHashAt(t, m, lastFileBlock), "state files hold a block that is no longer canonical")
}

func TestSetHeadDuringStateFileBuild(t *testing.T) {
	m, chainPack, lastFileBlock, publish := holdStateFilePublication(t)
	err := m.ExecModule.SetHead(t.Context(), lastFileBlock-1)
	publish()
	if err != nil {
		require.ErrorContains(t, err, "minimum unwindable block")
	}
	requireFilesOnCanonicalChain(t, m, chainPack)
}

func TestForkchoiceLoweringFinalityDuringStateFileBuild(t *testing.T) {
	for _, name := range []string{"same_head", "advance_head", "rewind_at_boundary"} {
		t.Run(name, func(t *testing.T) {
			m, chain, lastFileBlock, publish := holdStateFilePublication(t)
			ctx := t.Context()
			finalized := chain.Blocks[lastFileBlock-1]
			parentHash := finalized.ParentHash()
			extension, err := m.GenerateChainFrom(chain.TopBlock, 2, nil)
			require.NoError(t, err)
			status, err := m.InsertBlocks(ctx, extension.Blocks)
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			currentHead := extension.Blocks[0].Hash()
			require.Equal(t, execmodule.ExecutionStatusSuccess, updateForkChoice(t, m, currentHead, finalized.Hash(), finalized.Hash()).Status)
			head := currentHead
			switch name {
			case "advance_head":
				head = extension.TopBlock.Hash()
			case "rewind_at_boundary":
				// Forkchoice first unwinds to this older head's parent.
				head = finalized.Hash()
			}
			result := updateForkChoice(t, m, head, parentHash, parentHash)
			require.Contains(t, []execmodule.ExecutionStatus{execmodule.ExecutionStatusSuccess, execmodule.ExecutionStatusReorgTooDeep}, result.Status)
			publish()
			requireFilesOnCanonicalChain(t, m, chain)

			// The parent is finalized now; a sibling of its child must not replace a block in the files.
			header := finalized.Header()
			header.Extra = []byte("sibling")
			sibling := finalized.WithSeal(header)
			status, err = m.InsertBlocks(ctx, []*types.Block{sibling})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, status)
			result = updateForkChoice(t, m, sibling.Hash(), parentHash, parentHash)
			require.Contains(t, []execmodule.ExecutionStatus{execmodule.ExecutionStatusSuccess, execmodule.ExecutionStatusReorgTooDeep}, result.Status)
			requireFilesOnCanonicalChain(t, m, chain)
		})
	}
}

func TestStateFileBuildKeepsRewindsOutOfFiles(t *testing.T) {
	m, chainPack, lastFileBlock := newSnapshotBoundaryChain(t)
	ctx := t.Context()
	pruneWithFinalized(t, m, chainPack, lastFileBlock)
	require.Zero(t, m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator).EndTxNumMinimax(), "a block with a changeset must stay out of the files")
	require.NoError(t, m.ExecModule.SetHead(ctx, lastFileBlock-1))
	requireFilesOnCanonicalChain(t, m, chainPack)
}

// With every changeset kept, collation still deletes the changesets of the blocks it puts in files.
func TestStateFileBuildDeletesTheChangesetsOfCollatedBlocks(t *testing.T) {
	m, chainPack, lastFileBlock := newSnapshotBoundaryChain(t, execmoduletester.WithAlwaysGenerateChangesets(true))
	ctx := t.Context()
	pruneWithFinalized(t, m, chainPack, lastFileBlock+1)
	require.Equal(t, m.DB.StepSize(), m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator).EndTxNumMinimax())
	require.ErrorContains(t, m.ExecModule.SetHead(ctx, lastFileBlock-1), "minimum unwindable block")
	require.NoError(t, m.ExecModule.SetHead(ctx, lastFileBlock))
	requireFilesOnCanonicalChain(t, m, chainPack)
}

func pruneWithFinalized(t *testing.T, m *execmoduletester.ExecModuleTester, chainPack *blockgen.ChainPack, finalized uint64) {
	t.Helper()
	finalizedHash := chainPack.Blocks[finalized-1].Hash()
	require.Equal(t, execmodule.ExecutionStatusSuccess, updateForkChoice(t, m, chainPack.TopBlock.Hash(), finalizedHash, finalizedHash).Status)
	finished, err := m.ExecModule.Prune(t.Context())
	require.NoError(t, err)
	<-finished
	m.DB.(dbstate.HasAgg).Agg().(*dbstate.Aggregator).WaitForFiles()
}
