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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
)

func TestRepeatedForkchoicePersistsFinality(t *testing.T) {
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(4, nil)
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks))
	m.ExecModule.Drain()
	head, safe, finalized := chain.TopBlock.Hash(), chain.Blocks[2].Hash(), chain.Blocks[1].Hash()
	result, err := m.ExecModule.UpdateForkChoice(t.Context(), head, safe, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.Drain()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head, rawdb.ReadForkchoiceHead(tx))
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx))
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))
	result, err = m.ExecModule.UpdateForkChoice(t.Context(), head, common.Hash{0xff}, finalized)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusInvalidForkchoice, result.Status)
	m.ExecModule.Drain()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head, rawdb.ReadForkchoiceHead(tx))
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx), "rejected finality must not be persisted")
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))
	result, err = m.ExecModule.UpdateForkChoice(t.Context(), chain.Blocks[0].Hash(), common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.Drain()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		require.Equal(t, head, rawdb.ReadHeadBlockHash(tx), "an FCU below finality must not move the executed head")
		require.Equal(t, head, rawdb.ReadForkchoiceHead(tx))
		require.Equal(t, safe, rawdb.ReadForkchoiceSafe(tx))
		require.Equal(t, finalized, rawdb.ReadForkchoiceFinalized(tx))
		return nil
	}))
}

func TestCatchupCommitObservations(t *testing.T) {
	var mu sync.Mutex
	var observed []execmodule.StateTransitionPoint
	m := execmoduletester.New(t, execmoduletester.WithStateTransitionObserver(func(_ context.Context, point execmodule.StateTransitionPoint) {
		mu.Lock()
		defer mu.Unlock()
		observed = append(observed, point)
	}))
	m.ExecModule.Drain()
	mu.Lock()
	observed = nil
	mu.Unlock()
	chain, err := m.GenerateChain(20, nil)
	require.NoError(t, err)
	status, err := m.InsertBlocks(t.Context(), chain.Blocks)
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, status)
	result, err := m.UpdateForkChoice(t.Context(), chain.TopBlock.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.Drain()

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []execmodule.StateTransitionPoint{
		execmodule.StateTransitionCatchupCommitReady,
		execmodule.StateTransitionCatchupCommitComplete,
		execmodule.StateTransitionOverlayPublished,
		execmodule.StateTransitionCommitReady,
		execmodule.StateTransitionCommitComplete,
		execmodule.StateTransitionOverlayCleared,
	}, observed)
}
