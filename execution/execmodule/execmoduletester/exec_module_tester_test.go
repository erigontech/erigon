// Copyright 2024 The Erigon Authors
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

package execmoduletester_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// CLI usage (e.g. evm blocktest via BlockTest.RunCLI) constructs the tester with
// a nil testing.TB and owns its lifecycle. New must not dereference tb.
func TestNewWithNilTB(t *testing.T) {
	t.Parallel()
	m := execmoduletester.New(nil)
	require.NotNil(t, m)
	m.Close()
}

// TestUnexecutedGenesis: WithUnexecutedGenesis leaves the database without
// genesis state, the Prague system contracts included; executing genesis then
// deploys them, so the chain extends.
func TestUnexecutedGenesis(t *testing.T) {
	t.Parallel()
	systemContracts := func(m *execmoduletester.ExecModuleTester) (withdrawals, consolidations bool) {
		t.Helper()
		require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
			w, c := m.ChainConfig.GetWithdrawalRequestContract().Value(), m.ChainConfig.GetConsolidationRequestContract().Value()
			v, _, err := tx.GetLatest(kv.AccountsDomain, w[:], kv.GetLatestOptions{})
			if err != nil {
				return err
			}
			withdrawals = len(v) > 0
			v, _, err = tx.GetLatest(kv.AccountsDomain, c[:], kv.GetLatestOptions{})
			consolidations = len(v) > 0
			return err
		}))
		return withdrawals, consolidations
	}
	for _, unexecuted := range []bool{false, true} {
		opts := []execmoduletester.Option{execmoduletester.WithChainConfig(chain.TestChainOsakaConfig)}
		if unexecuted {
			opts = append(opts, execmoduletester.WithUnexecutedGenesis())
		}
		m := execmoduletester.New(t, opts...)
		require.True(t, m.ChainConfig.IsPrague(0))
		withdrawals, consolidations := systemContracts(m)
		require.Equal(t, !unexecuted, withdrawals, "withdrawal request contract, unexecuted=%v", unexecuted)
		require.Equal(t, !unexecuted, consolidations, "consolidation request contract, unexecuted=%v", unexecuted)
		if unexecuted {
			require.NoError(t, m.InsertChain(&blockgen.ChainPack{
				Headers:  []*types.Header{m.Genesis.HeaderNoCopy()},
				Blocks:   []*types.Block{m.Genesis},
				TopBlock: m.Genesis,
			}))
			withdrawals, consolidations = systemContracts(m)
			require.True(t, withdrawals, "withdrawal request contract after genesis")
			require.True(t, consolidations, "consolidation request contract after genesis")
		}
		next, err := m.GenerateChain(1, func(int, *blockgen.BlockGen) {})
		require.NoError(t, err, "unexecuted=%v", unexecuted)
		require.NoError(t, m.InsertChain(next), "unexecuted=%v", unexecuted)
	}
}

func TestInsertChain(t *testing.T) {
	t.Parallel()
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(100, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	err = m.InsertChain(chain)
	require.NoError(t, err)
}

func TestStateTransitionObserver(t *testing.T) {
	t.Parallel()
	observed := make(map[execmodule.StateTransitionPoint]int)
	var mu sync.Mutex
	m := execmoduletester.New(t, execmoduletester.WithStateTransitionObserver(func(_ context.Context, point execmodule.StateTransitionPoint) {
		mu.Lock()
		observed[point]++
		mu.Unlock()
	}))
	chain, err := m.GenerateChain(1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertValidateAndUfc1By1(t.Context(), chain.Blocks))
	m.ExecModule.WaitIdle(t.Context())

	mu.Lock()
	defer mu.Unlock()
	for _, point := range []execmodule.StateTransitionPoint{
		execmodule.StateTransitionOverlayPublished,
		execmodule.StateTransitionCommitReady,
		execmodule.StateTransitionCommitComplete,
		execmodule.StateTransitionOverlayCleared,
	} {
		require.Positivef(t, observed[point], "state transition %d was not observed", point)
	}
	published := observed[execmodule.StateTransitionOverlayPublished]
	require.Equal(t, published, observed[execmodule.StateTransitionCommitReady], "each published FCU result must reach the commit boundary")
	require.Equal(t, published, observed[execmodule.StateTransitionCommitComplete], "each published FCU result must become durable")
	require.Equal(t, published, observed[execmodule.StateTransitionOverlayCleared], "only a published overlay may emit a clear event")
}

func TestReorgsWithInsertChain(t *testing.T) {
	t.Parallel()
	m := execmoduletester.New(t)
	chain, err := m.GenerateChain(10, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	// insert initial chain
	err = m.InsertChain(chain)
	require.NoError(t, err)
	// Now generate three competing branches, one short and two longer ones
	short, err := m.GenerateChainFrom(chain.TopBlock, 2, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	long1, err := m.GenerateChainFrom(chain.TopBlock, 10, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{2}) // Need to make headers different from short branch
	})
	require.NoError(t, err)
	// Second long chain needs to be slightly shorter than the first long chain
	long2, err := m.GenerateChainFrom(chain.TopBlock, 9, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{3}) // Need to make headers different from short branch and another long branch
	})
	require.NoError(t, err)
	// insert short chain
	err = m.InsertChain(short)
	require.NoError(t, err)
	// insert long1 chain
	err = m.InsertChain(long1)
	require.NoError(t, err)
	// another short chain
	short2, err := m.GenerateChainFrom(long1.TopBlock, 2, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	// insert long2 chain
	err = m.InsertChain(long2)
	require.NoError(t, err)
	require.NoError(t, err)
	// insert short2 chain
	err = m.InsertChain(short2)
	require.NoError(t, err)
}
