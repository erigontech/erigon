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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestValidatedCandidateEvictionAndForkChoiceCleanup(t *testing.T) {
	for _, mode := range []struct {
		name     string
		parallel bool
	}{{"synchronous", false}, {"parallel", true}} {
		t.Run(mode.name, func(t *testing.T) {
			testValidatedCandidateEvictionAndForkChoiceCleanup(t, mode.parallel)
		})
	}
}

func testValidatedCandidateEvictionAndForkChoiceCleanup(t *testing.T, parallel bool) {
	t.Helper()
	m := execmoduletester.New(t, execmoduletester.WithParallelStateFlushing(parallel))
	blocks := make([]*types.Block, 5)
	states := make([]*execctx.SharedDomains, 5)
	validate := func(block *types.Block) {
		t.Helper()
		result, err := m.ValidateChain(t.Context(), block.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	}
	for i := range blocks {
		chain, err := m.GenerateChainFrom(m.Genesis, 1, func(_ int, b *blockgen.BlockGen) {
			b.SetCoinbase(common.Address{byte(i + 1)})
		})
		require.NoError(t, err)
		blocks[i] = chain.Blocks[0]
		_, err = m.InsertBlocks(t.Context(), chain.Blocks)
		require.NoError(t, err)
		if i == 4 {
			validate(blocks[0])
		}
		validate(blocks[i])
		_, _, states[i] = m.ForkValidator.ExtendingFork()
		require.NotNil(t, states[i])
	}
	require.True(t, m.ForkValidator.HasValidatedState(blocks[0].Hash()))
	require.False(t, m.ForkValidator.HasValidatedState(blocks[1].Hash()))
	require.Nil(t, states[1].GetCommitmentCtx())
	for _, i := range []int{0, 2, 3, 4} {
		require.True(t, m.ForkValidator.HasValidatedState(blocks[i].Hash()))
	}
	validate(blocks[1])
	require.False(t, m.ForkValidator.HasValidatedState(blocks[1].Hash()))

	result, err := m.UpdateForkChoice(t.Context(), blocks[0].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
	for i, block := range blocks {
		require.False(t, m.ForkValidator.HasValidatedState(block.Hash()))
		require.Nil(t, states[i].GetCommitmentCtx())
	}
}

func TestValidatedCandidateClosedAfterValidHashEviction(t *testing.T) {
	const maxReorgDepth = 2
	m := execmoduletester.New(t, execmoduletester.WithMaxReorgDepth(maxReorgDepth))
	var coinbase byte
	sibling := func() *types.Block {
		t.Helper()
		coinbase++
		chain, err := m.GenerateChainFrom(m.Genesis, 1, func(_ int, b *blockgen.BlockGen) {
			b.SetCoinbase(common.Address{coinbase})
		})
		require.NoError(t, err)
		_, err = m.InsertBlocks(t.Context(), chain.Blocks)
		require.NoError(t, err)
		return chain.Blocks[0]
	}
	validate := func(block *types.Block) {
		t.Helper()
		result, err := m.ValidateChain(t.Context(), block.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	}

	filler := make([]*types.Block, 8*maxReorgDepth)
	for i := range filler {
		filler[i] = sibling()
		validate(filler[i])
	}
	m.ForkValidator.ClearWithUnwind()

	retained := sibling()
	validate(retained)
	_, _, first := m.ForkValidator.ExtendingFork()
	require.NotNil(t, first)
	for _, block := range filler[1:] {
		validate(block)
	}
	validate(sibling())
	validate(retained)
	_, _, second := m.ForkValidator.ExtendingFork()
	require.NotNil(t, second)

	m.ForkValidator.ClearWithUnwind()
	require.Nil(t, first.GetCommitmentCtx())
	require.Nil(t, second.GetCommitmentCtx())
}

func TestValidatedCandidateClosedBySetHead(t *testing.T) {
	m := execmoduletester.New(t)
	canonical, err := m.GenerateChain(2, func(int, *blockgen.BlockGen) {})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(canonical))

	child, err := m.GenerateChainFrom(canonical.Blocks[1], 1, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	_, err = m.InsertBlocks(t.Context(), child.Blocks)
	require.NoError(t, err)
	result, err := m.ValidateChain(t.Context(), child.Blocks[0].Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	_, _, state := m.ForkValidator.ExtendingFork()
	require.NotNil(t, state)

	require.NoError(t, m.ExecModule.SetHead(t.Context(), 1))
	require.False(t, m.ForkValidator.HasValidatedState(child.Blocks[0].Hash()))
	require.Nil(t, state.GetCommitmentCtx())
}
