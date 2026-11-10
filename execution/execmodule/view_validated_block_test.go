// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package execmodule_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestViewValidatedBlockRequiresCommittedParent(t *testing.T) {
	m := execmoduletester.New(t)
	a, err := m.GenerateChainFrom(m.Genesis, 3, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{0x81})
	})
	require.NoError(t, err)
	b, err := m.GenerateChainFrom(m.Genesis, 2, func(_ int, blk *blockgen.BlockGen) {
		blk.SetCoinbase(common.Address{0x82})
	})
	require.NoError(t, err)
	_, err = m.InsertBlocks(t.Context(), a.Blocks)
	require.NoError(t, err)
	_, err = m.InsertBlocks(t.Context(), b.Blocks)
	require.NoError(t, err)
	requireValid(t, m, a.Blocks[0])
	updateForkChoice(t, m, a.Blocks[0].Hash(), common.Hash{}, common.Hash{})

	for _, tc := range []struct {
		name  string
		block *types.Block
		held  bool
	}{
		{"extends the committed head", a.Blocks[1], true},
		{"extends an uncommitted block", a.Blocks[2], false},
		{"sibling of the committed head", b.Blocks[0], false},
		{"extends a side fork at the head height", b.Blocks[1], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireValid(t, m, tc.block)
			called := false
			held, err := m.ExecModule.ViewValidatedBlock(t.Context(), tc.block.Hash(), func(_, _ kv.TemporalTx) error {
				called = true
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, tc.held, held)
			require.Equal(t, tc.held, called)
		})
	}
}
