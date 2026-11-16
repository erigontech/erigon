// Copyright 2026 The Erigon Authors
// This file is part of Erigon.

package execmodule_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestValidateChainAndView(t *testing.T) {
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

	// A view that reads only a copy of the state runs after the module is released.
	for _, tc := range []struct {
		name       string
		block      *types.Block
		moduleFree bool
	}{
		{"the committed head", a.Blocks[0], true},
		{"extends the committed head", a.Blocks[1], true},
		{"extends an uncommitted block", a.Blocks[2], false},
		{"sibling of the committed head", b.Blocks[0], false},
		{"extends a side fork at the head height", b.Blocks[1], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requireValid(t, m, tc.block)
			called, moduleFree := false, false
			result, view, err := m.ExecModule.ValidateChainAndView(t.Context(), tc.block, time.Second, func(kv.TemporalTx, kv.TemporalGetter) error {
				called, moduleFree = true, m.ExecModule.ModuleFree()
				return nil
			})
			require.NoError(t, err)
			require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
			if view != nil {
				require.NoError(t, view())
			}
			require.True(t, called)
			require.Equal(t, tc.moduleFree, moduleFree)
		})
	}
}

// A view reads the state it was taken from after a forkchoice update commits its block and the next one, and the
// module validates one more.
func TestValidateChainAndViewOutlivesACommit(t *testing.T) {
	m := execmoduletester.New(t)
	coinbase := common.Address{0x81}
	chain, err := m.GenerateChainFrom(m.Genesis, 4, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(coinbase)
	})
	require.NoError(t, err)
	_, err = m.InsertBlocks(t.Context(), chain.Blocks)
	require.NoError(t, err)
	requireValid(t, m, chain.Blocks[0])
	updateForkChoice(t, m, chain.Blocks[0].Hash(), common.Hash{}, common.Hash{})
	block := chain.Blocks[1]
	requireValid(t, m, block)

	_, view, err := m.ExecModule.ValidateChainAndView(t.Context(), block, time.Second, func(blockTx kv.TemporalTx, parentState kv.TemporalGetter) error {
		parent, _, err := parentState.GetLatest(kv.AccountsDomain, coinbase[:], kv.GetLatestOptions{})
		require.NoError(t, err)
		first, err := m.BlockReader.TxnumReader().Min(t.Context(), blockTx, block.NumberU64())
		require.NoError(t, err)
		last, err := m.BlockReader.TxnumReader().Max(t.Context(), blockTx, block.NumberU64())
		require.NoError(t, err)
		end, _, err := blockTx.GetAsOf(kv.AccountsDomain, coinbase[:], last+1)
		require.NoError(t, err)

		committed, err := m.DB.BeginTemporalRo(t.Context())
		require.NoError(t, err)
		defer committed.Rollback()
		wantParent, _, err := committed.GetAsOf(kv.AccountsDomain, coinbase[:], first)
		require.NoError(t, err)
		wantEnd, _, err := committed.GetAsOf(kv.AccountsDomain, coinbase[:], last+1)
		require.NoError(t, err)
		require.Equal(t, wantParent, parent)
		require.Equal(t, wantEnd, end)
		require.NotEqual(t, parent, end)
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, view)

	requireValid(t, m, chain.Blocks[2])
	require.Equal(t, execmodule.ExecutionStatusSuccess, updateForkChoice(t, m, chain.Blocks[2].Hash(), common.Hash{}, common.Hash{}).Status)
	requireValid(t, m, chain.Blocks[3])
	require.NoError(t, view())
}
