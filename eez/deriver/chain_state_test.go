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

package deriver_test

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
)

// TestStorageAfterBlock checks that Storage reads the state a block left,
// before the next block's opening system calls: block 3 records block 2's
// hash in the block hash history contract (EIP-2935) when it starts.
func TestStorageAfterBlock(t *testing.T) {
	spec := networks.Chiado699169
	l2 := spec.Config.L2.(*config.L2Config)
	_, chain := derivertest.NewL2Of(t, spec)
	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	blocks := []*types.Block{}
	for n := uint64(1); n <= 3; n++ {
		built, err := chain.Extend(t.Context(), parent, deriver.HeaderInputs{
			Number:     n,
			ParentHash: parent.Hash(),
			Time:       parent.Time + l2.BlockInterval,
			Extra:      []byte{},
			GasLimit:   l2.BlockGasLimit,
		}, nil, common.Hash{}, common.Hash{})
		require.NoError(t, err)
		blocks = append(blocks, built.Block)
		parent = built.Block.Header()
	}
	history := params.HistoryStorageAddress.Value()
	// The contract keeps block n's hash in slot n mod 8191.
	slot := common.Hash{31: 2}
	v, err := chain.Storage(t.Context(), 2, history, slot)
	require.NoError(t, err)
	require.Equal(t, common.Hash{}, v, "block 2's hash after block 2")
	v, err = chain.Storage(t.Context(), 3, history, slot)
	require.NoError(t, err)
	require.Equal(t, blocks[1].Hash(), v, "block 2's hash after block 3")
}

// TestNonceAfterBlock checks that Nonce reads the state a block left, before
// the next block's opening system calls: here the block hash history
// contract creates a contract on each call, so each block raises its nonce.
func TestNonceAfterBlock(t *testing.T) {
	spec := networks.Chiado699169
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	history := params.HistoryStorageAddress.Value()
	genesis.Alloc[history] = types.GenesisAccount{Nonce: 1, Code: []byte{
		byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.CREATE), byte(vm.POP), byte(vm.STOP),
	}}
	spec.Genesis = &genesis
	l2 := spec.Config.L2.(*config.L2Config)
	_, chain := derivertest.NewL2Of(t, spec)
	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	for n := uint64(1); n <= 3; n++ {
		built, err := chain.Extend(t.Context(), parent, deriver.HeaderInputs{
			Number:     n,
			ParentHash: parent.Hash(),
			Time:       parent.Time + l2.BlockInterval,
			Extra:      []byte{},
			GasLimit:   l2.BlockGasLimit,
		}, nil, common.Hash{}, common.Hash{})
		require.NoError(t, err)
		parent = built.Block.Header()
	}
	for n := uint64(0); n <= 3; n++ {
		nonce, err := chain.Nonce(t.Context(), n, history)
		require.NoError(t, err)
		require.Equal(t, 1+n, nonce, "after block %d", n)
	}
}

// TestForkChoiceRefused: a fork choice the execution module refuses is
// ErrForkChoiceRefused, and one to a chain holding a block that fails
// validation also ErrBadBlock.
func TestForkChoiceRefused(t *testing.T) {
	c := derivertest.NewComposer(t)
	b := c.Compose(nil,
		derivertest.Block{Coinbase: common.HexToAddress("0xa11ce"), Extra: []byte{}, Txs: [][]byte{}},
		derivertest.Block{Coinbase: common.HexToAddress("0xa11ce"), Extra: []byte{}, Txs: [][]byte{}},
	)
	_, chain := derivertest.NewL2(t)
	require.NoError(t, chain.Insert(t.Context(), b.Blocks))
	tip := b.Blocks[1].Hash()
	require.NoError(t, chain.ForkChoice(t.Context(), tip, tip, tip))
	err := chain.ForkChoice(t.Context(), b.Blocks[0].Hash(), tip, tip)
	require.ErrorIs(t, err, deriver.ErrForkChoiceRefused, "finalized above the head")
	require.NotErrorIs(t, err, deriver.ErrBadBlock)

	h := b.Blocks[0].Header()
	h.Root = common.Hash{0x01}
	bad := types.NewBlockFromNetwork(h, b.Blocks[0].Body(), nil)
	_, chain = derivertest.NewL2(t)
	require.NoError(t, chain.Insert(t.Context(), []*types.Block{bad}))
	err = chain.ForkChoice(t.Context(), bad.Hash(), common.Hash{}, common.Hash{})
	require.ErrorIs(t, err, deriver.ErrForkChoiceRefused)
	require.ErrorIs(t, err, deriver.ErrBadBlock)
}
