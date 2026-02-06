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

package mock_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
)

func TestInsertChain(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 100, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	err = m.InsertChain(chain)
	require.NoError(t, err)
}

func TestReorgsWithInsertChain(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	// insert initial chain
	err = m.InsertChain(chain)
	require.NoError(t, err)
	// Now generate three competing branches, one short and two longer ones
	short, err := blockgen.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(t, err)
	long1, err := blockgen.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 10, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{2}) // Need to make headers different from short branch
	})
	require.NoError(t, err)
	// Second long chain needs to be slightly shorter than the first long chain
	long2, err := blockgen.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 9, func(i int, b *blockgen.BlockGen) {
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
	short2, err := blockgen.GenerateChain(m.ChainConfig, long1.TopBlock, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
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
