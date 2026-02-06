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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func TestInit(t *testing.T) {
	t.Parallel()
	mock.Mock(t)
}

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

func TestMineBlockWith1Tx(t *testing.T) {
	t.Parallel()
	t.Skip("revive me")
	require, m := require.New(t), mock.Mock(t)

	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	require.NoError(err)
	{ // Do 1 step to start txPool

		// Send NewBlock message
		b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
			Block: chain.TopBlock,
			TD:    big.NewInt(1), // This is ignored anyway
		})
		require.NoError(err)
		m.ReceiveWg.Add(1)
		for _, err = range m.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
			require.NoError(err)
		}
		// Send all the headers
		b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
			RequestId:          1,
			BlockHeadersPacket: chain.Headers,
		})
		require.NoError(err)
		m.ReceiveWg.Add(1)
		for _, err = range m.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
			require.NoError(err)
		}
		m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

		initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
		err = stageloop.StageLoopIteration(m.Ctx, m.DB, m.Sync, initialCycle, firstCycle, m.Log, m.BlockReader, nil)
		if err != nil {
			t.Fatal(err)
		}
	}

	chain, err = blockgen.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 1, func(i int, gen *blockgen.BlockGen) {
		// In block 1, addr1 sends addr2 some ether.
		tx, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, &u256.Num1, nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(err)
		gen.AddTx(tx)
	})
	require.NoError(err)

	// Send NewBlock message
	b, err := rlp.EncodeToBytes(chain.TopBlock.Transactions())
	require.NoError(err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentryproto.InboundMessage{Id: sentryproto.MessageId_TRANSACTIONS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceed

	err = stageloop.MiningStep(m.Ctx, m.DB, m.MiningSync, "", log.Root())
	require.NoError(err)

	got := <-m.PendingBlocks
	require.Equal(chain.TopBlock.Transactions().Len(), got.Transactions().Len())
	got2 := <-m.MinedBlocks
	require.Equal(chain.TopBlock.Transactions().Len(), got2.Block.Transactions().Len())
}

func TestReorg(t *testing.T) {
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
	// insert long2 chain
	err = m.InsertChain(long2)
	require.NoError(t, err)
	require.NoError(t, err)
	// insert short2 chain
	err = m.InsertChain(short2)
	require.NoError(t, err)
}
