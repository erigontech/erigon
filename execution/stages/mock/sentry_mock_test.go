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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/execution/chain/params"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stages"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

func TestEmptyStageSync(t *testing.T) {
	t.Parallel()
	mock.Mock(t)
}

func TestHeaderStep(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 100, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: chain.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	// Send all the headers
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceed

	initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.NewTxContainer(nil, nil), m.Sync, initialCycle, firstCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}
}

func TestMineBlockWith1Tx(t *testing.T) {
	t.Parallel()
	t.Skip("revive me")
	require, m := require.New(t), mock.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
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
		for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
			require.NoError(err)
		}
		// Send all the headers
		b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
			RequestId:          1,
			BlockHeadersPacket: chain.Headers,
		})
		require.NoError(err)
		m.ReceiveWg.Add(1)
		for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
			require.NoError(err)
		}
		m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

		initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
		if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.NewTxContainer(nil, nil), m.Sync, initialCycle, firstCycle, log.New(), m.BlockReader, nil); err != nil {
			t.Fatal(err)
		}
	}

	chain, err = core.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 1, func(i int, gen *core.BlockGen) {
		// In block 1, addr1 sends addr2 some ether.
		tx, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), common.Address{1}, uint256.NewInt(10_000), params.TxGas, u256.Num1, nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
		require.NoError(err)
		gen.AddTx(tx)
	})
	require.NoError(err)

	// Send NewBlock message
	b, err := rlp.EncodeToBytes(chain.TopBlock.Transactions())
	require.NoError(err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_TRANSACTIONS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceed

	err = stages.MiningStep(m.Ctx, m.DB, m.MiningSync, "", log.Root())
	require.NoError(err)

	got := <-m.PendingBlocks
	require.Equal(chain.TopBlock.Transactions().Len(), got.Transactions().Len())
	got2 := <-m.MinedBlocks
	require.Equal(chain.TopBlock.Transactions().Len(), got2.Block.Transactions().Len())
}

func TestReorg(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: chain.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		t.Fatal(err)
	}
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send all the headers
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.NewTxContainer(nil, nil), m.Sync, initialCycle, firstCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}

	// Now generate three competing branches, one short and two longer ones
	short, err := core.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	long1, err := core.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{2}) // Need to make headers different from short branch
	})
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	// Second long chain needs to be slightly shorter than the first long chain
	long2, err := core.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 9, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{3}) // Need to make headers different from short branch and another long branch
	})
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}

	// Send NewBlock message for short branch
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		t.Fatal(err)
	}
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send headers of the short branch
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          2,
		BlockHeadersPacket: short.Headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.NewTxContainer(nil, nil), m.Sync, initialCycle, firstCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}

	// Send NewBlock message for long1 branch
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: long1.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		t.Fatal(err)
	}
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send headers of the long2 branch
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          3,
		BlockHeadersPacket: long2.Headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send headers of the long1 branch
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          4,
		BlockHeadersPacket: long1.Headers,
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	// This is unwind step
	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.NewTxContainer(nil, nil), m.Sync, initialCycle, firstCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}

	// another short chain
	// Now generate three competing branches, one short and two longer ones
	short2, err := core.GenerateChain(m.ChainConfig, long1.TopBlock, m.Engine, m.DB, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}

	// Send NewBlock message for short branch
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short2.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send headers of the short branch
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          5,
		BlockHeadersPacket: short2.Headers,
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.NewTxContainer(nil, nil), m.Sync, initialCycle, firstCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorReplace(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	short, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 11, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	long, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 15, func(i int, b *core.BlockGen) {
		if i < 10 {
			b.SetCoinbase(common.Address{1})
		} else {
			b.SetCoinbase(common.Address{2})
		}
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Create anchor from the long chain suffix
	var b []byte
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: long.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send headers of the long suffix
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: long.Headers[10:],
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	require.NoError(t, err)

	// Create anchor from the short chain suffix
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	require.NoError(t, err)

	// Send headers of the short suffix
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          2,
		BlockHeadersPacket: short.Headers[10:],
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	// Now send the prefix chain
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          3,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.NewTxContainer(nil, nil), m.Sync, initialCycle, firstCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorReplace2(t *testing.T) {
	t.Parallel()
	m := mock.Mock(t)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	short, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 20, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	long, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 30, func(i int, b *core.BlockGen) {
		if i < 10 {
			b.SetCoinbase(common.Address{1})
		} else {
			b.SetCoinbase(common.Address{2})
		}
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	// Create anchor from the long chain suffix
	var b []byte
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: long.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send headers of the long suffix
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: long.Headers[10:],
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Create anchor from the short chain suffix
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send headers of the short suffix (far end)
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          2,
		BlockHeadersPacket: short.Headers[15:],
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	// Send headers of the short suffix (near end)
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          3,
		BlockHeadersPacket: short.Headers[10:15],
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	// Now send the prefix chain
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          4,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}

	m.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed

	initialCycle, firstCycle := mock.MockInsertAsInitialCycle, false
	hook := stages.NewHook(m.Ctx, m.DB, m.Notifications, m.Sync, m.BlockReader, m.ChainConfig, m.Log, nil)
	if err := stages.StageLoopIteration(m.Ctx, m.DB, wrap.NewTxContainer(nil, nil), m.Sync, initialCycle, firstCycle, m.Log, m.BlockReader, hook); err != nil {
		t.Fatal(err)
	}
}
