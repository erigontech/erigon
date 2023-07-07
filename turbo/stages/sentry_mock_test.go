package stages_test

import (
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/engine"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/stages"
)

func TestEmptyStageSync(t *testing.T) {
	stages.Mock(t)
}

func TestHeaderStep(t *testing.T) {
	m := stages.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 100, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
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

	initialCycle := stages.MockInsertAsInitialCycle
	if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}
}

func TestMineBlockWith1Tx(t *testing.T) {
	t.Skip("revive me")
	require, m := require.New(t), stages.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
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

		initialCycle := stages.MockInsertAsInitialCycle
		if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, log.New(), m.BlockReader, nil); err != nil {
			t.Fatal(err)
		}
	}

	chain, err = core.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 1, func(i int, gen *core.BlockGen) {
		// In block 1, addr1 sends addr2 some ether.
		tx, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), libcommon.Address{1}, uint256.NewInt(10_000), params.TxGas, u256.Num1, nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
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

	err = stages.MiningStep(m.Ctx, m.DB, m.MiningSync, "")
	require.NoError(err)

	got := <-m.PendingBlocks
	require.Equal(chain.TopBlock.Transactions().Len(), got.Transactions().Len())
	got2 := <-m.MinedBlocks
	require.Equal(chain.TopBlock.Transactions().Len(), got2.Transactions().Len())
}

func TestReorg(t *testing.T) {
	m := stages.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
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

	initialCycle := stages.MockInsertAsInitialCycle
	if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}

	// Now generate three competing branches, one short and two longer ones
	short, err := core.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	long1, err := core.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{2}) // Need to make headers different from short branch
	})
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	// Second long chain needs to be slightly shorter than the first long chain
	long2, err := core.GenerateChain(m.ChainConfig, chain.TopBlock, m.Engine, m.DB, 9, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{3}) // Need to make headers different from short branch and another long branch
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

	initialCycle = false
	if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, m.Log, m.BlockReader, nil); err != nil {
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
	if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}

	// another short chain
	// Now generate three competing branches, one short and two longer ones
	short2, err := core.GenerateChain(m.ChainConfig, long1.TopBlock, m.Engine, m.DB, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
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

	initialCycle = stages.MockInsertAsInitialCycle
	if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorReplace(t *testing.T) {
	m := stages.Mock(t)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	short, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 11, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	long, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 15, func(i int, b *core.BlockGen) {
		if i < 10 {
			b.SetCoinbase(libcommon.Address{1})
		} else {
			b.SetCoinbase(libcommon.Address{2})
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

	initialCycle := stages.MockInsertAsInitialCycle
	if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, m.Log, m.BlockReader, nil); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorReplace2(t *testing.T) {
	m := stages.Mock(t)
	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	short, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 20, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	long, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 30, func(i int, b *core.BlockGen) {
		if i < 10 {
			b.SetCoinbase(libcommon.Address{1})
		} else {
			b.SetCoinbase(libcommon.Address{2})
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

	initialCycle := stages.MockInsertAsInitialCycle
	hook := stages.NewHook(m.Ctx, m.Notifications, m.Sync, m.BlockReader, m.ChainConfig, m.Log, m.UpdateHead)
	if err := stages.StageLoopIteration(m.Ctx, m.DB, nil, m.Sync, initialCycle, m.Log, m.BlockReader, hook); err != nil {
		t.Fatal(err)
	}
}

func TestForkchoiceToGenesis(t *testing.T) {
	m := stages.MockWithZeroTTD(t, false)

	// Trivial forkChoice: everything points to genesis
	forkChoiceMessage := engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      m.Genesis.Hash(),
		SafeBlockHash:      m.Genesis.Hash(),
		FinalizedBlockHash: m.Genesis.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)

	initialCycle := stages.MockInsertAsInitialCycle
	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	assert.Equal(t, m.Genesis.Hash(), rawdb.ReadHeadBlockHash(tx))

	payloadStatus := m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus.Status)
}

func TestBogusForkchoice(t *testing.T) {
	m := stages.MockWithZeroTTD(t, false)

	// Bogus forkChoice: head points to rubbish
	forkChoiceMessage := engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      libcommon.HexToHash("11111111111111111111"),
		SafeBlockHash:      m.Genesis.Hash(),
		FinalizedBlockHash: m.Genesis.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)

	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	initialCycle := stages.MockInsertAsInitialCycle
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	payloadStatus := m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_SYNCING, payloadStatus.Status)

	// Now send a correct forkChoice
	forkChoiceMessage = engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      m.Genesis.Hash(),
		SafeBlockHash:      m.Genesis.Hash(),
		FinalizedBlockHash: m.Genesis.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)

	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	payloadStatus = m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus.Status)
}

func TestPoSDownloader(t *testing.T) {
	m := stages.MockWithZeroTTD(t, true)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	require.NoError(t, err)

	// Send a payload whose parent isn't downloaded yet
	m.SendPayloadRequest(chain.TopBlock)

	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	initialCycle := stages.MockInsertAsInitialCycle
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	payloadStatus := m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_SYNCING, payloadStatus.Status)

	// Send the missing header
	b, err := rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers[0:1],
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	m.ReceiveWg.Wait()

	// First cycle: save the downloaded header
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	// Second cycle: process the previous beacon request
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	// Point forkChoice to the head
	forkChoiceMessage := engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      chain.TopBlock.Hash(),
		SafeBlockHash:      chain.TopBlock.Hash(),
		FinalizedBlockHash: chain.TopBlock.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	assert.Equal(t, chain.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))

	payloadStatus = m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus.Status)
	assert.Equal(t, chain.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))
}

// https://hackmd.io/GDc0maGsQeKfP8o2C7L52w
func TestPoSSyncWithInvalidHeader(t *testing.T) {
	m := stages.MockWithZeroTTD(t, true)

	chain, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, b *core.BlockGen) {
		b.SetCoinbase(libcommon.Address{1})
	})
	require.NoError(t, err)

	lastValidHeader := chain.Headers[0]

	invalidParent := types.CopyHeader(chain.Headers[1])
	invalidParent.Difficulty = libcommon.Big1

	invalidTip := chain.TopBlock.Header()
	invalidTip.ParentHash = invalidParent.Hash()

	// Send a payload with the parent missing
	payloadMessage := types.NewBlockFromStorage(invalidTip.Hash(), invalidTip, chain.TopBlock.Transactions(), nil, nil)
	m.SendPayloadRequest(payloadMessage)

	tx, err := m.DB.BeginRw(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	initialCycle := stages.MockInsertAsInitialCycle
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	payloadStatus1 := m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_SYNCING, payloadStatus1.Status)

	// Send the missing headers
	b, err := rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: eth.BlockHeadersPacket{invalidParent, lastValidHeader},
	})
	require.NoError(t, err)
	m.ReceiveWg.Add(1)
	for _, err = range m.Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.PeerId}) {
		require.NoError(t, err)
	}
	m.ReceiveWg.Wait()

	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	// Point forkChoice to the invalid tip
	forkChoiceMessage := engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      invalidTip.Hash(),
		SafeBlockHash:      invalidTip.Hash(),
		FinalizedBlockHash: invalidTip.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(t, err)

	bad, lastValidHash := m.HeaderDownload().IsBadHeaderPoS(invalidTip.Hash())
	assert.True(t, bad)
	assert.Equal(t, lastValidHash, lastValidHeader.Hash())
}

func TestPOSWrongTrieRootReorgs(t *testing.T) {
	t.Skip("Need some fixes for memory mutation to support DupSort")
	//defer log.Root().SetHandler(log.Root().GetHandler())
	//log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat())))
	require := require.New(t)
	m := stages.MockWithZeroTTDGnosis(t, true)

	// One empty block
	chain0, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 1, func(i int, gen *core.BlockGen) {
		gen.SetDifficulty(big.NewInt(0))
	})
	require.NoError(err)

	// One empty block, one block with transaction for 10k wei
	chain1, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, gen *core.BlockGen) {
		gen.SetDifficulty(big.NewInt(0))
		if i == 1 {
			// In block 1, addr1 sends addr2 10_000 wei.
			tx, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), libcommon.Address{1}, uint256.NewInt(10_000), params.TxGas,
				uint256.NewInt(1_000_000_000), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
			require.NoError(err)
			gen.AddTx(tx)
		}
	})
	require.NoError(err)

	// One empty block, one block with transaction for 20k wei
	chain2, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, gen *core.BlockGen) {
		gen.SetDifficulty(big.NewInt(0))
		if i == 1 {
			// In block 1, addr1 sends addr2 20_000 wei.
			tx, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), libcommon.Address{1}, uint256.NewInt(20_000), params.TxGas,
				uint256.NewInt(1_000_000_000), nil), *types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
			require.NoError(err)
			gen.AddTx(tx)
		}
	})
	require.NoError(err)

	// 3 empty blocks
	chain3, err := core.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 3, func(i int, gen *core.BlockGen) {
		gen.SetDifficulty(big.NewInt(0))
	})
	require.NoError(err)

	//------------------------------------------
	m.SendPayloadRequest(chain0.TopBlock)
	tx, err := m.DB.BeginRw(m.Ctx)

	require.NoError(err)
	defer tx.Rollback()
	initialCycle := stages.MockInsertAsInitialCycle
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)

	payloadStatus0 := m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus0.Status)
	forkChoiceMessage := engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      chain0.TopBlock.Hash(),
		SafeBlockHash:      chain0.TopBlock.Hash(),
		FinalizedBlockHash: chain0.TopBlock.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	assert.Equal(t, chain0.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))
	payloadStatus0 = m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus0.Status)
	assert.Equal(t, chain0.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))

	//------------------------------------------
	m.SendPayloadRequest(chain1.TopBlock)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	payloadStatus1 := m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus1.Status)
	forkChoiceMessage = engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      chain1.TopBlock.Hash(),
		SafeBlockHash:      chain1.TopBlock.Hash(),
		FinalizedBlockHash: chain1.TopBlock.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	assert.Equal(t, chain1.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))
	payloadStatus1 = m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus1.Status)
	assert.Equal(t, chain1.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))

	//------------------------------------------
	m.SendPayloadRequest(chain2.TopBlock)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	payloadStatus2 := m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus2.Status)
	forkChoiceMessage = engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      chain2.TopBlock.Hash(),
		SafeBlockHash:      chain2.TopBlock.Hash(),
		FinalizedBlockHash: chain2.TopBlock.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	assert.Equal(t, chain2.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))
	payloadStatus2 = m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus2.Status)
	assert.Equal(t, chain2.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))

	//------------------------------------------
	preTop3 := chain3.Blocks[chain3.Length()-2]
	m.SendPayloadRequest(preTop3)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	payloadStatus3 := m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus3.Status)
	m.SendPayloadRequest(chain3.TopBlock)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	payloadStatus3 = m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus3.Status)
	forkChoiceMessage = engine_helpers.ForkChoiceMessage{
		HeadBlockHash:      chain3.TopBlock.Hash(),
		SafeBlockHash:      chain3.TopBlock.Hash(),
		FinalizedBlockHash: chain3.TopBlock.Hash(),
	}
	m.SendForkChoiceRequest(&forkChoiceMessage)
	err = stages.StageLoopIteration(m.Ctx, m.DB, tx, m.Sync, initialCycle, m.Log, m.BlockReader, nil)
	require.NoError(err)
	stages.SendPayloadStatus(m.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	assert.Equal(t, chain3.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))
	payloadStatus3 = m.ReceivePayloadStatus()
	assert.Equal(t, engine.EngineStatus_VALID, payloadStatus3.Status)
	assert.Equal(t, chain3.TopBlock.Hash(), rawdb.ReadHeadBlockHash(tx))
}
