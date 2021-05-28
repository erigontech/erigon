package download

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"os"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/fetcher"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/erigon/gointerfaces"
	"github.com/ledgerwatch/erigon/gointerfaces/sentry"
	ptypes "github.com/ledgerwatch/erigon/gointerfaces/types"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/remote"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/txpool"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
	sentry.UnimplementedSentryServer
	ctx           context.Context
	cancel        context.CancelFunc
	db            ethdb.RwKV
	tmpdir        string
	engine        consensus.Engine
	chainConfig   *params.ChainConfig
	sync          *stagedsync.StagedSync
	miningSync    *stagedsync.StagedSync
	pendingBlocks chan *types.Block
	minedBlocks   chan *types.Block
	downloader    *ControlServerImpl
	key           *ecdsa.PrivateKey
	address       common.Address
	genesis       *types.Block
	sentryClient  *remote.SentryClientDirect
	peerId        *ptypes.H512
	receiveWg     sync.WaitGroup
	updateHead    func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int)

	stream       sentry.Sentry_ReceiveMessagesServer // Stream of annoucements and download responses
	txStream     sentry.Sentry_ReceiveTxMessagesServer
	uploadStream sentry.Sentry_ReceiveUploadMessagesServer
	streamWg     sync.WaitGroup
}

// Stream returns stream, waiting if necessary
func (ms *MockSentry) Stream() sentry.Sentry_ReceiveMessagesServer {
	ms.streamWg.Wait()
	return ms.stream
}
func (ms *MockSentry) TxStream() sentry.Sentry_ReceiveTxMessagesServer {
	ms.streamWg.Wait()
	return ms.txStream
}
func (ms *MockSentry) UploadStream() sentry.Sentry_ReceiveUploadMessagesServer {
	ms.streamWg.Wait()
	return ms.uploadStream
}

func (ms *MockSentry) PenalizePeer(context.Context, *sentry.PenalizePeerRequest) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) PeerMinBlock(context.Context, *sentry.PeerMinBlockRequest) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageByMinBlock(context.Context, *sentry.SendMessageByMinBlockRequest) (*sentry.SentPeers, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageById(context.Context, *sentry.SendMessageByIdRequest) (*sentry.SentPeers, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageToRandomPeers(context.Context, *sentry.SendMessageToRandomPeersRequest) (*sentry.SentPeers, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageToAll(context.Context, *sentry.OutboundMessageData) (*sentry.SentPeers, error) {
	return nil, nil
}
func (ms *MockSentry) SetStatus(context.Context, *sentry.StatusData) (*sentry.SetStatusReply, error) {
	return nil, nil
}
func (ms *MockSentry) ReceiveMessages(_ *emptypb.Empty, stream sentry.Sentry_ReceiveMessagesServer) error {
	ms.stream = stream
	ms.streamWg.Done()
	<-ms.ctx.Done()
	return nil
}
func (ms *MockSentry) ReceiveUploadMessages(_ *emptypb.Empty, stream sentry.Sentry_ReceiveUploadMessagesServer) error {
	ms.uploadStream = stream
	ms.streamWg.Done()
	<-ms.ctx.Done()
	return nil
}
func (ms *MockSentry) ReceiveTxMessages(_ *emptypb.Empty, stream sentry.Sentry_ReceiveTxMessagesServer) error {
	ms.txStream = stream
	ms.streamWg.Done()
	<-ms.ctx.Done()
	return nil
}

func mock(t *testing.T) *MockSentry {
	mock := &MockSentry{}
	mock.ctx, mock.cancel = context.WithCancel(context.Background())
	mock.db = ethdb.NewTestKV(t)
	var err error
	mock.tmpdir = t.TempDir()
	db := mock.db
	sm := ethdb.DefaultStorageMode
	mock.engine = ethash.NewFaker()
	mock.chainConfig = params.AllEthashProtocolChanges
	sendHeaderRequest := func(_ context.Context, r *headerdownload.HeaderRequest) []byte {
		return nil
	}
	propagateNewBlockHashes := func(context.Context, []headerdownload.Announce) {
	}
	penalize := func(context.Context, []headerdownload.PenaltyItem) {
	}
	batchSize := 1 * datasize.MB
	sendBodyRequest := func(context.Context, *bodydownload.BodyRequest) []byte {
		return nil
	}
	mock.updateHead = func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int) {
	}
	blockPropagator := func(ctx context.Context, block *types.Block, td *big.Int) {
	}
	blockDowloadTimeout := 10
	txCacher := core.NewTxSenderCacher(1)
	txPoolConfig := core.DefaultTxPoolConfig
	txPoolConfig.Journal = ""
	txPoolConfig.StartOnInit = true
	txPool := core.NewTxPool(txPoolConfig, mock.chainConfig, ethdb.NewObjectDatabase(mock.db), txCacher)
	txPoolP2PServer, err := txpool.NewP2PServer(mock.ctx, []remote.SentryClient{
		remote.NewSentryClientDirect(eth.ETH66, mock),
	}, txPool)
	if err != nil {
		t.Fatal(err)
	}
	fetchTx := func(peerID string, hashes []common.Hash) error {
		txPoolP2PServer.SendTxsRequest(context.TODO(), peerID, hashes)
		return nil
	}

	txPoolP2PServer.TxFetcher = fetcher.NewTxFetcher(txPool.Has, txPool.AddRemotes, fetchTx)
	mock.key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	mock.address = crypto.PubkeyToAddress(mock.key.PublicKey)
	funds := big.NewInt(1000000000)
	gspec := &core.Genesis{
		Config: mock.chainConfig,
		Alloc: core.GenesisAlloc{
			mock.address: {Balance: funds},
		},
	}
	// Committed genesis will be shared between download and mock sentry
	_, mock.genesis, err = core.SetupGenesisBlock(ethdb.NewObjectDatabase(mock.db), gspec, sm.History)
	if _, ok := err.(*params.ConfigCompatError); err != nil && !ok {
		t.Fatal(err)
	}

	//mock.genesis = gspec.MustCommit()
	blockDownloaderWindow := 128
	networkID := uint64(1)
	mock.sentryClient = remote.NewSentryClientDirect(eth.ETH66, mock)
	sentries := []remote.SentryClient{mock.sentryClient}
	mock.downloader, err = NewControlServer(db, "mock", mock.chainConfig, mock.genesis.Hash(), mock.engine, networkID, sentries, blockDownloaderWindow)
	if err != nil {
		t.Fatal(err)
	}
	mock.sync = stages.NewStagedSync(mock.ctx, sm,
		stagedsync.StageHeadersCfg(
			db,
			mock.downloader.hd,
			*mock.chainConfig,
			sendHeaderRequest,
			propagateNewBlockHashes,
			penalize,
			batchSize,
		),
		stagedsync.StageBlockHashesCfg(db, mock.tmpdir),
		stagedsync.StageBodiesCfg(
			db,
			mock.downloader.bd,
			sendBodyRequest,
			penalize,
			blockPropagator,
			blockDowloadTimeout,
			*mock.chainConfig,
			batchSize,
		),
		stagedsync.StageSendersCfg(db, mock.chainConfig, mock.tmpdir),
		stagedsync.StageExecuteBlocksCfg(
			db,
			sm.Receipts,
			sm.CallTraces,
			sm.TEVM,
			0,
			batchSize,
			nil,
			nil,
			nil,
			nil,
			mock.chainConfig,
			mock.engine,
			&vm.Config{NoReceipts: !sm.Receipts},
			mock.tmpdir,
		),
		stagedsync.StageTranspileCfg(
			db,
			batchSize,
			nil,
			nil,
			mock.chainConfig,
		),
		stagedsync.StageHashStateCfg(db, mock.tmpdir),
		stagedsync.StageTrieCfg(db, true, true, mock.tmpdir),
		stagedsync.StageHistoryCfg(db, mock.tmpdir),
		stagedsync.StageLogIndexCfg(db, mock.tmpdir),
		stagedsync.StageCallTracesCfg(db, 0, batchSize, mock.tmpdir, mock.chainConfig, mock.engine),
		stagedsync.StageTxLookupCfg(db, mock.tmpdir),
		stagedsync.StageTxPoolCfg(db, txPool, func() {
			mock.streamWg.Add(1)
			go txpool.RecvTxMessage(mock.ctx, mock.sentryClient, txPoolP2PServer.HandleInboundMessage, &mock.receiveWg)
			txPoolP2PServer.TxFetcher.Start()
		}),
		stagedsync.StageFinishCfg(db, mock.tmpdir),
	)
	if err = SetSentryStatus(mock.ctx, sentries, mock.downloader); err != nil {
		t.Fatal(err)
	}

	miningConfig := ethconfig.Defaults.Miner
	miningConfig.Enabled = true
	miningConfig.Noverify = false
	miningConfig.Etherbase = mock.address
	miningConfig.SigKey = mock.key

	mock.pendingBlocks = make(chan *types.Block, 1)
	mock.minedBlocks = make(chan *types.Block, 1)

	mock.miningSync = stagedsync.New(
		stagedsync.MiningStages(
			stagedsync.StageMiningCreateBlockCfg(db, miningConfig, *mock.chainConfig, mock.engine, txPool, mock.tmpdir),
			stagedsync.StageMiningExecCfg(db, miningConfig, nil, *mock.chainConfig, mock.engine, &vm.Config{}, mock.tmpdir),
			stagedsync.StageHashStateCfg(db, mock.tmpdir),
			stagedsync.StageTrieCfg(db, false, true, mock.tmpdir),
			stagedsync.StageMiningFinishCfg(db, *mock.chainConfig, mock.engine, mock.pendingBlocks, mock.minedBlocks, mock.ctx.Done()),
		),
		stagedsync.MiningUnwindOrder(),
		stagedsync.OptionalParameters{},
	)

	mock.peerId = gointerfaces.ConvertBytesToH512([]byte("12345"))
	mock.streamWg.Add(1)
	go RecvMessage(mock.ctx, mock.sentryClient, mock.downloader.HandleInboundMessage, &mock.receiveWg)
	t.Cleanup(func() {
		mock.cancel()
		txPool.Stop()
		txPoolP2PServer.TxFetcher.Stop()
	})
	return mock
}

func TestEmptyStageSync(t *testing.T) {
	mock(t)
}

func TestHeaderStep(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	m := mock(t)

	chain, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 100, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: chain.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)
	// Send all the headers
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)
	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	notifier := &remotedbserver.Events{}
	initialCycle := true
	highestSeenHeader := uint64(chain.TopBlock.NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle, nil, m.updateHead); err != nil {
		t.Fatal(err)
	}
}

func TestMineBlockWith1Tx(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	require, m := require.New(t), mock(t)

	chain, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 1, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	require.NoError(err)
	{ // Do 1 step to start txPool

		// Send NewBlock message
		b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
			Block: chain.TopBlock,
			TD:    big.NewInt(1), // This is ignored anyway
		})
		require.NoError(err)
		m.receiveWg.Add(1)
		err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
		require.NoError(err)
		// Send all the headers
		b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
			RequestId:          1,
			BlockHeadersPacket: chain.Headers,
		})
		require.NoError(err)
		m.receiveWg.Add(1)
		err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
		require.NoError(err)
		m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

		notifier := &remotedbserver.Events{}
		initialCycle := true
		highestSeenHeader := uint64(chain.TopBlock.NumberU64())
		if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle, nil, m.updateHead); err != nil {
			t.Fatal(err)
		}
	}

	chain, err = core.GenerateChain(m.chainConfig, chain.TopBlock, m.engine, m.db, 1, func(i int, gen *core.BlockGen) {
		// In block 1, addr1 sends addr2 some ether.
		tx, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.address), common.Address{1}, uint256.NewInt().SetUint64(10_000), params.TxGas, u256.Num1, nil), *types.LatestSignerForChainID(m.chainConfig.ChainID), m.key)
		require.NoError(err)
		gen.AddTx(tx)
	}, false /* intemediateHashes */)
	require.NoError(err)

	// Send NewBlock message
	b, err := rlp.EncodeToBytes(chain.TopBlock.Transactions())
	require.NoError(err)
	m.receiveWg.Add(1)
	err = m.TxStream().Send(&sentry.InboundMessage{Id: sentry.MessageId_TRANSACTIONS_66, Data: b, PeerId: m.peerId})
	require.NoError(err)
	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	err = stages.MiningStep(m.ctx, m.db, m.miningSync)
	require.NoError(err)

	got := <-m.pendingBlocks
	require.Equal(chain.TopBlock.Transactions().Len(), got.Transactions().Len())
	got2 := <-m.minedBlocks
	require.Equal(chain.TopBlock.Transactions().Len(), got2.Transactions().Len())
}

func TestReorg(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	m := mock(t)

	chain, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
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
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send all the headers
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)
	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	notifier := &remotedbserver.Events{}
	initialCycle := true
	highestSeenHeader := uint64(chain.TopBlock.NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle, nil, m.updateHead); err != nil {
		t.Fatal(err)
	}

	// Now generate three competing branches, one short and two longer ones
	short, err := core.GenerateChain(m.chainConfig, chain.TopBlock, m.engine, m.db, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	long1, err := core.GenerateChain(m.chainConfig, chain.TopBlock, m.engine, m.db, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{2}) // Need to make headers different from short branch
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	// Second long chain needs to be slightly shorter than the first long chain
	long2, err := core.GenerateChain(m.chainConfig, chain.TopBlock, m.engine, m.db, 9, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{3}) // Need to make headers different from short branch and another long branch
	}, false /* intemediateHashes */)
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
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the short branch
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          2,
		BlockHeadersPacket: short.Headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)
	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	highestSeenHeader = uint64(short.TopBlock.NumberU64())
	initialCycle = false
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle, nil, m.updateHead); err != nil {
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
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the long2 branch
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          3,
		BlockHeadersPacket: long2.Headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the long1 branch
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          4,
		BlockHeadersPacket: long1.Headers,
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)
	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	// This is unwind step
	highestSeenHeader = uint64(long1.TopBlock.NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle, nil, m.updateHead); err != nil {
		t.Fatal(err)
	}

	// another short chain
	// Now generate three competing branches, one short and two longer ones
	short2, err := core.GenerateChain(m.chainConfig, long1.TopBlock, m.engine, m.db, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}

	// Send NewBlock message for short branch
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short2.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the short branch
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          5,
		BlockHeadersPacket: short2.Headers,
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)
	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	highestSeenHeader = uint64(short2.TopBlock.NumberU64())
	initialCycle = false
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle, nil, m.updateHead); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorReplace(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	m := mock(t)

	chain, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	short, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 11, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	long, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 15, func(i int, b *core.BlockGen) {
		if i < 10 {
			b.SetCoinbase(common.Address{1})
		} else {
			b.SetCoinbase(common.Address{2})
		}
	}, false /* intemediateHashes */)
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
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the long suffix
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: long.Headers[10:],
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Create anchor from the short chain suffix
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the short suffix
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          2,
		BlockHeadersPacket: short.Headers[10:],
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	// Now send the prefix chain
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          3,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	highestSeenHeader := uint64(long.TopBlock.NumberU64())
	notifier := &remotedbserver.Events{}
	initialCycle := true
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle, nil, m.updateHead); err != nil {
		t.Fatal(err)
	}
}

func TestAnchorReplace2(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	m := mock(t)
	chain, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	short, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 20, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}

	long, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 30, func(i int, b *core.BlockGen) {
		if i < 10 {
			b.SetCoinbase(common.Address{1})
		} else {
			b.SetCoinbase(common.Address{2})
		}
	}, false /* intemediateHashes */)
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
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the long suffix
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: long.Headers[10:],
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_GET_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Create anchor from the short chain suffix
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the short suffix (far end)
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          2,
		BlockHeadersPacket: short.Headers[15:],
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the short suffix (near end)
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          3,
		BlockHeadersPacket: short.Headers[10:15],
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	// Now send the prefix chain
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          4,
		BlockHeadersPacket: chain.Headers,
	})
	require.NoError(t, err)
	m.receiveWg.Add(1)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	m.receiveWg.Wait() // Wait for all messages to be processed before we proceeed

	highestSeenHeader := uint64(long.TopBlock.NumberU64())
	notifier := &remotedbserver.Events{}
	initialCycle := true
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle, nil, m.updateHead); err != nil {
		t.Fatal(err)
	}
}
