package download

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
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
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
	sentry.UnimplementedSentryServer
	ctx          context.Context
	cancel       context.CancelFunc
	db           ethdb.RwKV
	tmpdir       string
	engine       consensus.Engine
	chainConfig  *params.ChainConfig
	sync         *stagedsync.StagedSync
	downloader   *ControlServerImpl
	key          *ecdsa.PrivateKey
	address      common.Address
	genesis      *types.Block
	sentryClient *SentryClientDirect
	stream       sentry.Sentry_ReceiveMessagesServer // Stream of annoucements and download responses
	streamLock   sync.Mutex
	peerId       *ptypes.H512
}

// Stream returns stream, waiting if necessary
func (ms *MockSentry) Stream() sentry.Sentry_ReceiveMessagesServer {
	for {
		ms.streamLock.Lock()
		if ms.stream != nil {
			ms.streamLock.Unlock()
			return ms.stream
		}
		ms.streamLock.Unlock()
		time.Sleep(time.Millisecond)
	}
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
func (ms *MockSentry) SetStatus(context.Context, *sentry.StatusData) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) ReceiveMessages(_ *emptypb.Empty, stream sentry.Sentry_ReceiveMessagesServer) error {
	ms.streamLock.Lock()
	ms.stream = stream
	ms.streamLock.Unlock()
	<-ms.ctx.Done()
	return nil
}
func (ms *MockSentry) ReceiveUploadMessages(*emptypb.Empty, sentry.Sentry_ReceiveUploadMessagesServer) error {
	return nil
}
func (ms *MockSentry) ReceiveTxMessages(*emptypb.Empty, sentry.Sentry_ReceiveTxMessagesServer) error {
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
	updateHead := func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int) {
	}
	blockPropagator := func(ctx context.Context, block *types.Block, td *big.Int) {
	}
	blockDowloadTimeout := 10
	txCacher := core.NewTxSenderCacher(1)
	txPoolConfig := core.DefaultTxPoolConfig
	txPoolConfig.Journal = ""
	txPoolConfig.StartOnInit = true
	txPool := core.NewTxPool(txPoolConfig, mock.chainConfig, ethdb.NewObjectDatabase(mock.db), txCacher)
	txSentryClient := &SentryClientDirect{}
	txSentryClient.SetServer(mock)
	txPoolServer, err := eth.NewTxPoolServer(mock.ctx, []sentry.SentryClient{txSentryClient}, txPool)
	if err != nil {
		t.Fatal(err)
	}
	fetchTx := func(peerID string, hashes []common.Hash) error {
		txPoolServer.SendTxsRequest(context.TODO(), peerID, hashes)
		return nil
	}

	txPoolServer.TxFetcher = fetcher.NewTxFetcher(txPool.Has, txPool.AddRemotes, fetchTx)
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
	mock.genesis = gspec.MustCommit(ethdb.NewObjectDatabase(mock.db))
	blockDownloaderWindow := 128
	networkID := uint64(1)
	mock.sentryClient = &SentryClientDirect{}
	mock.sentryClient.SetServer(mock)
	sentries := []sentry.SentryClient{mock.sentryClient}
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
			updateHead,
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
		stagedsync.StageHashStateCfg(db, mock.tmpdir),
		stagedsync.StageTrieCfg(db, true, true, mock.tmpdir),
		stagedsync.StageHistoryCfg(db, mock.tmpdir),
		stagedsync.StageLogIndexCfg(db, mock.tmpdir),
		stagedsync.StageCallTracesCfg(db, 0, batchSize, mock.tmpdir, mock.chainConfig, mock.engine),
		stagedsync.StageTxLookupCfg(db, mock.tmpdir),
		stagedsync.StageTxPoolCfg(db, txPool, func() {
			txPoolServer.Start()
			txPoolServer.TxFetcher.Start()
		}),
		stagedsync.StageFinishCfg(db, mock.tmpdir),
	)
	if err = SetSentryStatus(mock.ctx, sentries, mock.downloader); err != nil {
		t.Fatal(err)
	}
	mock.peerId = gointerfaces.ConvertBytesToH512([]byte("12345"))
	go RecvMessage(mock.ctx, mock.sentryClient, mock.downloader.HandleInboundMessage)
	t.Cleanup(func() {
		mock.cancel()
		txPool.Stop()
		txPoolServer.TxFetcher.Stop()
	})
	return mock
}

func TestEmptyStageSync(t *testing.T) {
	mock(t)
}

func TestHeaderStep(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	m := mock(t)

	blocks, _, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 100, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: blocks[len(blocks)-1],
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})
	require.NoError(t, err)
	// Send all the headers
	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: headers,
	})
	require.NoError(t, err)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	notifier := &remotedbserver.Events{}
	initialCycle := true
	highestSeenHeader := uint64(blocks[len(blocks)-1].NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}
}

func TestReorg(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	m := mock(t)

	blocks, _, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.db, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate blocks: %v", err)
	}
	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: blocks[len(blocks)-1],
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send all the headers
	headers := make([]*types.Header, len(blocks))
	for i, block := range blocks {
		headers[i] = block.Header()
	}
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	notifier := &remotedbserver.Events{}
	initialCycle := true
	highestSeenHeader := uint64(blocks[len(blocks)-1].NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}

	// Now generate three competing branches, one short and two longer ones
	short, _, err := core.GenerateChain(m.chainConfig, blocks[len(blocks)-1], m.engine, m.db, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	long1, _, err := core.GenerateChain(m.chainConfig, blocks[len(blocks)-1], m.engine, m.db, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{2}) // Need to make headers different from short branch
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	// Second long chain needs to be slightly shorter than the first long chain
	long2, _, err := core.GenerateChain(m.chainConfig, blocks[len(blocks)-1], m.engine, m.db, 9, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{3}) // Need to make headers different from short branch and another long branch
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}

	// Send NewBlock message for short branch
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short[len(short)-1],
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the short branch
	headers = make([]*types.Header, len(short))
	for i, block := range short {
		headers[i] = block.Header()
	}
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          2,
		BlockHeadersPacket: headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	highestSeenHeader = uint64(short[len(short)-1].NumberU64())
	initialCycle = false
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}

	// Send NewBlock message for long1 branch
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: long1[len(long1)-1],
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the long2 branch
	headers = make([]*types.Header, len(long2))
	for i, block := range long2 {
		headers[i] = block.Header()
	}
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          3,
		BlockHeadersPacket: headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the long1 branch
	headers = make([]*types.Header, len(long1))
	for i, block := range long1 {
		headers[i] = block.Header()
	}
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          4,
		BlockHeadersPacket: headers,
	})
	require.NoError(t, err)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	// This is unwind step
	highestSeenHeader = uint64(long1[len(long1)-1].NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}

	// another short chain
	// Now generate three competing branches, one short and two longer ones
	short2, _, err := core.GenerateChain(m.chainConfig, long1[len(long1)-1], m.engine, m.db, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}

	// Send NewBlock message for short branch
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: short2[len(short2)-1],
		TD:    big.NewInt(1), // This is ignored anyway
	})
	require.NoError(t, err)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	// Send headers of the short branch
	headers = make([]*types.Header, len(short2))
	for i, block := range short2 {
		headers[i] = block.Header()
	}
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          5,
		BlockHeadersPacket: headers,
	})
	require.NoError(t, err)
	err = m.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})
	require.NoError(t, err)

	highestSeenHeader = uint64(short2[len(short2)-1].NumberU64())
	initialCycle = false
	if err := stages.StageLoopStep(m.ctx, m.db, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}
}
