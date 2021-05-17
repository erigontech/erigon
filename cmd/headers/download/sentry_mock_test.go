package download

import (
	"context"
	"crypto/ecdsa"
	"io/ioutil"
	"math/big"
	"os"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/fetcher"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/gointerfaces"
	"github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	ptypes "github.com/ledgerwatch/turbo-geth/gointerfaces/types"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rlp"
	"github.com/ledgerwatch/turbo-geth/turbo/stages"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/bodydownload"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
	sentry.UnimplementedSentryServer
	ctx          context.Context
	cancel       context.CancelFunc
	memDb        ethdb.Database
	tmpdir       string
	engine       consensus.Engine
	chainConfig  *params.ChainConfig
	sync         *stagedsync.StagedSync
	downloader   *ControlServerImpl
	key          *ecdsa.PrivateKey
	address      common.Address
	genesis      *types.Block
	sentryClient *SentryClientDirect
	stream       sentry.Sentry_ReceiveMessagesServer // Stream of annoucements and download responses,s
	peerId       *ptypes.H512
}

func (ms *MockSentry) Close() {
	ms.cancel()
	ms.memDb.Close()
	os.RemoveAll(ms.tmpdir)
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
	ms.stream = stream
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
	mock.memDb = ethdb.NewMemDatabase()
	var err error
	mock.tmpdir, err = ioutil.TempDir("", "stagesync-test")
	if err != nil {
		t.Fatal(err)
	}
	db := mock.memDb.RwKV()
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
	increment := uint64(0)
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
	txPool := core.NewTxPool(txPoolConfig, mock.chainConfig, mock.memDb, txCacher)
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
	mock.genesis = gspec.MustCommit(mock.memDb)
	blockDownloaderWindow := 128
	networkID := uint64(1)
	mock.sentryClient = &SentryClientDirect{}
	mock.sentryClient.SetServer(mock)
	sentries := []sentry.SentryClient{mock.sentryClient}
	mock.downloader, err = NewControlServer(mock.memDb, "mock", mock.chainConfig, mock.genesis.Hash(), mock.engine, networkID, sentries, blockDownloaderWindow)
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
			increment,
		),
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
		stagedsync.StageSendersCfg(db, mock.chainConfig),
		stagedsync.StageExecuteBlocksCfg(
			db,
			sm.Receipts,
			sm.CallTraces,
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
	return mock
}

func TestEmptyStageSync(t *testing.T) {
	m := mock(t)
	defer m.Close()
}

func TestHeaderStep(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	m := mock(t)
	defer m.Close()

	blocks, _, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.memDb, 100, func(i int, b *core.BlockGen) {
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
	m.stream.Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})

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
	m.stream.Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})

	notifier := &remotedbserver.Events{}
	initialCycle := true
	highestSeenHeader := uint64(blocks[len(blocks)-1].NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.memDb, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}
}

func TestReorg(t *testing.T) {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	m := mock(t)
	defer m.Close()

	blocks, _, err := core.GenerateChain(m.chainConfig, m.genesis, m.engine, m.memDb, 10, func(i int, b *core.BlockGen) {
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
	m.stream.Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})

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
	m.stream.Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})

	notifier := &remotedbserver.Events{}
	initialCycle := true
	highestSeenHeader := uint64(blocks[len(blocks)-1].NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.memDb, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}

	// Now generate two competing branches, one short one longer
	short, _, err := core.GenerateChain(m.chainConfig, blocks[len(blocks)-1], m.engine, m.memDb, 2, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{1})
	}, false /* intemediateHashes */)
	if err != nil {
		t.Fatalf("generate short fork: %v", err)
	}
	long, _, err := core.GenerateChain(m.chainConfig, blocks[len(blocks)-1], m.engine, m.memDb, 10, func(i int, b *core.BlockGen) {
		b.SetCoinbase(common.Address{2}) // Need to make headers different from short branch
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
	m.stream.Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})

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
	m.stream.Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})

	highestSeenHeader = uint64(short[len(short)-1].NumberU64())
	initialCycle = false
	if err := stages.StageLoopStep(m.ctx, m.memDb, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}

	// Send NewBlock message for long branch
	b, err = rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: long[len(long)-1],
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		t.Fatal(err)
	}
	m.stream.Send(&sentry.InboundMessage{Id: sentry.MessageId_NewBlock, Data: b, PeerId: m.peerId})

	// Send headers of the long branch
	headers = make([]*types.Header, len(long))
	for i, block := range long {
		headers[i] = block.Header()
	}
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          3,
		BlockHeadersPacket: headers,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.stream.Send(&sentry.InboundMessage{Id: sentry.MessageId_BlockHeaders, Data: b, PeerId: m.peerId})

	// This is unwind step
	highestSeenHeader = uint64(long[len(long)-1].NumberU64())
	if err := stages.StageLoopStep(m.ctx, m.memDb, m.sync, highestSeenHeader, m.chainConfig, notifier, initialCycle); err != nil {
		t.Fatal(err)
	}
}
