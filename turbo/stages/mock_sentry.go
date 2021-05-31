package stages

import (
	"context"
	"crypto/ecdsa"
	"math/big"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/headers/download"
	"github.com/ledgerwatch/erigon/common"
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
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/remote"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/txpool"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
	sentry.UnimplementedSentryServer
	Ctx           context.Context
	cancel        context.CancelFunc
	DB            ethdb.RwKV
	tmpdir        string
	Engine        consensus.Engine
	ChainConfig   *params.ChainConfig
	Sync          *stagedsync.StagedSync
	MiningSync    *stagedsync.StagedSync
	PendingBlocks chan *types.Block
	MinedBlocks   chan *types.Block
	downloader    *download.ControlServerImpl
	Key           *ecdsa.PrivateKey
	Address       common.Address
	Genesis       *types.Block
	sentryClient  remote.SentryClient
	PeerId        *ptypes.H512
	ReceiveWg     sync.WaitGroup
	UpdateHead    func(Ctx context.Context, head uint64, hash common.Hash, td *uint256.Int)

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
	return &sentry.SetStatusReply{Protocol: sentry.Protocol_ETH66}, nil
}
func (ms *MockSentry) ReceiveMessages(_ *emptypb.Empty, stream sentry.Sentry_ReceiveMessagesServer) error {
	ms.stream = stream
	ms.streamWg.Done()
	<-ms.Ctx.Done()
	return nil
}
func (ms *MockSentry) ReceiveUploadMessages(_ *emptypb.Empty, stream sentry.Sentry_ReceiveUploadMessagesServer) error {
	ms.uploadStream = stream
	ms.streamWg.Done()
	<-ms.Ctx.Done()
	return nil
}
func (ms *MockSentry) ReceiveTxMessages(_ *emptypb.Empty, stream sentry.Sentry_ReceiveTxMessagesServer) error {
	ms.txStream = stream
	ms.streamWg.Done()
	<-ms.Ctx.Done()
	return nil
}

func MockWithGenesis(t *testing.T, gspec *core.Genesis, key *ecdsa.PrivateKey) *MockSentry {
	return MockWithGenesisStorageMode(t, gspec, key, ethdb.DefaultStorageMode)
}

func MockWithGenesisStorageMode(t *testing.T, gspec *core.Genesis, key *ecdsa.PrivateKey, sm ethdb.StorageMode) *MockSentry {
	mock := &MockSentry{}
	mock.Ctx, mock.cancel = context.WithCancel(context.Background())
	mock.DB = ethdb.NewTestKV(t)
	var err error
	mock.tmpdir = t.TempDir()
	mock.Engine = ethash.NewFaker()
	mock.ChainConfig = gspec.Config
	mock.Key = key
	mock.Address = crypto.PubkeyToAddress(mock.Key.PublicKey)
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
	mock.UpdateHead = func(Ctx context.Context, head uint64, hash common.Hash, td *uint256.Int) {
	}
	blockPropagator := func(Ctx context.Context, block *types.Block, td *big.Int) {
	}
	blockDowloadTimeout := 10
	txCacher := core.NewTxSenderCacher(1)
	txPoolConfig := core.DefaultTxPoolConfig
	txPoolConfig.Journal = ""
	txPoolConfig.StartOnInit = true
	txPool := core.NewTxPool(txPoolConfig, mock.ChainConfig, ethdb.NewObjectDatabase(mock.DB), txCacher)
	txSentryClient := remote.NewSentryClientDirect(eth.ETH66, mock)
	txPoolP2PServer, err := txpool.NewP2PServer(mock.Ctx, []remote.SentryClient{txSentryClient}, txPool)
	if err != nil {
		t.Fatal(err)
	}
	fetchTx := func(PeerId string, hashes []common.Hash) error {
		txPoolP2PServer.SendTxsRequest(context.TODO(), PeerId, hashes)
		return nil
	}

	txPoolP2PServer.TxFetcher = fetcher.NewTxFetcher(txPool.Has, txPool.AddRemotes, fetchTx)
	// Committed genesis will be shared between download and mock sentry
	_, mock.Genesis, err = core.SetupGenesisBlock(ethdb.NewObjectDatabase(mock.DB), gspec, sm.History)
	if _, ok := err.(*params.ConfigCompatError); err != nil && !ok {
		t.Fatal(err)
	}

	blockDownloaderWindow := 128
	networkID := uint64(1)
	mock.sentryClient = remote.NewSentryClientDirect(eth.ETH66, mock)
	sentries := []remote.SentryClient{mock.sentryClient}
	mock.downloader, err = download.NewControlServer(mock.DB, "mock", mock.ChainConfig, mock.Genesis.Hash(), mock.Engine, networkID, sentries, blockDownloaderWindow)
	if err != nil {
		t.Fatal(err)
	}
	mock.Sync = NewStagedSync(mock.Ctx, sm,
		stagedsync.StageHeadersCfg(
			mock.DB,
			mock.downloader.Hd,
			*mock.ChainConfig,
			sendHeaderRequest,
			propagateNewBlockHashes,
			penalize,
			batchSize,
		),
		stagedsync.StageBlockHashesCfg(mock.DB, mock.tmpdir),
		stagedsync.StageBodiesCfg(
			mock.DB,
			mock.downloader.Bd,
			sendBodyRequest,
			penalize,
			blockPropagator,
			blockDowloadTimeout,
			*mock.ChainConfig,
			batchSize,
		),
		stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, mock.tmpdir),
		stagedsync.StageExecuteBlocksCfg(
			mock.DB,
			sm.Receipts,
			sm.CallTraces,
			sm.TEVM,
			0,
			batchSize,
			nil,
			nil,
			nil,
			nil,
			mock.ChainConfig,
			mock.Engine,
			&vm.Config{NoReceipts: !sm.Receipts},
			mock.tmpdir,
		),
		stagedsync.StageTranspileCfg(
			mock.DB,
			batchSize,
			nil,
			nil,
			mock.ChainConfig,
		),
		stagedsync.StageHashStateCfg(mock.DB, mock.tmpdir),
		stagedsync.StageTrieCfg(mock.DB, true, true, mock.tmpdir),
		stagedsync.StageHistoryCfg(mock.DB, mock.tmpdir),
		stagedsync.StageLogIndexCfg(mock.DB, mock.tmpdir),
		stagedsync.StageCallTracesCfg(mock.DB, 0, batchSize, mock.tmpdir, mock.ChainConfig, mock.Engine),
		stagedsync.StageTxLookupCfg(mock.DB, mock.tmpdir),
		stagedsync.StageTxPoolCfg(mock.DB, txPool, func() {
			mock.streamWg.Add(1)
			go txpool.RecvTxMessage(mock.Ctx, mock.sentryClient, txPoolP2PServer.HandleInboundMessage, &mock.ReceiveWg)
			txPoolP2PServer.TxFetcher.Start()
		}),
		stagedsync.StageFinishCfg(mock.DB, mock.tmpdir),
		true, /* test */
	)

	miningConfig := ethconfig.Defaults.Miner
	miningConfig.Enabled = true
	miningConfig.Noverify = false
	miningConfig.Etherbase = mock.Address
	miningConfig.SigKey = mock.Key

	mock.PendingBlocks = make(chan *types.Block, 1)
	mock.MinedBlocks = make(chan *types.Block, 1)

	mock.MiningSync = stagedsync.New(
		stagedsync.MiningStages(
			stagedsync.StageMiningCreateBlockCfg(mock.DB, miningConfig, *mock.ChainConfig, mock.Engine, txPool, mock.tmpdir),
			stagedsync.StageMiningExecCfg(mock.DB, miningConfig, nil, *mock.ChainConfig, mock.Engine, &vm.Config{}, mock.tmpdir),
			stagedsync.StageHashStateCfg(mock.DB, mock.tmpdir),
			stagedsync.StageTrieCfg(mock.DB, false, true, mock.tmpdir),
			stagedsync.StageMiningFinishCfg(mock.DB, *mock.ChainConfig, mock.Engine, mock.PendingBlocks, mock.MinedBlocks, mock.Ctx.Done()),
		),
		stagedsync.MiningUnwindOrder(),
		stagedsync.OptionalParameters{},
	)

	mock.PeerId = gointerfaces.ConvertBytesToH512([]byte("12345"))
	mock.streamWg.Add(1)
	go download.RecvMessageLoop(mock.Ctx, mock.sentryClient, mock.downloader, &mock.ReceiveWg)
	t.Cleanup(func() {
		mock.cancel()
		txPool.Stop()
		txPoolP2PServer.TxFetcher.Stop()
	})
	return mock
}

// Mock is conviniece function to create a mock with some pre-set values
func Mock(t *testing.T) *MockSentry {
	funds := big.NewInt(1000000000)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.AllEthashProtocolChanges
	gspec := &core.Genesis{
		Config: chainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	return MockWithGenesis(t, gspec, key)
}

func (ms *MockSentry) InsertChain(chain *core.ChainPack) error {
	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: chain.TopBlock,
		TD:    big.NewInt(1), // This is ignored anyway
	})
	if err != nil {
		return err
	}
	ms.ReceiveWg.Add(1)
	if err = ms.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: ms.PeerId}); err != nil {
		return err
	}
	// Send all the headers
	b, err = rlp.EncodeToBytes(&eth.BlockHeadersPacket66{
		RequestId:          1,
		BlockHeadersPacket: chain.Headers,
	})
	if err != nil {
		return err
	}
	ms.ReceiveWg.Add(1)
	if err = ms.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: ms.PeerId}); err != nil {
		return err
	}
	// Send all the bodies
	packet := make(eth.BlockBodiesPacket, chain.Length)
	for i, block := range chain.Blocks {
		packet[i] = (*eth.BlockBody)(block.Body())
	}
	b, err = rlp.EncodeToBytes(&eth.BlockBodiesPacket66{
		RequestId:         1,
		BlockBodiesPacket: packet,
	})
	if err != nil {
		return err
	}
	ms.ReceiveWg.Add(1)
	if err = ms.Stream().Send(&sentry.InboundMessage{Id: sentry.MessageId_BLOCK_BODIES_66, Data: b, PeerId: ms.PeerId}); err != nil {
		return err
	}
	ms.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed
	notifier := &remotedbserver.Events{}
	initialCycle := true
	highestSeenHeader := uint64(chain.TopBlock.NumberU64())
	if err := StageLoopStep(ms.Ctx, ms.DB, ms.Sync, highestSeenHeader, ms.ChainConfig, notifier, initialCycle, nil, ms.UpdateHead, nil); err != nil {
		return err
	}
	return nil
}
