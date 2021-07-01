package stages

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	ptypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/fetcher"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/kv"
	"github.com/ledgerwatch/erigon/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/remote"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/stages/txpropagate"
	"github.com/ledgerwatch/erigon/turbo/txpool"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
	proto_sentry.UnimplementedSentryServer
	Ctx             context.Context
	t               *testing.T
	cancel          context.CancelFunc
	DB              ethdb.RwKV
	tmpdir          string
	Engine          consensus.Engine
	ChainConfig     *params.ChainConfig
	Sync            *stagedsync.StagedSync
	MiningSync      *stagedsync.StagedSync
	PendingBlocks   chan *types.Block
	MinedBlocks     chan *types.Block
	downloader      *download.ControlServerImpl
	Key             *ecdsa.PrivateKey
	Genesis         *types.Block
	SentryClient    remote.SentryClient
	PeerId          *ptypes.H512
	TxPoolP2PServer *txpool.P2PServer
	UpdateHead      func(Ctx context.Context, head uint64, hash common.Hash, td *uint256.Int)
	streams         map[proto_sentry.MessageId][]proto_sentry.Sentry_MessagesServer
	sentMessages    []*proto_sentry.OutboundMessageData
	StreamWg        sync.WaitGroup
	ReceiveWg       sync.WaitGroup
	Address         common.Address
}

// Stream returns stream, waiting if necessary
func (ms *MockSentry) Send(req *proto_sentry.InboundMessage) (errs []error) {
	ms.StreamWg.Wait()
	for _, stream := range ms.streams[req.Id] {
		if err := stream.Send(req); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (ms *MockSentry) PenalizePeer(context.Context, *proto_sentry.PenalizePeerRequest) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) PeerMinBlock(context.Context, *proto_sentry.PeerMinBlockRequest) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) SendMessageByMinBlock(_ context.Context, r *proto_sentry.SendMessageByMinBlockRequest) (*proto_sentry.SentPeers, error) {
	ms.sentMessages = append(ms.sentMessages, r.Data)
	return nil, nil
}
func (ms *MockSentry) SendMessageById(_ context.Context, r *proto_sentry.SendMessageByIdRequest) (*proto_sentry.SentPeers, error) {
	ms.sentMessages = append(ms.sentMessages, r.Data)
	return nil, nil
}
func (ms *MockSentry) SendMessageToRandomPeers(_ context.Context, r *proto_sentry.SendMessageToRandomPeersRequest) (*proto_sentry.SentPeers, error) {
	ms.sentMessages = append(ms.sentMessages, r.Data)
	return nil, nil
}
func (ms *MockSentry) SendMessageToAll(_ context.Context, r *proto_sentry.OutboundMessageData) (*proto_sentry.SentPeers, error) {
	ms.sentMessages = append(ms.sentMessages, r)
	return nil, nil
}
func (ms *MockSentry) SentMessage(i int) *proto_sentry.OutboundMessageData {
	return ms.sentMessages[i]
}
func (ms *MockSentry) SetStatus(context.Context, *proto_sentry.StatusData) (*proto_sentry.SetStatusReply, error) {
	return &proto_sentry.SetStatusReply{Protocol: proto_sentry.Protocol_ETH66}, nil
}
func (ms *MockSentry) Messages(req *proto_sentry.MessagesRequest, stream proto_sentry.Sentry_MessagesServer) error {
	if ms.streams == nil {
		ms.streams = map[proto_sentry.MessageId][]proto_sentry.Sentry_MessagesServer{}
	}

	for _, id := range req.Ids {
		ms.streams[id] = append(ms.streams[id], stream)
	}
	ms.StreamWg.Done()
	select {
	case <-ms.Ctx.Done():
		return nil
	case <-stream.Context().Done():
		return nil
	}
}

func MockWithGenesis(t *testing.T, gspec *core.Genesis, key *ecdsa.PrivateKey) *MockSentry {
	return MockWithGenesisStorageMode(t, gspec, key, ethdb.DefaultStorageMode)
}

func MockWithGenesisEngine(t *testing.T, gspec *core.Genesis, engine consensus.Engine) *MockSentry {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	return MockWithEverything(t, gspec, key, ethdb.DefaultStorageMode, engine)
}

func MockWithGenesisStorageMode(t *testing.T, gspec *core.Genesis, key *ecdsa.PrivateKey, sm ethdb.StorageMode) *MockSentry {
	return MockWithEverything(t, gspec, key, sm, ethash.NewFaker())
}

func MockWithEverything(t *testing.T, gspec *core.Genesis, key *ecdsa.PrivateKey, sm ethdb.StorageMode, engine consensus.Engine) *MockSentry {
	var tmpdir string
	if t != nil {
		tmpdir = t.TempDir()
	} else {
		tmpdir = os.TempDir()
	}
	mock := &MockSentry{
		t:           t,
		DB:          kv.NewTestKV(t),
		tmpdir:      tmpdir,
		Engine:      engine,
		ChainConfig: gspec.Config,
		Key:         key,
	}
	mock.Ctx, mock.cancel = context.WithCancel(context.Background())
	mock.Address = crypto.PubkeyToAddress(mock.Key.PublicKey)
	var err error
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
	txPoolConfig := core.DefaultTxPoolConfig
	txPoolConfig.Journal = ""
	txPoolConfig.StartOnInit = true
	txPool := core.NewTxPool(txPoolConfig, mock.ChainConfig, mock.DB)
	txSentryClient := remote.NewSentryClientDirect(eth.ETH66, mock)
	mock.TxPoolP2PServer, err = txpool.NewP2PServer(mock.Ctx, []remote.SentryClient{txSentryClient}, txPool)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		} else {
			panic(err)
		}
	}
	fetchTx := func(PeerId string, hashes []common.Hash) error {
		mock.TxPoolP2PServer.SendTxsRequest(context.TODO(), PeerId, hashes)
		return nil
	}

	mock.TxPoolP2PServer.TxFetcher = fetcher.NewTxFetcher(txPool.Has, txPool.AddRemotes, fetchTx)
	// Committed genesis will be shared between download and mock sentry
	_, mock.Genesis, err = core.CommitGenesisBlock(mock.DB, gspec, sm.History)
	if _, ok := err.(*params.ConfigCompatError); err != nil && !ok {
		if t != nil {
			t.Fatal(err)
		} else {
			panic(err)
		}
	}

	blockDownloaderWindow := 65536
	networkID := uint64(1)
	mock.SentryClient = remote.NewSentryClientDirect(eth.ETH66, mock)
	sentries := []remote.SentryClient{mock.SentryClient}
	mock.downloader, err = download.NewControlServer(mock.DB, "mock", mock.ChainConfig, mock.Genesis.Hash(), mock.Engine, networkID, sentries, blockDownloaderWindow)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		} else {
			panic(err)
		}
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
		stagedsync.StageHeadersSnapshotGenCfg(mock.DB, mock.tmpdir),
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
			mock.StreamWg.Add(1)
			go txpool.RecvTxMessageLoop(mock.Ctx, mock.SentryClient, mock.downloader, mock.TxPoolP2PServer.HandleInboundMessage, &mock.ReceiveWg)
			go txpropagate.BroadcastNewTxsToNetworks(mock.Ctx, txPool, mock.downloader)
			mock.StreamWg.Wait()
			mock.TxPoolP2PServer.TxFetcher.Start()
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
	mock.StreamWg.Add(1)
	go download.RecvMessageLoop(mock.Ctx, mock.SentryClient, mock.downloader, &mock.ReceiveWg)
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	go download.RecvUploadMessageLoop(mock.Ctx, mock.SentryClient, mock.downloader, &mock.ReceiveWg)
	mock.StreamWg.Wait()
	if t != nil {
		t.Cleanup(func() {
			mock.cancel()
			txPool.Stop()
			mock.TxPoolP2PServer.TxFetcher.Stop()
		})
	}
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

func (ms *MockSentry) EnableLogs() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	ms.t.Cleanup(func() {
		log.Root().SetHandler(log.Root().GetHandler())
	})
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
	for _, err = range ms.Send(&proto_sentry.InboundMessage{Id: proto_sentry.MessageId_NEW_BLOCK_66, Data: b, PeerId: ms.PeerId}) {
		if err != nil {
			return err
		}
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
	for _, err = range ms.Send(&proto_sentry.InboundMessage{Id: proto_sentry.MessageId_BLOCK_HEADERS_66, Data: b, PeerId: ms.PeerId}) {
		if err != nil {
			return err
		}
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
	for _, err = range ms.Send(&proto_sentry.InboundMessage{Id: proto_sentry.MessageId_BLOCK_BODIES_66, Data: b, PeerId: ms.PeerId}) {
		if err != nil {
			return err
		}
	}
	ms.ReceiveWg.Wait() // Wait for all messages to be processed before we proceeed
	notifier := &remotedbserver.Events{}
	initialCycle := false
	highestSeenHeader := uint64(chain.TopBlock.NumberU64())
	if err := StageLoopStep(ms.Ctx, ms.DB, ms.Sync, highestSeenHeader, ms.ChainConfig, notifier, initialCycle, nil, ms.UpdateHead, nil); err != nil {
		return err
	}
	// Check if the latest header was imported or rolled back
	if err = ms.DB.View(ms.Ctx, func(tx ethdb.Tx) error {
		if rawdb.ReadHeader(tx, chain.TopBlock.Hash(), chain.TopBlock.NumberU64()) == nil {
			return fmt.Errorf("did not import block %d %x", chain.TopBlock.NumberU64(), chain.TopBlock.Hash())
		}
		execAt, err := stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		if execAt == 0 {
			return fmt.Errorf("sentryMock.InsertChain end up with Execution stage progress = 0")
		}
		return nil
	}); err != nil {
		return err
	}
	if ms.downloader.Hd.IsBadHeader(chain.TopBlock.Hash()) {
		return fmt.Errorf("block %d %x was invalid", chain.TopBlock.NumberU64(), chain.TopBlock.Hash())
	}
	return nil
}
