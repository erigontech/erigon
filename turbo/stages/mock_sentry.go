package stages

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	ptypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"
)

type MockSentry struct {
	proto_sentry.UnimplementedSentryServer
	Ctx           context.Context
	Log           log.Logger
	t             *testing.T
	cancel        context.CancelFunc
	DB            kv.RwDB
	tmpdir        string
	snapshotDir   *dir.Rw
	Engine        consensus.Engine
	ChainConfig   *params.ChainConfig
	Sync          *stagedsync.Sync
	MiningSync    *stagedsync.Sync
	PendingBlocks chan *types.Block
	MinedBlocks   chan *types.Block
	downloader    *sentry.ControlServerImpl
	Key           *ecdsa.PrivateKey
	Genesis       *types.Block
	SentryClient  direct.SentryClient
	PeerId        *ptypes.H256
	UpdateHead    func(Ctx context.Context, head uint64, hash common.Hash, td *uint256.Int)
	streams       map[proto_sentry.MessageId][]proto_sentry.Sentry_MessagesServer
	sentMessages  []*proto_sentry.OutboundMessageData
	StreamWg      sync.WaitGroup
	ReceiveWg     sync.WaitGroup
	Address       common.Address

	Notifications *stagedsync.Notifications

	// TxPool
	TxPoolFetch      *txpool.Fetch
	TxPoolSend       *txpool.Send
	TxPoolGrpcServer *txpool.GrpcServer
	TxPool           *txpool.TxPool
	txPoolDB         kv.RwDB
}

func (ms *MockSentry) Close() {
	ms.cancel()
	if ms.txPoolDB != nil {
		ms.txPoolDB.Close()
	}
	ms.DB.Close()
	ms.snapshotDir.Close()
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
func (ms *MockSentry) Peers(req *proto_sentry.PeersRequest, server proto_sentry.Sentry_PeersServer) error {
	return nil
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
func (ms *MockSentry) HandShake(ctx context.Context, in *emptypb.Empty) (*proto_sentry.HandShakeReply, error) {
	return &proto_sentry.HandShakeReply{Protocol: proto_sentry.Protocol_ETH66}, nil
}
func (ms *MockSentry) SetStatus(context.Context, *proto_sentry.StatusData) (*proto_sentry.SetStatusReply, error) {
	return &proto_sentry.SetStatusReply{}, nil
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
	return MockWithGenesisPruneMode(t, gspec, key, prune.DefaultMode)
}

func MockWithGenesisEngine(t *testing.T, gspec *core.Genesis, engine consensus.Engine) *MockSentry {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	return MockWithEverything(t, gspec, key, prune.DefaultMode, engine, false)
}

func MockWithGenesisPruneMode(t *testing.T, gspec *core.Genesis, key *ecdsa.PrivateKey, prune prune.Mode) *MockSentry {
	return MockWithEverything(t, gspec, key, prune, ethash.NewFaker(), false)
}

func MockWithEverything(t *testing.T, gspec *core.Genesis, key *ecdsa.PrivateKey, prune prune.Mode, engine consensus.Engine, withTxPool bool) *MockSentry {
	var tmpdir string
	if t != nil {
		tmpdir = t.TempDir()
	} else {
		tmpdir = os.TempDir()
	}
	snapshotDir := &dir.Rw{Path: filepath.Join(tmpdir, "snapshots")} // we don't really lock here, to allow parallel tests
	var err error

	db := memdb.New()
	ctx, ctxCancel := context.WithCancel(context.Background())

	erigonGrpcServeer := remotedbserver.NewKvServer(ctx, db)
	mock := &MockSentry{
		Ctx: ctx, cancel: ctxCancel, DB: db,
		t:           t,
		Log:         log.New(),
		tmpdir:      tmpdir,
		snapshotDir: snapshotDir,
		Engine:      engine,
		ChainConfig: gspec.Config,
		Key:         key,
		Notifications: &stagedsync.Notifications{
			Events:               privateapi.NewEvents(),
			Accumulator:          shards.NewAccumulator(gspec.Config),
			StateChangesConsumer: erigonGrpcServeer,
		},
		UpdateHead: func(Ctx context.Context, head uint64, hash common.Hash, td *uint256.Int) {
		},
		PeerId: gointerfaces.ConvertHashToH256([32]byte{0x12, 0x34, 0x50}), // "12345"
	}
	if t != nil {
		t.Cleanup(mock.Close)
	}

	mock.Address = crypto.PubkeyToAddress(mock.Key.PublicKey)

	sendHeaderRequest := func(_ context.Context, r *headerdownload.HeaderRequest) (enode.ID, bool) { return enode.ID{}, false }
	propagateNewBlockHashes := func(context.Context, []headerdownload.Announce) {}
	penalize := func(context.Context, []headerdownload.PenaltyItem) {}
	cfg := ethconfig.Defaults
	cfg.StateStream = true
	cfg.BatchSize = 1 * datasize.MB
	cfg.BodyDownloadTimeoutSeconds = 10
	cfg.TxPool.Disable = !withTxPool
	cfg.TxPool.Journal = ""
	cfg.TxPool.StartOnInit = true

	mock.SentryClient = direct.NewSentryClientDirect(eth.ETH66, mock)
	sentries := []direct.SentryClient{mock.SentryClient}

	sendBodyRequest := func(context.Context, *bodydownload.BodyRequest) (enode.ID, bool) { return enode.ID{}, false }
	blockPropagator := func(Ctx context.Context, block *types.Block, td *big.Int) {}

	if !cfg.TxPool.Disable {
		poolCfg := txpool.DefaultConfig
		newTxs := make(chan txpool.Hashes, 1024)
		if t != nil {
			t.Cleanup(func() {
				close(newTxs)
			})
		}
		chainID, _ := uint256.FromBig(mock.ChainConfig.ChainID)
		mock.TxPool, err = txpool.New(newTxs, mock.DB, poolCfg, kvcache.NewDummy(), *chainID)
		if err != nil {
			t.Fatal(err)
		}
		mock.txPoolDB = memdb.NewPoolDB()

		stateChangesClient := direct.NewStateDiffClientDirect(erigonGrpcServeer)

		mock.TxPoolFetch = txpool.NewFetch(mock.Ctx, sentries, mock.TxPool, stateChangesClient, mock.DB, mock.txPoolDB, *chainID)
		mock.TxPoolFetch.SetWaitGroup(&mock.ReceiveWg)
		mock.TxPoolSend = txpool.NewSend(mock.Ctx, sentries, mock.TxPool)
		mock.TxPoolGrpcServer = txpool.NewGrpcServer(mock.Ctx, mock.TxPool, mock.txPoolDB, *chainID)

		mock.TxPoolFetch.ConnectCore()
		mock.StreamWg.Add(1)
		mock.TxPoolFetch.ConnectSentries()
		mock.StreamWg.Wait()

		go txpool.MainLoop(mock.Ctx, mock.txPoolDB, mock.DB, mock.TxPool, newTxs, mock.TxPoolSend, mock.TxPoolGrpcServer.NewSlotsStreams, func() {})
	}

	// Committed genesis will be shared between download and mock sentry
	_, mock.Genesis, err = core.CommitGenesisBlock(mock.DB, gspec)
	if _, ok := err.(*params.ConfigCompatError); err != nil && !ok {
		if t != nil {
			t.Fatal(err)
		} else {
			panic(err)
		}
	}
	blockReader := snapshotsync.NewBlockReader()

	blockDownloaderWindow := 65536
	networkID := uint64(1)
	mock.downloader, err = sentry.NewControlServer(mock.DB, "mock", mock.ChainConfig, mock.Genesis.Hash(), mock.Engine, networkID, sentries, blockDownloaderWindow, blockReader)
	if err != nil {
		if t != nil {
			t.Fatal(err)
		} else {
			panic(err)
		}
	}

	var allSnapshots *snapshotsync.RoSnapshots
	var snapshotsDownloader proto_downloader.DownloaderClient

	isBor := mock.ChainConfig.Bor != nil

	mock.Sync = stagedsync.New(
		stagedsync.DefaultStages(mock.Ctx, prune,
			stagedsync.StageHeadersCfg(
				mock.DB,
				mock.downloader.Hd,
				mock.downloader.Bd,
				*mock.ChainConfig,
				sendHeaderRequest,
				propagateNewBlockHashes,
				penalize,
				cfg.BatchSize,
				false,
				allSnapshots,
				snapshotsDownloader,
				blockReader,
				mock.tmpdir,
				mock.snapshotDir,
			),
			stagedsync.StageCumulativeIndexCfg(mock.DB),
			stagedsync.StageBlockHashesCfg(mock.DB, mock.tmpdir, mock.ChainConfig),
			stagedsync.StageBodiesCfg(
				mock.DB,
				mock.downloader.Bd,
				sendBodyRequest,
				penalize,
				blockPropagator,
				cfg.BodyDownloadTimeoutSeconds,
				*mock.ChainConfig,
				cfg.BatchSize,
				allSnapshots,
				blockReader,
			),
			stagedsync.StageIssuanceCfg(mock.DB, mock.ChainConfig, blockReader, true),
			stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, mock.tmpdir, prune, snapshotsync.NewBlockRetire(1, mock.tmpdir, allSnapshots, snapshotDir, mock.DB, snapshotsDownloader, mock.Notifications.Events)),
			stagedsync.StageExecuteBlocksCfg(
				mock.DB,
				prune,
				cfg.BatchSize,
				nil,
				mock.ChainConfig,
				mock.Engine,
				&vm.Config{},
				mock.Notifications.Accumulator,
				cfg.StateStream,
				mock.tmpdir,
				blockReader,
			),
			stagedsync.StageTranspileCfg(mock.DB, cfg.BatchSize, mock.ChainConfig),
			stagedsync.StageHashStateCfg(mock.DB, mock.tmpdir),
			stagedsync.StageTrieCfg(mock.DB, true, true, mock.tmpdir, blockReader),
			stagedsync.StageHistoryCfg(mock.DB, prune, mock.tmpdir),
			stagedsync.StageLogIndexCfg(mock.DB, prune, mock.tmpdir),
			stagedsync.StageCallTracesCfg(mock.DB, prune, 0, mock.tmpdir),
			stagedsync.StageTxLookupCfg(mock.DB, prune, mock.tmpdir, allSnapshots, isBor),
			stagedsync.StageFinishCfg(mock.DB, mock.tmpdir, mock.Log), true),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
	)

	mock.downloader.Hd.StartPoSDownloader(mock.Ctx, sendHeaderRequest, penalize)

	miningConfig := cfg.Miner
	miningConfig.Enabled = true
	miningConfig.Noverify = false
	miningConfig.Etherbase = mock.Address
	miningConfig.SigKey = mock.Key

	miner := stagedsync.NewMiningState(&miningConfig)
	mock.PendingBlocks = miner.PendingResultCh
	mock.MinedBlocks = miner.MiningResultCh
	mock.MiningSync = stagedsync.New(
		stagedsync.MiningStages(mock.Ctx,
			stagedsync.StageMiningCreateBlockCfg(mock.DB, miner, *mock.ChainConfig, mock.Engine, mock.TxPool, nil, nil, mock.tmpdir),
			stagedsync.StageMiningExecCfg(mock.DB, miner, nil, *mock.ChainConfig, mock.Engine, &vm.Config{}, mock.tmpdir),
			stagedsync.StageHashStateCfg(mock.DB, mock.tmpdir),
			stagedsync.StageTrieCfg(mock.DB, false, true, mock.tmpdir, blockReader),
			stagedsync.StageMiningFinishCfg(mock.DB, *mock.ChainConfig, mock.Engine, miner, mock.Ctx.Done()),
		),
		stagedsync.MiningUnwindOrder,
		stagedsync.MiningPruneOrder,
	)

	mock.StreamWg.Add(1)
	go sentry.RecvMessageLoop(mock.Ctx, mock.SentryClient, mock.downloader, &mock.ReceiveWg)
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	go sentry.RecvUploadMessageLoop(mock.Ctx, mock.SentryClient, mock.downloader, &mock.ReceiveWg)
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	go sentry.RecvUploadHeadersMessageLoop(mock.Ctx, mock.SentryClient, mock.downloader, &mock.ReceiveWg)
	mock.StreamWg.Wait()

	return mock
}

// Mock is convenience function to create a mock with some pre-set values
func Mock(t *testing.T) *MockSentry {
	funds := big.NewInt(1 * params.Ether)
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

func MockWithTxPool(t *testing.T) *MockSentry {
	funds := big.NewInt(1 * params.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.AllEthashProtocolChanges
	gspec := &core.Genesis{
		Config: chainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
		},
	}

	return MockWithEverything(t, gspec, key, prune.DefaultMode, ethash.NewFaker(), true)
}

func MockWithZeroTTD(t *testing.T) *MockSentry {
	funds := big.NewInt(1 * params.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.AllEthashProtocolChanges
	chainConfig.TerminalTotalDifficulty = common.Big0
	gspec := &core.Genesis{
		Config: chainConfig,
		Alloc: core.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	return MockWithGenesis(t, gspec, key)
}

func (ms *MockSentry) EnableLogs() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
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
	initialCycle := false
	highestSeenHeader := chain.TopBlock.NumberU64()
	if ms.TxPool != nil {
		ms.ReceiveWg.Add(1)
	}
	if _, err = StageLoopStep(ms.Ctx, ms.DB, ms.Sync, highestSeenHeader, ms.Notifications, initialCycle, ms.UpdateHead, nil); err != nil {
		return err
	}
	if ms.TxPool != nil {
		ms.ReceiveWg.Wait() // Wait for TxPool notification
	}
	// Check if the latest header was imported or rolled back
	if err = ms.DB.View(ms.Ctx, func(tx kv.Tx) error {
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

func (ms *MockSentry) SendPayloadRequest(message *engineapi.PayloadMessage) {
	ms.downloader.Hd.BeaconRequestList.AddPayloadRequest(message)
}

func (ms *MockSentry) SendForkChoiceRequest(message *engineapi.ForkChoiceMessage) {
	ms.downloader.Hd.BeaconRequestList.AddForkChoiceRequest(message)
}

func (ms *MockSentry) ReceivePayloadStatus() privateapi.PayloadStatus {
	return <-ms.downloader.Hd.PayloadStatusCh
}
