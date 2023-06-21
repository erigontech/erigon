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
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	ptypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/trie"
)

const MockInsertAsInitialCycle = false

type MockSentry struct {
	proto_sentry.UnimplementedSentryServer
	Ctx            context.Context
	Log            log.Logger
	tb             testing.TB
	cancel         context.CancelFunc
	DB             kv.RwDB
	Dirs           datadir.Dirs
	Engine         consensus.Engine
	gspec          *types.Genesis
	ChainConfig    *chain.Config
	Sync           *stagedsync.Sync
	MiningSync     *stagedsync.Sync
	PendingBlocks  chan *types.Block
	MinedBlocks    chan *types.Block
	sentriesClient *sentry.MultiClient
	Key            *ecdsa.PrivateKey
	Genesis        *types.Block
	SentryClient   direct.SentryClient
	PeerId         *ptypes.H512
	UpdateHead     func(Ctx context.Context, headHeight, headTime uint64, hash libcommon.Hash, td *uint256.Int)
	streams        map[proto_sentry.MessageId][]proto_sentry.Sentry_MessagesServer
	sentMessages   []*proto_sentry.OutboundMessageData
	StreamWg       sync.WaitGroup
	ReceiveWg      sync.WaitGroup
	Address        libcommon.Address

	Notifications *shards.Notifications

	// TxPool
	TxPoolFetch      *txpool.Fetch
	TxPoolSend       *txpool.Send
	TxPoolGrpcServer *txpool.GrpcServer
	TxPool           *txpool.TxPool
	txPoolDB         kv.RwDB

	HistoryV3      bool
	agg            *libstate.AggregatorV3
	BlockSnapshots *freezeblocks.RoSnapshots
	BlockReader    services.FullBlockReader
}

func (ms *MockSentry) Close() {
	ms.cancel()
	if ms.txPoolDB != nil {
		ms.txPoolDB.Close()
	}
	if ms.Engine != nil {
		ms.Engine.Close()
	}
	if ms.BlockSnapshots != nil {
		ms.BlockSnapshots.Close()
	}
	if ms.agg != nil {
		ms.agg.Close()
	}
	if ms.DB != nil {
		ms.DB.Close()
	}
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

func (ms *MockSentry) SetStatus(context.Context, *proto_sentry.StatusData) (*proto_sentry.SetStatusReply, error) {
	return &proto_sentry.SetStatusReply{}, nil
}

func (ms *MockSentry) PenalizePeer(context.Context, *proto_sentry.PenalizePeerRequest) (*emptypb.Empty, error) {
	return nil, nil
}
func (ms *MockSentry) PeerMinBlock(context.Context, *proto_sentry.PeerMinBlockRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (ms *MockSentry) HandShake(ctx context.Context, in *emptypb.Empty) (*proto_sentry.HandShakeReply, error) {
	return &proto_sentry.HandShakeReply{Protocol: proto_sentry.Protocol_ETH68}, nil
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

func (ms *MockSentry) Peers(context.Context, *emptypb.Empty) (*proto_sentry.PeersReply, error) {
	return &proto_sentry.PeersReply{}, nil
}
func (ms *MockSentry) PeerCount(context.Context, *proto_sentry.PeerCountRequest) (*proto_sentry.PeerCountReply, error) {
	return &proto_sentry.PeerCountReply{Count: 0}, nil
}
func (ms *MockSentry) PeerById(context.Context, *proto_sentry.PeerByIdRequest) (*proto_sentry.PeerByIdReply, error) {
	return &proto_sentry.PeerByIdReply{}, nil
}
func (ms *MockSentry) PeerEvents(req *proto_sentry.PeerEventsRequest, server proto_sentry.Sentry_PeerEventsServer) error {
	return nil
}

func (ms *MockSentry) NodeInfo(context.Context, *emptypb.Empty) (*ptypes.NodeInfoReply, error) {
	return nil, nil
}

func MockWithGenesis(tb testing.TB, gspec *types.Genesis, key *ecdsa.PrivateKey, withPosDownloader bool) *MockSentry {
	return MockWithGenesisPruneMode(tb, gspec, key, prune.DefaultMode, withPosDownloader)
}

func MockWithGenesisEngine(tb testing.TB, gspec *types.Genesis, engine consensus.Engine, withPosDownloader bool) *MockSentry {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	return MockWithEverything(tb, gspec, key, prune.DefaultMode, engine, false, withPosDownloader)
}

func MockWithGenesisPruneMode(tb testing.TB, gspec *types.Genesis, key *ecdsa.PrivateKey, prune prune.Mode, withPosDownloader bool) *MockSentry {
	return MockWithEverything(tb, gspec, key, prune, ethash.NewFaker(), false, withPosDownloader)
}

func MockWithEverything(tb testing.TB, gspec *types.Genesis, key *ecdsa.PrivateKey, prune prune.Mode, engine consensus.Engine, withTxPool bool, withPosDownloader bool) *MockSentry {
	var tmpdir string
	if tb != nil {
		tmpdir = tb.TempDir()
	} else {
		tmpdir = os.TempDir()
	}
	dirs := datadir.New(tmpdir)
	var err error

	cfg := ethconfig.Defaults
	cfg.StateStream = true
	cfg.BatchSize = 1 * datasize.MB
	cfg.Sync.BodyDownloadTimeoutSeconds = 10
	cfg.DeprecatedTxPool.Disable = !withTxPool
	cfg.DeprecatedTxPool.StartOnInit = true

	logger := log.New()
	ctx, ctxCancel := context.WithCancel(context.Background())
	histV3, db, agg := temporal.NewTestDB(tb, dirs, nil)
	cfg.HistoryV3 = histV3

	erigonGrpcServeer := remotedbserver.NewKvServer(ctx, db, nil, nil, logger)
	allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.Defaults.Snapshot, dirs.Snap, logger)
	mock := &MockSentry{
		Ctx: ctx, cancel: ctxCancel, DB: db, agg: agg,
		tb:          tb,
		Log:         logger,
		Dirs:        dirs,
		Engine:      engine,
		gspec:       gspec,
		ChainConfig: gspec.Config,
		Key:         key,
		Notifications: &shards.Notifications{
			Events:               shards.NewEvents(),
			Accumulator:          shards.NewAccumulator(),
			StateChangesConsumer: erigonGrpcServeer,
		},
		UpdateHead: func(Ctx context.Context, headHeight, headTime uint64, hash libcommon.Hash, td *uint256.Int) {
		},
		PeerId:         gointerfaces.ConvertHashToH512([64]byte{0x12, 0x34, 0x50}), // "12345"
		BlockSnapshots: allSnapshots,
		BlockReader:    freezeblocks.NewBlockReader(allSnapshots),
		HistoryV3:      cfg.HistoryV3,
	}
	if tb != nil {
		tb.Cleanup(mock.Close)
	}
	blockWriter := blockio.NewBlockWriter(mock.HistoryV3)

	mock.Address = crypto.PubkeyToAddress(mock.Key.PublicKey)

	sendHeaderRequest := func(_ context.Context, r *headerdownload.HeaderRequest) ([64]byte, bool) { return [64]byte{}, false }
	propagateNewBlockHashes := func(context.Context, []headerdownload.Announce) {}
	penalize := func(context.Context, []headerdownload.PenaltyItem) {}

	mock.SentryClient = direct.NewSentryClientDirect(direct.ETH68, mock)
	sentries := []direct.SentryClient{mock.SentryClient}

	sendBodyRequest := func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool) { return [64]byte{}, false }
	blockPropagator := func(Ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int) {}

	if !cfg.DeprecatedTxPool.Disable {
		poolCfg := txpoolcfg.DefaultConfig
		newTxs := make(chan types2.Announcements, 1024)
		if tb != nil {
			tb.Cleanup(func() {
				close(newTxs)
			})
		}
		chainID, _ := uint256.FromBig(mock.ChainConfig.ChainID)
		shanghaiTime := mock.ChainConfig.ShanghaiTime
		mock.TxPool, err = txpool.New(newTxs, mock.DB, poolCfg, kvcache.NewDummy(), *chainID, shanghaiTime, logger)
		if err != nil {
			tb.Fatal(err)
		}
		mock.txPoolDB = memdb.NewPoolDB(tmpdir)

		stateChangesClient := direct.NewStateDiffClientDirect(erigonGrpcServeer)

		mock.TxPoolFetch = txpool.NewFetch(mock.Ctx, sentries, mock.TxPool, stateChangesClient, mock.DB, mock.txPoolDB, *chainID, logger)
		mock.TxPoolFetch.SetWaitGroup(&mock.ReceiveWg)
		mock.TxPoolSend = txpool.NewSend(mock.Ctx, sentries, mock.TxPool, logger)
		mock.TxPoolGrpcServer = txpool.NewGrpcServer(mock.Ctx, mock.TxPool, mock.txPoolDB, *chainID, logger)

		mock.TxPoolFetch.ConnectCore()
		mock.StreamWg.Add(1)
		mock.TxPoolFetch.ConnectSentries()
		mock.StreamWg.Wait()

		go txpool.MainLoop(mock.Ctx, mock.txPoolDB, mock.DB, mock.TxPool, newTxs, mock.TxPoolSend, mock.TxPoolGrpcServer.NewSlotsStreams, func() {})
	}

	// Committed genesis will be shared between download and mock sentry
	_, mock.Genesis, err = core.CommitGenesisBlock(mock.DB, gspec, "", mock.Log)
	if _, ok := err.(*chain.ConfigCompatError); err != nil && !ok {
		if tb != nil {
			tb.Fatal(err)
		} else {
			panic(err)
		}
	}

	inMemoryExecution := func(batch kv.RwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync, err := NewInMemoryExecution(ctx, mock.DB, &ethconfig.Defaults, mock.sentriesClient, dirs, notifications, mock.BlockReader, blockWriter, agg, log.New() /* logging will be discarded */)
		if err != nil {
			return err
		}
		// We start the mining step
		if err := StateStep(ctx, batch, blockWriter, stateSync, mock.sentriesClient.Bd, header, body, unwindPoint, headersChain, bodiesChain); err != nil {
			logger.Warn("Could not validate block", "err", err)
			return err
		}
		progress, err := stages.GetStageProgress(batch, stages.IntermediateHashes)
		if err != nil {
			return err
		}
		if progress < header.Number.Uint64() {
			return fmt.Errorf("unsuccessful execution, progress %d < expected %d", progress, header.Number.Uint64())
		}
		return nil
	}
	forkValidator := engineapi.NewForkValidator(1, inMemoryExecution, dirs.Tmp, mock.BlockReader)
	networkID := uint64(1)
	mock.sentriesClient, err = sentry.NewMultiClient(
		mock.DB,
		"mock",
		mock.ChainConfig,
		mock.Genesis.Hash(),
		mock.Engine,
		networkID,
		sentries,
		cfg.Sync,
		mock.BlockReader,
		false,
		forkValidator,
		cfg.DropUselessPeers,
		logger,
	)

	mock.sentriesClient.IsMock = true
	if err != nil {
		if tb != nil {
			tb.Fatal(err)
		} else {
			panic(err)
		}
	}

	var snapshotsDownloader proto_downloader.DownloaderClient

	blockRetire := freezeblocks.NewBlockRetire(1, dirs, mock.BlockReader, blockWriter, mock.DB, mock.Notifications.Events, logger)
	mock.Sync = stagedsync.New(
		stagedsync.DefaultStages(mock.Ctx,
			stagedsync.StageSnapshotsCfg(mock.DB, *mock.ChainConfig, dirs, blockRetire, snapshotsDownloader, mock.BlockReader, mock.Notifications.Events, mock.HistoryV3, mock.agg),
			stagedsync.StageHeadersCfg(mock.DB, mock.sentriesClient.Hd, mock.sentriesClient.Bd, *mock.ChainConfig, sendHeaderRequest, propagateNewBlockHashes, penalize, cfg.BatchSize, false, mock.BlockReader, blockWriter, dirs.Tmp, mock.Notifications, engineapi.NewForkValidatorMock(1)),
			stagedsync.StageCumulativeIndexCfg(mock.DB, mock.BlockReader),
			stagedsync.StageBlockHashesCfg(mock.DB, mock.Dirs.Tmp, mock.ChainConfig, blockWriter),
			stagedsync.StageBodiesCfg(mock.DB, mock.sentriesClient.Bd, sendBodyRequest, penalize, blockPropagator, cfg.Sync.BodyDownloadTimeoutSeconds, *mock.ChainConfig, mock.BlockReader, cfg.HistoryV3, blockWriter),
			stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, false, dirs.Tmp, prune, mock.BlockReader, mock.sentriesClient.Hd),
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
				/*stateStream=*/ false,
				/*exec22=*/ cfg.HistoryV3,
				dirs,
				mock.BlockReader,
				mock.sentriesClient.Hd,
				mock.gspec,
				ethconfig.Defaults.Sync,
				mock.agg,
			),
			stagedsync.StageHashStateCfg(mock.DB, mock.Dirs, cfg.HistoryV3),
			stagedsync.StageTrieCfg(mock.DB, true, true, false, dirs.Tmp, mock.BlockReader, mock.sentriesClient.Hd, cfg.HistoryV3, mock.agg),
			stagedsync.StageHistoryCfg(mock.DB, prune, dirs.Tmp),
			stagedsync.StageLogIndexCfg(mock.DB, prune, dirs.Tmp),
			stagedsync.StageCallTracesCfg(mock.DB, prune, 0, dirs.Tmp),
			stagedsync.StageTxLookupCfg(mock.DB, prune, dirs.Tmp, mock.ChainConfig.Bor, mock.BlockReader),
			stagedsync.StageFinishCfg(mock.DB, dirs.Tmp, forkValidator),
			!withPosDownloader),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
		logger,
	)

	mock.sentriesClient.Hd.StartPoSDownloader(mock.Ctx, sendHeaderRequest, penalize)

	miningConfig := cfg.Miner
	miningConfig.Enabled = true
	miningConfig.Noverify = false
	miningConfig.Etherbase = mock.Address
	miningConfig.SigKey = mock.Key
	miningCancel := make(chan struct{})
	go func() {
		<-mock.Ctx.Done()
		close(miningCancel)
	}()

	miner := stagedsync.NewMiningState(&miningConfig)
	mock.PendingBlocks = miner.PendingResultCh
	mock.MinedBlocks = miner.MiningResultCh
	mock.MiningSync = stagedsync.New(
		stagedsync.MiningStages(mock.Ctx,
			stagedsync.StageMiningCreateBlockCfg(mock.DB, miner, *mock.ChainConfig, mock.Engine, nil, nil, dirs.Tmp, mock.BlockReader),
			stagedsync.StageMiningExecCfg(mock.DB, miner, nil, *mock.ChainConfig, mock.Engine, &vm.Config{}, dirs.Tmp, nil, 0, mock.TxPool, nil, mock.BlockReader),
			stagedsync.StageHashStateCfg(mock.DB, dirs, cfg.HistoryV3),
			stagedsync.StageTrieCfg(mock.DB, false, true, false, dirs.Tmp, mock.BlockReader, mock.sentriesClient.Hd, cfg.HistoryV3, mock.agg),
			stagedsync.StageMiningFinishCfg(mock.DB, *mock.ChainConfig, mock.Engine, miner, miningCancel, mock.BlockReader),
		),
		stagedsync.MiningUnwindOrder,
		stagedsync.MiningPruneOrder,
		logger,
	)

	mock.StreamWg.Add(1)
	go mock.sentriesClient.RecvMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	go mock.sentriesClient.RecvUploadMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	go mock.sentriesClient.RecvUploadHeadersMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
	mock.StreamWg.Wait()

	return mock
}

// Mock is convenience function to create a mock with some pre-set values
func Mock(tb testing.TB) *MockSentry {
	funds := big.NewInt(1 * params.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.TestChainConfig
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	return MockWithGenesis(tb, gspec, key, false)
}

func MockWithTxPool(t *testing.T) *MockSentry {
	funds := big.NewInt(1 * params.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.TestChainConfig
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}

	return MockWithEverything(t, gspec, key, prune.DefaultMode, ethash.NewFaker(), true, false)
}

func MockWithZeroTTD(t *testing.T, withPosDownloader bool) *MockSentry {
	funds := big.NewInt(1 * params.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.AllProtocolChanges
	chainConfig.TerminalTotalDifficulty = libcommon.Big0
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	return MockWithGenesis(t, gspec, key, withPosDownloader)
}

func MockWithZeroTTDGnosis(t *testing.T, withPosDownloader bool) *MockSentry {
	funds := big.NewInt(1 * params.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := params.TestChainAuraConfig
	chainConfig.TerminalTotalDifficulty = libcommon.Big0
	chainConfig.TerminalTotalDifficultyPassed = true
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	engine := ethconsensusconfig.CreateConsensusEngineBareBones(chainConfig, log.New())
	return MockWithGenesisEngine(t, gspec, engine, withPosDownloader)
}

func (ms *MockSentry) EnableLogs() {
	ms.Log.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
}

func (ms *MockSentry) numberOfPoWBlocks(chain *core.ChainPack) int {
	if ms.ChainConfig.TerminalTotalDifficulty == nil {
		return chain.Length()
	}
	return chain.NumberOfPoWBlocks()
}

func (ms *MockSentry) insertPoWBlocks(chain *core.ChainPack, tx kv.RwTx) error {
	n := ms.numberOfPoWBlocks(chain)
	if n == 0 {
		// No Proof-of-Work blocks
		return nil
	}

	// Send NewBlock message
	b, err := rlp.EncodeToBytes(&eth.NewBlockPacket{
		Block: chain.Blocks[n-1],
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
		BlockHeadersPacket: chain.Headers[0:n],
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
	packet := make(eth.BlockBodiesPacket, n)
	for i, block := range chain.Blocks[0:n] {
		packet[i] = block.Body()
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
	ms.ReceiveWg.Wait() // Wait for all messages to be processed before we proceed

	if ms.TxPool != nil {
		ms.ReceiveWg.Add(1)
	}
	initialCycle := MockInsertAsInitialCycle
	hook := NewHook(ms.Ctx, ms.Notifications, ms.Sync, ms.BlockReader, ms.ChainConfig, ms.Log, ms.UpdateHead)
	if err = StageLoopStep(ms.Ctx, ms.DB, tx, ms.Sync, initialCycle, ms.Log, ms.BlockReader, hook); err != nil {
		return err
	}
	if ms.TxPool != nil {
		ms.ReceiveWg.Wait() // Wait for TxPool notification
	}
	return nil
}

func (ms *MockSentry) insertPoSBlocks(chain *core.ChainPack, tx kv.RwTx) error {
	n := ms.numberOfPoWBlocks(chain)
	if n >= chain.Length() {
		return nil
	}

	for i := n; i < chain.Length(); i++ {
		if err := chain.Blocks[i].HashCheck(); err != nil {
			return err
		}
		ms.SendPayloadRequest(chain.Blocks[i])
	}

	initialCycle := MockInsertAsInitialCycle
	hook := NewHook(ms.Ctx, ms.Notifications, ms.Sync, ms.BlockReader, ms.ChainConfig, ms.Log, ms.UpdateHead)
	err := StageLoopStep(ms.Ctx, ms.DB, tx, ms.Sync, initialCycle, ms.Log, ms.BlockReader, hook)
	if err != nil {
		return err
	}
	SendPayloadStatus(ms.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	ms.ReceivePayloadStatus()

	fc := engineapi.ForkChoiceMessage{
		HeadBlockHash:      chain.TopBlock.Hash(),
		SafeBlockHash:      chain.TopBlock.Hash(),
		FinalizedBlockHash: chain.TopBlock.Hash(),
	}
	ms.SendForkChoiceRequest(&fc)
	err = StageLoopStep(ms.Ctx, ms.DB, tx, ms.Sync, initialCycle, ms.Log, ms.BlockReader, hook)
	if err != nil {
		return err
	}
	SendPayloadStatus(ms.HeaderDownload(), rawdb.ReadHeadBlockHash(tx), err)
	ms.ReceivePayloadStatus()

	return nil
}

func (ms *MockSentry) InsertChain(chain *core.ChainPack, tx kv.RwTx) error {
	externalTx := tx != nil
	if !externalTx {
		var err error
		tx, err = ms.DB.BeginRw(ms.Ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := ms.insertPoWBlocks(chain, tx); err != nil {
		return err
	}
	if err := ms.insertPoSBlocks(chain, tx); err != nil {
		return err
	}

	// Check if the latest header was imported or rolled back
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

	if ms.sentriesClient.Hd.IsBadHeader(chain.TopBlock.Hash()) {
		return fmt.Errorf("block %d %x was invalid", chain.TopBlock.NumberU64(), chain.TopBlock.Hash())
	}
	//if ms.HistoryV3 {
	//if err := ms.agg.BuildFiles(ms.Ctx, ms.DB); err != nil {
	//	return err
	//}
	//if err := ms.DB.UpdateNosync(ms.Ctx, func(tx kv.RwTx) error {
	//	ms.agg.SetTx(tx)
	//	if err := ms.agg.Prune(ms.Ctx, math.MaxUint64); err != nil {
	//		return err
	//	}
	//	return nil
	//}); err != nil {
	//	return err
	//}
	//}

	if !externalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (ms *MockSentry) SendPayloadRequest(message *types.Block) {
	ms.sentriesClient.Hd.BeaconRequestList.AddPayloadRequest(message)
}

func (ms *MockSentry) SendForkChoiceRequest(message *engineapi.ForkChoiceMessage) {
	ms.sentriesClient.Hd.BeaconRequestList.AddForkChoiceRequest(message)
}

func (ms *MockSentry) ReceivePayloadStatus() engineapi.PayloadStatus {
	return <-ms.sentriesClient.Hd.PayloadStatusCh
}

func (ms *MockSentry) HeaderDownload() *headerdownload.HeaderDownload {
	return ms.sentriesClient.Hd
}

func (ms *MockSentry) NewHistoryStateReader(blockNum uint64, tx kv.Tx) state.StateReader {
	r, err := rpchelper.CreateHistoryStateReader(tx, blockNum, 0, ms.HistoryV3, ms.ChainConfig.ChainName)
	if err != nil {
		panic(err)
	}
	return r
}

func (ms *MockSentry) NewStateReader(tx kv.Tx) state.StateReader {
	if ethconfig.EnableHistoryV4InTest {
		panic("implement me")
	}
	return state.NewPlainStateReader(tx)
}

func (ms *MockSentry) NewStateWriter(tx kv.RwTx, blockNum uint64) state.StateWriter {
	if ethconfig.EnableHistoryV4InTest {
		panic("implement me")
	}
	return state.NewPlainStateWriter(tx, tx, blockNum)
}

func (ms *MockSentry) CalcStateRoot(tx kv.Tx) libcommon.Hash {
	if ethconfig.EnableHistoryV4InTest {
		panic("implement me")
	}

	h, err := trie.CalcRoot("test", tx)
	if err != nil {
		panic(err)
	}
	return h
}
func (ms *MockSentry) HistoryV3Components() *libstate.AggregatorV3 {
	return ms.agg
}

func (ms *MockSentry) BlocksIO() (services.FullBlockReader, *blockio.BlockWriter) {
	return ms.BlockReader, blockio.NewBlockWriter(ms.HistoryV3)
}
