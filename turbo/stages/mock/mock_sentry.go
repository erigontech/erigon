package mock

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/c2h5oh/datasize"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv/temporal/temporaltest"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/semaphore"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
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
	"github.com/ledgerwatch/erigon-lib/wrap"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/consensuschain"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/rlp"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_chain_reader.go"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

const MockInsertAsInitialCycle = false

type MockSentry struct {
	proto_sentry.UnimplementedSentryServer
	Ctx                  context.Context
	Log                  log.Logger
	tb                   testing.TB
	cancel               context.CancelFunc
	DB                   kv.RwDB
	Dirs                 datadir.Dirs
	Engine               consensus.Engine
	gspec                *types.Genesis
	ChainConfig          *chain.Config
	Sync                 *stagedsync.Sync
	MiningSync           *stagedsync.Sync
	PendingBlocks        chan *types.Block
	MinedBlocks          chan *types.Block
	sentriesClient       *sentry_multi_client.MultiClient
	Key                  *ecdsa.PrivateKey
	Genesis              *types.Block
	SentryClient         direct.SentryClient
	PeerId               *ptypes.H512
	streams              map[proto_sentry.MessageId][]proto_sentry.Sentry_MessagesServer
	sentMessages         []*proto_sentry.OutboundMessageData
	StreamWg             sync.WaitGroup
	ReceiveWg            sync.WaitGroup
	Address              libcommon.Address
	Eth1ExecutionService *eth1.EthereumExecutionModule

	Notifications *shards.Notifications

	// TxPool
	TxPoolFetch      *txpool.Fetch
	TxPoolSend       *txpool.Send
	TxPoolGrpcServer *txpool.GrpcServer
	TxPool           *txpool.TxPool
	txPoolDB         kv.RwDB

	HistoryV3      bool
	agg            *libstate.Aggregator
	BlockSnapshots *freezeblocks.RoSnapshots
	BlockReader    services.FullBlockReader
	posStagedSync  *stagedsync.Sync
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

const blockBufferSize = 128

func MockWithGenesis(tb testing.TB, gspec *types.Genesis, key *ecdsa.PrivateKey, withPosDownloader bool) *MockSentry {
	return MockWithGenesisPruneMode(tb, gspec, key, blockBufferSize, prune.DefaultMode, withPosDownloader)
}

func MockWithGenesisEngine(tb testing.TB, gspec *types.Genesis, engine consensus.Engine, withPosDownloader, checkStateRoot bool) *MockSentry {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	return MockWithEverything(tb, gspec, key, prune.DefaultMode, engine, blockBufferSize, false, withPosDownloader, checkStateRoot)
}

func MockWithGenesisPruneMode(tb testing.TB, gspec *types.Genesis, key *ecdsa.PrivateKey, blockBufferSize int, prune prune.Mode, withPosDownloader bool) *MockSentry {
	var engine consensus.Engine

	switch {
	case gspec.Config.Bor != nil:
		engine = bor.NewFaker()
	default:
		engine = ethash.NewFaker()
	}

	checkStateRoot := true
	return MockWithEverything(tb, gspec, key, prune, engine, blockBufferSize, false, withPosDownloader, checkStateRoot)
}

func MockWithEverything(tb testing.TB, gspec *types.Genesis, key *ecdsa.PrivateKey, prune prune.Mode,
	engine consensus.Engine, blockBufferSize int, withTxPool, withPosDownloader, checkStateRoot bool,
) *MockSentry {
	tmpdir := os.TempDir()

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
	histV3, db, agg := temporaltest.NewTestDB(nil, dirs)
	cfg.HistoryV3 = histV3

	erigonGrpcServeer := remotedbserver.NewKvServer(ctx, db, nil, nil, nil, logger)
	allSnapshots := freezeblocks.NewRoSnapshots(ethconfig.Defaults.Snapshot, dirs.Snap, 0, logger)
	allBorSnapshots := freezeblocks.NewBorRoSnapshots(ethconfig.Defaults.Snapshot, dirs.Snap, 0, logger)
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
		PeerId:         gointerfaces.ConvertHashToH512([64]byte{0x12, 0x34, 0x50}), // "12345"
		BlockSnapshots: allSnapshots,
		BlockReader:    freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots),
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
		cancunTime := mock.ChainConfig.CancunTime
		maxBlobsPerBlock := mock.ChainConfig.GetMaxBlobsPerBlock()
		mock.TxPool, err = txpool.New(newTxs, mock.DB, poolCfg, kvcache.NewDummy(histV3), *chainID, shanghaiTime, nil /* agraBlock */, cancunTime, maxBlobsPerBlock, nil, logger)
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

		go txpool.MainLoop(mock.Ctx, mock.txPoolDB, mock.TxPool, newTxs, mock.TxPoolSend, mock.TxPoolGrpcServer.NewSlotsStreams, func() {})
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
	latestBlockBuiltStore := builder.NewLatestBlockBuiltStore()

	inMemoryExecution := func(txc wrap.TxContainer, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		terseLogger := log.New()
		terseLogger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync := stages2.NewInMemoryExecution(mock.Ctx, mock.DB, &cfg, mock.sentriesClient,
			dirs, notifications, mock.BlockReader, blockWriter, mock.agg, nil, terseLogger)
		chainReader := consensuschain.NewReader(mock.ChainConfig, txc.Tx, mock.BlockReader, logger)
		// We start the mining step
		if err := stages2.StateStep(ctx, chainReader, mock.Engine, txc, stateSync, header, body, unwindPoint, headersChain, bodiesChain, histV3); err != nil {
			logger.Warn("Could not validate block", "err", err)
			return err
		}
		var progress uint64
		if histV3 {
			progress, err = stages.GetStageProgress(txc.Tx, stages.Execution)
		} else {
			progress, err = stages.GetStageProgress(txc.Tx, stages.IntermediateHashes)
		}
		if err != nil {
			return err
		}
		if progress < header.Number.Uint64() {
			return fmt.Errorf("unsuccessful execution, progress %d < expected %d", progress, header.Number.Uint64())
		}
		return nil
	}
	forkValidator := engine_helpers.NewForkValidator(ctx, 1, inMemoryExecution, dirs.Tmp, mock.BlockReader)

	statusDataProvider := sentry.NewStatusDataProvider(
		db,
		mock.ChainConfig,
		mock.Genesis,
		mock.ChainConfig.ChainID.Uint64(),
	)

	maxBlockBroadcastPeers := func(header *types.Header) uint { return 0 }

	mock.sentriesClient, err = sentry_multi_client.NewMultiClient(
		mock.DB,
		mock.ChainConfig,
		mock.Engine,
		sentries,
		cfg.Sync,
		mock.BlockReader,
		blockBufferSize,
		statusDataProvider,
		false,
		maxBlockBroadcastPeers,
		false, /* disableBlockDownload */
		logger,
	)
	if err != nil {
		if tb != nil {
			tb.Fatal(err)
		} else {
			panic(err)
		}
	}
	mock.sentriesClient.IsMock = true

	var (
		snapDb         kv.RwDB
		snapDownloader = &proto_downloader.DownloaderClientMock{
			StatsFunc: func(ctx context.Context, in *proto_downloader.StatsRequest, opts ...grpc.CallOption) (*proto_downloader.StatsReply, error) {
				return &proto_downloader.StatsReply{Completed: true}, nil
			}}

		recents    *lru.ARCCache[libcommon.Hash, *bor.Snapshot]
		signatures *lru.ARCCache[libcommon.Hash, libcommon.Address]
	)
	if bor, ok := engine.(*bor.Bor); ok {
		snapDb = bor.DB
		recents = bor.Recents
		signatures = bor.Signatures
	}
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
	// proof-of-stake mining
	assembleBlockPOS := func(param *core.BlockBuilderParameters, interrupt *int32) (*types.BlockWithReceipts, error) {
		miningStatePos := stagedsync.NewProposingState(&cfg.Miner)
		miningStatePos.MiningConfig.Etherbase = param.SuggestedFeeRecipient
		proposingSync := stagedsync.New(
			cfg.Sync,
			stagedsync.MiningStages(mock.Ctx,
				stagedsync.StageMiningCreateBlockCfg(mock.DB, miner, *mock.ChainConfig, mock.Engine, nil, nil, dirs.Tmp, mock.BlockReader),
				stagedsync.StageBorHeimdallCfg(mock.DB, snapDb, miningStatePos, *mock.ChainConfig, nil, mock.BlockReader, nil, nil, nil, recents, signatures, false, nil),
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
					nil,
				),
				stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, cfg.Sync, false, dirs.Tmp, prune, mock.BlockReader, mock.sentriesClient.Hd, nil),
				stagedsync.StageMiningExecCfg(mock.DB, miner, nil, *mock.ChainConfig, mock.Engine, &vm.Config{}, dirs.Tmp, nil, 0, mock.TxPool, nil, mock.BlockReader),
				stagedsync.StageMiningFinishCfg(mock.DB, *mock.ChainConfig, mock.Engine, miner, miningCancel, mock.BlockReader, latestBlockBuiltStore),
			), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder,
			logger)
		// We start the mining step
		if err := stages2.MiningStep(ctx, mock.DB, proposingSync, tmpdir, logger); err != nil {
			return nil, err
		}
		block := <-miningStatePos.MiningResultPOSCh
		return block, nil
	}

	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)

	blockRetire := freezeblocks.NewBlockRetire(1, dirs, mock.BlockReader, blockWriter, mock.DB, mock.ChainConfig, mock.Notifications.Events, blockSnapBuildSema, logger)
	mock.Sync = stagedsync.New(
		cfg.Sync,
		stagedsync.DefaultStages(mock.Ctx,
			stagedsync.StageSnapshotsCfg(mock.DB, *mock.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, mock.BlockReader, mock.Notifications, mock.HistoryV3, mock.agg, false, false, nil),
			stagedsync.StageHeadersCfg(mock.DB, mock.sentriesClient.Hd, mock.sentriesClient.Bd, *mock.ChainConfig, cfg.Sync, sendHeaderRequest, propagateNewBlockHashes, penalize, cfg.BatchSize, false, mock.BlockReader, blockWriter, dirs.Tmp, mock.HistoryV3, mock.Notifications, nil),
			stagedsync.StageBorHeimdallCfg(mock.DB, snapDb, stagedsync.MiningState{}, *mock.ChainConfig, nil /* heimdallClient */, mock.BlockReader, nil, nil, nil, recents, signatures, false, nil),
			stagedsync.StageBlockHashesCfg(mock.DB, mock.Dirs.Tmp, mock.ChainConfig, blockWriter),
			stagedsync.StageBodiesCfg(mock.DB, mock.sentriesClient.Bd, sendBodyRequest, penalize, blockPropagator, cfg.Sync.BodyDownloadTimeoutSeconds, *mock.ChainConfig, mock.BlockReader, cfg.HistoryV3, blockWriter, nil),
			stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, cfg.Sync, false, dirs.Tmp, prune, mock.BlockReader, mock.sentriesClient.Hd, nil),
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
				nil,
			),
			stagedsync.StageHashStateCfg(mock.DB, mock.Dirs, cfg.HistoryV3),
			stagedsync.StageTrieCfg(mock.DB, checkStateRoot, true, false, dirs.Tmp, mock.BlockReader, mock.sentriesClient.Hd, cfg.HistoryV3, mock.agg),
			stagedsync.StageHistoryCfg(mock.DB, prune, dirs.Tmp),
			stagedsync.StageLogIndexCfg(mock.DB, prune, dirs.Tmp, nil),
			stagedsync.StageCallTracesCfg(mock.DB, prune, 0, dirs.Tmp),
			stagedsync.StageTxLookupCfg(mock.DB, prune, dirs.Tmp, mock.ChainConfig.Bor, mock.BlockReader),
			stagedsync.StageFinishCfg(mock.DB, dirs.Tmp, forkValidator),
			!withPosDownloader),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
		logger,
	)

	cfg.Genesis = gspec
	pipelineStages := stages2.NewPipelineStages(mock.Ctx, db, &cfg, p2p.Config{}, mock.sentriesClient, mock.Notifications,
		snapDownloader, mock.BlockReader, blockRetire, mock.agg, nil, forkValidator, logger, checkStateRoot)
	mock.posStagedSync = stagedsync.New(cfg.Sync, pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, logger)

	mock.Eth1ExecutionService = eth1.NewEthereumExecutionModule(mock.BlockReader, mock.DB, mock.posStagedSync, forkValidator, mock.ChainConfig, assembleBlockPOS, nil, mock.Notifications.Accumulator, mock.Notifications.StateChangesConsumer, logger, engine, histV3, cfg.Sync, ctx)

	mock.sentriesClient.Hd.StartPoSDownloader(mock.Ctx, sendHeaderRequest, penalize)

	mock.MiningSync = stagedsync.New(
		cfg.Sync,
		stagedsync.MiningStages(mock.Ctx,
			stagedsync.StageMiningCreateBlockCfg(mock.DB, miner, *mock.ChainConfig, mock.Engine, nil, nil, dirs.Tmp, mock.BlockReader),
			stagedsync.StageBorHeimdallCfg(mock.DB, snapDb, miner, *mock.ChainConfig, nil /*heimdallClient*/, mock.BlockReader, nil, nil, nil, recents, signatures, false, nil),
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
				nil,
			),
			stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, cfg.Sync, false, dirs.Tmp, prune, mock.BlockReader, mock.sentriesClient.Hd, nil),
			stagedsync.StageMiningExecCfg(mock.DB, miner, nil, *mock.ChainConfig, mock.Engine, &vm.Config{}, dirs.Tmp, nil, 0, mock.TxPool, nil, mock.BlockReader),
			stagedsync.StageMiningFinishCfg(mock.DB, *mock.ChainConfig, mock.Engine, miner, miningCancel, mock.BlockReader, latestBlockBuiltStore),
		),
		stagedsync.MiningUnwindOrder,
		stagedsync.MiningPruneOrder,
		logger,
	)

	cfg.Genesis = gspec

	mock.StreamWg.Add(1)
	go mock.sentriesClient.RecvMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	go mock.sentriesClient.RecvUploadMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	go mock.sentriesClient.RecvUploadHeadersMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
	mock.StreamWg.Wait()

	if histV3 {
		c := &core.ChainPack{
			Headers:  []*types.Header{mock.Genesis.HeaderNoCopy()},
			Blocks:   []*types.Block{mock.Genesis},
			TopBlock: mock.Genesis,
		}
		if err = mock.InsertChain(c); err != nil {
			tb.Fatal(err)
		}
	}
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

	checkStateRoot := true
	return MockWithEverything(t, gspec, key, prune.DefaultMode, ethash.NewFaker(), blockBufferSize, true, false, checkStateRoot)
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
	engine := ethconsensusconfig.CreateConsensusEngineBareBones(context.Background(), chainConfig, log.New())
	checkStateRoot := true
	return MockWithGenesisEngine(t, gspec, engine, withPosDownloader, checkStateRoot)
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

func (ms *MockSentry) insertPoWBlocks(chain *core.ChainPack) error {
	n := ms.numberOfPoWBlocks(chain)
	if n == 0 {
		// No Proof-of-Work blocks
		return nil
	}

	for i := 0; i < chain.Length(); i++ {
		if err := chain.Blocks[i].HashCheck(); err != nil {
			return err
		}
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
	hook := stages2.NewHook(ms.Ctx, ms.DB, ms.Notifications, ms.Sync, ms.BlockReader, ms.ChainConfig, ms.Log, nil)

	if err = stages2.StageLoopIteration(ms.Ctx, ms.DB, wrap.TxContainer{}, ms.Sync, initialCycle, true, ms.Log, ms.BlockReader, hook); err != nil {
		return err
	}
	if ms.TxPool != nil {
		ms.ReceiveWg.Wait() // Wait for TxPool notification
	}
	return nil
}

func (ms *MockSentry) insertPoSBlocks(chain *core.ChainPack) error {
	n := ms.numberOfPoWBlocks(chain)
	if n >= chain.Length() {
		return nil
	}

	wr := eth1_chain_reader.NewChainReaderEth1(ms.ChainConfig, direct.NewExecutionClientDirect(ms.Eth1ExecutionService), uint64(time.Hour))

	ctx := context.Background()
	for i := n; i < chain.Length(); i++ {
		if err := chain.Blocks[i].HashCheck(); err != nil {
			return err
		}
	}
	if err := wr.InsertBlocksAndWait(ctx, chain.Blocks); err != nil {
		return err
	}

	tipHash := chain.TopBlock.Hash()

	status, _, lvh, err := wr.UpdateForkChoice(ctx, tipHash, tipHash, tipHash)

	if err != nil {
		return err
	}
	if err := ms.DB.UpdateNosync(ms.Ctx, func(tx kv.RwTx) error {
		rawdb.WriteHeadBlockHash(tx, lvh)
		return nil
	}); err != nil {
		return err
	}
	if status != execution.ExecutionStatus_Success {
		return fmt.Errorf("insertion failed for block %d, code: %s", chain.Blocks[chain.Length()-1].NumberU64(), status.String())
	}

	return nil
}

func (ms *MockSentry) InsertChain(chain *core.ChainPack) error {

	if err := ms.insertPoWBlocks(chain); err != nil {
		return err
	}
	if err := ms.insertPoSBlocks(chain); err != nil {
		return err
	}

	roTx, err := ms.DB.BeginRo(ms.Ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	// Check if the latest header was imported or rolled back
	if rawdb.ReadHeader(roTx, chain.TopBlock.Hash(), chain.TopBlock.NumberU64()) == nil {
		return fmt.Errorf("did not import block %d %x", chain.TopBlock.NumberU64(), chain.TopBlock.Hash())
	}
	execAt, err := stages.GetStageProgress(roTx, stages.Execution)
	if err != nil {
		return err
	}

	if execAt < chain.TopBlock.NumberU64() {
		return fmt.Errorf("sentryMock.InsertChain end up with Execution stage progress: %d < %d", execAt, chain.TopBlock.NumberU64())
	}

	if ms.sentriesClient.Hd.IsBadHeader(chain.TopBlock.Hash()) {
		fmt.Printf("a3\n")
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

	return nil
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
	if ms.HistoryV3 {
		return state.NewReaderV4(tx.(kv.TemporalGetter))
	}
	return state.NewPlainStateReader(tx)
}
func (ms *MockSentry) HistoryV3Components() *libstate.Aggregator {
	return ms.agg
}

func (ms *MockSentry) BlocksIO() (services.FullBlockReader, *blockio.BlockWriter) {
	return ms.BlockReader, blockio.NewBlockWriter(ms.HistoryV3)
}
