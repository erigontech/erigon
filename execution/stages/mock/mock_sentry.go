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

package mock

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/gointerfaces"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	execution "github.com/erigontech/erigon-lib/gointerfaces/executionproto"
	proto_sentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	ptypes "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconsensusconfig"
	"github.com/erigontech/erigon/eth/tracers"
	debugtracer "github.com/erigontech/erigon/eth/tracers/debug"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/ethash"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/eth1"
	"github.com/erigontech/erigon/execution/eth1/eth1_chain_reader"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	stages2 "github.com/erigontech/erigon/execution/stages"
	"github.com/erigontech/erigon/execution/stages/bodydownload"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

const MockInsertAsInitialCycle = false

// MockSentry is a Netwrork Inverface mock. So, unit-tests can test many Erigon's components - but without net-interaction
type MockSentry struct {
	proto_sentry.UnimplementedSentryServer
	Ctx                  context.Context
	Log                  log.Logger
	tb                   testing.TB
	cancel               context.CancelFunc
	DB                   kv.TemporalRwDB
	Dirs                 datadir.Dirs
	Engine               consensus.Engine
	ChainConfig          *chain.Config
	Sync                 *stagedsync.Sync
	MiningSync           *stagedsync.Sync
	PendingBlocks        chan *types.Block
	MinedBlocks          chan *types.BlockWithReceipts
	sentriesClient       *sentry_multi_client.MultiClient
	Key                  *ecdsa.PrivateKey
	Genesis              *types.Block
	SentryClient         direct.SentryClient
	PeerId               *ptypes.H512
	streams              map[proto_sentry.MessageId][]proto_sentry.Sentry_MessagesServer
	sentMessages         []*proto_sentry.OutboundMessageData
	StreamWg             sync.WaitGroup
	ReceiveWg            sync.WaitGroup
	Address              common.Address
	Eth1ExecutionService *eth1.EthereumExecutionModule
	retirementStart      chan bool
	retirementDone       chan struct{}
	retirementWg         sync.WaitGroup

	Notifications *shards.Notifications

	// TxPool
	TxPool           *txpool.TxPool
	TxPoolGrpcServer txpoolproto.TxpoolServer

	HistoryV3      bool
	cfg            ethconfig.Config
	BlockSnapshots *freezeblocks.RoSnapshots
	BlockReader    services.FullBlockReader
	ReceiptsReader *receipts.Generator
	posStagedSync  *stagedsync.Sync
	bgComponentsEg errgroup.Group
}

func (ms *MockSentry) Close() {
	ms.cancel()
	if ms.Engine != nil {
		ms.Engine.Close()
	}
	if ms.BlockSnapshots != nil {
		ms.BlockSnapshots.Close()
	}
	if ms.DB != nil {
		ms.DB.Close()
	}
	if err := ms.bgComponentsEg.Wait(); err != nil {
		require.Equal(ms.tb, context.Canceled, err) // upon waiting for clean exit we should get ctx cancelled
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
	return MockWithGenesisPruneMode(tb, gspec, key, blockBufferSize, prune.MockMode, withPosDownloader)
}

func MockWithGenesisEngine(tb testing.TB, gspec *types.Genesis, engine consensus.Engine, withPosDownloader bool) *MockSentry {
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	return MockWithEverything(tb, gspec, key, prune.MockMode, engine, blockBufferSize, false, withPosDownloader)
}

func MockWithGenesisPruneMode(tb testing.TB, gspec *types.Genesis, key *ecdsa.PrivateKey, blockBufferSize int, prune prune.Mode, withPosDownloader bool) *MockSentry {
	var engine consensus.Engine

	switch {
	case gspec.Config.Bor != nil:
		engine = bor.NewFaker()
	default:
		engine = ethash.NewFaker()
	}

	return MockWithEverything(tb, gspec, key, prune, engine, blockBufferSize, false, withPosDownloader)
}

func MockWithEverything(tb testing.TB, gspec *types.Genesis, key *ecdsa.PrivateKey, prune prune.Mode, engine consensus.Engine, blockBufferSize int, withTxPool, withPosDownloader bool) *MockSentry {
	tmpdir := os.TempDir()
	if tb != nil {
		tmpdir = tb.TempDir()
	}
	ctrl := gomock.NewController(tb)
	dirs := datadir.New(tmpdir)
	var err error

	cfg := ethconfig.Defaults
	cfg.StateStream = true
	cfg.BatchSize = 5 * datasize.MB
	cfg.Sync.BodyDownloadTimeoutSeconds = 10
	cfg.TxPool.Disable = !withTxPool
	cfg.Dirs = dirs
	cfg.AlwaysGenerateChangesets = true
	cfg.PersistReceiptsCacheV2 = true
	cfg.ChaosMonkey = false
	cfg.Snapshot.ChainName = gspec.Config.ChainName
	cfg.Genesis = gspec

	logLvl := log.LvlError
	if lvl, ok := os.LookupEnv("MOCK_SENTRY_LOG_LEVEL"); ok {
		logLvl, err = log.LvlFromString(lvl)
		if err != nil {
			panic(err)
		}
	}

	logger := log.Root()
	logger.SetHandler(log.LvlFilterHandler(logLvl, log.StderrHandler))

	ctx, ctxCancel := context.WithCancel(context.Background())
	db := temporaltest.NewTestDB(tb, dirs)

	if _, err := snaptype.LoadSalt(dirs.Snap, true, logger); err != nil {
		panic(err)
	}

	erigonGrpcServeer := remotedbserver.NewKvServer(ctx, db, nil, nil, nil, logger)
	allSnapshots := freezeblocks.NewRoSnapshots(cfg.Snapshot, dirs.Snap, logger)
	allBorSnapshots := heimdall.NewRoSnapshots(cfg.Snapshot, dirs.Snap, logger)

	br := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)

	mock := &MockSentry{
		Ctx: ctx, cancel: ctxCancel, DB: db,
		tb:             tb,
		Log:            logger,
		Dirs:           dirs,
		Engine:         engine,
		ChainConfig:    gspec.Config,
		Key:            key,
		Notifications:  shards.NewNotifications(erigonGrpcServeer),
		PeerId:         gointerfaces.ConvertHashToH512([64]byte{0x12, 0x34, 0x50}), // "12345"
		BlockSnapshots: allSnapshots,
		BlockReader:    br,
		ReceiptsReader: receipts.NewGenerator(br, engine, 5*time.Second),
		HistoryV3:      true,
		cfg:            cfg,
	}
	mock.retirementStart, _ = mock.Notifications.Events.AddRetirementStartSubscription()
	mock.retirementDone, _ = mock.Notifications.Events.AddRetirementDoneSubscription()

	if tb != nil {
		tb.Cleanup(mock.Close)
		tb.Cleanup(func() {
			// Wait for all the background snapshot retirements launched by any stages2.StageLoopIteration to finish
			mock.retirementWg.Wait()
		})
	}

	// Committed genesis will be shared between download and mock sentry
	_, mock.Genesis, err = genesiswrite.CommitGenesisBlock(mock.DB, gspec, datadir.New(tmpdir), mock.Log)
	if _, ok := err.(*chain.ConfigCompatError); err != nil && !ok {
		if tb != nil {
			tb.Fatal(err)
		} else {
			panic(err)
		}
	}

	blockWriter := blockio.NewBlockWriter()

	mock.Address = crypto.PubkeyToAddress(mock.Key.PublicKey)

	sendHeaderRequest := func(_ context.Context, r *headerdownload.HeaderRequest) ([64]byte, bool) { return [64]byte{}, false }
	propagateNewBlockHashes := func(context.Context, []headerdownload.Announce) {}
	penalize := func(context.Context, []headerdownload.PenaltyItem) {}

	mock.SentryClient = direct.NewSentryClientDirect(direct.ETH68, mock)
	sentries := []proto_sentry.SentryClient{mock.SentryClient}

	sendBodyRequest := func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool) { return [64]byte{}, false }
	blockPropagator := func(Ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int) {}
	if !cfg.TxPool.Disable {
		poolCfg := txpoolcfg.DefaultConfig
		stateChangesClient := direct.NewStateDiffClientDirect(erigonGrpcServeer)
		mock.TxPool, mock.TxPoolGrpcServer, err = txpool.Assemble(
			ctx,
			poolCfg,
			mock.DB,
			kvcache.NewDummy(),
			sentries,
			stateChangesClient,
			func() {}, /* builderNotifyNewTxns */
			logger,
			nil,
			txpool.WithP2PFetcherWg(&mock.ReceiveWg),
			txpool.WithP2PSenderWg(nil),
			txpool.WithFeeCalculator(nil),
			txpool.WithPoolDBInitializer(func(_ context.Context, _ txpoolcfg.Config, _ log.Logger) (kv.RwDB, error) {
				return memdb.NewWithLabel(tmpdir, kv.TxPoolDB), nil
			}),
		)
		if err != nil {
			tb.Fatal(err)
		}

		mock.StreamWg.Add(1)
		mock.bgComponentsEg.Go(func() error { return mock.TxPool.Run(ctx) })
		mock.StreamWg.Wait()
	}

	latestBlockBuiltStore := builder.NewLatestBlockBuiltStore()
	inMemoryExecution := func(txc wrap.TxContainer, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		terseLogger := log.New()
		terseLogger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync := stages2.NewInMemoryExecution(mock.Ctx, mock.DB, &cfg, mock.sentriesClient,
			dirs, notifications, mock.BlockReader, blockWriter, nil, terseLogger)
		chainReader := consensuschain.NewReader(mock.ChainConfig, txc.Tx, mock.BlockReader, logger)
		// We start the mining step
		if err := stages2.StateStep(ctx, chainReader, mock.Engine, txc, stateSync, header, body, unwindPoint, headersChain, bodiesChain, true); err != nil {
			logger.Warn("Could not validate block", "err", err)
			return errors.Join(consensus.ErrInvalidBlock, err)
		}
		var progress uint64
		progress, err = stages.GetStageProgress(txc.Tx, stages.Execution)
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
		logger,
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
		false, /* enableWitProtocol */
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

	snapDownloader := mockDownloader(ctrl)

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
		miningStatePos := stagedsync.NewMiningState(&cfg.Miner)
		miningStatePos.MiningConfig.Etherbase = param.SuggestedFeeRecipient
		proposingSync := stagedsync.New(
			cfg.Sync,
			stagedsync.MiningStages(mock.Ctx,
				stagedsync.StageMiningCreateBlockCfg(mock.DB, miner, mock.ChainConfig, mock.Engine, nil, dirs.Tmp, mock.BlockReader),
				stagedsync.StageExecuteBlocksCfg(
					mock.DB,
					prune,
					cfg.BatchSize,
					mock.ChainConfig,
					mock.Engine,
					&vm.Config{},
					mock.Notifications,
					cfg.StateStream,
					/*stateStream=*/ false,
					dirs,
					mock.BlockReader,
					mock.sentriesClient.Hd,
					gspec,
					cfg.Sync,
					nil,
				),
				stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, cfg.Sync, false, dirs.Tmp, prune, mock.BlockReader, mock.sentriesClient.Hd),
				stagedsync.StageMiningExecCfg(mock.DB, miner, nil, mock.ChainConfig, mock.Engine, &vm.Config{}, dirs.Tmp, nil, 0, mock.TxPool, mock.BlockReader),
				stagedsync.StageMiningFinishCfg(mock.DB, mock.ChainConfig, mock.Engine, miner, miningCancel, mock.BlockReader, latestBlockBuiltStore),
				false,
			), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder,
			logger, stages.ModeBlockProduction)
		// We start the mining step
		if err := stages2.MiningStep(ctx, mock.DB, proposingSync, tmpdir, logger); err != nil {
			return nil, err
		}
		block := <-miningStatePos.MiningResultCh
		return block, nil
	}

	blockRetire := freezeblocks.NewBlockRetire(1, dirs, mock.BlockReader, blockWriter, mock.DB, nil, nil, mock.ChainConfig, &cfg, mock.Notifications.Events, nil, logger)
	mock.Sync = stagedsync.New(
		cfg.Sync,
		stagedsync.DefaultStages(mock.Ctx,
			stagedsync.StageSnapshotsCfg(mock.DB, mock.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, mock.BlockReader, mock.Notifications, false, false, false, nil, prune),
			stagedsync.StageHeadersCfg(mock.DB, mock.sentriesClient.Hd, mock.sentriesClient.Bd, mock.ChainConfig, cfg.Sync, sendHeaderRequest, propagateNewBlockHashes, penalize, cfg.BatchSize, false, mock.BlockReader, blockWriter, dirs.Tmp, mock.Notifications),
			stagedsync.StageBlockHashesCfg(mock.DB, mock.Dirs.Tmp, mock.ChainConfig, blockWriter),
			stagedsync.StageBodiesCfg(mock.DB, mock.sentriesClient.Bd, sendBodyRequest, penalize, blockPropagator, cfg.Sync.BodyDownloadTimeoutSeconds, mock.ChainConfig, mock.BlockReader, blockWriter),
			stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, cfg.Sync, false, dirs.Tmp, prune, mock.BlockReader, mock.sentriesClient.Hd), stagedsync.StageExecuteBlocksCfg(
				mock.DB,
				prune,
				cfg.BatchSize,
				mock.ChainConfig,
				mock.Engine,
				&vm.Config{},
				mock.Notifications,
				cfg.StateStream,
				/*stateStream=*/ false,
				dirs,
				mock.BlockReader,
				mock.sentriesClient.Hd,
				gspec,
				cfg.Sync,
				nil,
			), stagedsync.StageTxLookupCfg(mock.DB, prune, dirs.Tmp, mock.ChainConfig.Bor, mock.BlockReader), stagedsync.StageFinishCfg(mock.DB, dirs.Tmp, forkValidator), !withPosDownloader),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
		logger, stages.ModeApplyingBlocks,
	)

	var tracer *tracers.Tracer
	if dir, ok := os.LookupEnv("MOCK_SENTRY_DEBUG_TRACER_OUTPUT_DIR"); ok {
		tracer = debugtracer.New(dir, debugtracer.WithRecordOptions(debugtracer.RecordOptions{
			DisableOnOpcodeStackRecording:  true,
			DisableOnOpcodeMemoryRecording: true,
		}))
	}

	cfg.Genesis = gspec
	pipelineStages := stages2.NewPipelineStages(mock.Ctx, db, &cfg, mock.sentriesClient, mock.Notifications, snapDownloader, mock.BlockReader, blockRetire, nil, forkValidator, tracer)
	mock.posStagedSync = stagedsync.New(cfg.Sync, pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, logger, stages.ModeApplyingBlocks)

	mock.Eth1ExecutionService = eth1.NewEthereumExecutionModule(mock.BlockReader, mock.DB, mock.posStagedSync, forkValidator, mock.ChainConfig, assembleBlockPOS, nil, mock.Notifications.Accumulator, mock.Notifications.RecentLogs, mock.Notifications.StateChangesConsumer, logger, engine, cfg.Sync, ctx)

	mock.sentriesClient.Hd.StartPoSDownloader(mock.Ctx, sendHeaderRequest, penalize)

	mock.MiningSync = stagedsync.New(
		cfg.Sync,
		stagedsync.MiningStages(mock.Ctx,
			stagedsync.StageMiningCreateBlockCfg(mock.DB, miner, mock.ChainConfig, mock.Engine, nil, dirs.Tmp, mock.BlockReader),
			stagedsync.StageExecuteBlocksCfg(
				mock.DB,
				prune,
				cfg.BatchSize,
				mock.ChainConfig,
				mock.Engine,
				&vm.Config{},
				mock.Notifications,
				cfg.StateStream,
				/*stateStream=*/ false,
				dirs,
				mock.BlockReader,
				mock.sentriesClient.Hd,
				gspec,
				cfg.Sync,
				nil,
			),
			stagedsync.StageSendersCfg(mock.DB, mock.ChainConfig, cfg.Sync, false, dirs.Tmp, prune, mock.BlockReader, mock.sentriesClient.Hd),
			stagedsync.StageMiningExecCfg(mock.DB, miner, nil, mock.ChainConfig, mock.Engine, &vm.Config{}, dirs.Tmp, nil, 0, mock.TxPool, mock.BlockReader),
			stagedsync.StageMiningFinishCfg(mock.DB, mock.ChainConfig, mock.Engine, miner, miningCancel, mock.BlockReader, latestBlockBuiltStore),
			false,
		),
		stagedsync.MiningUnwindOrder,
		stagedsync.MiningPruneOrder,
		logger, stages.ModeBlockProduction,
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

	//app expecting that genesis will always be in db
	c := &core.ChainPack{
		Headers:  []*types.Header{mock.Genesis.HeaderNoCopy()},
		Blocks:   []*types.Block{mock.Genesis},
		TopBlock: mock.Genesis,
	}
	if err = mock.InsertChain(c); err != nil {
		tb.Fatal(err)
	}

	return mock
}

func mockDownloader(ctrl *gomock.Controller) *proto_downloader.MockDownloaderClient {
	snapDownloader := proto_downloader.NewMockDownloaderClient(ctrl)

	snapDownloader.EXPECT().
		Add(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&emptypb.Empty{}, nil).
		AnyTimes()
	snapDownloader.EXPECT().
		SetLogPrefix(gomock.Any(), gomock.Any()).
		Return(&emptypb.Empty{}, nil).
		AnyTimes()
	snapDownloader.EXPECT().
		Completed(gomock.Any(), gomock.Any()).
		Return(&proto_downloader.CompletedReply{Completed: true}, nil).
		AnyTimes()

	return snapDownloader
}

// Mock is convenience function to create a mock with some pre-set values
func Mock(tb testing.TB) *MockSentry {
	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.TestChainConfig
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	return MockWithGenesis(tb, gspec, key, false)
}

func MockWithTxPool(t *testing.T) *MockSentry {
	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.TestChainConfig
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}

	return MockWithEverything(t, gspec, key, prune.MockMode, ethash.NewFaker(), blockBufferSize, true, false)
}

func MockWithTxPoolCancun(t *testing.T) *MockSentry {
	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.AllProtocolChanges
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}

	return MockWithEverything(t, gspec, key, prune.MockMode, ethash.NewFaker(), blockBufferSize, true, false)
}

func MockWithTxPoolOsaka(t *testing.T) *MockSentry {
	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.AllProtocolChanges
	chainConfig.OsakaTime = big.NewInt(0)
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}

	return MockWithEverything(t, gspec, key, prune.MockMode, ethash.NewFaker(), blockBufferSize, true, false)
}

func MockWithZeroTTD(t *testing.T, withPosDownloader bool) *MockSentry {
	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.AllProtocolChanges
	chainConfig.TerminalTotalDifficulty = common.Big0
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	return MockWithGenesis(t, gspec, key, withPosDownloader)
}

func MockWithZeroTTDGnosis(t *testing.T, withPosDownloader bool) *MockSentry {
	funds := big.NewInt(1 * common.Ether)
	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	address := crypto.PubkeyToAddress(key.PublicKey)
	chainConfig := chain.TestChainAuraConfig
	chainConfig.TerminalTotalDifficulty = common.Big0
	chainConfig.TerminalTotalDifficultyPassed = true
	gspec := &types.Genesis{
		Config: chainConfig,
		Alloc: types.GenesisAlloc{
			address: {Balance: funds},
		},
	}
	engine := ethconsensusconfig.CreateConsensusEngineBareBones(context.Background(), chainConfig, log.New())
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

func (ms *MockSentry) Cfg() ethconfig.Config { return ms.cfg }
func (ms *MockSentry) insertPoWBlocks(chain *core.ChainPack) error {
	n := ms.numberOfPoWBlocks(chain)
	if n == 0 {
		// No Proof-of-Work blocks
		return nil
	}
	for i := 0; i < chain.Length(); i++ {
		if err := chain.Blocks[i].HashCheck(false); err != nil {
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
	initialCycle, firstCycle := MockInsertAsInitialCycle, false
	hook := stages2.NewHook(ms.Ctx, ms.DB, ms.Notifications, ms.Sync, ms.BlockReader, ms.ChainConfig, ms.Log, nil)

	if err = stages2.StageLoopIteration(ms.Ctx, ms.DB, wrap.NewTxContainer(nil, nil), ms.Sync, initialCycle, firstCycle, ms.Log, ms.BlockReader, hook); err != nil {
		return err
	}
	// Wait to know if a new background retirement has started
	if retirementStarted := <-ms.retirementStart; retirementStarted {
		// If so, increment the background retirement counter and start a task to watch for its completion
		ms.retirementWg.Add(1)
		go func() {
			defer ms.retirementWg.Done()
			<-ms.retirementDone
		}()
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
		if err := chain.Blocks[i].HashCheck(false); err != nil {
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
		return fmt.Errorf("block %d %x was invalid", chain.TopBlock.NumberU64(), chain.TopBlock.Hash())
	}
	return nil
}

func (ms *MockSentry) HeaderDownload() *headerdownload.HeaderDownload {
	return ms.sentriesClient.Hd
}

func (ms *MockSentry) NewHistoryStateReader(blockNum uint64, tx kv.TemporalTx) state.StateReader {
	r, err := rpchelper.CreateHistoryStateReader(tx, blockNum, 0, ms.BlockReader.TxnumReader(ms.Ctx))
	if err != nil {
		panic(err)
	}
	return r
}

func (ms *MockSentry) NewStateReader(tx kv.TemporalGetter) state.StateReader {
	return state.NewReaderV3(tx)
}

func (ms *MockSentry) BlocksIO() (services.FullBlockReader, *blockio.BlockWriter) {
	return ms.BlockReader, blockio.NewBlockWriter()
}
