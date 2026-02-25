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

package execmoduletester

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/consensuschain"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/builder/builderstages"
	"github.com/erigontech/erigon/execution/chain"
	enginehelpers "github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/bodydownload"
	"github.com/erigontech/erigon/execution/stagedsync/headerdownload"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	debugtracer "github.com/erigontech/erigon/execution/tracing/tracers/debug"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon/node/gointerfaces/executionproto"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"
)

type StateChangesClient interface {
	StateChanges(ctx context.Context, in *remoteproto.StateChangeRequest, opts ...grpc.CallOption) (remoteproto.KV_StateChangesClient, error)
}

// ExecModuleTester aims to construct all parts necessary to test the PoS GRPC API of our EthereumExecModule.
type ExecModuleTester struct {
	sentryproto.UnimplementedSentryServer
	Ctx             context.Context
	Log             log.Logger
	tb              testing.TB
	cancel          context.CancelFunc
	DB              kv.TemporalRwDB
	Dirs            datadir.Dirs
	Engine          rules.Engine
	ChainConfig     *chain.Config
	Sync            *stagedsync.Sync
	MiningSync      *stagedsync.Sync
	PendingBlocks   chan *types.Block
	MinedBlocks     chan *types.BlockWithReceipts
	sentriesClient  *sentry_multi_client.MultiClient
	Key             *ecdsa.PrivateKey
	Genesis         *types.Block
	SentryClient    direct.SentryClient
	PeerId          *typesproto.H512
	streams         map[sentryproto.MessageId][]sentryproto.Sentry_MessagesServer
	sentMessages    []*sentryproto.OutboundMessageData
	StreamWg        sync.WaitGroup
	ReceiveWg       sync.WaitGroup
	Address         common.Address
	ForkValidator   *enginehelpers.ForkValidator
	ExecModule      *execmodule.ExecModule
	StateCache      *execmodule.Cache
	retirementStart chan bool
	retirementDone  chan struct{}
	retirementWg    sync.WaitGroup

	Notifications      *shards.Notifications
	stateChangesClient StateChangesClient

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

func (emt *ExecModuleTester) Close() {
	emt.cancel()
	if err := emt.bgComponentsEg.Wait(); err != nil {
		require.Equal(emt.tb, context.Canceled, err) // upon waiting for clean exit we should get ctx cancelled
	}
	if emt.Engine != nil {
		emt.Engine.Close()
	}
	if emt.BlockSnapshots != nil {
		emt.BlockSnapshots.Close()
	}
	if emt.DB != nil {
		emt.DB.Close()
	}
}

// Stream returns stream, waiting if necessary
func (emt *ExecModuleTester) Send(req *sentryproto.InboundMessage) (errs []error) {
	emt.StreamWg.Wait()
	for _, stream := range emt.streams[req.Id] {
		if err := stream.Send(req); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func (emt *ExecModuleTester) SetStatus(context.Context, *sentryproto.StatusData) (*sentryproto.SetStatusReply, error) {
	return &sentryproto.SetStatusReply{}, nil
}

func (emt *ExecModuleTester) PenalizePeer(context.Context, *sentryproto.PenalizePeerRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (emt *ExecModuleTester) SetPeerMinimumBlock(context.Context, *sentryproto.SetPeerMinimumBlockRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (emt *ExecModuleTester) SetPeerLatestBlock(context.Context, *sentryproto.SetPeerLatestBlockRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (emt *ExecModuleTester) SetPeerBlockRange(context.Context, *sentryproto.SetPeerBlockRangeRequest) (*emptypb.Empty, error) {
	return nil, nil
}

func (emt *ExecModuleTester) HandShake(ctx context.Context, in *emptypb.Empty) (*sentryproto.HandShakeReply, error) {
	return &sentryproto.HandShakeReply{Protocol: sentryproto.Protocol_ETH69}, nil
}
func (emt *ExecModuleTester) SendMessageByMinBlock(_ context.Context, r *sentryproto.SendMessageByMinBlockRequest) (*sentryproto.SentPeers, error) {
	emt.sentMessages = append(emt.sentMessages, r.Data)
	return nil, nil
}
func (emt *ExecModuleTester) SendMessageById(_ context.Context, r *sentryproto.SendMessageByIdRequest) (*sentryproto.SentPeers, error) {
	emt.sentMessages = append(emt.sentMessages, r.Data)
	return nil, nil
}
func (emt *ExecModuleTester) SendMessageToRandomPeers(_ context.Context, r *sentryproto.SendMessageToRandomPeersRequest) (*sentryproto.SentPeers, error) {
	emt.sentMessages = append(emt.sentMessages, r.Data)
	return nil, nil
}
func (emt *ExecModuleTester) SendMessageToAll(_ context.Context, r *sentryproto.OutboundMessageData) (*sentryproto.SentPeers, error) {
	emt.sentMessages = append(emt.sentMessages, r)
	return nil, nil
}
func (emt *ExecModuleTester) SentMessage(i int) (*sentryproto.OutboundMessageData, error) {
	if i < 0 || i >= len(emt.sentMessages) {
		return nil, fmt.Errorf("no sent message for index %d found", i)
	}
	return emt.sentMessages[i], nil
}

func (emt *ExecModuleTester) Messages(req *sentryproto.MessagesRequest, stream sentryproto.Sentry_MessagesServer) error {
	if emt.streams == nil {
		emt.streams = map[sentryproto.MessageId][]sentryproto.Sentry_MessagesServer{}
	}

	for _, id := range req.Ids {
		emt.streams[id] = append(emt.streams[id], stream)
	}
	emt.StreamWg.Done()
	select {
	case <-emt.Ctx.Done():
		return nil
	case <-stream.Context().Done():
		return nil
	}
}

func (emt *ExecModuleTester) Peers(context.Context, *emptypb.Empty) (*sentryproto.PeersReply, error) {
	return &sentryproto.PeersReply{}, nil
}
func (emt *ExecModuleTester) PeerCount(context.Context, *sentryproto.PeerCountRequest) (*sentryproto.PeerCountReply, error) {
	return &sentryproto.PeerCountReply{Count: 0}, nil
}
func (emt *ExecModuleTester) PeerById(context.Context, *sentryproto.PeerByIdRequest) (*sentryproto.PeerByIdReply, error) {
	return &sentryproto.PeerByIdReply{}, nil
}
func (emt *ExecModuleTester) PeerEvents(req *sentryproto.PeerEventsRequest, server sentryproto.Sentry_PeerEventsServer) error {
	return nil
}

func (emt *ExecModuleTester) NodeInfo(context.Context, *emptypb.Empty) (*typesproto.NodeInfoReply, error) {
	return nil, nil
}

type Option func(*options)

func WithStepSize(stepSize uint64) Option {
	return func(opts *options) {
		opts.stepSize = &stepSize
	}
}

func WithExperimentalBAL() Option {
	return func(opts *options) {
		opts.experimentalBAL = true
	}
}

func WithGenesisSpec(gspec *types.Genesis) Option {
	return func(opts *options) {
		opts.genesis = gspec
	}
}

func WithKey(key *ecdsa.PrivateKey) Option {
	return func(opts *options) {
		opts.key = key
	}
}

func WithEngine(engine rules.Engine) Option {
	return func(opts *options) {
		opts.engine = engine
	}
}

func WithPruneMode(pm prune.Mode) Option {
	return func(opts *options) {
		opts.pruneMode = &pm
	}
}

func WithBlockBufferSize(size int) Option {
	return func(opts *options) {
		opts.blockBufferSize = size
	}
}

func WithTxPool() Option {
	return func(opts *options) {
		opts.withTxPool = true
	}
}

func WithChainConfig(cfg *chain.Config) Option {
	return func(opts *options) {
		opts.chainConfig = cfg
	}
}

type options struct {
	stepSize        *uint64
	experimentalBAL bool
	genesis         *types.Genesis
	chainConfig     *chain.Config
	key             *ecdsa.PrivateKey
	engine          rules.Engine
	pruneMode       *prune.Mode
	blockBufferSize int
	withTxPool      bool
}

func applyOptions(opts []Option) options {
	defaultKey, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	defaultPruneMode := prune.MockMode
	opt := options{
		key:             defaultKey,
		pruneMode:       &defaultPruneMode,
		blockBufferSize: 128,
		chainConfig:     chain.TestChainConfig,
	}
	for _, o := range opts {
		o(&opt)
	}
	// genesis depends on key and chainConfig
	if opt.genesis == nil {
		address := crypto.PubkeyToAddress(opt.key.PublicKey)
		opt.genesis = &types.Genesis{
			Config: opt.chainConfig,
			Alloc: types.GenesisAlloc{
				address: {Balance: big.NewInt(1 * common.Ether)},
			},
		}
	}
	// engine depends on genesis
	if opt.engine == nil {
		switch {
		case opt.genesis.Config.Bor != nil:
			opt.engine = bor.NewFaker()
		default:
			opt.engine = ethash.NewFaker()
		}
	}
	return opt
}

// New creates an ExecModuleTester. When called with no options, it uses
// sensible defaults (TestChainConfig, 1 Ether alloc, ethash.NewFaker, etc.).
// Use With* options to customise.
func New(tb testing.TB, opts ...Option) *ExecModuleTester {
	opt := applyOptions(opts)

	gspec := opt.genesis
	key := opt.key
	engine := opt.engine
	pruneMode := *opt.pruneMode
	blockBufferSize := opt.blockBufferSize
	withTxPool := opt.withTxPool
	tmpdir, err := os.MkdirTemp("", "mock-sentry-*")
	if err != nil {
		panic(err)
	}
	if tb != nil {
		// we can't use tb.TempDir() here becuase things like TestExecutionSpecBlockchain
		// produces test names that cause 'file name too long' errors
		tb.Cleanup(func() {
			dir.RemoveAll(tmpdir)
		})
	}
	ctrl := gomock.NewController(tb)
	dirs := datadir.New(tmpdir)

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
	cfg.Prune = pruneMode
	cfg.ExperimentalBAL = opt.experimentalBAL
	cfg.FcuBackgroundPrune = false
	cfg.FcuBackgroundCommit = false

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
	var db kv.TemporalRwDB
	if opt.stepSize != nil {
		db = temporaltest.NewTestDBWithStepSize(tb, dirs, *opt.stepSize)
	} else {
		db = temporaltest.NewTestDB(tb, dirs)
	}

	if _, err := snaptype.LoadSalt(dirs.Snap, true, logger); err != nil {
		panic(err)
	}

	erigonGrpcServer := remotedbserver.NewKvServer(ctx, db, nil, nil, nil, logger)
	allSnapshots := freezeblocks.NewRoSnapshots(cfg.Snapshot, dirs.Snap, logger)
	allBorSnapshots := heimdall.NewRoSnapshots(cfg.Snapshot, dirs.Snap, logger)

	br := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)

	mock := &ExecModuleTester{
		Ctx: ctx, cancel: ctxCancel, DB: db,
		tb:                 tb,
		Log:                logger,
		Dirs:               dirs,
		Engine:             engine,
		ChainConfig:        gspec.Config,
		Key:                key,
		Notifications:      shards.NewNotifications(erigonGrpcServer),
		stateChangesClient: direct.NewStateDiffClientDirect(erigonGrpcServer),
		PeerId:             gointerfaces.ConvertHashToH512([64]byte{0x12, 0x34, 0x50}), // "12345"
		BlockSnapshots:     allSnapshots,
		BlockReader:        br,
		ReceiptsReader:     receipts.NewGenerator(dirs, br, engine, nil, 5*time.Second),
		HistoryV3:          true,
		cfg:                cfg,
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

	mock.SentryClient, err = direct.NewSentryClientDirect(direct.ETH68, mock, nil)
	require.NoError(tb, err)
	sentries := []sentryproto.SentryClient{mock.SentryClient}

	sendBodyRequest := func(context.Context, *bodydownload.BodyRequest) ([64]byte, bool) { return [64]byte{}, false }
	blockPropagator := func(Ctx context.Context, header *types.Header, body *types.RawBody, td *big.Int) {}
	if !cfg.TxPool.Disable {
		poolCfg := txpoolcfg.DefaultConfig
		stateChangesClient := direct.NewStateDiffClientDirect(erigonGrpcServer)
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
			//txpool.WithP2PFetcherWg(&mock.ReceiveWg), // this seems unecessary now status changes are async
			txpool.WithP2PSenderWg(nil),
			txpool.WithFeeCalculator(nil),
			txpool.WithPoolDBInitializer(func(_ context.Context, _ txpoolcfg.Config, _ log.Logger) (kv.RwDB, error) {
				return mdbx.New(dbcfg.TxPoolDB, logger).InMem(tb, tmpdir).MustOpen(), nil
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
	inMemoryExecution := func(sd *execctx.SharedDomains, tx kv.TemporalRwTx, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		terseLogger := log.New()
		terseLogger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync := stageloop.NewInMemoryExecution(mock.Ctx, mock.DB, &cfg, mock.sentriesClient,
			dirs, notifications, mock.BlockReader, blockWriter, nil, terseLogger)
		chainReader := consensuschain.NewReader(mock.ChainConfig, tx, mock.BlockReader, logger)
		// We start the mining step
		if err := stageloop.StateStep(ctx, chainReader, mock.Engine, sd, tx, stateSync, unwindPoint, headersChain, bodiesChain); err != nil {
			logger.Warn("Could not validate block", "err", err)
			return err
		}
		var progress uint64
		progress, err = stages.GetStageProgress(tx, stages.Execution)
		if err != nil {
			return err
		}
		lastNum := headersChain[len(headersChain)-1].Number.Uint64()
		if progress < lastNum {
			return fmt.Errorf("unsuccessful execution, progress %d < expected %d", progress, lastNum)
		}
		return nil
	}
	forkValidator := enginehelpers.NewForkValidator(ctx, 1, inMemoryExecution, dirs.Tmp, mock.BlockReader, cfg.MaxReorgDepth)
	mock.ForkValidator = forkValidator

	statusDataProvider := sentry.NewStatusDataProvider(
		db,
		mock.ChainConfig,
		mock.Genesis,
		mock.ChainConfig.ChainID.Uint64(),
		logger,
		mock.BlockReader,
	)

	maxBlockBroadcastPeers := func(header *types.Header) uint { return 0 }

	mock.sentriesClient, err = sentry_multi_client.NewMultiClient(
		mock.Dirs,
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

	snapDownloader := mockDownloader(ctrl, mock.Dirs.Snap)

	miningConfig := cfg.Builder
	miningConfig.Etherbase = mock.Address
	miningCancel := make(chan struct{})
	go func() {
		<-mock.Ctx.Done()
		close(miningCancel)
	}()

	// proof-of-stake block building
	assembleBlockPOS := func(param *builder.Parameters, interrupt *atomic.Bool) (*types.BlockWithReceipts, error) {
		builderStatePos := builderstages.NewBuilderState(&cfg.Builder)
		builderStatePos.BuilderConfig.Etherbase = param.SuggestedFeeRecipient
		proposingSync := stagedsync.New(
			cfg.Sync,
			builderstages.BuilderStages(
				mock.Ctx,
				builderstages.StageBuilderCreateBlockCfg(builderStatePos, mock.ChainConfig, mock.Engine, param, mock.BlockReader),
				stagedsync.StageExecuteBlocksCfg(
					mock.DB,
					pruneMode,
					cfg.BatchSize,
					mock.ChainConfig,
					mock.Engine,
					&vm.Config{},
					mock.Notifications,
					cfg.StateStream,
					false, /*badBlockHalt*/
					dirs,
					mock.BlockReader,
					mock.sentriesClient.Hd,
					gspec,
					cfg.Sync,
					nil,
					false, /*experimentalBAL*/
				),
				stagedsync.StageSendersCfg(mock.ChainConfig, cfg.Sync, false /* badBlockHalt */, dirs.Tmp, pruneMode, mock.BlockReader, mock.sentriesClient.Hd),
				builderstages.StageBuilderExecCfg(builderStatePos, nil /* notifier */, mock.ChainConfig, mock.Engine, &vm.Config{}, dirs.Tmp, interrupt, param.PayloadId, mock.TxPool, mock.BlockReader),
				builderstages.StageBuilderFinishCfg(mock.ChainConfig, mock.Engine, builderStatePos, miningCancel, mock.BlockReader, latestBlockBuiltStore),
			),
			builderstages.BuilderUnwindOrder,
			builderstages.BuilderPruneOrder,
			logger,
			stages.ModeBlockProduction,
		)
		// We start the mining step
		if err := stageloop.MiningStep(ctx, mock.DB, proposingSync, tmpdir, logger); err != nil {
			return nil, err
		}
		block := <-builderStatePos.BuilderResultCh
		return block, nil
	}

	blockRetire := freezeblocks.NewBlockRetire(1, dirs, mock.BlockReader, blockWriter, mock.DB, nil, nil, mock.ChainConfig, &cfg, mock.Notifications.Events, nil, logger)
	mock.Sync = stagedsync.New(
		cfg.Sync,
		stagedsync.DefaultStages(
			mock.Ctx,
			stagedsync.StageSnapshotsCfg(mock.DB, mock.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, mock.BlockReader, mock.Notifications, false, false, false, nil, pruneMode),
			stagedsync.StageHeadersCfg(mock.sentriesClient.Hd, mock.ChainConfig, cfg.Sync, sendHeaderRequest, propagateNewBlockHashes, penalize, false /* noP2PDiscovery */, mock.BlockReader),
			stagedsync.StageBlockHashesCfg(mock.Dirs.Tmp, blockWriter),
			stagedsync.StageBodiesCfg(mock.sentriesClient.Bd, sendBodyRequest, penalize, blockPropagator, cfg.Sync.BodyDownloadTimeoutSeconds, mock.ChainConfig, mock.BlockReader, blockWriter),
			stagedsync.StageSendersCfg(mock.ChainConfig, cfg.Sync, false /* badBlockHalt */, dirs.Tmp, pruneMode, mock.BlockReader, mock.sentriesClient.Hd),
			stagedsync.StageExecuteBlocksCfg(
				mock.DB,
				pruneMode,
				cfg.BatchSize,
				mock.ChainConfig,
				mock.Engine,
				&vm.Config{},
				mock.Notifications,
				cfg.StateStream,
				false, /*badBlockHalt*/
				dirs,
				mock.BlockReader,
				mock.sentriesClient.Hd,
				gspec,
				cfg.Sync,
				nil,
				false, /*experimentalBAL*/
			),
			stagedsync.StageTxLookupCfg(pruneMode, dirs.Tmp, mock.BlockReader),
			stagedsync.StageFinishCfg(forkValidator),
		),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
		logger,
		stages.ModeApplyingBlocks,
	)

	var tracer *tracers.Tracer
	if dir, ok := os.LookupEnv("MOCK_SENTRY_DEBUG_TRACER_OUTPUT_DIR"); ok {
		tracer = debugtracer.New(dir, debugtracer.WithRecordOptions(debugtracer.RecordOptions{
			DisableOnOpcodeStackRecording:  true,
			DisableOnOpcodeMemoryRecording: true,
		}))
	}

	cfg.Genesis = gspec
	pipelineStages := stageloop.NewPipelineStages(mock.Ctx, db, &cfg, mock.sentriesClient, mock.Notifications, snapDownloader, mock.BlockReader, blockRetire, nil, forkValidator, tracer)
	mock.posStagedSync = stagedsync.New(cfg.Sync, pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, logger, stages.ModeApplyingBlocks)

	hook := stageloop.NewHook(mock.Ctx, mock.Notifications, mock.posStagedSync, mock.ChainConfig, logger, nil, nil, nil)

	mock.StateCache = &execmodule.Cache{}
	onlySnapDownloadOnStart := cfg.Genesis.Config.Bor != nil

	mock.ExecModule = execmodule.NewExecModule(
		ctx,
		mock.BlockReader,
		mock.DB,
		mock.posStagedSync,
		forkValidator,
		mock.ChainConfig,
		assembleBlockPOS,
		hook,
		mock.Notifications.Accumulator,
		mock.Notifications.RecentReceipts,
		mock.StateCache,
		mock.Notifications.StateChangesConsumer,
		logger,
		engine,
		cfg.Sync,
		cfg.FcuBackgroundPrune,
		cfg.FcuBackgroundCommit,
		onlySnapDownloadOnStart,
	)

	mock.sentriesClient.Hd.StartPoSDownloader(mock.Ctx, sendHeaderRequest, penalize)

	// pow mining
	// TODO(yperbasis) remove pow mining
	miner := builderstages.NewBuilderState(&miningConfig)
	mock.PendingBlocks = miner.PendingResultCh
	mock.MinedBlocks = miner.BuilderResultCh
	mock.MiningSync = stagedsync.New(
		cfg.Sync,
		builderstages.BuilderStages(
			mock.Ctx,
			builderstages.StageBuilderCreateBlockCfg(miner, mock.ChainConfig, mock.Engine, nil, mock.BlockReader),
			stagedsync.StageExecuteBlocksCfg(
				mock.DB,
				pruneMode,
				cfg.BatchSize,
				mock.ChainConfig,
				mock.Engine,
				&vm.Config{},
				mock.Notifications,
				cfg.StateStream,
				/*badBlockHalt*/ false,
				dirs,
				mock.BlockReader,
				mock.sentriesClient.Hd,
				gspec,
				cfg.Sync,
				nil,
				/*experimentalBAL*/ false,
			),
			stagedsync.StageSendersCfg(mock.ChainConfig, cfg.Sync, false /* badBlockHalt */, dirs.Tmp, pruneMode, mock.BlockReader, mock.sentriesClient.Hd),
			builderstages.StageBuilderExecCfg(miner, nil, mock.ChainConfig, mock.Engine, &vm.Config{}, dirs.Tmp, nil, 0, mock.TxPool, mock.BlockReader),
			builderstages.StageBuilderFinishCfg(mock.ChainConfig, mock.Engine, miner, miningCancel, mock.BlockReader, latestBlockBuiltStore),
		),
		builderstages.BuilderUnwindOrder,
		builderstages.BuilderPruneOrder,
		logger,
		stages.ModeBlockProduction,
	)

	mock.StreamWg.Add(1)
	mock.bgComponentsEg.Go(func() error {
		mock.sentriesClient.RecvMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
		return nil
	})
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	mock.bgComponentsEg.Go(func() error {
		mock.sentriesClient.RecvUploadMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
		return nil
	})
	mock.StreamWg.Wait()
	mock.StreamWg.Add(1)
	mock.bgComponentsEg.Go(func() error {
		mock.sentriesClient.RecvUploadHeadersMessageLoop(mock.Ctx, mock.SentryClient, &mock.ReceiveWg)
		return nil
	})
	mock.StreamWg.Wait()

	//app expecting that genesis will always be in db
	c := &blockgen.ChainPack{
		Headers:  []*types.Header{mock.Genesis.HeaderNoCopy()},
		Blocks:   []*types.Block{mock.Genesis},
		TopBlock: mock.Genesis,
	}
	if err = mock.InsertChain(c); err != nil {
		tb.Fatal(err)
	}

	return mock
}

func mockDownloader(ctrl *gomock.Controller, snapRoot string) downloader.Client {
	snapDownloader := downloaderproto.NewMockDownloaderClient(ctrl)

	snapDownloader.EXPECT().
		Download(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&emptypb.Empty{}, nil).
		AnyTimes()

	return downloader.NewRpcClient(snapDownloader, snapRoot)
}

func (emt *ExecModuleTester) EnableLogs() {
	emt.Log.SetHandler(log.LvlFilterHandler(log.LvlInfo, log.StderrHandler))
}

func (emt *ExecModuleTester) Cfg() ethconfig.Config { return emt.cfg }

func (emt *ExecModuleTester) insertPoSBlocks(chain *blockgen.ChainPack) error {
	wr := chainreader.NewChainReaderEth1(emt.ChainConfig, direct.NewExecutionClientDirect(emt.ExecModule), time.Hour)

	streamCtx, cancel := context.WithCancel(emt.Ctx)
	defer cancel()
	stream, err := emt.stateChangesClient.StateChanges(streamCtx, &remoteproto.StateChangeRequest{WithStorage: false, WithTransactions: false}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}

	insertedBlocks := map[uint64]struct{}{}

	for i := 0; i < chain.Length(); i++ {
		if err := chain.Blocks[i].HashCheck(false); err != nil {
			return err
		}
		insertedBlocks[chain.Blocks[i].NumberU64()] = struct{}{}
	}

	var balEntries []*executionproto.BlockAccessListEntry
	for i, bal := range chain.BlockAccessLists {
		if len(bal) > 0 {
			block := chain.Blocks[i]
			balEntries = append(balEntries, &executionproto.BlockAccessListEntry{
				BlockHash:       gointerfaces.ConvertHashToH256(block.Hash()),
				BlockNumber:     block.NumberU64(),
				BlockAccessList: bal,
			})
		}
	}
	if err := wr.InsertBlocksAndWaitWithAccessLists(emt.Ctx, chain.Blocks, balEntries); err != nil {
		return err
	}

	tipHash := chain.TopBlock.Hash()

	status, verr, _, err := wr.UpdateForkChoice(emt.Ctx, tipHash, tipHash, tipHash)
	if err != nil {
		return err
	}

	if status != executionproto.ExecutionStatus_Success {
		if verr != nil {
			return fmt.Errorf("insertion failed for block %d, code: %s err: %s", chain.Blocks[chain.Length()-1].NumberU64(), status.String(), *verr)
		}
		return fmt.Errorf("insertion failed for block %d, code: %s", chain.Blocks[chain.Length()-1].NumberU64(), status.String())
	}

	// UpdateForkChoice calls commit asyncronously so we need to
	// wait for confimation that the headers are processed before
	// returning to the caller
	lastSeenBlock := chain.Headers[0].Number.Uint64()

	for len(insertedBlocks) > 0 {
		req, err := stream.Recv()
		if err != nil {
			if emt.Ctx.Err() != nil {
				return nil
			}
			if streamCtx.Err() != nil {
				return fmt.Errorf("block insert recv timed out: %d remaining", len(insertedBlocks))
			}
			return fmt.Errorf("block insert recv failed: %w, %d remaining", err, len(insertedBlocks))
		}

		for _, change := range req.ChangeBatch {
			if change.Direction == remoteproto.Direction_UNWIND {
				continue
			}
			for lastSeenBlock <= change.BlockHeight {
				delete(insertedBlocks, lastSeenBlock)
				lastSeenBlock++
			}
		}
	}

	return nil
}

func (emt *ExecModuleTester) InsertChain(chain *blockgen.ChainPack) error {
	if err := emt.insertPoSBlocks(chain); err != nil {
		return err
	}
	roTx, err := emt.DB.BeginRo(emt.Ctx)
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
	if rawdb.ReadHeadBlockHash(roTx) != chain.TopBlock.Hash() {
		return fmt.Errorf("did not import block %d %x", chain.TopBlock.NumberU64(), chain.TopBlock.Hash())
	}
	if emt.sentriesClient.Hd.IsBadHeader(chain.TopBlock.Hash()) {
		return fmt.Errorf("block %d %x was invalid", chain.TopBlock.NumberU64(), chain.TopBlock.Hash())
	}
	return nil
}

func (emt *ExecModuleTester) HeaderDownload() *headerdownload.HeaderDownload {
	return emt.sentriesClient.Hd
}

func (emt *ExecModuleTester) NewHistoryStateReader(blockNum uint64, tx kv.TemporalTx) state.StateReader {
	r, err := rpchelper.CreateHistoryStateReader(context.Background(), tx, blockNum, 0, emt.BlockReader.TxnumReader())
	if err != nil {
		panic(err)
	}
	return r
}

func (emt *ExecModuleTester) NewStateReader(tx kv.TemporalGetter) state.StateReader {
	return state.NewReaderV3(tx)
}

func (emt *ExecModuleTester) BlocksIO() (services.FullBlockReader, *blockio.BlockWriter) {
	return emt.BlockReader, blockio.NewBlockWriter()
}

func (emt *ExecModuleTester) Current(tx kv.Tx) *types.Block {
	if tx != nil {
		b, err := emt.BlockReader.CurrentBlock(tx)
		if err != nil {
			panic(err)
		}
		return b
	}
	tx, err := emt.DB.BeginRo(context.Background())
	if err != nil {
		panic(err)
	}
	defer tx.Rollback()
	b, err := emt.BlockReader.CurrentBlock(tx)
	if err != nil {
		panic(err)
	}
	return b
}
