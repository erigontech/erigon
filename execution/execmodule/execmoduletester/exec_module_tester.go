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
	"bytes"
	"context"
	"crypto/ecdsa"
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
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
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
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/protocol/rules/merge"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	debugtracer "github.com/erigontech/erigon/execution/tracing/tracers/debug"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/components/storage"
	snapshotpkg "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/downloaderproto"
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
	Ctx            context.Context
	Log            log.Logger
	tb             testing.TB
	cancel         context.CancelFunc
	DB             kv.TemporalRwDB
	Dirs           datadir.Dirs
	Engine         rules.Engine
	ChainConfig    *chain.Config
	Sync           *stagedsync.Sync
	MiningSync     *stagedsync.Sync
	PendingBlocks  chan *types.Block
	MinedBlocks    chan *types.BlockWithReceipts
	sentriesClient *sentry_multi_client.MultiClient
	Key            *ecdsa.PrivateKey
	Genesis        *types.Block
	SentryClient   direct.SentryClient
	PeerId         *typesproto.H512
	streams        map[sentryproto.MessageId][]sentryproto.Sentry_MessagesServer
	sentMessagesMu sync.Mutex
	sentMessages   []*sentryproto.OutboundMessageData
	StreamWg       sync.WaitGroup
	ReceiveWg      sync.WaitGroup
	Address        common.Address
	ForkValidator  *execmodule.ForkValidator
	ExecModule     *execmodule.ExecModule
	StateCache     *execmodule.Cache

	// adminUnwindInventory is the *snapshot.Inventory pointer the
	// admin-unwind Provider observes. Populated lazily — empty at
	// construction time, repopulated by RescanAdminUnwindInventory()
	// after blocks execute and the aggregator writes files.
	adminUnwindInventory *snapshotpkg.Inventory
	adminUnwindSnapDir   string

	// AdminUnwindProvider is the storage.Provider wired as the
	// admin-unwind Unwinder when WithAdminUnwindWired() is set. nil
	// otherwise — the default tester has no Provider and SetHead
	// rejects past-diffset targets with the legacy error.
	AdminUnwindProvider *storage.Provider
	retirementStart     chan bool
	retirementDone      chan struct{}
	retirementWg        sync.WaitGroup

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
	if err := emt.bgComponentsEg.Wait(); err != nil && emt.tb != nil {
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
	if emt.tb == nil && emt.Dirs.DataDir != "" {
		dir.RemoveAll(emt.Dirs.DataDir)
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
	emt.sentMessagesMu.Lock()
	emt.sentMessages = append(emt.sentMessages, r.Data)
	emt.sentMessagesMu.Unlock()
	return nil, nil
}
func (emt *ExecModuleTester) SendMessageById(_ context.Context, r *sentryproto.SendMessageByIdRequest) (*sentryproto.SentPeers, error) {
	emt.sentMessagesMu.Lock()
	emt.sentMessages = append(emt.sentMessages, r.Data)
	emt.sentMessagesMu.Unlock()
	return nil, nil
}
func (emt *ExecModuleTester) SendMessageToRandomPeers(_ context.Context, r *sentryproto.SendMessageToRandomPeersRequest) (*sentryproto.SentPeers, error) {
	emt.sentMessagesMu.Lock()
	emt.sentMessages = append(emt.sentMessages, r.Data)
	emt.sentMessagesMu.Unlock()
	return nil, nil
}
func (emt *ExecModuleTester) SendMessageToAll(_ context.Context, r *sentryproto.OutboundMessageData) (*sentryproto.SentPeers, error) {
	emt.sentMessagesMu.Lock()
	emt.sentMessages = append(emt.sentMessages, r)
	emt.sentMessagesMu.Unlock()
	return nil, nil
}
func (emt *ExecModuleTester) SentMessage(i int) (*sentryproto.OutboundMessageData, error) {
	emt.sentMessagesMu.Lock()
	defer emt.sentMessagesMu.Unlock()
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

// WithoutExperimentalBAL disables experimental BAL (and the parallel executor
// it forces) for tests that exercise patterns the parallel executor doesn't
// yet handle correctly (e.g. intra-block SELFDESTRUCT + CREATE2 reincarnation).
func WithoutExperimentalBAL() Option {
	return func(opts *options) {
		opts.experimentalBAL = false
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

func WithTxPool() Option {
	return func(opts *options) {
		opts.withTxPool = true
	}
}

func WithEnableDomain(domain kv.Domain) Option {
	return func(opts *options) {
		opts.enableDomains = append(opts.enableDomains, domain)
	}
}

func WithChainConfig(cfg *chain.Config) Option {
	return func(opts *options) {
		opts.chainConfig = cfg
	}
}

func WithFcuBackgroundCommit() Option {
	return func(opts *options) {
		opts.fcuBackgroundCommit = true
	}
}

// WithAlwaysGenerateChangesets pins --experimental.always-generate-changesets
// regardless of the tester default: true for tests that reorg deeper than
// MaxReorgDepth, false for tests that rely on the windowed-changesets
// production behaviour.
func WithAlwaysGenerateChangesets(v bool) Option {
	return func(opts *options) {
		opts.alwaysGenerateChangesets = &v
	}
}

func WithFcuBackgroundPrune() Option {
	return func(opts *options) {
		opts.fcuBackgroundPrune = true
	}
}

// WithAdminUnwindWired enables the admin SetHead mode-B path: the
// tester constructs a minimal storage.Provider whose BlockAligned()
// returns true and whose Unwind implements the snapshot-trim +
// writable-shadow wipe + commitment verification chain. The Provider
// is bridged into the ExecModule's Unwinder slot via an inline
// adapter (mirroring node/eth's providerUnwinderAdapter).
//
// Without this option (default), the tester leaves the Unwinder slot
// nil — mode B is unreachable and any target past
// CanUnwindToBlockNum is rejected with the legacy error.
func WithAdminUnwindWired() Option {
	return func(opts *options) {
		opts.adminUnwindWired = true
	}
}

type options struct {
	stepSize                 *uint64
	experimentalBAL          bool
	genesis                  *types.Genesis
	chainConfig              *chain.Config
	key                      *ecdsa.PrivateKey
	engine                   rules.Engine
	pruneMode                *prune.Mode
	withTxPool               bool
	enableDomains            []kv.Domain
	fcuBackgroundCommit      bool
	fcuBackgroundPrune       bool
	adminUnwindWired         bool
	alwaysGenerateChangesets *bool
}

func applyOptions(opts []Option) options {
	defaultKey, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	defaultPruneMode := prune.MockMode
	opt := options{
		key:             defaultKey,
		pruneMode:       &defaultPruneMode,
		chainConfig:     chain.TestChainBerlinConfig,
		experimentalBAL: false,
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
		case opt.genesis.Config.TerminalTotalDifficultyPassed:
			opt.engine = merge.NewFaker(ethash.NewFaker())
		default:
			opt.engine = ethash.NewFaker()
		}
	}
	return opt
}

// New creates an ExecModuleTester. When called with no options, it uses
// sensible defaults (TestChainBerlinConfig, 1 Ether alloc, ethash.NewFaker, etc.).
// Use With* options to customise.
func New(tb testing.TB, opts ...Option) *ExecModuleTester {
	opt := applyOptions(opts)

	gspec := opt.genesis
	key := opt.key
	engine := opt.engine
	pruneMode := *opt.pruneMode
	withTxPool := opt.withTxPool
	tmpdir, err := os.MkdirTemp("", "mock-sentry-*")
	if err != nil {
		panic(err)
	}
	if tb != nil {
		// we can't use tb.TempDir() here because some tests produce names long
		// enough to cause 'file name too long' errors when reused as paths
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
	if opt.alwaysGenerateChangesets != nil {
		cfg.AlwaysGenerateChangesets = *opt.alwaysGenerateChangesets
	}
	cfg.PersistReceiptsCacheV2 = true
	cfg.ChaosMonkey = false
	cfg.Snapshot.ChainName = gspec.Config.ChainName
	cfg.Genesis = gspec
	cfg.Prune = pruneMode
	cfg.ExperimentalBAL = opt.experimentalBAL
	cfg.FcuBackgroundPrune = opt.fcuBackgroundPrune
	cfg.FcuBackgroundCommit = opt.fcuBackgroundCommit

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

	// Enable domains before any background goroutines start (e.g. InsertChain
	// spawns a pipeline that calls agg.OpenFolder concurrently).
	if len(opt.enableDomains) > 0 {
		agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
		for _, domain := range opt.enableDomains {
			agg.EnableDomain(domain)
		}
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
	_, mock.Genesis, err = genesiswrite.CommitGenesisBlock(mock.DB, gspec, "", datadir.New(tmpdir), mock.Log)
	if _, ok := err.(*chain.ConfigCompatError); err != nil && !ok {
		if tb != nil {
			tb.Fatal(err)
		} else {
			panic(err)
		}
	}

	// Deploy Prague system contracts (EIP-7002, EIP-7251) when Prague is active.
	// These are required for the Merge engine's FinalizeAndAssemble to process
	// withdrawal and consolidation requests.
	if gspec.Config.IsPrague(0) {
		if err := blockgen.InitPraguePreDeploys(mock.DB, gspec.Config, mock.Log); err != nil {
			if tb != nil {
				tb.Fatal(err)
			} else {
				panic(err)
			}
		}
	}

	blockWriter := blockio.NewBlockWriter()

	mock.Address = crypto.PubkeyToAddress(mock.Key.PublicKey)

	mock.SentryClient, err = direct.NewSentryClientDirect(direct.ETH68, mock, nil)
	require.NoError(tb, err)
	sentries := []sentryproto.SentryClient{mock.SentryClient}

	if !cfg.TxPool.Disable {
		poolCfg := txpoolcfg.DefaultConfig
		stateChangesClient := direct.NewStateDiffClientDirect(erigonGrpcServer)
		mock.TxPool, mock.TxPoolGrpcServer, err = txpool.Assemble(
			ctx,
			poolCfg,
			mock.DB,
			kvcache.NewSimple(),
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
	// ForkValidator is created after pipelineStagedSync and PipelineExecutor are ready (below).

	statusDataProvider := sentry.NewStatusDataProvider(
		db,
		mock.ChainConfig,
		mock.Genesis,
		mock.ChainConfig.ChainID.Uint64(),
		logger,
		mock.BlockReader,
	)

	mock.sentriesClient, err = sentry_multi_client.NewMultiClient(
		mock.Dirs,
		mock.DB,
		mock.ChainConfig,
		mock.Engine,
		sentries,
		mock.BlockReader,
		statusDataProvider,
		false,
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

	snapDownloader := mockDownloader(ctrl, mock.Dirs.Snap)

	miningCancel := make(chan struct{})
	go func() {
		<-mock.Ctx.Done()
		close(miningCancel)
	}()

	readAheader := exec.NewBlockReadAheader()
	blkBuilder := builder.NewBuilder(
		mock.Ctx,
		mock.DB,
		&cfg.Builder,
		mock.ChainConfig,
		mock.Engine,
		mock.BlockReader,
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
			gspec,
			cfg.Sync,
			false, /*experimentalBAL*/
			readAheader,
		),
		nil, /*notifier*/
		&vm.Config{},
		dirs.Tmp,
		mock.TxPool,
		miningCancel,
		latestBlockBuiltStore,
		nil, /*sdProvider*/
		logger,
	)

	blockRetire := freezeblocks.NewBlockRetire(1, dirs, mock.BlockReader, blockWriter, mock.DB, nil, nil, mock.ChainConfig, &cfg, mock.Notifications.Events, nil, logger)
	mock.Sync = stagedsync.New(
		cfg.Sync,
		stagedsync.DefaultStages(
			mock.Ctx,
			stagedsync.StageSnapshotsCfg(mock.DB, mock.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, mock.BlockReader, mock.Notifications, false, false, false, pruneMode, nil, nil),
			stagedsync.StageHeadersCfg(mock.BlockReader),
			stagedsync.StageBlockHashesCfg(mock.Dirs.Tmp, blockWriter),
			stagedsync.StageBodiesCfg(mock.BlockReader, blockWriter),
			stagedsync.StageSendersCfg(mock.ChainConfig, cfg.Sync, false /* badBlockHalt */, dirs.Tmp, pruneMode, mock.BlockReader, readAheader),
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
				gspec,
				cfg.Sync,
				false, /*experimentalBAL*/
				readAheader,
			),
			stagedsync.StageTxLookupCfg(pruneMode, dirs.Tmp, mock.BlockReader),
			stagedsync.StageFinishCfg(),
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
	pipelineStages := stageloop.NewPipelineStages(mock.Ctx, db, &cfg, mock.sentriesClient, mock.Notifications, snapDownloader, mock.BlockReader, blockRetire, tracer, nil, readAheader)
	mock.posStagedSync = stagedsync.New(cfg.Sync, pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, logger, stages.ModeApplyingBlocks)

	// Create validation Sync and PipelineExecutor.
	validationNotifications := shards.NewNotifications(nil)
	validationSync := stageloop.NewInMemoryExecution(mock.Ctx, mock.DB, &cfg, mock.sentriesClient,
		validationNotifications, mock.BlockReader, blockWriter, logger, readAheader)
	dispatcher := execmodule.NewDispatcher(mock.ChainConfig, mock.Notifications.Events, mock.Notifications.StateChangesConsumer, logger)
	pipelineExecutor := execmodule.NewPipelineExecutor(mock.posStagedSync, mock.DB, mock.BlockReader, mock.ChainConfig, mock.Engine, validationSync, validationNotifications, dispatcher, logger)

	hook := stageloop.NewHook(mock.Ctx, mock.Notifications, mock.posStagedSync, mock.ChainConfig, logger, dispatcher, nil, nil, nil)

	mock.StateCache = &execmodule.Cache{}
	onlySnapDownloadOnStart := cfg.Genesis.Config.Bor != nil

	accum := &execmodule.Accumulation{
		Accumulator:    mock.Notifications.Accumulator,
		RecentReceipts: mock.Notifications.RecentReceipts,
	}

	// AdminUnwind wiring (default disabled). When WithAdminUnwindWired()
	// is set, construct a minimal storage.Provider via
	// NewProviderForUnwindTest and an inline adapter satisfying
	// execmodule.Unwinder. Mirrors node/eth's providerUnwinderAdapter
	// — kept inline so the tester doesn't pull in node/eth (heavy + would
	// be wrong-direction layering).
	var unwinder execmodule.Unwinder
	if opt.adminUnwindWired {
		agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
		// Snapshot files the test aggregator builds during execution
		// are real on-disk artefacts, but the Inventory is empty at
		// tester-construction time (blocks haven't executed yet).
		// RescanAdminUnwindInventory() (called by the test after
		// InsertChain) populates the shared *snapshotpkg.Inventory by
		// scanning the snap dir; the Provider observes the same pointer.
		mock.adminUnwindInventory = snapshotpkg.NewInventory()
		mock.adminUnwindSnapDir = dirs.Snap
		mock.AdminUnwindProvider = storage.NewProviderForUnwindTest(storage.UnwindTestDeps{
			ChainDB:     mock.DB,
			BlockReader: br,
			Aggregator:  agg,
			ChainConfig: mock.ChainConfig,
			Inventory:   mock.adminUnwindInventory,
			SnapDir:     dirs.Snap,
			SnapTmpDir:  dirs.Tmp,
			Logger:      logger,
		})
		unwinder = testProviderUnwinder{p: mock.AdminUnwindProvider}
	}

	mock.ExecModule = execmodule.NewExecModule(
		ctx,
		mock.BlockReader,
		mock.DB,
		pipelineExecutor,
		1, // currentBlockNumber
		mock.ChainConfig,
		blkBuilder.Build,
		hook,
		accum,
		mock.StateCache,
		logger,
		engine,
		cfg.Sync,
		cfg.FcuBackgroundPrune,
		cfg.FcuBackgroundCommit,
		onlySnapDownloadOnStart,
		readAheader,
		func() error { return nil },
		unwinder,
	)
	mock.ForkValidator = mock.ExecModule.ForkValidator()

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
	wr := chainreader.NewChainReaderEth1(emt.ChainConfig, emt.ExecModule, time.Hour)

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

	balMap := make(map[common.Hash][]byte)
	for i, bal := range chain.BlockAccessLists {
		if len(bal) > 0 {
			block := chain.Blocks[i]
			balMap[block.Hash()] = bal
		}
	}
	if err := wr.InsertBlocksAndWaitWithAccessLists(emt.Ctx, chain.Blocks, balMap); err != nil {
		return err
	}

	tipHash := chain.TopBlock.Hash()

	status, verr, _, err := wr.UpdateForkChoice(emt.Ctx, tipHash, tipHash, tipHash)
	if err != nil {
		return err
	}

	if status != execmodule.ExecutionStatusSuccess {
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
	return nil
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

// TruncateChangeSetsBelow deletes ChangeSets3 entries whose block-number
// prefix is < blockNum. Used by admin SetHead mode-B tests to lift
// rawtemporaldb.CanUnwindToBlockNum past a chosen target so the dispatch
// in execmodule.SetHead routes to setHeadModeB instead of the legacy
// "minimum unwindable block" rejection.
//
// CanUnwindToBlockNum derives the floor from the lowest blockNum in
// ChangeSets3 (via changeset.ReadLowestUnwindableBlock, with minor
// off-by-one adjustment). Truncating below N makes the floor N-1; a
// SetHead target < N-1 then routes through mode B.
//
// Test helper only. Production diffset retention is managed by the
// execution-component prune lifecycle.
// RescanAdminUnwindInventory rescans the snap dir and adds every
// state-domain and block snapshot primary file to the Inventory the
// admin-unwind Provider observes. Required between block execution
// (which produces files) and SetHead (which iterates the Inventory
// for snapshot-trim) — without it, mode B's snapshot-trim sub-op
// remains a no-op even when over-step files exist on disk.
//
// No-op when WithAdminUnwindWired() was not set. Safe to call
// multiple times: AddFile is idempotent for already-tracked names.
func (emt *ExecModuleTester) RescanAdminUnwindInventory(tb testing.TB) {
	tb.Helper()
	if emt.adminUnwindInventory == nil {
		return
	}
	// Quiesce the aggregator before snapshotting the on-disk layout into the
	// inventory. Background build+merge that runs after this scan would delete
	// boundary-step files (e.g. merging v2.0-storage.1-2.kv into a wider file),
	// leaving the inventory pointing at files the mode-B regen path can no
	// longer open. Admin SetHead is a deliberate stop-the-world op, so waiting
	// for the aggregator to settle here mirrors production.
	if agg, ok := emt.DB.(dbstate.HasAgg); ok {
		if a, ok := agg.Agg().(*dbstate.Aggregator); ok {
			<-a.WaitForBuildAndMerge(emt.Ctx)
		}
	}
	freshly := storage.BuildInventoryFromSnapDirForTest(emt.adminUnwindSnapDir)
	for _, f := range freshly.AllLocalFiles() {
		_ = emt.adminUnwindInventory.AddFile(f)
	}
}

func (emt *ExecModuleTester) TruncateChangeSetsBelow(tb testing.TB, blockNum uint64) {
	tb.Helper()
	rwTx, err := emt.DB.BeginRw(emt.Ctx)
	require.NoError(tb, err)
	defer rwTx.Rollback()

	c, err := rwTx.RwCursor(kv.ChangeSets3)
	require.NoError(tb, err)
	defer c.Close()

	upper := dbutils.EncodeBlockNumber(blockNum)
	for k, _, err := c.First(); k != nil && bytes.Compare(k, upper) < 0; k, _, err = c.Next() {
		require.NoError(tb, err)
		require.NoError(tb, c.DeleteCurrent())
	}
	require.NoError(tb, rwTx.Commit())
}

// testProviderUnwinder bridges execmodule.Unwinder to *storage.Provider
// inside the tester. Mirrors node/eth's providerUnwinderAdapter
// field-for-field; duplicated here to avoid importing node/eth (which
// would be heavyweight and wrong-direction layering for a test helper).
type testProviderUnwinder struct {
	p *storage.Provider
}

func (a testProviderUnwinder) BlockAligned() bool { return a.p.BlockAligned() }

func (a testProviderUnwinder) Unwind(ctx context.Context, toBlock uint64, args execmodule.UnwindArgs) error {
	return a.p.Unwind(ctx, toBlock, storage.UnwindOpts{Tx: args.Tx, Engine: args.Engine})
}

func (a testProviderUnwinder) FinalizeUnwind() error { return a.p.FinalizeUnwind() }

func (a testProviderUnwinder) AbortUnwind() { a.p.AbortUnwind() }
