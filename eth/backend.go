// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

// Package eth implements the Ethereum protocol.
package eth

import (
	"context"
	"errors"
	"fmt"
	"google.golang.org/protobuf/types/known/emptypb"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/etl"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	txpool2 "github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon-lib/txpool/txpooluitl"
	types2 "github.com/ledgerwatch/erigon-lib/types"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloader"
	"github.com/ledgerwatch/erigon/cmd/downloader/downloadergrpc"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/parlia"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethutils"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/ethstats"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snapshotsynccli"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/log/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config contains the configuration options of the ETH protocol.
// Deprecated: use ethconfig.Config instead.
type Config = ethconfig.Config

// Ethereum implements the Ethereum full node service.
type Ethereum struct {
	config *ethconfig.Config
	log    log.Logger

	// DB interfaces
	chainDB    kv.RwDB
	privateAPI *grpc.Server

	engine consensus.Engine

	gasPrice  *uint256.Int
	etherbase common.Address

	networkID uint64

	lock              sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	chainConfig       *params.ChainConfig
	genesisHash       common.Hash
	quitMining        chan struct{}
	miningSealingQuit chan struct{}
	pendingBlocks     chan *types.Block
	minedBlocks       chan *types.Block

	// downloader fields
	sentryCtx           context.Context
	sentryCancel        context.CancelFunc
	sentryControlServer *sentry.ControlServerImpl
	sentryServers       []*sentry.SentryServerImpl
	sentries            []direct.SentryClient

	stagedSync *stagedsync.Sync

	downloaderClient proto_downloader.DownloaderClient

	notifications *stagedsync.Notifications

	waitForStageLoopStop chan struct{}
	waitForMiningStop    chan struct{}

	txPool2DB               kv.RwDB
	txPool2                 *txpool2.TxPool
	newTxs2                 chan types2.Hashes
	txPool2Fetch            *txpool2.Fetch
	txPool2Send             *txpool2.Send
	txPool2GrpcServer       txpool_proto.TxpoolServer
	notifyMiningAboutNewTxs chan struct{}

	downloadProtocols *downloader.Protocols
}

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(stack *node.Node, config *ethconfig.Config, txpoolCfg txpool2.Config, logger log.Logger) (*Ethereum, error) {
	if config.Miner.GasPrice == nil || config.Miner.GasPrice.Cmp(common.Big0) <= 0 {
		log.Warn("Sanitizing invalid miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.Defaults.Miner.GasPrice)
		config.Miner.GasPrice = new(big.Int).Set(ethconfig.Defaults.Miner.GasPrice)
	}

	tmpdir := filepath.Join(stack.Config().DataDir, etl.TmpDirName)
	if err := os.RemoveAll(tmpdir); err != nil { // clean it on startup
		return nil, fmt.Errorf("clean tmp dir: %s, %w", tmpdir, err)
	}

	// Assemble the Ethereum object
	chainKv, err := node.OpenDatabase(stack.Config(), logger, kv.ChainDB)
	if err != nil {
		return nil, err
	}

	// Check if we have an already initialized chain and fall back to
	// that if so. Otherwise we need to generate a new genesis spec.
	if err := chainKv.View(context.Background(), func(tx kv.Tx) error {
		h, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			panic(err)
		}
		if h != (common.Hash{}) {
			config.Genesis = nil // fallback to db content
		}
		return nil
	}); err != nil {
		panic(err)
	}

	chainConfig, genesis, genesisErr := core.CommitGenesisBlock(chainKv, config.Genesis)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}

	config.SyncMode = ethconfig.SyncModeByChainName(chainConfig.ChainName, config.SyncModeCli)
	config.Snapshot.Enabled = config.SyncMode == ethconfig.SnapSync

	types.SetHeaderSealFlag(chainConfig.IsHeaderWithSeal())
	log.Info("Initialised chain configuration", "config", chainConfig, "genesis", genesis.Hash())

	// Apply special hacks for BSC params
	if chainConfig.Parlia != nil {
		params.ApplyBinanceSmartChainParams()
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	// kv_remote architecture does blocks on stream.Send - means current architecture require unlimited amount of txs to provide good throughput
	//limiter := make(chan struct{}, kv.ReadersLimit)
	kvRPC := remotedbserver.NewKvServer(ctx, chainKv) // mdbx.NewMDBX(logger).RoTxsLimiter(limiter).Readonly().Path(filepath.Join(stack.Config().DataDir, "chaindata")).Label(kv.ChainDB).MustOpen())
	backend := &Ethereum{
		sentryCtx:            ctx,
		sentryCancel:         ctxCancel,
		config:               config,
		log:                  logger,
		chainDB:              chainKv,
		networkID:            config.NetworkID,
		etherbase:            config.Miner.Etherbase,
		chainConfig:          chainConfig,
		genesisHash:          genesis.Hash(),
		waitForStageLoopStop: make(chan struct{}),
		waitForMiningStop:    make(chan struct{}),
		sentries:             []direct.SentryClient{},
		notifications: &stagedsync.Notifications{
			Events:               privateapi.NewEvents(),
			Accumulator:          shards.NewAccumulator(chainConfig),
			StateChangesConsumer: kvRPC,
		},
	}
	backend.gasPrice, _ = uint256.FromBig(config.Miner.GasPrice)

	var consensusConfig interface{}

	if chainConfig.Clique != nil {
		consensusConfig = &config.Clique
	} else if chainConfig.Aura != nil {
		config.Aura.Etherbase = config.Miner.Etherbase
		consensusConfig = &config.Aura
	} else if chainConfig.Parlia != nil {
		consensusConfig = &config.Parlia
	} else if chainConfig.Bor != nil {
		consensusConfig = &config.Bor
	} else {
		consensusConfig = &config.Ethash
	}

	backend.engine = ethconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify, config.HeimdallURL, config.WithoutHeimdall, stack.DataDir())

	log.Info("Initialising Ethereum protocol", "network", config.NetworkID)

	if err := chainKv.Update(context.Background(), func(tx kv.RwTx) error {
		if err = stagedsync.UpdateMetrics(tx); err != nil {
			return err
		}

		config.Prune, err = prune.EnsureNotChanged(tx, config.Prune)
		if err != nil {
			return err
		}
		if err := snapshotsynccli.EnsureNotChanged(tx, config.Snapshot); err != nil {
			return err
		}
		log.Info("Effective", "prune_flags", config.Prune.String(), "snapshot_flags", config.Snapshot.String())

		return nil
	}); err != nil {
		return nil, err
	}

	if config.TxPool.Journal != "" {
		config.TxPool.Journal = stack.ResolvePath(config.TxPool.Journal)
	}

	if len(stack.Config().P2P.SentryAddr) > 0 {
		for _, addr := range stack.Config().P2P.SentryAddr {
			sentryClient, err := sentry.GrpcClient(backend.sentryCtx, addr)
			if err != nil {
				return nil, err
			}
			backend.sentries = append(backend.sentries, sentryClient)
		}
	} else {
		var readNodeInfo = func() *eth.NodeInfo {
			var res *eth.NodeInfo
			_ = backend.chainDB.View(context.Background(), func(tx kv.Tx) error {
				res = eth.ReadNodeInfo(tx, backend.chainConfig, backend.genesisHash, backend.networkID)
				return nil
			})

			return res
		}

		d66, err := setupDiscovery(backend.config.EthDiscoveryURLs)
		if err != nil {
			return nil, err
		}

		cfg66 := stack.Config().P2P
		cfg66.NodeDatabase = filepath.Join(stack.Config().DataDir, "nodes", "eth66")
		server66 := sentry.NewSentryServer(backend.sentryCtx, d66, readNodeInfo, &cfg66, eth.ETH66)
		backend.sentryServers = append(backend.sentryServers, server66)
		backend.sentries = []direct.SentryClient{direct.NewSentryClientDirect(eth.ETH66, server66)}

		go func() {
			logEvery := time.NewTicker(120 * time.Second)
			defer logEvery.Stop()

			var logItems []interface{}

			for {
				select {
				case <-backend.sentryCtx.Done():
					return
				case <-logEvery.C:
					logItems = logItems[:0]
					for _, srv := range backend.sentryServers {
						logItems = append(logItems, eth.ProtocolToString[srv.Protocol.Version], strconv.Itoa(srv.SimplePeerCount()))
					}
					log.Info("[p2p] GoodPeers", logItems...)
				}
			}
		}()
	}

	var blockReader interfaces.FullBlockReader
	var allSnapshots *snapshotsync.RoSnapshots
	if config.Snapshot.Enabled {
		allSnapshots = snapshotsync.NewRoSnapshots(config.Snapshot, config.SnapshotDir.Path)
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)

		if len(stack.Config().DownloaderAddr) > 0 {
			// connect to external Downloader
			backend.downloaderClient, err = downloadergrpc.NewClient(ctx, stack.Config().DownloaderAddr)
		} else {
			// start embedded Downloader
			backend.downloadProtocols, err = downloader.New(config.Torrent, config.SnapshotDir)
			if err != nil {
				return nil, err
			}
			if err = downloader.CreateTorrentFilesAndAdd(ctx, config.SnapshotDir, backend.downloadProtocols.TorrentClient); err != nil {
				return nil, fmt.Errorf("CreateTorrentFilesAndAdd: %w", err)
			}
			bittorrentServer, err := downloader.NewGrpcServer(backend.downloadProtocols.DB, backend.downloadProtocols, config.SnapshotDir, false)
			if err != nil {
				return nil, fmt.Errorf("new server: %w", err)
			}

			backend.downloaderClient = direct.NewDownloaderClient(bittorrentServer)
		}
		if err != nil {
			return nil, err
		}
	} else {
		blockReader = snapshotsync.NewBlockReader()
	}

	backend.sentryControlServer, err = sentry.NewControlServer(chainKv, stack.Config().NodeName(), chainConfig, genesis.Hash(), backend.engine, backend.config.NetworkID, backend.sentries, config.BlockDownloaderWindow, blockReader)
	if err != nil {
		return nil, err
	}
	config.BodyDownloadTimeoutSeconds = 30

	var miningRPC txpool_proto.MiningServer
	if config.TxPool.Disable {
		backend.txPool2GrpcServer = &txpool2.GrpcDisabled{}
	} else {
		//cacheConfig := kvcache.DefaultCoherentCacheConfig
		//cacheConfig.MetricsLabel = "txpool"

		stateDiffClient := direct.NewStateDiffClientDirect(kvRPC)
		backend.newTxs2 = make(chan types2.Hashes, 1024)
		//defer close(newTxs)
		backend.txPool2DB, backend.txPool2, backend.txPool2Fetch, backend.txPool2Send, backend.txPool2GrpcServer, err = txpooluitl.AllComponents(
			ctx, txpoolCfg, kvcache.NewDummy(), backend.newTxs2, backend.chainDB, backend.sentries, stateDiffClient,
		)
		if err != nil {
			return nil, err
		}
	}

	backend.notifyMiningAboutNewTxs = make(chan struct{}, 1)
	backend.quitMining = make(chan struct{})
	backend.miningSealingQuit = make(chan struct{})
	backend.pendingBlocks = make(chan *types.Block, 1)
	backend.minedBlocks = make(chan *types.Block, 1)

	miner := stagedsync.NewMiningState(&config.Miner)
	backend.pendingBlocks = miner.PendingResultCh
	backend.minedBlocks = miner.MiningResultCh

	// proof-of-work mining
	mining := stagedsync.New(
		stagedsync.MiningStages(backend.sentryCtx,
			stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miner, *backend.chainConfig, backend.engine, backend.txPool2, backend.txPool2DB, nil, tmpdir),
			stagedsync.StageMiningExecCfg(backend.chainDB, miner, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir),
			stagedsync.StageHashStateCfg(backend.chainDB, tmpdir),
			stagedsync.StageTrieCfg(backend.chainDB, false, true, tmpdir, blockReader),
			stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miner, backend.miningSealingQuit),
		), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder)

	var ethashApi *ethash.API
	if casted, ok := backend.engine.(*ethash.Ethash); ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}
	// proof-of-stake mining
	assembleBlockPOS := func(param *core.BlockProposerParametersPOS) (*types.Block, error) {
		miningStatePos := stagedsync.NewMiningState(&config.Miner)
		miningStatePos.MiningConfig.Etherbase = param.SuggestedFeeRecipient
		proposingSync := stagedsync.New(
			stagedsync.MiningStages(backend.sentryCtx,
				stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miningStatePos, *backend.chainConfig, backend.engine, backend.txPool2, backend.txPool2DB, param, tmpdir),
				stagedsync.StageMiningExecCfg(backend.chainDB, miningStatePos, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir),
				stagedsync.StageHashStateCfg(backend.chainDB, tmpdir),
				stagedsync.StageTrieCfg(backend.chainDB, false, true, tmpdir, blockReader),
				stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miningStatePos, backend.miningSealingQuit),
			), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder)
		// We start the mining step
		if err := stages2.MiningStep(ctx, backend.chainDB, proposingSync); err != nil {
			return nil, err
		}
		block := <-miningStatePos.MiningResultPOSCh
		return block, nil
	}

	// Initialize ethbackend
	ethBackendRPC := privateapi.NewEthBackendServer(ctx, backend, backend.chainDB, backend.notifications.Events,
		blockReader, chainConfig, backend.sentryControlServer.Hd.BeaconRequestList, backend.sentryControlServer.Hd.PayloadStatusCh,
		assembleBlockPOS, config.Miner.EnabledPOS)
	miningRPC = privateapi.NewMiningServer(ctx, backend, ethashApi)
	// If we enabled the proposer flag we initiates the block proposing thread
	if config.Miner.EnabledPOS && chainConfig.TerminalTotalDifficulty != nil {
		ethBackendRPC.StartProposer()
	}
	if stack.Config().PrivateApiAddr != "" {
		var creds credentials.TransportCredentials
		if stack.Config().TLSConnection {
			creds, err = grpcutil.TLS(stack.Config().TLSCACert, stack.Config().TLSCertFile, stack.Config().TLSKeyFile)
			if err != nil {
				return nil, err
			}
		}
		backend.privateAPI, err = privateapi.StartGrpc(
			kvRPC,
			ethBackendRPC,
			backend.txPool2GrpcServer,
			miningRPC,
			stack.Config().PrivateApiAddr,
			stack.Config().PrivateApiRateLimit,
			creds,
			stack.Config().HealthCheck)
		if err != nil {
			return nil, err
		}
	}

	if !config.TxPool.Disable {
		backend.txPool2Fetch.ConnectCore()
		backend.txPool2Fetch.ConnectSentries()
		var newTxsBroadcaster *txpool2.NewSlotsStreams
		if casted, ok := backend.txPool2GrpcServer.(*txpool2.GrpcServer); ok {
			newTxsBroadcaster = casted.NewSlotsStreams
		}
		go txpool2.MainLoop(backend.sentryCtx,
			backend.txPool2DB, backend.chainDB,
			backend.txPool2, backend.newTxs2, backend.txPool2Send, newTxsBroadcaster,
			func() {
				select {
				case backend.notifyMiningAboutNewTxs <- struct{}{}:
				default:
				}
			})
	}
	go func() {
		defer debug.LogPanic()
		for {
			select {
			case b := <-backend.minedBlocks:
				//p2p
				//backend.sentryControlServer.BroadcastNewBlock(context.Background(), b, b.Difficulty())
				//rpcdaemon
				if err := miningRPC.(*privateapi.MiningServer).BroadcastMinedBlock(b); err != nil {
					log.Error("txpool rpc mined block broadcast", "err", err)
				}
				if err := backend.sentryControlServer.Hd.AddMinedHeader(b.Header()); err != nil {
					log.Error("add mined block to header downloader", "err", err)
				}
				if err := backend.sentryControlServer.Bd.AddMinedBlock(b); err != nil {
					log.Error("add mined block to body downloader", "err", err)
				}

			case b := <-backend.pendingBlocks:
				if err := miningRPC.(*privateapi.MiningServer).BroadcastPendingBlock(b); err != nil {
					log.Error("txpool rpc pending block broadcast", "err", err)
				}
			case <-backend.quitMining:
				return
			}
		}
	}()

	if err := backend.StartMining(context.Background(), backend.chainDB, mining, backend.config.Miner, backend.gasPrice, backend.quitMining); err != nil {
		return nil, err
	}

	var headCh chan *types.Block
	if config.Ethstats != "" {
		headCh = make(chan *types.Block, 1)
	}

	backend.stagedSync, err = stages2.NewStagedSync(backend.sentryCtx, backend.log, backend.chainDB,
		stack.Config().P2P, *config, chainConfig.TerminalTotalDifficulty,
		backend.sentryControlServer, tmpdir, backend.notifications,
		backend.downloaderClient, allSnapshots, config.SnapshotDir, headCh)
	if err != nil {
		return nil, err
	}

	backend.sentryControlServer.Hd.StartPoSDownloader(backend.sentryCtx, backend.sentryControlServer.SendHeaderRequest, backend.sentryControlServer.Penalize)

	emptyBadHash := config.BadBlockHash == common.Hash{}
	if !emptyBadHash {
		var badBlockHeader *types.Header
		if err = chainKv.View(context.Background(), func(tx kv.Tx) error {
			header, hErr := rawdb.ReadHeaderByHash(tx, config.BadBlockHash)
			badBlockHeader = header
			return hErr
		}); err != nil {
			return nil, err
		}

		if badBlockHeader != nil {
			unwindPoint := badBlockHeader.Number.Uint64() - 1
			backend.stagedSync.UnwindTo(unwindPoint, config.BadBlockHash)
		}
	}

	//eth.APIBackend = &EthAPIBackend{stack.Config().ExtRPCEnabled(), stack.Config().AllowUnprotectedTxs, eth, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.Miner.GasPrice
	}
	//eth.APIBackend.gpo = gasprice.NewOracle(eth.APIBackend, gpoParams)
	if config.Ethstats != "" {
		if err := ethstats.New(stack, backend.sentryServers, chainKv, backend.engine, config.Ethstats, backend.networkID, ctx.Done(), headCh); err != nil {
			return nil, err
		}
	}
	// start HTTP API
	httpRpcCfg := stack.Config().Http
	if httpRpcCfg.Enabled {
		ethRpcClient, txPoolRpcClient, miningRpcClient, starkNetRpcClient, stateCache, ff, err := cli.EmbeddedServices(
			ctx, chainKv, httpRpcCfg.StateCache, blockReader,
			ethBackendRPC,
			backend.txPool2GrpcServer,
			miningRPC,
		)
		if err != nil {
			return nil, err
		}

		var borDb kv.RoDB
		if casted, ok := backend.engine.(*bor.Bor); ok {
			borDb = casted.DB
		}
		apiList := commands.APIList(chainKv, borDb, ethRpcClient, txPoolRpcClient, miningRpcClient, starkNetRpcClient, ff, stateCache, blockReader, httpRpcCfg)
		go func() {
			if err := cli.StartRpcServer(ctx, httpRpcCfg, apiList); err != nil {
				log.Error(err.Error())
				return
			}
		}()
	}

	// Register the backend on the node
	stack.RegisterAPIs(backend.APIs())
	stack.RegisterLifecycle(backend)
	return backend, nil
}

func (s *Ethereum) APIs() []rpc.API {
	return []rpc.API{}
}

func (s *Ethereum) Etherbase() (eb common.Address, err error) {
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()

	if etherbase != (common.Address{}) {
		return etherbase, nil
	}
	return common.Address{}, fmt.Errorf("etherbase must be explicitly specified")
}

// isLocalBlock checks whether the specified block is mined
// by local miner accounts.
//
// We regard two types of accounts as local miner account: etherbase
// and accounts specified via `txpool.locals` flag.
func (s *Ethereum) isLocalBlock(block *types.Block) bool { //nolint
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()
	return ethutils.IsLocalBlock(s.engine, etherbase, s.config.TxPool.Locals, block.Header())
}

// shouldPreserve checks whether we should preserve the given block
// during the chain reorg depending on whether the author of block
// is a local account.
func (s *Ethereum) shouldPreserve(block *types.Block) bool { //nolint
	// The reason we need to disable the self-reorg preserving for clique
	// is it can be probable to introduce a deadlock.
	//
	// e.g. If there are 7 available signers
	//
	// r1   A
	// r2     B
	// r3       C
	// r4         D
	// r5   A      [X] F G
	// r6    [X]
	//
	// In the round5, the inturn signer E is offline, so the worst case
	// is A, F and G sign the block of round5 and reject the block of opponents
	// and in the round6, the last available signer B is offline, the whole
	// network is stuck.
	if _, ok := s.engine.(*clique.Clique); ok {
		return false
	}
	return s.isLocalBlock(block)
}

// StartMining starts the miner with the given number of CPU threads. If mining
// is already running, this method adjust the number of threads allowed to use
// and updates the minimum price required by the transaction pool.
func (s *Ethereum) StartMining(ctx context.Context, db kv.RwDB, mining *stagedsync.Sync, cfg params.MiningConfig, gasPrice *uint256.Int, quitCh chan struct{}) error {
	if !cfg.Enabled {
		return nil
	}

	// Configure the local mining address
	eb, err := s.Etherbase()
	if err != nil {
		log.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %w", err)
	}
	if c, ok := s.engine.(*clique.Clique); ok {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		c.Authorize(eb, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(message), cfg.SigKey)
		})
	}
	if p, ok := s.engine.(*parlia.Parlia); ok {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		p.Authorize(eb, func(validator common.Address, payload []byte, chainId *big.Int) ([]byte, error) {
			return crypto.Sign(payload, cfg.SigKey)
		})
	}

	if s.chainConfig.ChainID.Uint64() > 10 {
		go func() {
			skipCycleEvery := time.NewTicker(4 * time.Second)
			defer skipCycleEvery.Stop()
			for range skipCycleEvery.C {
				select {
				case s.sentryControlServer.Hd.SkipCycleHack <- struct{}{}:
				default:
				}
			}
		}()
	}

	go func() {
		defer debug.LogPanic()
		defer close(s.waitForMiningStop)

		mineEvery := time.NewTicker(3 * time.Second)
		defer mineEvery.Stop()

		var works bool
		var hasWork bool
		errc := make(chan error, 1)

		tx, err := s.chainDB.BeginRo(ctx)
		if err != nil {
			log.Warn("mining", "err", err)
			return
		}

		for {
			mineEvery.Reset(3 * time.Second)
			select {
			case <-s.notifyMiningAboutNewTxs:
				hasWork = true
			case <-mineEvery.C:
				hasWork = true
			case err := <-errc:
				works = false
				hasWork = false
				if errors.Is(err, libcommon.ErrStopped) {
					return
				}
				if err != nil {
					log.Warn("mining", "err", err)
				}
			case <-quitCh:
				return
			}
			// Check if we transitioned and if we did halt POW mining
			headNumber, err := stages.GetStageProgress(tx, stages.Headers)
			if err != nil {
				log.Warn("mining", "err", err)
				return
			}

			isTrans, err := rawdb.Transitioned(tx, headNumber, s.chainConfig.TerminalTotalDifficulty)
			if err != nil {
				log.Warn("mining", "err", err)
				return
			}
			if isTrans {
				return
			}

			if !works && hasWork {
				works = true
				go func() { errc <- stages2.MiningStep(ctx, db, mining) }()
			}
		}
	}()

	return nil
}

func (s *Ethereum) IsMining() bool { return s.config.Miner.Enabled }

func (s *Ethereum) ChainKV() kv.RwDB            { return s.chainDB }
func (s *Ethereum) NetVersion() (uint64, error) { return s.networkID, nil }
func (s *Ethereum) NetPeerCount() (uint64, error) {
	var sentryPc uint64 = 0

	log.Trace("sentry", "peer count", sentryPc)
	for _, sc := range s.sentries {
		ctx := context.Background()
		reply, err := sc.PeerCount(ctx, &proto_sentry.PeerCountRequest{})
		if err != nil {
			log.Warn("sentry", "err", err)
			return 0, nil
		}
		sentryPc += reply.Count
	}

	return sentryPc, nil
}

func (s *Ethereum) NodesInfo(limit int) (*remote.NodesInfoReply, error) {
	if limit == 0 || limit > len(s.sentries) {
		limit = len(s.sentries)
	}

	nodes := make([]*prototypes.NodeInfoReply, 0, limit)
	for i := 0; i < limit; i++ {
		sc := s.sentries[i]

		nodeInfo, err := sc.NodeInfo(context.Background(), nil)
		if err != nil {
			log.Error("sentry nodeInfo", "err", err)
		}

		nodes = append(nodes, nodeInfo)
	}

	nodesInfo := &remote.NodesInfoReply{NodesInfo: nodes}
	sort.Sort(nodesInfo)

	return nodesInfo, nil
}

func (s *Ethereum) Peers(ctx context.Context) (*remote.PeersReply, error) {
	var reply remote.PeersReply
	for _, sentryClient := range s.sentries {
		peers, err := sentryClient.Peers(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, fmt.Errorf("Ethereum backend SentryClient.Peers error: %w", err)
		}
		reply.Peers = append(reply.Peers, peers.Peers...)
	}
	return &reply, nil
}

// Protocols returns all the currently configured
// network protocols to start.
func (s *Ethereum) Protocols() []p2p.Protocol {
	var protocols []p2p.Protocol
	for i := range s.sentryServers {
		protocols = append(protocols, s.sentryServers[i].Protocol)
	}
	return protocols
}

// Start implements node.Lifecycle, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *Ethereum) Start() error {
	for i := range s.sentries {
		go func(i int) {
			sentry.RecvMessageLoop(s.sentryCtx, s.sentries[i], s.sentryControlServer, nil)
		}(i)
		go func(i int) {
			sentry.RecvUploadMessageLoop(s.sentryCtx, s.sentries[i], s.sentryControlServer, nil)
		}(i)
		go func(i int) {
			sentry.RecvUploadHeadersMessageLoop(s.sentryCtx, s.sentries[i], s.sentryControlServer, nil)
		}(i)
	}
	time.Sleep(10 * time.Millisecond) // just to reduce logs order confusion

	go stages2.StageLoop(s.sentryCtx, s.chainDB, s.stagedSync, s.sentryControlServer.Hd, s.notifications, s.sentryControlServer.UpdateHead, s.waitForStageLoopStop, s.config.SyncLoopThrottle)

	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	// Stop all the peer-related stuff first.
	s.sentryCancel()
	if s.downloadProtocols != nil {
		s.downloadProtocols.Close()
	}
	if s.privateAPI != nil {
		shutdownDone := make(chan bool)
		go func() {
			defer close(shutdownDone)
			s.privateAPI.GracefulStop()
		}()
		select {
		case <-time.After(1 * time.Second): // shutdown deadline
			s.privateAPI.Stop()
		case <-shutdownDone:
		}
	}
	if s.quitMining != nil {
		close(s.quitMining)
	}

	//s.miner.Stop()
	s.engine.Close()
	<-s.waitForStageLoopStop
	if s.config.Miner.Enabled {
		<-s.waitForMiningStop
	}
	for _, sentryServer := range s.sentryServers {
		sentryServer.Close()
	}
	s.chainDB.Close()
	if s.txPool2DB != nil {
		s.txPool2DB.Close()
	}
	return nil
}

func (s *Ethereum) ChainDB() kv.RwDB {
	return s.chainDB
}

func (s *Ethereum) StagedSync() *stagedsync.Sync {
	return s.stagedSync
}

func (s *Ethereum) Notifications() *stagedsync.Notifications {
	return s.notifications
}

func (s *Ethereum) SentryCtx() context.Context {
	return s.sentryCtx
}

func (s *Ethereum) SentryControlServer() *sentry.ControlServerImpl {
	return s.sentryControlServer
}
