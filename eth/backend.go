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
	"io/fs"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/erigon-lib/chain/networkname"
	"github.com/ledgerwatch/erigon-lib/diagnostics"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadergrpc"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/persistence"
	"github.com/ledgerwatch/erigon/cl/persistence/format/snapshot_format/getters"
	clcore "github.com/ledgerwatch/erigon/cl/phase1/core"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/cl/sentinel"
	"github.com/ledgerwatch/erigon/cl/sentinel/service"

	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
	"github.com/ledgerwatch/erigon/turbo/builder"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_block_downloader"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1"
	"github.com/ledgerwatch/erigon/turbo/execution/eth1/eth1_chain_reader.go"
	"github.com/ledgerwatch/erigon/turbo/jsonrpc"
	"github.com/ledgerwatch/erigon/turbo/silkworm"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/direct"
	downloader3 "github.com/ledgerwatch/erigon-lib/downloader"
	"github.com/ledgerwatch/erigon-lib/downloader/downloadercfg"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	proto_sentry "github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	prototypes "github.com/ledgerwatch/erigon-lib/gointerfaces/types"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon-lib/txpool/txpooluitl"
	types2 "github.com/ledgerwatch/erigon-lib/types"

	"github.com/ledgerwatch/erigon/cmd/caplin/caplin1"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/common/debug"

	rpcsentinel "github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/finality/flags"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdallgrpc"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/merge"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/eth/ethutils"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/ethstats"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

// Config contains the configuration options of the ETH protocol.
// Deprecated: use ethconfig.Config instead.
type Config = ethconfig.Config

// Ethereum implements the Ethereum full node service.
type Ethereum struct {
	config *ethconfig.Config

	// DB interfaces
	chainDB    kv.RwDB
	privateAPI *grpc.Server

	engine consensus.Engine

	gasPrice  *uint256.Int
	etherbase libcommon.Address

	networkID uint64

	lock         sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	chainConfig  *chain.Config
	apiList      []rpc.API
	genesisBlock *types.Block
	genesisHash  libcommon.Hash

	eth1ExecutionServer *eth1.EthereumExecutionModule

	ethBackendRPC      *privateapi.EthBackendServer
	engineBackendRPC   *engineapi.EngineServer
	miningRPC          txpool_proto.MiningServer
	stateChangesClient txpool.StateChangesClient

	miningSealingQuit chan struct{}
	pendingBlocks     chan *types.Block
	minedBlocks       chan *types.Block

	// downloader fields
	sentryCtx      context.Context
	sentryCancel   context.CancelFunc
	sentriesClient *sentry_multi_client.MultiClient
	sentryServers  []*sentry.GrpcServer

	stagedSync         *stagedsync.Sync
	pipelineStagedSync *stagedsync.Sync
	syncStages         []*stagedsync.Stage
	syncUnwindOrder    stagedsync.UnwindOrder
	syncPruneOrder     stagedsync.PruneOrder

	downloaderClient proto_downloader.DownloaderClient

	notifications      *shards.Notifications
	unsubscribeEthstat func()

	waitForStageLoopStop chan struct{}
	waitForMiningStop    chan struct{}

	txPoolDB                kv.RwDB
	txPool                  *txpool.TxPool
	newTxs                  chan types2.Announcements
	txPoolFetch             *txpool.Fetch
	txPoolSend              *txpool.Send
	txPoolGrpcServer        txpool_proto.TxpoolServer
	notifyMiningAboutNewTxs chan struct{}
	forkValidator           *engine_helpers.ForkValidator
	downloader              *downloader3.Downloader

	agg            *libstate.AggregatorV3
	blockSnapshots *freezeblocks.RoSnapshots
	blockReader    services.FullBlockReader
	blockWriter    *blockio.BlockWriter
	kvRPC          *remotedbserver.KvServer
	logger         log.Logger

	sentinel rpcsentinel.SentinelClient

	silkworm                 *silkworm.Silkworm
	silkwormRPCDaemonService *silkworm.RpcDaemonService
	silkwormSentryService    *silkworm.SentryService
}

func splitAddrIntoHostAndPort(addr string) (host string, port int, err error) {
	idx := strings.LastIndexByte(addr, ':')
	if idx < 0 {
		return "", 0, errors.New("invalid address format")
	}
	host = addr[:idx]
	port, err = strconv.Atoi(addr[idx+1:])
	return
}

const blockBufferSize = 128

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(ctx context.Context, stack *node.Node, config *ethconfig.Config, logger log.Logger) (*Ethereum, error) {
	config.Snapshot.Enabled = config.Sync.UseSnapshots
	if config.Miner.GasPrice == nil || config.Miner.GasPrice.Cmp(libcommon.Big0) <= 0 {
		logger.Warn("Sanitizing invalid miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.Defaults.Miner.GasPrice)
		config.Miner.GasPrice = new(big.Int).Set(ethconfig.Defaults.Miner.GasPrice)
	}

	dirs := stack.Config().Dirs
	tmpdir := dirs.Tmp
	if err := RemoveContents(tmpdir); err != nil { // clean it on startup
		return nil, fmt.Errorf("clean tmp dir: %s, %w", tmpdir, err)
	}

	// Assemble the Ethereum object
	chainKv, err := node.OpenDatabase(ctx, stack.Config(), kv.ChainDB, "", false, logger)
	if err != nil {
		return nil, err
	}
	latestBlockBuiltStore := builder.NewLatestBlockBuiltStore()

	if err := chainKv.Update(context.Background(), func(tx kv.RwTx) error {
		if err = stagedsync.UpdateMetrics(tx); err != nil {
			return err
		}

		config.Prune, err = prune.EnsureNotChanged(tx, config.Prune)
		if err != nil {
			return err
		}

		config.HistoryV3, err = kvcfg.HistoryV3.WriteOnce(tx, config.HistoryV3)
		if err != nil {
			return err
		}

		isCorrectSync, useSnapshots, err := snap.EnsureNotChanged(tx, config.Snapshot)
		if err != nil {
			return err
		}
		// if we are in the incorrect syncmode then we change it to the appropriate one
		if !isCorrectSync {
			config.Sync.UseSnapshots = useSnapshots
			config.Snapshot.Enabled = ethconfig.UseSnapshotsByChainName(config.Genesis.Config.ChainName) && useSnapshots
		}

		return nil
	}); err != nil {
		return nil, err
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	// kv_remote architecture does blocks on stream.Send - means current architecture require unlimited amount of txs to provide good throughput
	backend := &Ethereum{
		sentryCtx:            ctx,
		sentryCancel:         ctxCancel,
		config:               config,
		chainDB:              chainKv,
		networkID:            config.NetworkID,
		etherbase:            config.Miner.Etherbase,
		waitForStageLoopStop: make(chan struct{}),
		waitForMiningStop:    make(chan struct{}),
		notifications: &shards.Notifications{
			Events:      shards.NewEvents(),
			Accumulator: shards.NewAccumulator(),
		},
		logger: logger,
	}

	var chainConfig *chain.Config
	var genesis *types.Block
	if err := backend.chainDB.Update(context.Background(), func(tx kv.RwTx) error {
		h, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			panic(err)
		}
		genesisSpec := config.Genesis
		if h != (libcommon.Hash{}) { // fallback to db content
			genesisSpec = nil
		}
		var genesisErr error
		chainConfig, genesis, genesisErr = core.WriteGenesisBlock(tx, genesisSpec, config.OverrideCancunTime, tmpdir, logger)
		if _, ok := genesisErr.(*chain.ConfigCompatError); genesisErr != nil && !ok {
			return genesisErr
		}

		return nil
	}); err != nil {
		panic(err)
	}
	backend.chainConfig = chainConfig
	backend.genesisBlock = genesis
	backend.genesisHash = genesis.Hash()

	logger.Info("Initialised chain configuration", "config", chainConfig, "genesis", genesis.Hash())

	// Check if we have an already initialized chain and fall back to
	// that if so. Otherwise we need to generate a new genesis spec.
	blockReader, blockWriter, allSnapshots, agg, err := setUpBlockReader(ctx, chainKv, config.Dirs, config.Snapshot, config.HistoryV3, chainConfig.Bor != nil, logger)
	if err != nil {
		return nil, err
	}
	backend.agg, backend.blockSnapshots, backend.blockReader, backend.blockWriter = agg, allSnapshots, blockReader, blockWriter

	if config.HistoryV3 {
		backend.chainDB, err = temporal.New(backend.chainDB, agg, systemcontracts.SystemContractCodeLookup[config.Genesis.Config.ChainName])
		if err != nil {
			return nil, err
		}
		chainKv = backend.chainDB //nolint
	}

	if err := backend.setUpSnapDownloader(ctx, config.Downloader); err != nil {
		return nil, err
	}

	kvRPC := remotedbserver.NewKvServer(ctx, backend.chainDB, allSnapshots, agg, logger)
	backend.notifications.StateChangesConsumer = kvRPC
	backend.kvRPC = kvRPC

	backend.gasPrice, _ = uint256.FromBig(config.Miner.GasPrice)

	if config.SilkwormLibraryPath != "" {
		backend.silkworm, err = silkworm.New(config.SilkwormLibraryPath, config.Dirs.DataDir)
		if err != nil {
			return nil, err
		}
	}

	var sentries []direct.SentryClient
	if len(stack.Config().P2P.SentryAddr) > 0 {
		for _, addr := range stack.Config().P2P.SentryAddr {
			sentryClient, err := sentry_multi_client.GrpcClient(backend.sentryCtx, addr)
			if err != nil {
				return nil, err
			}
			sentries = append(sentries, sentryClient)
		}
	} else if config.SilkwormSentry {
		apiPort := 53774
		apiAddr := fmt.Sprintf("127.0.0.1:%d", apiPort)
		p2pConfig := stack.Config().P2P

		collectNodeURLs := func(nodes []*enode.Node) []string {
			var urls []string
			for _, n := range nodes {
				urls = append(urls, n.URLv4())
			}
			return urls
		}

		settings := silkworm.SentrySettings{
			ClientId:    p2pConfig.Name,
			ApiPort:     apiPort,
			Port:        p2pConfig.ListenPort(),
			Nat:         p2pConfig.NATSpec,
			NetworkId:   config.NetworkID,
			NodeKey:     crypto.FromECDSA(p2pConfig.PrivateKey),
			StaticPeers: collectNodeURLs(p2pConfig.StaticNodes),
			Bootnodes:   collectNodeURLs(p2pConfig.BootstrapNodes),
			NoDiscover:  p2pConfig.NoDiscovery,
			MaxPeers:    p2pConfig.MaxPeers,
		}

		silkwormSentryService := backend.silkworm.NewSentryService(settings)
		backend.silkwormSentryService = &silkwormSentryService

		sentryClient, err := sentry_multi_client.GrpcClient(backend.sentryCtx, apiAddr)
		if err != nil {
			return nil, err
		}
		sentries = append(sentries, sentryClient)
	} else {
		var readNodeInfo = func() *eth.NodeInfo {
			var res *eth.NodeInfo
			_ = backend.chainDB.View(context.Background(), func(tx kv.Tx) error {
				res = eth.ReadNodeInfo(tx, backend.chainConfig, backend.genesisHash, backend.networkID)
				return nil
			})

			return res
		}

		discovery := func() enode.Iterator {
			d, err := setupDiscovery(backend.config.EthDiscoveryURLs)
			if err != nil {
				panic(err)
			}
			return d
		}

		refCfg := stack.Config().P2P
		listenHost, listenPort, err := splitAddrIntoHostAndPort(refCfg.ListenAddr)
		if err != nil {
			return nil, err
		}

		var pi int // points to next port to be picked from refCfg.AllowedPorts
		for _, protocol := range refCfg.ProtocolVersion {
			cfg := refCfg
			cfg.NodeDatabase = filepath.Join(stack.Config().Dirs.Nodes, eth.ProtocolToString[protocol])

			// pick port from allowed list
			var picked bool
			for ; pi < len(refCfg.AllowedPorts) && !picked; pi++ {
				pc := int(refCfg.AllowedPorts[pi])
				if pc == 0 {
					// For ephemeral ports probing to see if the port is taken does not
					// make sense.
					picked = true
					break
				}
				if !checkPortIsFree(fmt.Sprintf("%s:%d", listenHost, pc)) {
					logger.Warn("bind protocol to port has failed: port is busy", "protocols", fmt.Sprintf("eth/%d", refCfg.ProtocolVersion), "port", pc)
					continue
				}
				if listenPort != pc {
					listenPort = pc
				}
				pi++
				picked = true
				break
			}
			if !picked {
				return nil, fmt.Errorf("run out of allowed ports for p2p eth protocols %v. Extend allowed port list via --p2p.allowed-ports", cfg.AllowedPorts)
			}

			cfg.ListenAddr = fmt.Sprintf("%s:%d", listenHost, listenPort)

			server := sentry.NewGrpcServer(backend.sentryCtx, discovery, readNodeInfo, &cfg, protocol, logger)
			backend.sentryServers = append(backend.sentryServers, server)
			sentries = append(sentries, direct.NewSentryClientDirect(protocol, server))
		}

		go func() {
			logEvery := time.NewTicker(180 * time.Second)
			defer logEvery.Stop()

			var logItems []interface{}

			for {
				select {
				case <-backend.sentryCtx.Done():
					return
				case <-logEvery.C:
					logItems = logItems[:0]
					peerCountMap := map[uint]int{}
					for _, srv := range backend.sentryServers {
						counts := srv.SimplePeerCount()
						for protocol, count := range counts {
							peerCountMap[protocol] += count
						}
					}
					for protocol, count := range peerCountMap {
						logItems = append(logItems, eth.ProtocolToString[protocol], strconv.Itoa(count))
					}
					logger.Info("[p2p] GoodPeers", logItems...)
				}
			}
		}()
	}

	var currentBlock *types.Block
	if err := chainKv.View(context.Background(), func(tx kv.Tx) error {
		currentBlock, err = blockReader.CurrentBlock(tx)
		return err
	}); err != nil {
		panic(err)
	}

	currentBlockNumber := uint64(0)
	if currentBlock != nil {
		currentBlockNumber = currentBlock.NumberU64()
	}

	logger.Info("Initialising Ethereum protocol", "network", config.NetworkID)
	var consensusConfig interface{}

	if chainConfig.Clique != nil {
		consensusConfig = &config.Clique
	} else if chainConfig.Aura != nil {
		consensusConfig = &config.Aura
	} else if chainConfig.Bor != nil {
		consensusConfig = &config.Bor
	} else {
		consensusConfig = &config.Ethash
	}
	var heimdallClient heimdall.IHeimdallClient
	if chainConfig.Bor != nil {
		if !config.WithoutHeimdall {
			if config.HeimdallgRPCAddress != "" {
				heimdallClient = heimdallgrpc.NewHeimdallGRPCClient(config.HeimdallgRPCAddress, logger)
			} else {
				heimdallClient = heimdall.NewHeimdallClient(config.HeimdallURL, logger)
			}
		}

		flags.Milestone = config.WithHeimdallMilestones
	}

	backend.engine = ethconsensusconfig.CreateConsensusEngine(ctx, stack.Config(), chainConfig, consensusConfig, config.Miner.Notify, config.Miner.Noverify, heimdallClient, config.WithoutHeimdall, blockReader, false /* readonly */, logger)

	inMemoryExecution := func(batch kv.RwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		terseLogger := log.New()
		terseLogger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync := stages2.NewInMemoryExecution(backend.sentryCtx, backend.chainDB, config, backend.sentriesClient,
			dirs, notifications, blockReader, blockWriter, backend.agg, backend.silkworm, terseLogger)
		chainReader := stagedsync.NewChainReaderImpl(chainConfig, batch, blockReader, logger)
		// We start the mining step
		if err := stages2.StateStep(ctx, chainReader, backend.engine, batch, backend.blockWriter, stateSync, backend.sentriesClient.Bd, header, body, unwindPoint, headersChain, bodiesChain, config.HistoryV3); err != nil {
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
	backend.forkValidator = engine_helpers.NewForkValidator(ctx, currentBlockNumber, inMemoryExecution, tmpdir, backend.blockReader)

	// limit "new block" broadcasts to at most 10 random peers at time
	maxBlockBroadcastPeers := func(header *types.Header) uint { return 10 }

	// unlimited "new block" broadcasts to all peers for blocks announced by Bor validators
	if borEngine, ok := backend.engine.(*bor.Bor); ok {
		defaultValue := maxBlockBroadcastPeers(nil)
		maxBlockBroadcastPeers = func(header *types.Header) uint {
			isValidator, err := borEngine.IsValidator(header)
			if err != nil {
				logger.Warn("maxBlockBroadcastPeers: borEngine.IsValidator has failed", "err", err)
				return defaultValue
			}
			if isValidator {
				// 0 means send to all
				return 0
			}
			return defaultValue
		}
	}

	backend.sentriesClient, err = sentry_multi_client.NewMultiClient(
		chainKv,
		stack.Config().NodeName(),
		chainConfig,
		genesis.Hash(),
		genesis.Time(),
		backend.engine,
		backend.config.NetworkID,
		sentries,
		config.Sync,
		blockReader,
		blockBufferSize,
		stack.Config().SentryLogPeerInfo,
		backend.forkValidator,
		maxBlockBroadcastPeers,
		logger,
	)
	if err != nil {
		return nil, err
	}

	config.TxPool.NoGossip = config.DisableTxPoolGossip
	var miningRPC txpool_proto.MiningServer
	stateDiffClient := direct.NewStateDiffClientDirect(kvRPC)
	if config.DeprecatedTxPool.Disable {
		backend.txPoolGrpcServer = &txpool.GrpcDisabled{}
	} else {
		//cacheConfig := kvcache.DefaultCoherentCacheConfig
		//cacheConfig.MetricsLabel = "txpool"

		backend.newTxs = make(chan types2.Announcements, 1024)
		//defer close(newTxs)
		backend.txPoolDB, backend.txPool, backend.txPoolFetch, backend.txPoolSend, backend.txPoolGrpcServer, err = txpooluitl.AllComponents(
			ctx, config.TxPool, kvcache.NewDummy(), backend.newTxs, backend.chainDB, backend.sentriesClient.Sentries(), stateDiffClient, logger,
		)
		if err != nil {
			return nil, err
		}
	}

	backend.notifyMiningAboutNewTxs = make(chan struct{}, 1)
	backend.miningSealingQuit = make(chan struct{})
	backend.pendingBlocks = make(chan *types.Block, 1)
	backend.minedBlocks = make(chan *types.Block, 1)

	miner := stagedsync.NewMiningState(&config.Miner)
	backend.pendingBlocks = miner.PendingResultCh
	backend.minedBlocks = miner.MiningResultCh

	var (
		snapDb     kv.RwDB
		recents    *lru.ARCCache[libcommon.Hash, *bor.Snapshot]
		signatures *lru.ARCCache[libcommon.Hash, libcommon.Address]
	)
	if bor, ok := backend.engine.(*bor.Bor); ok {
		snapDb = bor.DB
		recents = bor.Recents
		signatures = bor.Signatures
	}
	// proof-of-work mining
	mining := stagedsync.New(
		stagedsync.MiningStages(backend.sentryCtx,
			stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miner, *backend.chainConfig, backend.engine, backend.txPoolDB, nil, tmpdir, backend.blockReader),
			stagedsync.StageBorHeimdallCfg(backend.chainDB, snapDb, miner, *backend.chainConfig, heimdallClient, backend.blockReader, nil, nil, recents, signatures),
			stagedsync.StageMiningExecCfg(backend.chainDB, miner, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir, nil, 0, backend.txPool, backend.txPoolDB, blockReader),
			stagedsync.StageHashStateCfg(backend.chainDB, dirs, config.HistoryV3),
			stagedsync.StageTrieCfg(backend.chainDB, false, true, true, tmpdir, blockReader, nil, config.HistoryV3, backend.agg),
			stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miner, backend.miningSealingQuit, backend.blockReader, latestBlockBuiltStore),
		), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder,
		logger)

	var ethashApi *ethash.API
	if casted, ok := backend.engine.(*ethash.Ethash); ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}

	// proof-of-stake mining
	assembleBlockPOS := func(param *core.BlockBuilderParameters, interrupt *int32) (*types.BlockWithReceipts, error) {
		miningStatePos := stagedsync.NewProposingState(&config.Miner)
		miningStatePos.MiningConfig.Etherbase = param.SuggestedFeeRecipient
		proposingSync := stagedsync.New(
			stagedsync.MiningStages(backend.sentryCtx,
				stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miningStatePos, *backend.chainConfig, backend.engine, backend.txPoolDB, param, tmpdir, backend.blockReader),
				stagedsync.StageBorHeimdallCfg(backend.chainDB, snapDb, miningStatePos, *backend.chainConfig, heimdallClient, backend.blockReader, nil, nil, recents, signatures),
				stagedsync.StageMiningExecCfg(backend.chainDB, miningStatePos, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir, interrupt, param.PayloadId, backend.txPool, backend.txPoolDB, blockReader),
				stagedsync.StageHashStateCfg(backend.chainDB, dirs, config.HistoryV3),
				stagedsync.StageTrieCfg(backend.chainDB, false, true, true, tmpdir, blockReader, nil, config.HistoryV3, backend.agg),
				stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miningStatePos, backend.miningSealingQuit, backend.blockReader, latestBlockBuiltStore),
			), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder,
			logger)
		// We start the mining step
		if err := stages2.MiningStep(ctx, backend.chainDB, proposingSync, tmpdir); err != nil {
			return nil, err
		}
		block := <-miningStatePos.MiningResultPOSCh
		return block, nil
	}

	// Initialize ethbackend
	ethBackendRPC := privateapi.NewEthBackendServer(ctx, backend, backend.chainDB, backend.notifications.Events, blockReader, logger, latestBlockBuiltStore)
	// intiialize engine backend
	var engine *execution_client.ExecutionClientDirect

	blockRetire := freezeblocks.NewBlockRetire(1, dirs, blockReader, blockWriter, backend.chainDB, backend.notifications.Events, logger)

	miningRPC = privateapi.NewMiningServer(ctx, backend, ethashApi, logger)

	var creds credentials.TransportCredentials
	if stack.Config().PrivateApiAddr != "" {
		if stack.Config().TLSConnection {
			creds, err = grpcutil.TLS(stack.Config().TLSCACert, stack.Config().TLSCertFile, stack.Config().TLSKeyFile)
			if err != nil {
				return nil, err
			}
		}
		backend.privateAPI, err = privateapi.StartGrpc(
			kvRPC,
			ethBackendRPC,
			backend.txPoolGrpcServer,
			miningRPC,
			stack.Config().PrivateApiAddr,
			stack.Config().PrivateApiRateLimit,
			creds,
			stack.Config().HealthCheck,
			logger)
		if err != nil {
			return nil, fmt.Errorf("private api: %w", err)
		}
	}

	if currentBlock == nil {
		currentBlock = genesis
	}
	// We start the transaction pool on startup, for a couple of reasons:
	// 1) Hive tests requires us to do so and starting it from eth_sendRawTransaction is not viable as we have not enough data
	// to initialize it properly.
	// 2) we cannot propose for block 1 regardless.
	go func() {
		time.Sleep(10 * time.Millisecond)
		baseFee := uint64(0)
		if currentBlock.BaseFee() != nil {
			baseFee = misc.CalcBaseFee(chainConfig, currentBlock.Header()).Uint64()
		}
		blobFee := chainConfig.GetMinBlobGasPrice()
		if currentBlock.Header().ExcessBlobGas != nil {
			excessBlobGas := misc.CalcExcessBlobGas(chainConfig, currentBlock.Header())
			b, err := misc.GetBlobGasPrice(chainConfig, excessBlobGas)
			if err == nil {
				blobFee = b.Uint64()
			}
		}
		backend.notifications.Accumulator.StartChange(currentBlock.NumberU64(), currentBlock.Hash(), nil, false)
		backend.notifications.Accumulator.SendAndReset(ctx, backend.notifications.StateChangesConsumer, baseFee, blobFee, currentBlock.GasLimit(), 0)
	}()

	if !config.DeprecatedTxPool.Disable {
		backend.txPoolFetch.ConnectCore()
		backend.txPoolFetch.ConnectSentries()
		var newTxsBroadcaster *txpool.NewSlotsStreams
		if casted, ok := backend.txPoolGrpcServer.(*txpool.GrpcServer); ok {
			newTxsBroadcaster = casted.NewSlotsStreams
		}
		go txpool.MainLoop(backend.sentryCtx,
			backend.txPoolDB, backend.chainDB,
			backend.txPool, backend.newTxs, backend.txPoolSend, newTxsBroadcaster,
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
				// Add mined header and block body before broadcast. This is because the broadcast call
				// will trigger the staged sync which will require headers and blocks to be available
				// in their respective cache in the download stage. If not found, it would cause a
				// liveness issue for the chain.
				if err := backend.sentriesClient.Hd.AddMinedHeader(b.Header()); err != nil {
					logger.Error("add mined block to header downloader", "err", err)
				}
				backend.sentriesClient.Bd.AddToPrefetch(b.Header(), b.RawBody())

				//p2p
				//backend.sentriesClient.BroadcastNewBlock(context.Background(), b, b.Difficulty())
				//rpcdaemon
				if err := miningRPC.(*privateapi.MiningServer).BroadcastMinedBlock(b); err != nil {
					logger.Error("txpool rpc mined block broadcast", "err", err)
				}
				logger.Trace("BroadcastMinedBlock successful", "number", b.Number(), "GasUsed", b.GasUsed(), "txn count", b.Transactions().Len())
				backend.sentriesClient.PropagateNewBlockHashes(ctx, []headerdownload.Announce{
					{
						Number: b.NumberU64(),
						Hash:   b.Hash(),
					},
				})

			case b := <-backend.pendingBlocks:
				if err := miningRPC.(*privateapi.MiningServer).BroadcastPendingBlock(b); err != nil {
					logger.Error("txpool rpc pending block broadcast", "err", err)
				}
			case <-backend.sentriesClient.Hd.QuitPoWMining:
				return
			}
		}
	}()

	if err := backend.StartMining(context.Background(), backend.chainDB, mining, backend.config.Miner, backend.gasPrice, backend.sentriesClient.Hd.QuitPoWMining, tmpdir); err != nil {
		return nil, err
	}

	backend.ethBackendRPC, backend.miningRPC, backend.stateChangesClient = ethBackendRPC, miningRPC, stateDiffClient

	backend.syncStages = stages2.NewDefaultStages(backend.sentryCtx, backend.chainDB, snapDb, stack.Config().P2P, config, backend.sentriesClient, backend.notifications, backend.downloaderClient,
		blockReader, blockRetire, backend.agg, backend.silkworm, backend.forkValidator, heimdallClient, recents, signatures, logger)
	backend.syncUnwindOrder = stagedsync.DefaultUnwindOrder
	backend.syncPruneOrder = stagedsync.DefaultPruneOrder
	backend.stagedSync = stagedsync.New(backend.syncStages, backend.syncUnwindOrder, backend.syncPruneOrder, logger)

	hook := stages2.NewHook(backend.sentryCtx, backend.chainDB, backend.notifications, backend.stagedSync, backend.blockReader, backend.chainConfig, backend.logger, backend.sentriesClient.UpdateHead)

	checkStateRoot := true
	pipelineStages := stages2.NewPipelineStages(ctx, chainKv, config, backend.sentriesClient, backend.notifications, backend.downloaderClient, blockReader, blockRetire, backend.agg, backend.silkworm, backend.forkValidator, logger, checkStateRoot)
	backend.pipelineStagedSync = stagedsync.New(pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, logger)
	backend.eth1ExecutionServer = eth1.NewEthereumExecutionModule(blockReader, chainKv, backend.pipelineStagedSync, backend.forkValidator, chainConfig, assembleBlockPOS, hook, backend.notifications.Accumulator, backend.notifications.StateChangesConsumer, logger, backend.engine, config.HistoryV3)
	executionRpc := direct.NewExecutionClientDirect(backend.eth1ExecutionServer)
	engineBackendRPC := engineapi.NewEngineServer(
		ctx,
		logger,
		chainConfig,
		executionRpc,
		backend.sentriesClient.Hd,
		engine_block_downloader.NewEngineBlockDownloader(ctx, logger, backend.sentriesClient.Hd, executionRpc,
			backend.sentriesClient.Bd, backend.sentriesClient.BroadcastNewBlock, backend.sentriesClient.SendBodyRequest, blockReader,
			chainKv, chainConfig, tmpdir, config.Sync.BodyDownloadTimeoutSeconds),
		false,
		config.Miner.EnabledPOS)
	backend.engineBackendRPC = engineBackendRPC
	engine, err = execution_client.NewExecutionClientDirect(ctx, eth1_chain_reader.NewChainReaderEth1(ctx, chainConfig, executionRpc, 1000))
	if err != nil {
		return nil, err
	}

	// If we choose not to run a consensus layer, run our embedded.
	if config.InternalCL && clparams.EmbeddedSupported(config.NetworkID) {
		genesisCfg, networkCfg, beaconCfg := clparams.GetConfigsByNetwork(clparams.NetworkType(config.NetworkID))
		if err != nil {
			return nil, err
		}
		state, err := clcore.RetrieveBeaconState(ctx, beaconCfg, genesisCfg,
			clparams.GetCheckpointSyncEndpoint(clparams.NetworkType(config.NetworkID)))
		if err != nil {
			return nil, err
		}
		forkDigest, err := fork.ComputeForkDigest(beaconCfg, genesisCfg)
		if err != nil {
			return nil, err
		}

		rawBeaconBlockChainDb := persistence.AferoRawBeaconBlockChainFromOsPath(beaconCfg, dirs.CaplinHistory)

		client, err := service.StartSentinelService(&sentinel.SentinelConfig{
			IpAddr:        config.LightClientDiscoveryAddr,
			Port:          int(config.LightClientDiscoveryPort),
			TCPPort:       uint(config.LightClientDiscoveryTCPPort),
			GenesisConfig: genesisCfg,
			NetworkConfig: networkCfg,
			BeaconConfig:  beaconCfg,
			TmpDir:        tmpdir,
		}, rawBeaconBlockChainDb, &service.ServerConfig{Network: "tcp", Addr: fmt.Sprintf("%s:%d", config.SentinelAddr, config.SentinelPort)}, creds, &cltypes.Status{
			ForkDigest:     forkDigest,
			FinalizedRoot:  state.FinalizedCheckpoint().BlockRoot(),
			FinalizedEpoch: state.FinalizedCheckpoint().Epoch(),
			HeadSlot:       state.FinalizedCheckpoint().Epoch() * beaconCfg.SlotsPerEpoch,
			HeadRoot:       state.FinalizedCheckpoint().BlockRoot(),
		}, logger)
		if err != nil {
			return nil, err
		}

		backend.sentinel = client

		go func() {
			eth1Getter := getters.NewExecutionSnapshotReader(ctx, blockReader, backend.chainDB)
			if err := caplin1.RunCaplinPhase1(ctx, client, engine, beaconCfg, genesisCfg, state, nil, dirs, config.BeaconRouter, eth1Getter, backend.downloaderClient, config.CaplinConfig.Backfilling); err != nil {
				logger.Error("could not start caplin", "err", err)
			}
			ctxCancel()
		}()
	}

	return backend, nil
}

func (s *Ethereum) Init(stack *node.Node, config *ethconfig.Config) error {
	ethBackendRPC, miningRPC, stateDiffClient := s.ethBackendRPC, s.miningRPC, s.stateChangesClient
	blockReader := s.blockReader
	ctx := s.sentryCtx
	chainKv := s.chainDB
	var err error

	s.sentriesClient.Hd.StartPoSDownloader(s.sentryCtx, s.sentriesClient.SendHeaderRequest, s.sentriesClient.Penalize)

	emptyBadHash := config.BadBlockHash == libcommon.Hash{}
	if !emptyBadHash {
		var badBlockHeader *types.Header
		if err = chainKv.View(context.Background(), func(tx kv.Tx) error {
			header, hErr := rawdb.ReadHeaderByHash(tx, config.BadBlockHash)
			badBlockHeader = header
			return hErr
		}); err != nil {
			return err
		}

		if badBlockHeader != nil {
			unwindPoint := badBlockHeader.Number.Uint64() - 1
			s.stagedSync.UnwindTo(unwindPoint, stagedsync.BadBlock(config.BadBlockHash, fmt.Errorf("Init unwind")))
		}
	}

	//eth.APIBackend = &EthAPIBackend{stack.Config().ExtRPCEnabled(), stack.Config().AllowUnprotectedTxs, eth, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.Miner.GasPrice
	}
	//eth.APIBackend.gpo = gasprice.NewOracle(eth.APIBackend, gpoParams)
	if config.Ethstats != "" {
		var headCh chan [][]byte
		headCh, s.unsubscribeEthstat = s.notifications.Events.AddHeaderSubscription()
		if err := ethstats.New(stack, s.sentryServers, chainKv, s.blockReader, s.engine, config.Ethstats, s.networkID, ctx.Done(), headCh); err != nil {
			return err
		}
	}
	// start HTTP API
	httpRpcCfg := stack.Config().Http
	ethRpcClient, txPoolRpcClient, miningRpcClient, stateCache, ff, err := cli.EmbeddedServices(ctx, chainKv, httpRpcCfg.StateCache, blockReader, ethBackendRPC,
		s.txPoolGrpcServer, miningRPC, stateDiffClient, s.logger)
	if err != nil {
		return err
	}

	s.apiList = jsonrpc.APIList(chainKv, ethRpcClient, txPoolRpcClient, miningRpcClient, ff, stateCache, blockReader, s.agg, &httpRpcCfg, s.engine, s.logger)

	if config.SilkwormRpcDaemon && httpRpcCfg.Enabled {
		silkwormRPCDaemonService := s.silkworm.NewRpcDaemonService(chainKv)
		s.silkwormRPCDaemonService = &silkwormRPCDaemonService
	} else {
		go func() {
			if err := cli.StartRpcServer(ctx, &httpRpcCfg, s.apiList, s.logger); err != nil {
				s.logger.Error("cli.StartRpcServer error", "err", err)
			}
		}()
	}

	go s.engineBackendRPC.Start(&httpRpcCfg, s.chainDB, s.blockReader, ff, stateCache, s.agg, s.engine, ethRpcClient, txPoolRpcClient, miningRpcClient)

	// Register the backend on the node
	stack.RegisterLifecycle(s)
	return nil
}

func (s *Ethereum) APIs() []rpc.API {
	return s.apiList
}

func (s *Ethereum) Etherbase() (eb libcommon.Address, err error) {
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()

	if etherbase != (libcommon.Address{}) {
		return etherbase, nil
	}
	return libcommon.Address{}, fmt.Errorf("etherbase must be explicitly specified")
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
	return ethutils.IsLocalBlock(s.engine, etherbase, s.config.DeprecatedTxPool.Locals, block.Header())
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
func (s *Ethereum) StartMining(ctx context.Context, db kv.RwDB, mining *stagedsync.Sync, cfg params.MiningConfig, gasPrice *uint256.Int, quitCh chan struct{}, tmpDir string) error {

	var borcfg *bor.Bor
	if b, ok := s.engine.(*bor.Bor); ok {
		borcfg = b
		b.HeaderProgress(s.sentriesClient.Hd)
	} else if br, ok := s.engine.(*merge.Merge); ok {
		if b, ok := br.InnerEngine().(*bor.Bor); ok {
			borcfg = b
			b.HeaderProgress(s.sentriesClient.Hd)
		}
	}

	//if borcfg == nil {
	if !cfg.Enabled {
		return nil
	}
	//}

	// Configure the local mining address
	eb, err := s.Etherbase()
	if err != nil {
		s.logger.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %w", err)
	}

	if borcfg != nil {
		if cfg.Enabled {
			if cfg.SigKey == nil {
				s.logger.Error("Etherbase account unavailable locally", "err", err)
				return fmt.Errorf("signer missing: %w", err)
			}

			borcfg.Authorize(eb, func(_ libcommon.Address, mimeType string, message []byte) ([]byte, error) {
				return crypto.Sign(crypto.Keccak256(message), cfg.SigKey)
			})
		} else {
			// for the bor dev network without heimdall we need the authorizer to be set otherwise there is no
			// validator defined in the bor validator set and non mining nodes will reject all blocks
			// this assumes in this mode we're only running a single validator

			if s.chainConfig.ChainName == networkname.BorDevnetChainName && s.config.WithoutHeimdall {
				borcfg.Authorize(eb, func(addr libcommon.Address, _ string, _ []byte) ([]byte, error) {
					return nil, &bor.UnauthorizedSignerError{Number: 0, Signer: addr.Bytes()}
				})
			}

			return nil
		}
	}

	var clq *clique.Clique
	if c, ok := s.engine.(*clique.Clique); ok {
		clq = c
	} else if cl, ok := s.engine.(*merge.Merge); ok {
		if c, ok := cl.InnerEngine().(*clique.Clique); ok {
			clq = c
		}
	}
	if clq != nil {
		if cfg.SigKey == nil {
			s.logger.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		clq.Authorize(eb, func(_ libcommon.Address, mimeType string, message []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(message), cfg.SigKey)
		})
	}

	go func() {
		defer debug.LogPanic()
		defer close(s.waitForMiningStop)

		mineEvery := time.NewTicker(cfg.Recommit)
		defer mineEvery.Stop()

		// Listen on a new head subscription. This allows us to maintain the block time by
		// triggering mining after the block is passed through all stages.
		newHeadCh, closeNewHeadCh := s.notifications.Events.AddHeaderSubscription()
		defer closeNewHeadCh()

		s.logger.Info("Starting to mine", "etherbase", eb)

		var works bool
		hasWork := true // Start mining immediately
		errc := make(chan error, 1)

		for {
			// Only reset if some work was done previously as we'd like to rely
			// on the `miner.recommit` as backup.
			if hasWork {
				mineEvery.Reset(cfg.Recommit)
			}

			// Only check for case if you're already mining (i.e. works = true) and
			// waiting for error or you don't have any work yet (i.e. hasWork = false).
			if works || !hasWork {
				select {
				case <-newHeadCh:
					hasWork = true
				case <-s.notifyMiningAboutNewTxs:
					// Skip mining based on new tx notif for bor consensus
					hasWork = s.chainConfig.Bor == nil
					if hasWork {
						s.logger.Debug("Start mining new block based on txpool notif")
					}
				case <-mineEvery.C:
					s.logger.Debug("Start mining new block based on miner.recommit")
					hasWork = true
				case err := <-errc:
					works = false
					hasWork = false
					if errors.Is(err, libcommon.ErrStopped) {
						return
					}
					if err != nil {
						s.logger.Warn("mining", "err", err)
					}
				case <-quitCh:
					return
				}
			}

			if !works && hasWork {
				works = true
				hasWork = false
				mineEvery.Reset(cfg.Recommit)
				go func() { errc <- stages2.MiningStep(ctx, db, mining, tmpDir) }()
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

	s.logger.Trace("sentry", "peer count", sentryPc)
	for _, sc := range s.sentriesClient.Sentries() {
		ctx := context.Background()
		reply, err := sc.PeerCount(ctx, &proto_sentry.PeerCountRequest{})
		if err != nil {
			s.logger.Warn("sentry", "err", err)
			return 0, nil
		}
		sentryPc += reply.Count
	}

	return sentryPc, nil
}

func (s *Ethereum) NodesInfo(limit int) (*remote.NodesInfoReply, error) {
	if limit == 0 || limit > len(s.sentriesClient.Sentries()) {
		limit = len(s.sentriesClient.Sentries())
	}

	nodes := make([]*prototypes.NodeInfoReply, 0, limit)
	for i := 0; i < limit; i++ {
		sc := s.sentriesClient.Sentries()[i]

		nodeInfo, err := sc.NodeInfo(context.Background(), nil)
		if err != nil {
			s.logger.Error("sentry nodeInfo", "err", err)
			continue
		}

		nodes = append(nodes, nodeInfo)
	}

	nodesInfo := &remote.NodesInfoReply{NodesInfo: nodes}
	slices.SortFunc(nodesInfo.NodesInfo, remote.NodeInfoReplyCmp)

	return nodesInfo, nil
}

// sets up blockReader and client downloader
func (s *Ethereum) setUpSnapDownloader(ctx context.Context, downloaderCfg *downloadercfg.Cfg) error {
	var err error
	if s.config.Snapshot.NoDownloader {
		return nil
	}
	var discover bool
	if err := s.chainDB.View(ctx, func(tx kv.Tx) error {
		p, err := stages.GetStageProgress(tx, stages.Snapshots)
		if err != nil {
			return err
		}
		discover = p == 0
		return nil
	}); err != nil {
		return err
	}
	if s.config.Snapshot.DownloaderAddr != "" {
		// connect to external Downloader
		s.downloaderClient, err = downloadergrpc.NewClient(ctx, s.config.Snapshot.DownloaderAddr)
	} else {
		// start embedded Downloader
		s.downloader, err = downloader3.New(ctx, downloaderCfg, s.config.Dirs, s.logger, log.LvlDebug, discover)
		if err != nil {
			return err
		}
		s.downloader.MainLoopInBackground(true)
		bittorrentServer, err := downloader3.NewGrpcServer(s.downloader)
		if err != nil {
			return fmt.Errorf("new server: %w", err)
		}

		s.downloaderClient = direct.NewDownloaderClient(bittorrentServer)
	}
	s.agg.OnFreeze(func(frozenFileNames []string) {
		events := s.notifications.Events
		events.OnNewSnapshot()
		if s.downloaderClient != nil {
			req := &proto_downloader.DownloadRequest{Items: make([]*proto_downloader.DownloadItem, 0, len(frozenFileNames))}
			for _, fName := range frozenFileNames {
				req.Items = append(req.Items, &proto_downloader.DownloadItem{
					Path: filepath.Join("history", fName),
				})
			}
			if _, err := s.downloaderClient.Download(ctx, req); err != nil {
				s.logger.Warn("[snapshots] notify downloader", "err", err)
			}
		}
	})
	return err
}

func setUpBlockReader(ctx context.Context, db kv.RwDB, dirs datadir.Dirs, snConfig ethconfig.BlocksFreezing, histV3 bool, isBor bool, logger log.Logger) (services.FullBlockReader, *blockio.BlockWriter, *freezeblocks.RoSnapshots, *libstate.AggregatorV3, error) {
	allSnapshots := freezeblocks.NewRoSnapshots(snConfig, dirs.Snap, logger)
	var allBorSnapshots *freezeblocks.BorRoSnapshots
	if isBor {
		allBorSnapshots = freezeblocks.NewBorRoSnapshots(snConfig, dirs.Snap, logger)
	}
	var err error
	if !snConfig.NoDownloader {
		allSnapshots.OptimisticalyReopenWithDB(db)
		if isBor {
			allBorSnapshots.OptimisticalyReopenWithDB(db)
		}
	}
	blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)
	blockWriter := blockio.NewBlockWriter(histV3)

	agg, err := libstate.NewAggregatorV3(ctx, dirs.SnapHistory, dirs.Tmp, ethconfig.HistoryV3AggregationStep, db, logger)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if err = agg.OpenFolder(); err != nil {
		return nil, nil, nil, nil, err
	}
	return blockReader, blockWriter, allSnapshots, agg, nil
}

func (s *Ethereum) Peers(ctx context.Context) (*remote.PeersReply, error) {
	var reply remote.PeersReply
	for _, sentryClient := range s.sentriesClient.Sentries() {
		peers, err := sentryClient.Peers(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.Peers error: %w", err)
		}
		reply.Peers = append(reply.Peers, peers.Peers...)
	}

	return &reply, nil
}

func (s *Ethereum) DiagnosticsPeersData() map[string]*diagnostics.PeerStatistics {
	var reply map[string]*diagnostics.PeerStatistics = make(map[string]*diagnostics.PeerStatistics)
	for _, sentryServer := range s.sentryServers {
		peers := sentryServer.DiagnosticsPeersData()

		for key, value := range peers {
			reply[key] = value
		}
	}

	return reply
}

func (s *Ethereum) AddPeer(ctx context.Context, req *remote.AddPeerRequest) (*remote.AddPeerReply, error) {
	for _, sentryClient := range s.sentriesClient.Sentries() {
		_, err := sentryClient.AddPeer(ctx, &proto_sentry.AddPeerRequest{Url: req.Url})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.AddPeers error: %w", err)
		}
	}
	return &remote.AddPeerReply{Success: true}, nil
}

// Protocols returns all the currently configured
// network protocols to start.
func (s *Ethereum) Protocols() []p2p.Protocol {
	protocols := make([]p2p.Protocol, 0, len(s.sentryServers))
	for i := range s.sentryServers {
		protocols = append(protocols, s.sentryServers[i].Protocols...)
	}
	return protocols
}

// Start implements node.Lifecycle, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *Ethereum) Start() error {
	s.sentriesClient.StartStreamLoops(s.sentryCtx)
	time.Sleep(10 * time.Millisecond) // just to reduce logs order confusion

	hook := stages2.NewHook(s.sentryCtx, s.chainDB, s.notifications, s.stagedSync, s.blockReader, s.chainConfig, s.logger, s.sentriesClient.UpdateHead)

	currentTDProvider := func() *big.Int {
		currentTD, err := readCurrentTotalDifficulty(s.sentryCtx, s.chainDB, s.blockReader)
		if err != nil {
			panic(err)
		}
		return currentTD
	}

	if params.IsChainPoS(s.chainConfig, currentTDProvider) {
		s.waitForStageLoopStop = nil // TODO: Ethereum.Stop should wait for execution_server shutdown
		go s.eth1ExecutionServer.Start(s.sentryCtx)
	} else {
		go stages2.StageLoop(s.sentryCtx, s.chainDB, s.stagedSync, s.sentriesClient.Hd, s.waitForStageLoopStop, s.config.Sync.LoopThrottle, s.logger, s.blockReader, hook, s.config.ForcePartialCommit)
	}

	if s.chainConfig.Bor != nil {
		s.engine.(*bor.Bor).Start(s.chainDB)
	}

	if s.silkwormRPCDaemonService != nil {
		if err := s.silkwormRPCDaemonService.Start(); err != nil {
			s.logger.Error("silkworm.StartRpcDaemon error", "err", err)
		}
	}
	if s.silkwormSentryService != nil {
		if err := s.silkwormSentryService.Start(); err != nil {
			s.logger.Error("silkworm.SentryStart error", "err", err)
		}
	}

	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	// Stop all the peer-related stuff first.
	s.sentryCancel()
	if s.unsubscribeEthstat != nil {
		s.unsubscribeEthstat()
	}
	if s.downloader != nil {
		s.downloader.Close()
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
	libcommon.SafeClose(s.sentriesClient.Hd.QuitPoWMining)
	_ = s.engine.Close()
	if s.waitForStageLoopStop != nil {
		<-s.waitForStageLoopStop
	}
	if s.config.Miner.Enabled {
		<-s.waitForMiningStop
	}
	for _, sentryServer := range s.sentryServers {
		sentryServer.Close()
	}
	if s.txPoolDB != nil {
		s.txPoolDB.Close()
	}
	if s.agg != nil {
		s.agg.Close()
	}
	s.chainDB.Close()

	if s.silkwormRPCDaemonService != nil {
		if err := s.silkwormRPCDaemonService.Stop(); err != nil {
			s.logger.Error("silkworm.StopRpcDaemon error", "err", err)
		}
	}
	if s.silkwormSentryService != nil {
		if err := s.silkwormSentryService.Stop(); err != nil {
			s.logger.Error("silkworm.SentryStop error", "err", err)
		}
	}
	if s.silkworm != nil {
		s.silkworm.Close()
	}

	return nil
}

func (s *Ethereum) ChainDB() kv.RwDB {
	return s.chainDB
}

func (s *Ethereum) ChainConfig() *chain.Config {
	return s.chainConfig
}

func (s *Ethereum) StagedSync() *stagedsync.Sync {
	return s.stagedSync
}

func (s *Ethereum) Notifications() *shards.Notifications {
	return s.notifications
}

func (s *Ethereum) SentryCtx() context.Context {
	return s.sentryCtx
}

func (s *Ethereum) SentryControlServer() *sentry_multi_client.MultiClient {
	return s.sentriesClient
}
func (s *Ethereum) BlockIO() (services.FullBlockReader, *blockio.BlockWriter) {
	return s.blockReader, s.blockWriter
}

func (s *Ethereum) TxpoolServer() txpool_proto.TxpoolServer {
	return s.txPoolGrpcServer
}

// RemoveContents is like os.RemoveAll, but preserve dir itself
func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// ignore due to windows
			_ = os.MkdirAll(dir, 0o755)
			return nil
		}
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(dir, name))
		if err != nil {
			return err
		}
	}
	return nil
}

func checkPortIsFree(addr string) (free bool) {
	c, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
	if err != nil {
		return true
	}
	c.Close()
	return false
}

func readCurrentTotalDifficulty(ctx context.Context, db kv.RwDB, blockReader services.FullBlockReader) (*big.Int, error) {
	var currentTD *big.Int
	err := db.View(ctx, func(tx kv.Tx) error {
		h, err := blockReader.CurrentBlock(tx)
		if err != nil {
			return err
		}
		if h == nil {
			currentTD = nil
			return nil
		}

		currentTD, err = rawdb.ReadTd(tx, h.Hash(), h.NumberU64())
		return err
	})
	return currentTD, err
}

func (s *Ethereum) Sentinel() rpcsentinel.SentinelClient {
	return s.sentinel
}
