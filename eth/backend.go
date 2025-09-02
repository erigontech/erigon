// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/mdbx-go/mdbx"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon-lib/common/disk"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/event"
	protodownloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/gointerfaces/grpcutil"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	rpcsentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	protosentry "github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/gointerfaces/txpoolproto"
	prototypes "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format/getters"
	executionclient "github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	rpcdaemoncli "github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/genesiswrite"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/downloader/downloadercfg"
	"github.com/erigontech/erigon/db/downloader/downloadergrpc"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/diagnostics/mem"
	"github.com/erigontech/erigon/eth/consensuschain"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconsensusconfig"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/ethstats"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/chain/networkname"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/clique"
	"github.com/erigontech/erigon/execution/consensus/ethash"
	"github.com/erigontech/erigon/execution/consensus/merge"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/eth1"
	"github.com/erigontech/erigon/execution/eth1/eth1_chain_reader"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	stages2 "github.com/erigontech/erigon/execution/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/p2p/sentry/libsentry"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"
	polygonsync "github.com/erigontech/erigon/polygon/sync"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
	privateapi2 "github.com/erigontech/erigon/turbo/privateapi"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/silkworm"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains
)

// Config contains the configuration options of the ETH protocol.
// Deprecated: use ethconfig.Config instead.
type Config = ethconfig.Config

// Ethereum implements the Ethereum full node service.
type Ethereum struct {
	config *ethconfig.Config

	// DB interfaces
	chainDB    kv.TemporalRwDB
	privateAPI *grpc.Server

	engine consensus.Engine

	gasPrice  *uint256.Int
	etherbase common.Address

	networkID uint64

	lock         sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	chainConfig  *chain.Config
	apiList      []rpc.API
	genesisBlock *types.Block
	genesisHash  common.Hash

	eth1ExecutionServer *eth1.EthereumExecutionModule

	ethBackendRPC       *privateapi2.EthBackendServer
	ethRpcClient        rpchelper.ApiBackend
	engineBackendRPC    *engineapi.EngineServer
	miningRPC           *privateapi2.MiningServer
	miningRpcClient     txpoolproto.MiningClient
	stateDiffClient     *direct.StateDiffClientDirect
	rpcFilters          *rpchelper.Filters
	rpcDaemonStateCache kvcache.Cache

	miningSealingQuit   chan struct{}
	pendingBlocks       chan *types.Block
	minedBlocks         chan *types.Block
	minedBlockObservers *event.Observers[*types.Block]

	sentryCtx      context.Context
	sentryCancel   context.CancelFunc
	sentriesClient *sentry_multi_client.MultiClient
	sentryServers  []*sentry.GrpcServer

	stagedSync         *stagedsync.Sync
	pipelineStagedSync *stagedsync.Sync
	syncStages         []*stagedsync.Stage
	syncUnwindOrder    stagedsync.UnwindOrder
	syncPruneOrder     stagedsync.PruneOrder

	downloaderClient protodownloader.DownloaderClient

	notifications *shards.Notifications

	unsubscribeEthstat func()

	waitForStageLoopStop chan struct{}
	waitForMiningStop    chan struct{}

	txPool                    *txpool.TxPool
	txPoolGrpcServer          txpoolproto.TxpoolServer
	txPoolRpcClient           txpoolproto.TxpoolClient
	shutterPool               *shutter.Pool
	blockBuilderNotifyNewTxns chan struct{}
	forkValidator             *engine_helpers.ForkValidator
	downloader                *downloader.Downloader

	blockSnapshots *freezeblocks.RoSnapshots
	blockReader    services.FullBlockReader
	blockWriter    *blockio.BlockWriter
	kvRPC          *remotedbserver.KvServer
	logger         log.Logger

	sentinel rpcsentinel.SentinelClient

	silkworm                 *silkworm.Silkworm
	silkwormRPCDaemonService *silkworm.RpcDaemonService
	silkwormSentryService    *silkworm.SentryService

	polygonSyncService  *polygonsync.Service
	polygonDownloadSync *stagedsync.Sync
	polygonBridge       *bridge.Service
	heimdallService     *heimdall.Service
	stopNode            func() error
	bgComponentsEg      errgroup.Group
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

func checkAndSetCommitmentHistoryFlag(tx kv.RwTx, logger log.Logger, dirs datadir.Dirs, cfg *ethconfig.Config) error {

	isCommitmentHistoryEnabled, ok, err := rawdb.ReadDBCommitmentHistoryEnabled(tx)
	if err != nil {
		return err
	}
	if !ok {
		if !cfg.KeepExecutionProofs {
			if err := rawdb.WriteDBCommitmentHistoryEnabled(tx, cfg.KeepExecutionProofs); err != nil {
				return err
			}
			return nil
		}
		// we need to make sure we do not run from an old version so check amount of keys in kv.AccountDomain
		c, err := tx.Count(kv.TblAccountVals)
		if err != nil {
			return fmt.Errorf("failed to count keys in kv.AccountDomain: %w", err)
		}
		if c > 0 {
			return fmt.Errorf("commitment history is not enabled in the database. restart erigon after deleting the chaindata folder: %s", dirs.Chaindata)
		}

		if err := rawdb.WriteDBCommitmentHistoryEnabled(tx, cfg.KeepExecutionProofs); err != nil {
			return err
		}
		return nil
	}
	if cfg.KeepExecutionProofs != isCommitmentHistoryEnabled {
		return fmt.Errorf(
			"flag '--prune.experimental.include-commitment-history' mismatch: db: %v; config: %v. please restart Erigon '--prune.experimental.include-commitment-history=%v' or delete the chaindata folder: %s",
			isCommitmentHistoryEnabled, cfg.KeepExecutionProofs, cfg.KeepExecutionProofs, dirs.Chaindata)
	}
	if err := rawdb.WriteDBCommitmentHistoryEnabled(tx, cfg.KeepExecutionProofs); err != nil {
		return err
	}
	if cfg.KeepExecutionProofs {
		logger.Warn("[experiment] enabling commitment history. this is an experimental flag so run at your own risk!", "enabled", cfg.KeepExecutionProofs)
	}
	return nil
}

const blockBufferSize = 128

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(ctx context.Context, stack *node.Node, config *ethconfig.Config, logger log.Logger, tracer *tracers.Tracer) (*Ethereum, error) {
	if config.Miner.GasPrice == nil || config.Miner.GasPrice.Sign() <= 0 {
		logger.Warn("Sanitizing invalid miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.Defaults.Miner.GasPrice)
		config.Miner.GasPrice = new(big.Int).Set(ethconfig.Defaults.Miner.GasPrice)
	}
	dirs := stack.Config().Dirs

	tmpdir := dirs.Tmp
	if err := RemoveContents(tmpdir); err != nil { // clean it on startup
		return nil, fmt.Errorf("clean tmp dir: %s, %w", tmpdir, err)
	}

	// Assemble the Ethereum object
	rawChainDB, err := node.OpenDatabase(ctx, stack.Config(), kv.ChainDB, "", false, logger)
	if err != nil {
		return nil, err
	}
	latestBlockBuiltStore := builder.NewLatestBlockBuiltStore()

	if err := rawChainDB.Update(context.Background(), func(tx kv.RwTx) error {
		var notChanged bool

		inConfig := config.PersistReceiptsCacheV2
		notChanged, config.PersistReceiptsCacheV2, err = kvcfg.PersistReceipts.EnsureNotChanged(tx, inConfig)
		if err != nil {
			return err
		}
		if !notChanged {
			logger.Warn("--persist.receipt changed since the last run, enabling historical receipts cache. full resync will be required to use the new configuration. if you do not need this feature, ignore this warning.", "inDB", config.PersistReceiptsCacheV2, "inConfig", inConfig)
		}
		if config.PersistReceiptsCacheV2 {
			statecfg.EnableHistoricalRCache()
		}

		if err := checkAndSetCommitmentHistoryFlag(tx, logger, dirs, config); err != nil {
			return err
		}
		if err = stages.UpdateMetrics(tx); err != nil {
			return err
		}

		config.Prune, err = prune.EnsureNotChanged(tx, config.Prune)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	ctx, ctxCancel := context.WithCancel(context.Background())

	// kv_remote architecture does blocks on stream.Send - means current architecture require unlimited amount of txs to provide good throughput
	backend := &Ethereum{
		sentryCtx:                 ctx,
		sentryCancel:              ctxCancel,
		config:                    config,
		networkID:                 config.NetworkID,
		etherbase:                 config.Miner.Etherbase,
		waitForStageLoopStop:      make(chan struct{}),
		waitForMiningStop:         make(chan struct{}),
		blockBuilderNotifyNewTxns: make(chan struct{}, 1),
		miningSealingQuit:         make(chan struct{}),
		minedBlocks:               make(chan *types.Block, 1),
		minedBlockObservers:       event.NewObservers[*types.Block](),
		logger:                    logger,
		stopNode: func() error {
			return stack.Close()
		},
	}

	var chainConfig *chain.Config
	var genesis *types.Block
	if err := rawChainDB.Update(context.Background(), func(tx kv.RwTx) error {

		genesisConfig, err := rawdb.ReadGenesis(tx)
		if err != nil {
			return err
		}

		if genesisConfig != nil {
			config.Genesis = genesisConfig
		}

		if tracer != nil && tracer.Hooks != nil && tracer.Hooks.OnBlockchainInit != nil {
			tracer.Hooks.OnBlockchainInit(config.Genesis.Config)
		}

		h, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			panic(err)
		}
		genesisSpec := config.Genesis
		if h != (common.Hash{}) { // fallback to db content
			genesisSpec = nil
		}
		var genesisErr error
		chainConfig, genesis, genesisErr = genesiswrite.WriteGenesisBlock(tx, genesisSpec, config.OverrideOsakaTime, dirs, logger)
		if _, ok := genesisErr.(*chain.ConfigCompatError); genesisErr != nil && !ok {
			return genesisErr
		}

		return nil
	}); err != nil {
		panic(err)
	}
	chainConfig.AllowAA = config.AllowAA
	backend.chainConfig = chainConfig
	backend.genesisBlock = genesis
	backend.genesisHash = genesis.Hash()

	setDefaultMinerGasLimit(chainConfig, config, logger)

	setBorDefaultMinerGasPrice(chainConfig, config, logger)
	setBorDefaultTxPoolPriceLimit(chainConfig, config.TxPool, logger)

	logger.Info("Initialised chain configuration", "config", chainConfig, "genesis", genesis.Hash())
	if dbg.OnlyCreateDB {
		logger.Info("done")
		os.Exit(1)
	}

	segmentsBuildLimiter := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))

	// Check if we have an already initialized chain and fall back to
	// that if so. Otherwise we need to generate a new genesis spec.
	blockReader, blockWriter, allSnapshots, allBorSnapshots, bridgeStore, heimdallStore, temporalDb, err := setUpBlockReader(ctx, rawChainDB, config.Dirs, config, chainConfig, stack.Config(), logger, segmentsBuildLimiter)
	if err != nil {
		return nil, err
	}
	backend.blockSnapshots, backend.blockReader, backend.blockWriter = allSnapshots, blockReader, blockWriter
	backend.chainDB = temporalDb

	// Can happen in some configurations
	if err := backend.setUpSnapDownloader(ctx, stack.Config(), config.Downloader, chainConfig); err != nil {
		return nil, err
	}

	kvRPC := remotedbserver.NewKvServer(ctx, backend.chainDB, allSnapshots, allBorSnapshots, temporalDb.Debug(), logger)
	backend.notifications = shards.NewNotifications(kvRPC)
	backend.kvRPC = kvRPC

	backend.gasPrice, _ = uint256.FromBig(config.Miner.GasPrice)

	if config.SilkwormExecution || config.SilkwormRpcDaemon || config.SilkwormSentry {
		logLevel, err := log.LvlFromString(config.SilkwormVerbosity)
		if err != nil {
			return nil, err
		}
		backend.silkworm, err = silkworm.New(config.Dirs.DataDir, mdbx.Version(), config.SilkwormNumContexts, logLevel)
		if err != nil {
			return nil, err
		}
	}

	p2pConfig := stack.Config().P2P
	var sentries []protosentry.SentryClient
	if len(p2pConfig.SentryAddr) > 0 {
		for _, addr := range p2pConfig.SentryAddr {
			sentryClient, err := sentry_multi_client.GrpcClient(backend.sentryCtx, addr)
			if err != nil {
				return nil, err
			}
			sentries = append(sentries, sentryClient)
		}
	} else if config.SilkwormSentry {
		apiPort := 53774
		apiAddr := fmt.Sprintf("127.0.0.1:%d", apiPort)

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

		silkwormSentryService := silkworm.NewSentryService(backend.silkworm, settings)
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

		p2pConfig.DiscoveryDNS = backend.config.EthDiscoveryURLs

		listenHost, listenPort, err := splitAddrIntoHostAndPort(p2pConfig.ListenAddr)
		if err != nil {
			return nil, err
		}

		var pi int // points to next port to be picked from refCfg.AllowedPorts
		for _, protocol := range p2pConfig.ProtocolVersion {
			cfg := p2pConfig
			cfg.NodeDatabase = filepath.Join(stack.Config().Dirs.Nodes, eth.ProtocolToString[protocol])

			// pick port from allowed list
			var picked bool
			for ; pi < len(cfg.AllowedPorts); pi++ {
				pc := int(cfg.AllowedPorts[pi])
				if pc == 0 {
					// For ephemeral ports probing to see if the port is taken does not
					// make sense.
					picked = true
					break
				}
				if !checkPortIsFree(fmt.Sprintf("%s:%d", listenHost, pc)) {
					logger.Warn("bind protocol to port has failed: port is busy", "protocols", fmt.Sprintf("eth/%d", cfg.ProtocolVersion), "port", pc)
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

			// TODO: Auto-enable WIT protocol for Bor chains if not explicitly set
			server := sentry.NewGrpcServer(backend.sentryCtx, nil, readNodeInfo, &cfg, protocol, logger)
			backend.sentryServers = append(backend.sentryServers, server)
			sentries = append(sentries, direct.NewSentryClientDirect(protocol, server))
		}

		go func() {
			logEvery := time.NewTicker(90 * time.Second)
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
					if len(peerCountMap) == 0 {
						logger.Warn("[p2p] No GoodPeers")
					} else {
						for protocol, count := range peerCountMap {
							logItems = append(logItems, eth.ProtocolToString[protocol], strconv.Itoa(count))
						}
						logger.Info("[p2p] GoodPeers", logItems...)
					}
				}
			}
		}()
	}

	// setup periodic logging and prometheus updates
	go mem.LogMemStats(ctx, logger)
	go disk.UpdateDiskStats(ctx, logger)
	go dbg.SaveHeapProfileNearOOMPeriodically(ctx, dbg.SaveHeapWithLogger(&logger))
	go kv.CollectTableSizesPeriodically(ctx, backend.chainDB, kv.ChainDB, logger)

	var currentBlock *types.Block
	if err := backend.chainDB.View(context.Background(), func(tx kv.Tx) error {
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
		consensusConfig = chainConfig.Bor
	} else {
		consensusConfig = &config.Ethash
	}

	var heimdallClient heimdall.Client
	var bridgeClient bridge.Client
	var polygonBridge *bridge.Service
	var heimdallService *heimdall.Service
	var bridgeRPC *bridge.BackendServer
	var heimdallRPC *heimdall.BackendServer

	if chainConfig.Bor != nil {
		if !config.WithoutHeimdall {
			heimdallClient = heimdall.NewHttpClient(config.HeimdallURL, logger, poshttp.WithApiVersioner(ctx))
			bridgeClient = bridge.NewHttpClient(config.HeimdallURL, logger, poshttp.WithApiVersioner(ctx))
		} else {
			heimdallClient = heimdall.NewIdleClient(config.Miner)
			bridgeClient = bridge.NewIdleClient()
		}
		borConfig := consensusConfig.(*borcfg.BorConfig)

		polygonBridge = bridge.NewService(bridge.ServiceConfig{
			Store:        bridgeStore,
			Logger:       logger,
			BorConfig:    borConfig,
			EventFetcher: bridgeClient,
		})

		if err := heimdallStore.Milestones().Prepare(ctx); err != nil {
			return nil, err
		}

		_, err := heimdallStore.Milestones().DeleteFromBlockNum(ctx, 0)
		if err != nil {
			return nil, err
		}

		heimdallService = heimdall.NewService(heimdall.ServiceConfig{
			Store:     heimdallStore,
			BorConfig: borConfig,
			Client:    heimdallClient,
			Logger:    logger,
		})

		bridgeRPC = bridge.NewBackendServer(ctx, polygonBridge)
		heimdallRPC = heimdall.NewBackendServer(ctx, heimdallService)

		backend.polygonBridge = polygonBridge
		backend.heimdallService = heimdallService
	}

	backend.engine = ethconsensusconfig.CreateConsensusEngine(ctx, stack.Config(), chainConfig, consensusConfig, config.Miner.Notify, config.Miner.Noverify, config.WithoutHeimdall, blockReader, false /* readonly */, logger, polygonBridge, heimdallService)

	inMemoryExecution := func(txc wrap.TxContainer, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		terseLogger := log.New()
		terseLogger.SetHandler(log.LvlFilterHandler(log.LvlWarn, log.StderrHandler))
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync := stages2.NewInMemoryExecution(backend.sentryCtx, backend.chainDB, config, backend.sentriesClient,
			dirs, notifications, blockReader, blockWriter, backend.silkworm, terseLogger)
		chainReader := consensuschain.NewReader(chainConfig, txc.Tx, blockReader, logger)
		// We start the mining step
		if err := stages2.StateStep(ctx, chainReader, backend.engine, txc, stateSync, header, body, unwindPoint, headersChain, bodiesChain, config.ImportMode); err != nil {
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
	backend.forkValidator = engine_helpers.NewForkValidator(ctx, currentBlockNumber, inMemoryExecution, tmpdir, backend.blockReader)

	statusDataProvider := sentry.NewStatusDataProvider(
		backend.chainDB,
		chainConfig,
		genesis,
		backend.config.NetworkID,
		logger,
	)

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

	sentryMcDisableBlockDownload := chainConfig.Bor != nil || config.ElBlockDownloaderV2
	backend.sentriesClient, err = sentry_multi_client.NewMultiClient(
		backend.chainDB,
		chainConfig,
		backend.engine,
		sentries,
		config.Sync,
		blockReader,
		blockBufferSize,
		statusDataProvider,
		stack.Config().SentryLogPeerInfo,
		maxBlockBroadcastPeers,
		sentryMcDisableBlockDownload,
		stack.Config().P2P.EnableWitProtocol,
		logger,
	)
	if err != nil {
		return nil, err
	}

	var ethashApi *ethash.API
	if casted, ok := backend.engine.(*ethash.Ethash); ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}

	backend.miningRPC = privateapi2.NewMiningServer(ctx, backend, ethashApi, logger)
	backend.ethBackendRPC = privateapi2.NewEthBackendServer(
		ctx,
		backend,
		backend.chainDB,
		backend.notifications,
		blockReader,
		bridgeStore,
		logger,
		latestBlockBuiltStore,
		chainConfig,
	)

	backend.stateDiffClient = direct.NewStateDiffClientDirect(kvRPC)
	var txnProvider txnprovider.TxnProvider
	if config.TxPool.Disable {
		backend.txPoolGrpcServer = &txpool.GrpcDisabled{}
	} else {
		sentries := backend.sentriesClient.Sentries()
		blockBuilderNotifyNewTxns := func() {
			select {
			case backend.blockBuilderNotifyNewTxns <- struct{}{}:
			default:
			}
		}
		backend.txPool, backend.txPoolGrpcServer, err = txpool.Assemble(
			ctx,
			config.TxPool,
			backend.chainDB,
			kvcache.NewDummy(),
			sentries,
			backend.stateDiffClient,
			blockBuilderNotifyNewTxns,
			logger,
			direct.NewEthBackendClientDirect(backend.ethBackendRPC),
		)
		if err != nil {
			return nil, err
		}

		txnProvider = backend.txPool
	}

	httpRpcCfg := stack.Config().Http
	ethRpcClient, txPoolRpcClient, miningRpcClient, rpcDaemonStateCache, rpcFilters := rpcdaemoncli.EmbeddedServices(
		ctx,
		backend.chainDB,
		httpRpcCfg.StateCache,
		httpRpcCfg.RpcFiltersConfig,
		blockReader,
		backend.ethBackendRPC,
		backend.txPoolGrpcServer,
		backend.miningRPC,
		backend.stateDiffClient,
		logger,
	)
	backend.ethRpcClient = ethRpcClient
	backend.txPoolRpcClient = txPoolRpcClient
	backend.miningRpcClient = miningRpcClient
	backend.rpcDaemonStateCache = rpcDaemonStateCache
	backend.rpcFilters = rpcFilters

	if config.Shutter.Enabled {
		if config.TxPool.Disable {
			panic("can't enable shutter pool when devp2p txpool is disabled")
		}

		baseApi := jsonrpc.NewBaseApi(
			backend.rpcFilters,
			backend.rpcDaemonStateCache,
			blockReader,
			httpRpcCfg.WithDatadir,
			httpRpcCfg.EvmCallTimeout,
			backend.engine,
			httpRpcCfg.Dirs,
			backend.polygonBridge,
		)
		ethApi := jsonrpc.NewEthAPI(
			baseApi,
			backend.chainDB,
			backend.ethRpcClient,
			backend.txPoolRpcClient,
			backend.miningRpcClient,
			httpRpcCfg.Gascap,
			httpRpcCfg.Feecap,
			httpRpcCfg.ReturnDataLimit,
			httpRpcCfg.AllowUnprotectedTxs,
			httpRpcCfg.MaxGetProofRewindBlockCount,
			httpRpcCfg.WebsocketSubscribeLogsChannelSize,
			logger,
		)
		contractBackend := contracts.NewDirectBackend(ethApi)
		baseTxnProvider := backend.txPool
		currentBlockNumReader := func(ctx context.Context) (*uint64, error) {
			tx, err := backend.chainDB.BeginRo(ctx)
			if err != nil {
				return nil, err
			}

			defer tx.Rollback()
			return chain.CurrentBlockNumber(tx)
		}
		backend.shutterPool = shutter.NewPool(
			logger,
			config.Shutter,
			baseTxnProvider,
			contractBackend,
			backend.stateDiffClient,
			currentBlockNumReader,
		)
		txnProvider = backend.shutterPool
	}

	miner := stagedsync.NewMiningState(&config.Miner)
	backend.pendingBlocks = miner.PendingResultCh

	var signatures *lru.ARCCache[common.Hash, common.Address]

	if bor, ok := backend.engine.(*bor.Bor); ok {
		signatures = bor.Signatures
	}

	astridEnabled := chainConfig.Bor != nil

	// proof-of-work mining
	mining := stagedsync.New(
		config.Sync,
		stagedsync.MiningStages(backend.sentryCtx,
			stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miner, backend.chainConfig, backend.engine, nil, tmpdir, backend.blockReader),
			stagedsync.StageExecuteBlocksCfg(
				backend.chainDB,
				config.Prune,
				config.BatchSize,
				chainConfig,
				backend.engine,
				&vm.Config{},
				backend.notifications,
				config.StateStream,
				/*stateStream=*/ false,
				dirs,
				blockReader,
				backend.sentriesClient.Hd,
				config.Genesis,
				config.Sync,
				stages2.SilkwormForExecutionStage(backend.silkworm, config),
			),
			stagedsync.StageSendersCfg(backend.chainDB, chainConfig, config.Sync, false, dirs.Tmp, config.Prune, blockReader, backend.sentriesClient.Hd),
			stagedsync.StageMiningExecCfg(backend.chainDB, miner, backend.notifications.Events, backend.chainConfig, backend.engine, &vm.Config{}, tmpdir, nil, 0, txnProvider, blockReader),
			stagedsync.StageMiningFinishCfg(backend.chainDB, backend.chainConfig, backend.engine, miner, backend.miningSealingQuit, backend.blockReader, latestBlockBuiltStore),
			astridEnabled,
		), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder,
		logger, stages.ModeBlockProduction)

	// proof-of-stake mining
	assembleBlockPOS := func(param *core.BlockBuilderParameters, interrupt *int32) (*types.BlockWithReceipts, error) {
		miningStatePos := stagedsync.NewMiningState(&config.Miner)
		miningStatePos.MiningConfig.Etherbase = param.SuggestedFeeRecipient
		proposingSync := stagedsync.New(
			config.Sync,
			stagedsync.MiningStages(backend.sentryCtx,
				stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miningStatePos, backend.chainConfig, backend.engine, param, tmpdir, backend.blockReader),
				stagedsync.StageExecuteBlocksCfg(
					backend.chainDB,
					config.Prune,
					config.BatchSize,
					chainConfig,
					backend.engine,
					&vm.Config{},
					backend.notifications,
					config.StateStream,
					/*stateStream=*/ false,
					dirs,
					blockReader,
					backend.sentriesClient.Hd,
					config.Genesis,
					config.Sync,
					stages2.SilkwormForExecutionStage(backend.silkworm, config),
				),
				stagedsync.StageSendersCfg(backend.chainDB, chainConfig, config.Sync, false, dirs.Tmp, config.Prune, blockReader, backend.sentriesClient.Hd),
				stagedsync.StageMiningExecCfg(backend.chainDB, miningStatePos, backend.notifications.Events, backend.chainConfig, backend.engine, &vm.Config{}, tmpdir, interrupt, param.PayloadId, txnProvider, blockReader),
				stagedsync.StageMiningFinishCfg(backend.chainDB, backend.chainConfig, backend.engine, miningStatePos, backend.miningSealingQuit, backend.blockReader, latestBlockBuiltStore),
				astridEnabled,
			), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder, logger, stages.ModeBlockProduction)
		// We start the mining step
		if err := stages2.MiningStep(ctx, backend.chainDB, proposingSync, tmpdir, logger); err != nil {
			return nil, err
		}
		block := <-miningStatePos.MiningResultCh
		return block, nil
	}

	blockRetire := freezeblocks.NewBlockRetire(1, dirs, blockReader, blockWriter, backend.chainDB, heimdallStore, bridgeStore, backend.chainConfig, config, backend.notifications.Events, segmentsBuildLimiter, logger)
	var creds credentials.TransportCredentials
	if stack.Config().PrivateApiAddr != "" {
		if stack.Config().TLSConnection {
			creds, err = grpcutil.TLS(stack.Config().TLSCACert, stack.Config().TLSCertFile, stack.Config().TLSKeyFile)
			if err != nil {
				return nil, err
			}
		}
		backend.privateAPI, err = privateapi2.StartGrpc(
			kvRPC,
			backend.ethBackendRPC,
			backend.txPoolGrpcServer,
			backend.miningRPC,
			bridgeRPC,
			heimdallRPC,
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

	go func() {
		defer dbg.LogPanic()
		for {
			select {
			case b := <-backend.minedBlocks:
				if !sentryMcDisableBlockDownload {
					// Add mined header and block body before broadcast. This is because the broadcast call
					// will trigger the staged sync which will require headers and blocks to be available
					// in their respective cache in the download stage. If not found, it would cause a
					// liveness issue for the chain.
					if err := backend.sentriesClient.Hd.AddMinedHeader(b.Header()); err != nil {
						logger.Error("add mined block to header downloader", "err", err)
					}
					backend.sentriesClient.Bd.AddToPrefetch(b.Header(), b.RawBody())
				}

				backend.minedBlockObservers.Notify(b)

				//p2p
				//backend.sentriesClient.BroadcastNewBlock(context.Background(), b, b.Difficulty())
				//rpcdaemon
				if err := backend.miningRPC.BroadcastMinedBlock(b); err != nil {
					logger.Error("txpool rpc mined block broadcast", "err", err)
				}

			case b := <-backend.pendingBlocks:
				if err := backend.miningRPC.BroadcastPendingBlock(b); err != nil {
					logger.Error("txpool rpc pending block broadcast", "err", err)
				}
			case <-backend.sentriesClient.Hd.QuitPoWMining:
				return
			}
		}
	}()

	if err := backend.StartMining(
		ctx,
		backend.chainDB,
		backend.stateDiffClient,
		mining,
		miner,
		backend.gasPrice,
		backend.sentriesClient.Hd.QuitPoWMining,
		heimdallStore,
		tmpdir,
		logger); err != nil {
		return nil, err
	}

	backend.syncStages = stages2.NewDefaultStages(backend.sentryCtx, backend.chainDB, p2pConfig, config, backend.sentriesClient, backend.notifications, backend.downloaderClient,
		blockReader, blockRetire, backend.silkworm, backend.forkValidator, signatures, logger, tracer)
	backend.syncUnwindOrder = stagedsync.DefaultUnwindOrder
	backend.syncPruneOrder = stagedsync.DefaultPruneOrder

	backend.stagedSync = stagedsync.New(config.Sync, backend.syncStages, backend.syncUnwindOrder, backend.syncPruneOrder, logger, stages.ModeApplyingBlocks)

	hook := stages2.NewHook(backend.sentryCtx, backend.chainDB, backend.notifications, backend.stagedSync, backend.blockReader, backend.chainConfig, backend.logger, backend.sentriesClient.SetStatus)

	pipelineStages := stages2.NewPipelineStages(ctx, backend.chainDB, config, backend.sentriesClient, backend.notifications, backend.downloaderClient, blockReader, blockRetire, backend.silkworm, backend.forkValidator, tracer)
	backend.pipelineStagedSync = stagedsync.New(config.Sync, pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, logger, stages.ModeApplyingBlocks)
	backend.eth1ExecutionServer = eth1.NewEthereumExecutionModule(blockReader, backend.chainDB, backend.pipelineStagedSync, backend.forkValidator, chainConfig, assembleBlockPOS, hook, backend.notifications.Accumulator, backend.notifications.RecentLogs, backend.notifications.StateChangesConsumer, logger, backend.engine, config.Sync, ctx)
	executionRpc := direct.NewExecutionClientDirect(backend.eth1ExecutionServer)

	var executionEngine executionclient.ExecutionEngine

	executionEngine, err = executionclient.NewExecutionClientDirect(eth1_chain_reader.NewChainReaderEth1(chainConfig, executionRpc, 1000))
	if err != nil {
		return nil, err
	}

	engineBackendRPC := engineapi.NewEngineServer(
		logger,
		chainConfig,
		executionRpc,
		backend.sentriesClient.Hd,
		engine_block_downloader.NewEngineBlockDownloader(ctx,
			logger, backend.sentriesClient.Hd, executionRpc,
			backend.sentriesClient.Bd, backend.sentriesClient.BroadcastNewBlock, backend.sentriesClient.SendBodyRequest, blockReader,
			backend.chainDB, chainConfig, tmpdir, config.Sync, config.ElBlockDownloaderV2, sentryMux(sentries), statusDataProvider),
		config.InternalCL && !config.CaplinConfig.EnableEngineAPI, // If the chain supports the engine API, then we should not make the server fail.
		false,
		config.Miner.EnabledPOS,
		!config.PolygonPosSingleSlotFinality,
	)
	backend.engineBackendRPC = engineBackendRPC
	// If we choose not to run a consensus layer, run our embedded.
	if config.InternalCL && (clparams.EmbeddedSupported(config.NetworkID) || config.CaplinConfig.IsDevnet()) {
		config.CaplinConfig.NetworkId = clparams.NetworkType(config.NetworkID)
		config.CaplinConfig.LoopBlockLimit = uint64(config.LoopBlockLimit)
		if config.CaplinConfig.EnableEngineAPI {
			jwtSecretHex, err := os.ReadFile(httpRpcCfg.JWTSecretPath)
			if err != nil {
				logger.Error("failed to read jwt secret", "err", err, "path", httpRpcCfg.JWTSecretPath)
				return nil, err
			}
			jwtSecret := common.FromHex(strings.TrimSpace(string(jwtSecretHex)))
			executionEngine, err = executionclient.NewExecutionClientRPC(jwtSecret, httpRpcCfg.AuthRpcHTTPListenAddress, httpRpcCfg.AuthRpcPort)
			if err != nil {
				logger.Error("failed to create execution client", "err", err)
				return nil, err
			}
		}
		go func() {
			eth1Getter := getters.NewExecutionSnapshotReader(ctx, blockReader, backend.chainDB)
			if err := caplin1.RunCaplinService(ctx, executionEngine, config.CaplinConfig, dirs, eth1Getter, backend.downloaderClient, creds, segmentsBuildLimiter); err != nil {
				logger.Error("could not start caplin", "err", err)
			}
			ctxCancel()
		}()
	}

	if chainConfig.Bor != nil {
		backend.polygonSyncService = polygonsync.NewService(
			config,
			logger,
			chainConfig,
			sentryMux(sentries),
			p2pConfig.MaxPeers,
			statusDataProvider,
			executionRpc,
			config.LoopBlockLimit,
			polygonBridge,
			heimdallService,
			backend.notifications,
			backend.engineBackendRPC,
			backend,
		)

		// we need to initiate download before the heimdall services start rather than
		// waiting for the stage loop to start

		if !config.Snapshot.NoDownloader && backend.downloaderClient == nil {
			panic("expect to have non-nil downloaderClient")
		}
		backend.polygonDownloadSync = stagedsync.New(backend.config.Sync, stagedsync.DownloadSyncStages(
			backend.sentryCtx, stagedsync.StageSnapshotsCfg(
				backend.chainDB, backend.sentriesClient.ChainConfig, config.Sync, dirs, blockRetire, backend.downloaderClient,
				blockReader, backend.notifications, false, false, false, backend.silkworm, config.Prune,
			)), nil, nil, backend.logger, stages.ModeApplyingBlocks)

		// these range extractors set the db to the local db instead of the chain db
		// TODO this needs be refactored away by having a retire/merge component per
		// snapshot instead of global processing in the stage loop
		type extractableStore interface {
			RangeExtractor() snaptype.RangeExtractor
		}

		if withRangeExtractor, ok := heimdallStore.Spans().(extractableStore); ok {
			allBorSnapshots.SetRangeExtractor(heimdall.Spans, withRangeExtractor.RangeExtractor())
		}

		if withRangeExtractor, ok := heimdallStore.Checkpoints().(extractableStore); ok {
			allBorSnapshots.SetRangeExtractor(heimdall.Checkpoints, withRangeExtractor.RangeExtractor())
		}

		if withRangeExtractor, ok := heimdallStore.Milestones().(extractableStore); ok {
			allBorSnapshots.SetRangeExtractor(heimdall.Milestones, withRangeExtractor.RangeExtractor())
		}

		if withRangeExtractor, ok := bridgeStore.(extractableStore); ok {
			allBorSnapshots.SetRangeExtractor(heimdall.Events, withRangeExtractor.RangeExtractor())
		}
	}

	go func() {
		if err := temporalDb.Debug().MergeLoop(ctx); err != nil {
			logger.Error("snapashot merge loop error", "err", err)
		}
	}()

	return backend, nil
}

func (s *Ethereum) Init(stack *node.Node, config *ethconfig.Config, chainConfig *chain.Config) error {
	blockReader := s.blockReader
	ctx := s.sentryCtx
	chainKv := s.chainDB
	var err error

	if chainConfig.Bor == nil {
		if !config.ElBlockDownloaderV2 {
			s.sentriesClient.Hd.StartPoSDownloader(s.sentryCtx, s.sentriesClient.SendHeaderRequest, s.sentriesClient.Penalize)
		}
	}

	emptyBadHash := config.BadBlockHash == common.Hash{}
	if !emptyBadHash {
		if err = chainKv.View(ctx, func(tx kv.Tx) error {
			badBlockHeader, hErr := rawdb.ReadHeaderByHash(tx, config.BadBlockHash)
			if badBlockHeader != nil {
				unwindPoint := badBlockHeader.Number.Uint64() - 1
				if err := s.stagedSync.UnwindTo(unwindPoint, stagedsync.BadBlock(config.BadBlockHash, errors.New("Init unwind")), tx); err != nil {
					return err
				}
			}
			return hErr
		}); err != nil {
			return err
		}
	}

	//eth.APIBackend = &EthAPIBackend{stack.Config().ExtRPCEnabled(), stack.Config().AllowUnprotectedTxs, eth, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.Miner.GasPrice
	}
	// start HTTP API
	httpRpcCfg := stack.Config().Http
	//eth.APIBackend.gpo = gasprice.NewOracle(eth.APIBackend, gpoParams)
	if config.Ethstats != "" {
		var headCh chan [][]byte
		headCh, s.unsubscribeEthstat = s.notifications.Events.AddHeaderSubscription()
		if err := ethstats.New(stack, s.sentryServers, chainKv, s.blockReader, s.engine, config.Ethstats, s.networkID, ctx.Done(), headCh, s.txPoolRpcClient); err != nil {
			return err
		}
	}

	s.apiList = jsonrpc.APIList(chainKv, s.ethRpcClient, s.txPoolRpcClient, s.miningRpcClient, s.rpcFilters, s.rpcDaemonStateCache, blockReader, &httpRpcCfg, s.engine, s.logger, s.polygonBridge, s.heimdallService)

	if config.SilkwormRpcDaemon && httpRpcCfg.Enabled {
		interface_log_settings := silkworm.RpcInterfaceLogSettings{
			Enabled:         config.SilkwormRpcLogEnabled,
			ContainerFolder: config.SilkwormRpcLogDirPath,
			MaxFileSizeMB:   config.SilkwormRpcLogMaxFileSize,
			MaxFiles:        config.SilkwormRpcLogMaxFiles,
			DumpResponse:    config.SilkwormRpcLogDumpResponse,
		}
		settings := silkworm.RpcDaemonSettings{
			EthLogSettings:       interface_log_settings,
			EthAPIHost:           httpRpcCfg.HttpListenAddress,
			EthAPIPort:           httpRpcCfg.HttpPort,
			EthAPISpec:           httpRpcCfg.API,
			NumWorkers:           config.SilkwormRpcNumWorkers,
			CORSDomains:          httpRpcCfg.HttpCORSDomain,
			JWTFilePath:          httpRpcCfg.JWTSecretPath,
			JSONRPCCompatibility: config.SilkwormRpcJsonCompatibility,
			WebSocketEnabled:     httpRpcCfg.WebsocketEnabled,
			WebSocketCompression: httpRpcCfg.WebsocketCompression,
			HTTPCompression:      httpRpcCfg.HttpCompression,
		}
		silkwormRPCDaemonService := silkworm.NewRpcDaemonService(s.silkworm, chainKv, settings)
		s.silkwormRPCDaemonService = &silkwormRPCDaemonService
	} else {
		go func() {
			if err := rpcdaemoncli.StartRpcServer(ctx, &httpRpcCfg, s.apiList, s.logger); err != nil {
				s.logger.Error("cli.StartRpcServer error", "err", err)
			}
		}()
	}

	if chainConfig.Bor == nil || config.PolygonPosSingleSlotFinality {
		go s.engineBackendRPC.Start(ctx, &httpRpcCfg, s.chainDB, s.blockReader, s.rpcFilters, s.rpcDaemonStateCache, s.engine, s.ethRpcClient, s.txPoolRpcClient, s.miningRpcClient)
	}

	// Register the backend on the node
	stack.RegisterLifecycle(s)
	return nil
}

func (s *Ethereum) APIs() []rpc.API {
	return s.apiList
}

func (s *Ethereum) Etherbase() (eb common.Address, err error) {
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()

	if etherbase != (common.Address{}) {
		return etherbase, nil
	}
	return common.Address{}, errors.New("etherbase must be explicitly specified")
}

// StartMining starts the miner with the given number of CPU threads. If mining
// is already running, this method adjust the number of threads allowed to use
// and updates the minimum price required by the transaction pool.
func (s *Ethereum) StartMining(ctx context.Context, db kv.RwDB, stateDiffClient *direct.StateDiffClientDirect, mining *stagedsync.Sync, miner stagedsync.MiningState, gasPrice *uint256.Int, quitCh chan struct{}, heimdallStore heimdall.Store, tmpDir string, logger log.Logger) error {

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

	if !miner.MiningConfig.Enabled {
		return nil
	}

	// Configure the local mining address
	eb, err := s.Etherbase()
	if err != nil {
		s.logger.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %w", err)
	}

	if miner.MiningConfig.Enabled {
		if s.chainConfig.ChainName == networkname.Dev {
			miner.MiningConfig.SigKey = core.DevnetSignPrivateKey
		}
		if miner.MiningConfig.SigKey == nil {
			s.logger.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}
		if borcfg != nil {
			borcfg.Authorize(eb, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
				return crypto.Sign(crypto.Keccak256(message), miner.MiningConfig.SigKey)
			})
		} else if s.chainConfig.Consensus == chain.CliqueConsensus {
			s.engine.(*clique.Clique).Authorize(eb, func(_ common.Address, _ string, msg []byte) ([]byte, error) {
				return crypto.Sign(crypto.Keccak256(msg), miner.MiningConfig.SigKey)
			})
		} else {
			s.logger.Error("mining is not supported after the Merge")
			return errors.New("mining is not supported after the Merge")
		}
	} else {
		// for the bor dev network without heimdall we need the authorizer to be set otherwise there is no
		// validator defined in the bor validator set and non mining nodes will reject all blocks
		// this assumes in this mode we're only running a single validator

		if s.chainConfig.ChainName == networkname.BorDevnet && s.config.WithoutHeimdall {
			borcfg.Authorize(eb, func(addr common.Address, _ string, _ []byte) ([]byte, error) {
				return nil, &heimdall.UnauthorizedSignerError{Number: 0, Signer: addr.Bytes()}
			})
		}

		return nil
	}

	streamCtx, streamCancel := context.WithCancel(ctx)
	stream, err := stateDiffClient.StateChanges(streamCtx, &remote.StateChangeRequest{WithStorage: false, WithTransactions: true}, grpc.WaitForReady(true))

	if err != nil {
		streamCancel()
		return err
	}

	stateChangeCh := make(chan *remote.StateChange)

	go func() {
		for req, err := stream.Recv(); ; req, err = stream.Recv() {
			select {
			case <-ctx.Done():
				return
			default:
			}

			if err == nil {
				for _, change := range req.ChangeBatch {
					stateChangeCh <- change
				}
			}
		}
	}()

	go func() {
		defer dbg.LogPanic()
		defer close(s.waitForMiningStop)
		defer streamCancel()

		mineEvery := time.NewTicker(miner.MiningConfig.Recommit)

		defer mineEvery.Stop()

		s.logger.Info("Starting to mine", "etherbase", eb)

		var working bool
		var waiting atomic.Bool

		hasWork := true // Start mining immediately
		errc := make(chan error, 1)

		for {
			// Only reset if some work was done previously as we'd like to rely
			// on the `miner.recommit` as backup.
			if hasWork {
				mineEvery.Reset(miner.MiningConfig.Recommit)
			}

			// Only check for case if you're already mining (i.e. working = true) and
			// waiting for error or you don't have any work yet (i.e. hasWork = false).
			if working || !hasWork {
				select {
				case stateChanges := <-stateChangeCh:
					block := stateChanges.BlockHeight
					s.logger.Debug("Start mining based on previous block", "block", block)
					// TODO - can do mining clean up here as we have previous
					// block info in the state channel
					hasWork = true

				case <-s.blockBuilderNotifyNewTxns:
					//log.Warn("[dbg] blockBuilderNotifyNewTxns")

					// Skip mining based on new txn notif for bor consensus
					hasWork = s.chainConfig.Bor == nil
					if hasWork {
						s.logger.Debug("Start mining based on txpool notif")
					}
				case <-mineEvery.C:
					//log.Warn("[dbg] mineEvery", "working", working, "waiting", waiting.Load())
					if !(working || waiting.Load()) {
						s.logger.Debug("Start mining based on miner.recommit", "duration", miner.MiningConfig.Recommit)
					}
					hasWork = !(working || waiting.Load())
				case err := <-errc:
					working = false
					hasWork = false
					if errors.Is(err, common.ErrStopped) {
						return
					}
					if err != nil {
						s.logger.Warn("mining", "err", err)
					}
				case <-quitCh:
					return
				case <-ctx.Done():
					return
				}
			}

			if !working && hasWork {
				working = true
				hasWork = false
				mineEvery.Reset(miner.MiningConfig.Recommit)
				go func() {
					err = stages2.MiningStep(ctx, db, mining, tmpDir, logger)

					waiting.Store(true)
					defer func() {
						waiting.Store(false)
						errc <- err
					}()

					if err != nil {
						return
					}

					for {
						select {
						case block := <-miner.MiningResultCh:
							if block != nil {
								s.logger.Debug("Mined block", "block", block.Block.Number())
								s.minedBlocks <- block.Block
							}
							return
						case <-ctx.Done():
							errc <- ctx.Err()
							return
						}
					}
				}()
			}
		}
	}()

	return nil
}

func (s *Ethereum) IsMining() bool { return s.config.Miner.Enabled }

func (s *Ethereum) RegisterMinedBlockObserver(callback func(msg *types.Block)) event.UnregisterFunc {
	return s.minedBlockObservers.Register(callback)
}

func (s *Ethereum) ChainKV() kv.RwDB            { return s.chainDB }
func (s *Ethereum) NetVersion() (uint64, error) { return s.networkID, nil }
func (s *Ethereum) NetPeerCount() (uint64, error) {
	var sentryPc uint64 = 0

	s.logger.Trace("sentry", "peer count", sentryPc)
	for _, sc := range s.sentriesClient.Sentries() {
		ctx := context.Background()
		reply, err := sc.PeerCount(ctx, &protosentry.PeerCountRequest{})
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
func (s *Ethereum) setUpSnapDownloader(
	ctx context.Context,
	nodeCfg *nodecfg.Config,
	downloaderCfg *downloadercfg.Cfg,
	cc *chain.Config,
) (err error) {
	s.chainDB.OnFilesChange(func(frozenFileNames []string) {
		s.logger.Warn("files changed...sending notification")
		events := s.notifications.Events
		events.OnNewSnapshot()
		if downloaderCfg != nil && downloaderCfg.ChainName == "" {
			return
		}
		if s.config.Snapshot.NoDownloader || s.downloaderClient == nil || len(frozenFileNames) == 0 {
			return
		}

		req := &protodownloader.AddRequest{Items: make([]*protodownloader.AddItem, 0, len(frozenFileNames))}
		for _, fName := range frozenFileNames {
			req.Items = append(req.Items, &protodownloader.AddItem{
				Path: fName,
			})
		}
		if _, err := s.downloaderClient.Add(ctx, req); err != nil {
			s.logger.Warn("[snapshots] downloader.Add", "err", err)
		}
	}, func(deletedFiles []string) {
		if downloaderCfg != nil && downloaderCfg.ChainName == "" {
			return
		}
		if s.config.Snapshot.NoDownloader || s.downloaderClient == nil || len(deletedFiles) == 0 {
			return
		}

		if _, err := s.downloaderClient.Delete(ctx, &protodownloader.DeleteRequest{Paths: deletedFiles}); err != nil {
			s.logger.Warn("[snapshots] downloader.Delete", "err", err)
		}
	})

	if s.config.Snapshot.NoDownloader {
		return nil
	}

	if s.config.Snapshot.DownloaderAddr != "" {
		// connect to external Downloader
		s.downloaderClient, err = downloadergrpc.NewClient(ctx, s.config.Snapshot.DownloaderAddr)
	} else {
		if downloaderCfg == nil || downloaderCfg.ChainName == "" {
			return nil
		}
		// Always disable the asynchronous adder. We will do it here to support downloader.verify.
		downloaderCfg.AddTorrentsFromDisk = false

		s.downloader, err = downloader.New(ctx, downloaderCfg, s.logger, log.LvlDebug)
		if err != nil {
			return err
		}
		s.downloader.HandleTorrentClientStatus(nodeCfg.DebugMux)

		// start embedded Downloader
		err = s.downloader.AddTorrentsFromDisk(ctx)
		if err != nil {
			return fmt.Errorf("adding torrents from disk: %w", err)
		}

		bittorrentServer, err := downloader.NewGrpcServer(s.downloader)
		if err != nil {
			return fmt.Errorf("new server: %w", err)
		}
		s.downloader.MainLoopInBackground(true)

		s.downloaderClient = direct.NewDownloaderClient(bittorrentServer)
	}
	return err
}

func setUpBlockReader(ctx context.Context, db kv.RwDB, dirs datadir.Dirs, snConfig *ethconfig.Config, chainConfig *chain.Config, nodeConfig *nodecfg.Config, logger log.Logger, blockSnapBuildSema *semaphore.Weighted) (*freezeblocks.BlockReader, *blockio.BlockWriter, *freezeblocks.RoSnapshots, *heimdall.RoSnapshots, bridge.Store, heimdall.Store, kv.TemporalRwDB, error) {
	allSnapshots := freezeblocks.NewRoSnapshots(snConfig.Snapshot, dirs.Snap, logger)

	var allBorSnapshots *heimdall.RoSnapshots
	var bridgeStore bridge.Store
	var heimdallStore heimdall.Store

	if chainConfig.Bor != nil {
		allBorSnapshots = heimdall.NewRoSnapshots(snConfig.Snapshot, dirs.Snap, logger)
		bridgeStore = bridge.NewSnapshotStore(bridge.NewMdbxStore(dirs.DataDir, logger, false, int64(nodeConfig.Http.DBReadConcurrency)), allBorSnapshots, chainConfig.Bor)
		heimdallStore = heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, dirs.DataDir, false, int64(nodeConfig.Http.DBReadConcurrency)), allBorSnapshots)
	}
	blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)

	_, knownSnapCfg := snapcfg.KnownCfg(chainConfig.ChainName)
	createNewSaltFileIfNeeded := snConfig.Snapshot.NoDownloader || snConfig.Snapshot.DisableDownloadE3 || !knownSnapCfg
	salt, err := state.GetStateIndicesSalt(dirs, createNewSaltFileIfNeeded, logger)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	if _, err := snaptype.LoadSalt(dirs.Snap, createNewSaltFileIfNeeded, logger); err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	agg, err := state.NewAggregator2(ctx, dirs, config3.DefaultStepSize, salt, db, logger)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	agg.SetProduceMod(snConfig.Snapshot.ProduceE3)

	allSegmentsDownloadComplete, err := rawdb.AllSegmentsDownloadCompleteFromDB(db)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	if allSegmentsDownloadComplete {
		allSnapshots.OptimisticalyOpenFolder()
		if chainConfig.Bor != nil {
			allBorSnapshots.OptimisticalyOpenFolder()
		}
		_ = agg.OpenFolder()
	} else {
		logger.Debug("[rpc] download of segments not complete yet. please wait StageSnapshots to finish")
	}

	temporalDb, err := temporal.New(db, agg)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}

	blockWriter := blockio.NewBlockWriter()

	return blockReader, blockWriter, allSnapshots, allBorSnapshots, bridgeStore, heimdallStore, temporalDb, nil
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

func (s *Ethereum) AddPeer(ctx context.Context, req *remote.AddPeerRequest) (*remote.AddPeerReply, error) {
	for _, sentryClient := range s.sentriesClient.Sentries() {
		_, err := sentryClient.AddPeer(ctx, &protosentry.AddPeerRequest{Url: req.Url})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.AddPeers error: %w", err)
		}
	}
	return &remote.AddPeerReply{Success: true}, nil
}

func (s *Ethereum) RemovePeer(ctx context.Context, req *remote.RemovePeerRequest) (*remote.RemovePeerReply, error) {
	for _, sentryClient := range s.sentriesClient.Sentries() {
		_, err := sentryClient.RemovePeer(ctx, &protosentry.RemovePeerRequest{Url: req.Url})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.RemovePeers error: %w", err)
		}
	}
	return &remote.RemovePeerReply{Success: true}, nil
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

	hook := stages2.NewHook(s.sentryCtx, s.chainDB, s.notifications, s.stagedSync, s.blockReader, s.chainConfig, s.logger, s.sentriesClient.SetStatus)

	currentTDProvider := func() *big.Int {
		currentTD, err := readCurrentTotalDifficulty(s.sentryCtx, s.chainDB, s.blockReader)
		if err != nil {
			panic(err)
		}
		return currentTD
	}

	if chainspec.IsChainPoS(s.chainConfig, currentTDProvider) {
		diaglib.Send(diaglib.SyncStageList{StagesList: diaglib.InitStagesFromList(s.pipelineStagedSync.StagesIdsList())})
		s.waitForStageLoopStop = nil // TODO: Ethereum.Stop should wait for execution_server shutdown
		go s.eth1ExecutionServer.Start(s.sentryCtx)
	} else if s.chainConfig.Bor != nil {
		diaglib.Send(diaglib.SyncStageList{StagesList: diaglib.InitStagesFromList(s.stagedSync.StagesIdsList())})
		s.waitForStageLoopStop = nil // Shutdown is handled by context
		s.bgComponentsEg.Go(func() error {
			defer s.logger.Info("[polygon.sync] goroutine terminated")
			// when we're running in stand alone mode we need to run the downloader before we start the
			// polygon services because they will wait for it to complete before opening their stores
			// which make use of snapshots and expect them to be initialize
			// TODO: get the snapshots to call the downloader directly - which will avoid this
			go func() {
				err := stages2.StageLoopIteration(s.sentryCtx, s.chainDB, wrap.NewTxContainer(nil, nil), s.polygonDownloadSync, true, true, s.logger, s.blockReader, hook)

				if err != nil && !errors.Is(err, context.Canceled) {
					s.logger.Error("[polygon.sync] downloader stage crashed - stopping node", "err", err)
					err = s.stopNode()
					if err != nil {
						s.logger.Error("could not stop node", "err", err)
					}
				}
			}()

			ctx := s.sentryCtx
			err := s.polygonSyncService.Run(ctx)
			if err == nil || errors.Is(err, context.Canceled) {
				return err
			}

			s.logger.Error("[polygon.sync] crashed - stopping node", "err", err)
			go func() { // call stopNode in another goroutine to avoid deadlock
				stopErr := s.stopNode()
				if stopErr != nil {
					s.logger.Error("[polygon.sync] could not stop node", "err", stopErr)
				}
			}()

			return err
		})
	} else {
		diaglib.Send(diaglib.SyncStageList{StagesList: diaglib.InitStagesFromList(s.stagedSync.StagesIdsList())})
		go stages2.StageLoop(s.sentryCtx, s.chainDB, s.stagedSync, s.sentriesClient.Hd, s.waitForStageLoopStop, s.config.Sync.LoopThrottle, s.logger, s.blockReader, hook)
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

	if s.txPool != nil {
		// We start the transaction pool on startup, for a couple of reasons:
		// 1) Hive tests requires us to do so and starting it from eth_sendRawTransaction is not viable as we have not enough data
		// to initialize it properly.
		// 2) we cannot propose for block 1 regardless.
		s.bgComponentsEg.Go(func() error {
			defer s.logger.Info("[devp2p] txn pool goroutine terminated")
			err := s.txPool.Run(s.sentryCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("[devp2p] Run error", "err", err)
			}
			return err
		})
	}

	if s.shutterPool != nil {
		s.bgComponentsEg.Go(func() error {
			defer s.logger.Info("[shutter] pool goroutine terminated")
			err := s.shutterPool.Run(s.sentryCtx)
			if err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("[shutter] Run error", "err", err)
			}
			return err
		})
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
	common.SafeClose(s.sentriesClient.Hd.QuitPoWMining)
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
		if err := s.silkworm.Close(); err != nil {
			s.logger.Error("silkworm.Close error", "err", err)
		}
	}

	if err := s.bgComponentsEg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("background component error", "err", err)
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

func (s *Ethereum) PipelineStagedSync() *stagedsync.Sync {
	return s.pipelineStagedSync
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

func (s *Ethereum) TxpoolServer() txpoolproto.TxpoolServer {
	return s.txPoolGrpcServer
}

func (s *Ethereum) ExecutionModule() *eth1.EthereumExecutionModule {
	return s.eth1ExecutionServer
}

// RemoveContents is like dir.RemoveAll, but preserve dir itself
func RemoveContents(dirname string) error {
	d, err := os.Open(dirname)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// ignore due to windows
			_ = os.MkdirAll(dirname, 0o755)
			return nil
		}
		return err
	}
	defer d.Close()
	files, err := dir.ReadDir(dirname)
	if err != nil {
		return err
	}
	for _, file := range files {
		err = dir.RemoveAll(filepath.Join(dirname, file.Name()))
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

func (s *Ethereum) DataDir() string {
	return s.config.Dirs.DataDir
}

// setBorDefaultMinerGasPrice enforces Miner.GasPrice to be equal to BorDefaultMinerGasPrice (25gwei by default)
func setBorDefaultMinerGasPrice(chainConfig *chain.Config, config *ethconfig.Config, logger log.Logger) {
	if chainConfig.Bor != nil && (config.Miner.GasPrice == nil || config.Miner.GasPrice.Cmp(ethconfig.BorDefaultMinerGasPrice) != 0) {
		logger.Warn("Sanitizing invalid bor miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.BorDefaultMinerGasPrice)
		config.Miner.GasPrice = ethconfig.BorDefaultMinerGasPrice
	}
}

func setDefaultMinerGasLimit(chainConfig *chain.Config, config *ethconfig.Config, logger log.Logger) {
	if config.Miner.GasLimit == nil {
		gasLimit := ethconfig.DefaultBlockGasLimitByChain(config)
		config.Miner.GasLimit = &gasLimit
	}
}

// setBorDefaultTxPoolPriceLimit enforces MinFeeCap to be equal to BorDefaultTxPoolPriceLimit (25gwei by default)
func setBorDefaultTxPoolPriceLimit(chainConfig *chain.Config, config txpoolcfg.Config, logger log.Logger) {
	if chainConfig.Bor != nil && config.MinFeeCap != txpoolcfg.BorDefaultTxPoolPriceLimit {
		logger.Warn("Sanitizing invalid bor min fee cap", "provided", config.MinFeeCap, "updated", txpoolcfg.BorDefaultTxPoolPriceLimit)
		config.MinFeeCap = txpoolcfg.BorDefaultTxPoolPriceLimit
	}
}

func sentryMux(sentries []protosentry.SentryClient) protosentry.SentryClient {
	return libsentry.NewSentryMultiplexer(sentries)
}

type engineAPISwitcher struct {
	backend *Ethereum
}

func (e *engineAPISwitcher) SetConsuming(consuming bool) {
	if e.backend.engineBackendRPC == nil {
		return
	}

	e.backend.engineBackendRPC.SetConsuming(consuming)
}
