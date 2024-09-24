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

	erigonchain "github.com/gateway-fm/cdk-erigon-lib/chain"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/zk/sequencer"
	"github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/log/v3"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/common/datadir"
	"github.com/gateway-fm/cdk-erigon-lib/common/dir"
	"github.com/gateway-fm/cdk-erigon-lib/direct"
	downloader3 "github.com/gateway-fm/cdk-erigon-lib/downloader"
	"github.com/gateway-fm/cdk-erigon-lib/downloader/downloadercfg"
	"github.com/gateway-fm/cdk-erigon-lib/downloader/downloadergrpc"
	proto_downloader "github.com/gateway-fm/cdk-erigon-lib/gointerfaces/downloader"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces/grpcutil"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces/remote"
	proto_sentry "github.com/gateway-fm/cdk-erigon-lib/gointerfaces/sentry"
	txpool_proto "github.com/gateway-fm/cdk-erigon-lib/gointerfaces/txpool"
	prototypes "github.com/gateway-fm/cdk-erigon-lib/gointerfaces/types"
	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcache"
	"github.com/gateway-fm/cdk-erigon-lib/kv/kvcfg"
	"github.com/gateway-fm/cdk-erigon-lib/kv/remotedbserver"
	libstate "github.com/gateway-fm/cdk-erigon-lib/state"
	types2 "github.com/gateway-fm/cdk-erigon-lib/types"
	"github.com/ledgerwatch/erigon/zk/txpool/txpooluitl"

	"github.com/ledgerwatch/erigon/chain"

	"net/url"
	"path"

	"github.com/0xPolygonHermez/zkevm-data-streamer/datastreamer"
	log2 "github.com/0xPolygonHermez/zkevm-data-streamer/log"
	"github.com/ledgerwatch/erigon/cl/clparams"
	clcore "github.com/ledgerwatch/erigon/cmd/erigon-cl/core"
	"github.com/ledgerwatch/erigon/cmd/lightclient/lightclient"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/commands"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel"
	"github.com/ledgerwatch/erigon/cmd/sentinel/sentinel/service"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/serenity"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state/historyv2read"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethconsensusconfig"
	"github.com/ledgerwatch/erigon/eth/ethutils"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/ethstats"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/smt/pkg/db"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/zk/contracts"
	"github.com/ledgerwatch/erigon/zk/datastream/client"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/l1_cache"
	"github.com/ledgerwatch/erigon/zk/legacy_executor_verifier"
	zkStages "github.com/ledgerwatch/erigon/zk/stages"
	"github.com/ledgerwatch/erigon/zk/syncer"
	txpool2 "github.com/ledgerwatch/erigon/zk/txpool"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/erigon/zk/witness"
	"github.com/ledgerwatch/erigon/zkevm/etherman"
)

// Config contains the configuration options of the ETH protocol.
// Deprecated: use ethconfig.Config instead.
type Config = ethconfig.Config

type PreStartTasks struct {
	WarmUpDataStream bool
}

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
	genesisBlock *types.Block
	genesisHash  libcommon.Hash

	ethBackendRPC      *privateapi.EthBackendServer
	miningRPC          txpool_proto.MiningServer
	stateChangesClient txpool2.StateChangesClient

	miningSealingQuit chan struct{}
	pendingBlocks     chan *types.Block
	minedBlocks       chan *types.Block

	// downloader fields
	sentryCtx      context.Context
	sentryCancel   context.CancelFunc
	sentriesClient *sentry.MultiClient
	sentryServers  []*sentry.GrpcServer

	stagedSync      *stagedsync.Sync
	syncStages      []*stagedsync.Stage
	syncUnwindOrder stagedsync.UnwindOrder
	syncPruneOrder  stagedsync.PruneOrder

	downloaderClient proto_downloader.DownloaderClient

	notifications      *shards.Notifications
	unsubscribeEthstat func()

	waitForStageLoopStop chan struct{}
	waitForMiningStop    chan struct{}

	txPool2DB               kv.RwDB
	txPool2                 *txpool2.TxPool
	newTxs2                 chan types2.Announcements
	txPool2Fetch            *txpool2.Fetch
	txPool2Send             *txpool2.Send
	txPool2GrpcServer       txpool_proto.TxpoolServer
	notifyMiningAboutNewTxs chan struct{}
	forkValidator           *engineapi.ForkValidator
	downloader              *downloader3.Downloader

	agg            *libstate.AggregatorV3
	blockSnapshots *snapshotsync.RoSnapshots
	blockReader    services.FullBlockReader
	kvRPC          *remotedbserver.KvServer

	// zk
	dataStream      *datastreamer.StreamServer
	l1Syncer        *syncer.L1Syncer
	etherManClients []*etherman.Client
	l1Cache         *l1_cache.L1Cache

	preStartTasks *PreStartTasks
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

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(stack *node.Node, config *ethconfig.Config) (*Ethereum, error) {
	if config.Miner.GasPrice == nil || config.Miner.GasPrice.Cmp(libcommon.Big0) <= 0 {
		log.Warn("Sanitizing invalid miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.Defaults.Miner.GasPrice)
		config.Miner.GasPrice = new(big.Int).Set(ethconfig.Defaults.Miner.GasPrice)
	}

	dirs := stack.Config().Dirs
	tmpdir := dirs.Tmp
	if err := RemoveContents(tmpdir); err != nil { // clean it on startup
		return nil, fmt.Errorf("clean tmp dir: %s, %w", tmpdir, err)
	}

	// Assemble the Ethereum object
	chainKv, err := node.OpenDatabase(stack.Config(), kv.ChainDB)
	if err != nil {
		return nil, err
	}

	var currentBlock *types.Block

	// Check if we have an already initialized chain and fall back to
	// that if so. Otherwise we need to generate a new genesis spec.
	var chainConfig *chain.Config
	var genesis *types.Block
	if err := chainKv.Update(context.Background(), func(tx kv.RwTx) error {
		h, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			panic(err)
		}
		genesisSpec := config.Genesis
		if h != (libcommon.Hash{}) { // fallback to db content
			genesisSpec = nil
		}
		var genesisErr error
		chainConfig, genesis, genesisErr = core.WriteGenesisBlock(tx, genesisSpec, config.OverrideShanghaiTime, tmpdir)
		if _, ok := genesisErr.(*erigonchain.ConfigCompatError); genesisErr != nil && !ok {
			return genesisErr
		}

		currentBlock = rawdb.ReadCurrentBlock(tx)
		return nil
	}); err != nil {
		panic(err)
	}

	config.Snapshot.Enabled = config.Sync.UseSnapshots

	log.Info("Initialised chain configuration", "config", chainConfig, "genesis", genesis.Hash())

	if err := chainKv.Update(context.Background(), func(tx kv.RwTx) error {
		if err = stagedsync.UpdateMetrics(tx); err != nil {
			return err
		}

		config.Prune, err = prune.EnsureNotChanged(tx, config.Prune)
		if err != nil {
			return err
		}
		isCorrectSync, useSnapshots, err := snap.EnsureNotChanged(tx, config.Snapshot)
		if err != nil {
			return err
		}

		config.HistoryV3, err = kvcfg.HistoryV3.WriteOnce(tx, config.HistoryV3)
		if err != nil {
			return err
		}

		config.TransactionsV3, err = kvcfg.TransactionsV3.WriteOnce(tx, config.TransactionsV3)
		if err != nil {
			return err
		}

		// if we are in the incorrect syncmode then we change it to the appropriate one
		if !isCorrectSync {
			log.Warn("Incorrect snapshot enablement", "got", config.Sync.UseSnapshots, "change_to", useSnapshots)
			config.Sync.UseSnapshots = useSnapshots
			config.Snapshot.Enabled = ethconfig.UseSnapshotsByChainName(chainConfig.ChainName) && useSnapshots
		}
		log.Info("Effective", "prune_flags", config.Prune.String(), "snapshot_flags", config.Snapshot.String(), "history.v3", config.HistoryV3)

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
		chainConfig:          chainConfig,
		genesisBlock:         genesis,
		genesisHash:          genesis.Hash(),
		waitForStageLoopStop: make(chan struct{}),
		waitForMiningStop:    make(chan struct{}),
		notifications: &shards.Notifications{
			Events:      shards.NewEvents(),
			Accumulator: shards.NewAccumulator(),
		},
		preStartTasks: &PreStartTasks{},
	}
	blockReader, allSnapshots, agg, err := backend.setUpBlockReader(ctx, config.Dirs, config.Snapshot, config.Downloader, backend.notifications.Events, config.TransactionsV3)
	if err != nil {
		return nil, err
	}
	backend.agg, backend.blockSnapshots, backend.blockReader = agg, allSnapshots, blockReader

	if config.HistoryV3 {
		backend.chainDB, err = temporal.New(backend.chainDB, agg, accounts.ConvertV3toV2, historyv2read.RestoreCodeHash, accounts.DecodeIncarnationFromStorage, systemcontracts.SystemContractCodeLookup[chainConfig.ChainName])
		if err != nil {
			return nil, err
		}
		chainKv = backend.chainDB
	}

	kvRPC := remotedbserver.NewKvServer(ctx, chainKv, allSnapshots, agg)
	backend.notifications.StateChangesConsumer = kvRPC
	backend.kvRPC = kvRPC

	backend.gasPrice, _ = uint256.FromBig(config.Miner.GasPrice)

	var sentries []direct.SentryClient
	if len(stack.Config().P2P.SentryAddr) > 0 {
		for _, addr := range stack.Config().P2P.SentryAddr {
			sentryClient, err := sentry.GrpcClient(backend.sentryCtx, addr)
			if err != nil {
				return nil, err
			}
			sentries = append(sentries, sentryClient)
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
				if !checkPortIsFree(fmt.Sprintf("%s:%d", listenHost, pc)) {
					log.Warn("bind protocol to port has failed: port is busy", "protocols", fmt.Sprintf("eth/%d", refCfg.ProtocolVersion), "port", pc)
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

			server := sentry.NewGrpcServer(backend.sentryCtx, discovery, readNodeInfo, &cfg, protocol)
			backend.sentryServers = append(backend.sentryServers, server)
			sentries = append(sentries, direct.NewSentryClientDirect(protocol, server))
		}

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
					log.Info("[p2p] GoodPeers", logItems...)
				}
			}
		}()
	}

	inMemoryExecution := func(batch kv.RwTx, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody,
		notifications *shards.Notifications) error {
		// Needs its own notifications to not update RPC daemon and txpool about pending blocks
		stateSync, err := stages2.NewInMemoryExecution(backend.sentryCtx, backend.chainDB, config, backend.sentriesClient, dirs, notifications, allSnapshots, backend.agg)
		if err != nil {
			return err
		}
		// We start the mining step
		if err := stages2.StateStep(ctx, batch, stateSync, backend.sentriesClient.Bd, header, body, unwindPoint, headersChain, bodiesChain, true /* quiet */); err != nil {
			log.Warn("Could not validate block", "err", err)
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
	currentBlockNumber := uint64(0)
	if currentBlock != nil {
		currentBlockNumber = currentBlock.NumberU64()
	}

	log.Info("Initialising Ethereum protocol", "network", config.NetworkID)
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
	backend.engine = ethconsensusconfig.CreateConsensusEngine(chainConfig, consensusConfig, config.Miner.Notify, config.Miner.Noverify, config.HeimdallgRPCAddress, config.HeimdallURL, config.WithoutHeimdall, stack.DataDir(), allSnapshots, false /* readonly */, backend.chainDB)
	backend.forkValidator = engineapi.NewForkValidator(currentBlockNumber, inMemoryExecution, tmpdir)

	backend.sentriesClient, err = sentry.NewMultiClient(
		chainKv,
		stack.Config().NodeName(),
		chainConfig,
		genesis.Hash(),
		backend.engine,
		backend.config.NetworkID,
		sentries,
		config.Sync,
		blockReader,
		stack.Config().SentryLogPeerInfo,
		backend.forkValidator,
		config.DropUselessPeers,
	)
	if err != nil {
		return nil, err
	}

	var miningRPC txpool_proto.MiningServer
	stateDiffClient := direct.NewStateDiffClientDirect(kvRPC)
	if config.DeprecatedTxPool.Disable {
		backend.txPool2GrpcServer = &txpool2.GrpcDisabled{}
	} else {
		//cacheConfig := kvcache.DefaultCoherentCacheConfig
		//cacheConfig.MetricsLabel = "txpool"

		backend.newTxs2 = make(chan types2.Announcements, 1024)
		//defer close(newTxs)
		backend.txPool2DB, backend.txPool2, backend.txPool2Fetch, backend.txPool2Send, backend.txPool2GrpcServer, err = txpooluitl.AllComponents(
			ctx, config.TxPool, config, kvcache.NewDummy(), backend.newTxs2, backend.chainDB, backend.sentriesClient.Sentries(), stateDiffClient,
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

	// proof-of-work mining
	mining := stagedsync.New(
		stagedsync.MiningStages(backend.sentryCtx,
			stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miner, *backend.chainConfig, backend.engine, backend.txPool2, backend.txPool2DB, nil, tmpdir),
			stagedsync.StageMiningExecCfg(backend.chainDB, miner, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir, nil, 0, backend.txPool2, backend.txPool2DB, allSnapshots, config.TransactionsV3),
			stagedsync.StageHashStateCfg(backend.chainDB, dirs, config.HistoryV3, backend.agg),
			stagedsync.StageTrieCfg(backend.chainDB, false, true, true, tmpdir, blockReader, nil, config.HistoryV3, backend.agg),
			stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miner, backend.miningSealingQuit),
		), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder)

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
				stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miningStatePos, *backend.chainConfig, backend.engine, backend.txPool2, backend.txPool2DB, param, tmpdir),
				stagedsync.StageMiningExecCfg(backend.chainDB, miningStatePos, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir, interrupt, param.PayloadId, backend.txPool2, backend.txPool2DB, allSnapshots, config.TransactionsV3),
				stagedsync.StageHashStateCfg(backend.chainDB, dirs, config.HistoryV3, backend.agg),
				stagedsync.StageTrieCfg(backend.chainDB, false, true, true, tmpdir, blockReader, nil, config.HistoryV3, backend.agg),
				stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miningStatePos, backend.miningSealingQuit),
			), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder)
		// We start the mining step
		if err := stages2.MiningStep(ctx, backend.chainDB, proposingSync, tmpdir); err != nil {
			return nil, err
		}
		block := <-miningStatePos.MiningResultPOSCh
		return block, nil
	}

	// Initialize ethbackend
	ethBackendRPC := privateapi.NewEthBackendServer(ctx, backend, backend.chainDB, backend.notifications.Events,
		blockReader, chainConfig, assembleBlockPOS, backend.sentriesClient.Hd, config.Miner.EnabledPOS)
	miningRPC = privateapi.NewMiningServer(ctx, backend, ethashApi)

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
			backend.txPool2GrpcServer,
			miningRPC,
			stack.Config().PrivateApiAddr,
			stack.Config().PrivateApiRateLimit,
			creds,
			stack.Config().HealthCheck)
		if err != nil {
			return nil, fmt.Errorf("private api: %w", err)
		}
	}

	// If we choose not to run a consensus layer, run our embedded.
	if !config.ExternalCL && clparams.EmbeddedSupported(config.NetworkID) {
		genesisCfg, networkCfg, beaconCfg := clparams.GetConfigsByNetwork(clparams.NetworkType(config.NetworkID))
		if err != nil {
			return nil, err
		}
		client, err := service.StartSentinelService(&sentinel.SentinelConfig{
			IpAddr:        config.LightClientDiscoveryAddr,
			Port:          int(config.LightClientDiscoveryPort),
			TCPPort:       uint(config.LightClientDiscoveryTCPPort),
			GenesisConfig: genesisCfg,
			NetworkConfig: networkCfg,
			BeaconConfig:  beaconCfg,
			TmpDir:        tmpdir,
		}, chainKv, &service.ServerConfig{Network: "tcp", Addr: fmt.Sprintf("%s:%d", config.SentinelAddr, config.SentinelPort)}, creds, nil)
		if err != nil {
			return nil, err
		}

		lc, err := lightclient.NewLightClient(ctx, genesisCfg, beaconCfg, ethBackendRPC, nil, client, currentBlockNumber, false)
		if err != nil {
			return nil, err
		}
		bs, err := clcore.RetrieveBeaconState(ctx, beaconCfg, genesisCfg,
			clparams.GetCheckpointSyncEndpoint(clparams.NetworkType(config.NetworkID)))

		if err != nil {
			return nil, err
		}

		if err := lc.BootstrapCheckpoint(ctx, bs.FinalizedCheckpoint().Root); err != nil {
			return nil, err
		}

		go lc.Start()
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
			baseFee = currentBlock.BaseFee().Uint64()
		}
		backend.notifications.Accumulator.StartChange(currentBlock.NumberU64(), currentBlock.Hash(), nil, false)
		backend.notifications.Accumulator.SendAndReset(ctx, backend.notifications.StateChangesConsumer, baseFee, currentBlock.GasLimit())

	}()

	tx, err := backend.chainDB.BeginRw(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	if !config.DeprecatedTxPool.Disable {
		// we need to start the pool before stage loop itself
		// the pool holds the info about how execution stage should work - as regular or as limbo recovery
		if err := backend.txPool2.StartIfNotStarted(ctx, backend.txPool2DB, tx); err != nil {
			return nil, err
		}

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
				// Add mined header and block body before broadcast. This is because the broadcast call
				// will trigger the staged sync which will require headers and blocks to be available
				// in their respective cache in the download stage. If not found, it would cause a
				// liveness issue for the chain.
				if err := backend.sentriesClient.Hd.AddMinedHeader(b.Header()); err != nil {
					log.Error("add mined block to header downloader", "err", err)
				}
				backend.sentriesClient.Bd.AddToPrefetch(b.Header(), b.RawBody())

				//p2p
				//backend.sentriesClient.BroadcastNewBlock(context.Background(), b, b.Difficulty())
				//rpcdaemon
				if err := miningRPC.(*privateapi.MiningServer).BroadcastMinedBlock(b); err != nil {
					log.Error("txpool rpc mined block broadcast", "err", err)
				}
				log.Trace("BroadcastMinedBlock successful", "number", b.Number(), "GasUsed", b.GasUsed(), "txn count", b.Transactions().Len())
				backend.sentriesClient.PropagateNewBlockHashes(ctx, []headerdownload.Announce{
					{
						Number: b.NumberU64(),
						Hash:   b.Hash(),
					},
				})

			case b := <-backend.pendingBlocks:
				if err := miningRPC.(*privateapi.MiningServer).BroadcastPendingBlock(b); err != nil {
					log.Error("txpool rpc pending block broadcast", "err", err)
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

	// create buckets
	if err := createBuckets(tx); err != nil {
		return nil, err
	}

	// record erigon version
	err = recordStartupVersionInDb(tx)
	if err != nil {
		log.Warn("failed to record erigon version in db", "err", err)
	}

	executionProgress, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return nil, err
	}

	if backend.config.Zk != nil {
		// zkevm: create a data stream server if we have the appropriate config for one.  This will be started on the call to Init
		// alongside the http server
		httpCfg := stack.Config().Http
		if httpCfg.DataStreamPort > 0 && httpCfg.DataStreamHost != "" {
			file := stack.Config().Dirs.DataDir + "/data-stream"
			logConfig := &log2.Config{
				Environment: "production",
				Level:       "warn",
				Outputs:     nil,
			}
			// todo [zkevm] read the stream version from config and figure out what system id is used for
			backend.dataStream, err = datastreamer.NewServer(uint16(httpCfg.DataStreamPort), uint8(backend.config.DatastreamVersion), 1, datastreamer.StreamType(1), file, httpCfg.DataStreamWriteTimeout, httpCfg.DataStreamInactivityTimeout, httpCfg.DataStreamInactivityCheckInterval, logConfig)
			if err != nil {
				return nil, err
			}

			// recovery here now, if the stream got into a bad state we want to be able to delete the file and have
			// the stream re-populated from scratch.  So we check the stream for the latest header and if it is
			// 0 we can just set the datastream progress to 0 also which will force a re-population of the stream
			latestHeader := backend.dataStream.GetHeader()
			if latestHeader.TotalEntries == 0 {
				log.Info("[dataStream] setting the stream progress to 0")
				backend.preStartTasks.WarmUpDataStream = true
			}
		}

		// entering ZK territory!
		cfg := backend.config

		backend.chainConfig.AllowFreeTransactions = cfg.AllowFreeTransactions
		l1Urls := strings.Split(cfg.L1RpcUrl, ",")

		if cfg.Zk.L1CacheEnabled {
			l1Cache, err := l1_cache.NewL1Cache(ctx, path.Join(stack.DataDir(), "l1cache"), cfg.Zk.L1CachePort)
			if err != nil {
				return nil, err
			}
			backend.l1Cache = l1Cache

			var cacheL1Urls []string
			for _, l1Url := range l1Urls {
				encoded := url.QueryEscape(l1Url)
				cacheL1Url := fmt.Sprintf("http://localhost:%d?endpoint=%s&chainid=%d", cfg.Zk.L1CachePort, encoded, cfg.L2ChainId)
				cacheL1Urls = append(cacheL1Urls, cacheL1Url)
			}
			l1Urls = cacheL1Urls
		}

		backend.etherManClients = make([]*etherman.Client, len(l1Urls))
		for i, url := range l1Urls {
			backend.etherManClients[i] = newEtherMan(cfg, chainConfig.ChainName, url)
		}

		isSequencer := sequencer.IsSequencer()

		// if the L1 block sync is set we're in recovery so can't run as a sequencer
		if cfg.L1SyncStartBlock > 0 {
			if !isSequencer {
				panic("you cannot launch in l1 sync mode as an RPC node")
			}
			log.Info("Starting sequencer in L1 recovery mode", "startBlock", cfg.L1SyncStartBlock)
		}

		seqAndVerifTopics := [][]libcommon.Hash{{
			contracts.SequencedBatchTopicPreEtrog,
			contracts.SequencedBatchTopicEtrog,
			contracts.RollbackBatchesTopic,
			contracts.VerificationTopicPreEtrog,
			contracts.VerificationTopicEtrog,
			contracts.VerificationValidiumTopicEtrog,
		}}

		seqAndVerifL1Contracts := []libcommon.Address{cfg.AddressRollup, cfg.AddressAdmin, cfg.AddressZkevm}

		var l1Topics [][]libcommon.Hash
		var l1Contracts []libcommon.Address
		if isSequencer {
			l1Topics = [][]libcommon.Hash{{
				contracts.InitialSequenceBatchesTopic,
				contracts.AddNewRollupTypeTopic,
				contracts.CreateNewRollupTopic,
				contracts.UpdateRollupTopic,
			}}
			l1Contracts = []libcommon.Address{cfg.AddressZkevm, cfg.AddressRollup}
		} else {
			l1Topics = seqAndVerifTopics
			l1Contracts = seqAndVerifL1Contracts
		}

		ethermanClients := make([]syncer.IEtherman, len(backend.etherManClients))
		for i, c := range backend.etherManClients {
			ethermanClients[i] = c.EthClient
		}

		seqVerSyncer := syncer.NewL1Syncer(
			ctx,
			ethermanClients,
			seqAndVerifL1Contracts,
			seqAndVerifTopics,
			cfg.L1BlockRange,
			cfg.L1QueryDelay,
			cfg.L1HighestBlockType,
		)

		backend.l1Syncer = syncer.NewL1Syncer(
			ctx,
			ethermanClients,
			l1Contracts,
			l1Topics,
			cfg.L1BlockRange,
			cfg.L1QueryDelay,
			cfg.L1HighestBlockType,
		)

		log.Info("Rollup ID", "rollupId", cfg.L1RollupId)

		// check contract addresses in config against L1
		if cfg.Zk.L1ContractAddressCheck {
			success, err := l1ContractAddressCheck(ctx, cfg.Zk, backend.l1Syncer)
			if !success || err != nil {
				//log.Warn("Contract address check failed", "success", success, "err", err)
				panic("Contract address check failed")
			}
			log.Info("Contract address check passed")
		} else {
			log.Info("Contract address check skipped")
		}

		l1InfoTreeSyncer := syncer.NewL1Syncer(
			ctx,
			ethermanClients,
			[]libcommon.Address{cfg.AddressGerManager},
			[][]libcommon.Hash{{contracts.UpdateL1InfoTreeTopic}},
			cfg.L1BlockRange,
			cfg.L1QueryDelay,
			cfg.L1HighestBlockType,
		)

		if isSequencer {
			// if we are sequencing transactions, we do the sequencing loop...
			witnessGenerator := witness.NewGenerator(
				config.Dirs,
				config.HistoryV3,
				backend.agg,
				backend.blockReader,
				backend.chainConfig,
				backend.config.Zk,
				backend.engine,
			)

			var legacyExecutors []*legacy_executor_verifier.Executor = make([]*legacy_executor_verifier.Executor, 0, len(cfg.ExecutorUrls))
			if len(cfg.ExecutorUrls) > 0 && cfg.ExecutorUrls[0] != "" {
				levCfg := legacy_executor_verifier.Config{
					GrpcUrls:              cfg.ExecutorUrls,
					Timeout:               cfg.ExecutorRequestTimeout,
					MaxConcurrentRequests: cfg.ExecutorMaxConcurrentRequests,
					OutputLocation:        cfg.ExecutorPayloadOutput,
				}
				executors := legacy_executor_verifier.NewExecutors(levCfg)
				for _, e := range executors {
					legacyExecutors = append(legacyExecutors, e)
				}
			}

			verifier := legacy_executor_verifier.NewLegacyExecutorVerifier(
				*cfg.Zk,
				legacyExecutors,
				backend.chainConfig,
				backend.chainDB,
				witnessGenerator,
				backend.dataStream,
			)

			if cfg.Zk.Limbo {
				limboSubPoolProcessor := txpool.NewLimboSubPoolProcessor(ctx, cfg.Zk, backend.chainConfig, backend.chainDB, backend.txPool2, verifier)
				limboSubPoolProcessor.StartWork()
			}

			// we need to make sure the pool is always aware of the latest block for when
			// we switch context from being an RPC node to a sequencer
			backend.txPool2.ForceUpdateLatestBlock(executionProgress)

			l1BlockSyncer := syncer.NewL1Syncer(
				ctx,
				ethermanClients,
				[]libcommon.Address{cfg.AddressZkevm, cfg.AddressRollup},
				[][]libcommon.Hash{{
					contracts.SequenceBatchesTopic,
				}},
				cfg.L1BlockRange,
				cfg.L1QueryDelay,
				cfg.L1HighestBlockType,
			)

			backend.syncStages = stages2.NewSequencerZkStages(
				backend.sentryCtx,
				backend.chainDB,
				config,
				backend.sentriesClient,
				backend.notifications,
				backend.downloaderClient,
				allSnapshots,
				backend.agg,
				backend.forkValidator,
				backend.engine,
				backend.dataStream,
				backend.l1Syncer,
				seqVerSyncer,
				l1InfoTreeSyncer,
				l1BlockSyncer,
				backend.txPool2,
				backend.txPool2DB,
				verifier,
			)

			backend.syncUnwindOrder = zkStages.ZkSequencerUnwindOrder

		} else {
			/*
			 if we are syncing from for the RPC, we do the normal ZK sync loop

			  ZZZZZZZ  K   K  RRRRR   PPPPP   CCCC
			      Z    K  K   R   R   P   P  C
			     Z     KKK    RRRR    PPPP   C
			    Z      K  K   R  R    P      C
			  ZZZZZZZ  K   K  R   R   P       CCCC

			*/
			latestForkId, err := stages.GetStageProgress(tx, stages.ForkId)
			if err != nil {
				return nil, err
			}
			streamClient := initDataStreamClient(ctx, cfg.Zk, uint16(latestForkId))

			backend.syncStages = stages2.NewDefaultZkStages(
				backend.sentryCtx,
				backend.chainDB,
				config,
				backend.sentriesClient,
				backend.notifications,
				backend.downloaderClient,
				allSnapshots,
				backend.agg,
				backend.forkValidator,
				backend.engine,
				backend.l1Syncer,
				l1InfoTreeSyncer,
				streamClient,
				backend.dataStream,
			)

			backend.syncUnwindOrder = zkStages.ZkUnwindOrder
		}
		// TODO: SEQ: prune order

	} else {
		backend.syncStages = stages2.NewDefaultStages(backend.sentryCtx, backend.chainDB, stack.Config().P2P, config, backend.sentriesClient, backend.notifications, backend.downloaderClient, allSnapshots, backend.agg, backend.forkValidator, backend.engine)
		backend.syncUnwindOrder = stagedsync.DefaultUnwindOrder
		backend.syncPruneOrder = stagedsync.DefaultPruneOrder
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return backend, nil
}

func createBuckets(tx kv.RwTx) error {
	if err := hermez_db.CreateHermezBuckets(tx); err != nil {
		return err
	}

	if err := db.CreateEriDbBuckets(tx); err != nil {
		return err
	}

	return nil
}

func recordStartupVersionInDb(tx kv.RwTx) error {
	version := utils.GetVersion()

	hdb := hermez_db.NewHermezDb(tx)
	written, err := hdb.WriteErigonVersion(version, time.Now())
	if err != nil {
		return err
	}

	if !written {
		log.Info(fmt.Sprintf("Erigon started at version %s, version already run", version))
	} else {
		log.Info(fmt.Sprintf("Erigon started at version %s, for the first time", version))
	}
	return nil
}

// creates an EtherMan instance with default parameters
func newEtherMan(cfg *ethconfig.Config, l2ChainName, url string) *etherman.Client {
	ethmanConf := etherman.Config{
		URL:                       url,
		L1ChainID:                 cfg.L1ChainId,
		L2ChainID:                 cfg.L2ChainId,
		L2ChainName:               l2ChainName,
		PoEAddr:                   cfg.AddressRollup,
		MaticAddr:                 cfg.L1MaticContractAddress,
		GlobalExitRootManagerAddr: cfg.AddressGerManager,
	}

	em, err := etherman.NewClient(ethmanConf)
	//panic on error
	if err != nil {
		panic(err)
	}
	return em
}

// creates a datastream client with default parameters
func initDataStreamClient(ctx context.Context, cfg *ethconfig.Zk, latestForkId uint16) *client.StreamClient {
	return client.NewClient(ctx, cfg.L2DataStreamerUrl, cfg.DatastreamVersion, cfg.L2DataStreamerTimeout, latestForkId)
}

func (backend *Ethereum) Init(stack *node.Node, config *ethconfig.Config) error {
	ethBackendRPC, miningRPC, stateDiffClient := backend.ethBackendRPC, backend.miningRPC, backend.stateChangesClient
	blockReader := backend.blockReader
	ctx := backend.sentryCtx
	chainKv := backend.chainDB
	var err error

	backend.stagedSync = stagedsync.New(backend.syncStages, backend.syncUnwindOrder, backend.syncPruneOrder)

	backend.sentriesClient.Hd.StartPoSDownloader(backend.sentryCtx, backend.sentriesClient.SendHeaderRequest, backend.sentriesClient.Penalize)

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
		var headCh chan [][]byte
		headCh, backend.unsubscribeEthstat = backend.notifications.Events.AddHeaderSubscription()
		if err := ethstats.New(stack, backend.sentryServers, chainKv, backend.engine, config.Ethstats, backend.networkID, ctx.Done(), headCh); err != nil {
			return err
		}
	}
	// start HTTP API
	httpRpcCfg := stack.Config().Http
	ethRpcClient, txPoolRpcClient, miningRpcClient, stateCache, ff, err := cli.EmbeddedServices(ctx, chainKv, httpRpcCfg.StateCache, blockReader, ethBackendRPC, backend.txPool2GrpcServer, miningRPC, stateDiffClient)
	if err != nil {
		return err
	}

	var borDb kv.RoDB
	if casted, ok := backend.engine.(*bor.Bor); ok {
		borDb = casted.DB
	}
	apiList := commands.APIList(chainKv, borDb, ethRpcClient, txPoolRpcClient, backend.txPool2, miningRpcClient, ff, stateCache, blockReader, backend.agg, httpRpcCfg, backend.engine, config, backend.l1Syncer)
	authApiList := commands.AuthAPIList(chainKv, ethRpcClient, txPoolRpcClient, miningRpcClient, ff, stateCache, blockReader, backend.agg, httpRpcCfg, backend.engine, config)
	go func() {
		if err := cli.StartRpcServer(ctx, httpRpcCfg, apiList, authApiList); err != nil {
			log.Error(err.Error())
			return
		}
	}()

	go func() {
		if err := cli.StartDataStream(backend.dataStream); err != nil {
			log.Error(err.Error())
			return
		}
	}()

	// Register the backend on the node
	stack.RegisterLifecycle(backend)
	return nil
}

func (s *Ethereum) PreStart() error {
	if s.preStartTasks.WarmUpDataStream {
		log.Info("[PreStart] warming up data stream")
		tx, err := s.chainDB.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		// we don't know when the server has actually started as it doesn't expose a signal that is has spun up
		// so here we loop and take a brief pause waiting for it to be ready
		attempts := 0
		for {
			_, err = zkStages.CatchupDatastream(s.sentryCtx, "stream-catchup", tx, s.dataStream, s.chainConfig.ChainID.Uint64())
			if err != nil {
				if errors.Is(err, datastreamer.ErrAtomicOpNotAllowed) {
					attempts++
					if attempts == 10 {
						return err
					}
					time.Sleep(500 * time.Millisecond)
					continue
				}
				return err
			} else {
				break
			}
		}
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func (s *Ethereum) APIs() []rpc.API {
	return []rpc.API{}
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
	if !cfg.Enabled {
		return nil
	}

	// Configure the local mining address
	eb, err := s.Etherbase()
	if err != nil {
		log.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %w", err)
	}

	var clq *clique.Clique
	if c, ok := s.engine.(*clique.Clique); ok {
		clq = c
	} else if cl, ok := s.engine.(*serenity.Serenity); ok {
		if c, ok := cl.InnerEngine().(*clique.Clique); ok {
			clq = c
		}
	}
	if clq != nil {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		clq.Authorize(eb, func(_ libcommon.Address, mimeType string, message []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(message), cfg.SigKey)
		})
	}

	var borcfg *bor.Bor
	if b, ok := s.engine.(*bor.Bor); ok {
		borcfg = b
	} else if br, ok := s.engine.(*serenity.Serenity); ok {
		if b, ok := br.InnerEngine().(*bor.Bor); ok {
			borcfg = b
		}
	}
	if borcfg != nil {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		borcfg.Authorize(eb, func(_ libcommon.Address, mimeType string, message []byte) ([]byte, error) {
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

		var works bool
		var hasWork bool
		errc := make(chan error, 1)

		for {
			mineEvery.Reset(cfg.Recommit)
			select {
			case <-s.notifyMiningAboutNewTxs:
				log.Debug("Start mining new block based on txpool notif")
				hasWork = true
			case <-newHeadCh:
				log.Debug("Start mining new block based on new head channel")
				hasWork = true
			case <-mineEvery.C:
				log.Debug("Start mining new block based on miner.recommit")
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

			if !works && hasWork {
				works = true
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

	log.Trace("sentry", "peer count", sentryPc)
	for _, sc := range s.sentriesClient.Sentries() {
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
	if limit == 0 || limit > len(s.sentriesClient.Sentries()) {
		limit = len(s.sentriesClient.Sentries())
	}

	nodes := make([]*prototypes.NodeInfoReply, 0, limit)
	for i := 0; i < limit; i++ {
		sc := s.sentriesClient.Sentries()[i]

		nodeInfo, err := sc.NodeInfo(context.Background(), nil)
		if err != nil {
			log.Error("sentry nodeInfo", "err", err)
		}

		nodes = append(nodes, nodeInfo)
	}

	nodesInfo := &remote.NodesInfoReply{NodesInfo: nodes}
	slices.SortFunc(nodesInfo.NodesInfo, remote.NodeInfoReplyLess)

	return nodesInfo, nil
}

// sets up blockReader and client downloader
func (s *Ethereum) setUpBlockReader(ctx context.Context, dirs datadir.Dirs, snConfig ethconfig.Snapshot, downloaderCfg *downloadercfg.Cfg, notifications *shards.Events, transactionsV3 bool) (services.FullBlockReader, *snapshotsync.RoSnapshots, *libstate.AggregatorV3, error) {
	allSnapshots := snapshotsync.NewRoSnapshots(snConfig, dirs.Snap)
	var err error
	if !snConfig.NoDownloader {
		allSnapshots.OptimisticalyReopenWithDB(s.chainDB)
	}
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(allSnapshots, transactionsV3)

	if !snConfig.NoDownloader {
		if snConfig.DownloaderAddr != "" {
			// connect to external Downloader
			s.downloaderClient, err = downloadergrpc.NewClient(ctx, snConfig.DownloaderAddr)
		} else {
			// start embedded Downloader
			s.downloader, err = downloader3.New(ctx, downloaderCfg)
			if err != nil {
				return nil, nil, nil, err
			}
			s.downloader.MainLoopInBackground(ctx, true)
			bittorrentServer, err := downloader3.NewGrpcServer(s.downloader)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("new server: %w", err)
			}

			s.downloaderClient = direct.NewDownloaderClient(bittorrentServer)
		}
		if err != nil {
			return nil, nil, nil, err
		}
	}

	dir.MustExist(dirs.SnapHistory)
	agg, err := libstate.NewAggregatorV3(ctx, dirs.SnapHistory, dirs.Tmp, ethconfig.HistoryV3AggregationStep, s.chainDB)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = agg.OpenFolder(); err != nil {
		return nil, nil, nil, err
	}
	agg.OnFreeze(func(frozenFileNames []string) {
		notifications.OnNewSnapshot()
		req := &proto_downloader.DownloadRequest{Items: make([]*proto_downloader.DownloadItem, 0, len(frozenFileNames))}
		for _, fName := range frozenFileNames {
			req.Items = append(req.Items, &proto_downloader.DownloadItem{
				Path: filepath.Join("history", fName),
			})
		}
	})

	return blockReader, allSnapshots, agg, nil
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

	// don't start the stageloop if debug.no-sync flag set
	if s.config.DebugNoSync {
		return nil
	}

	go stages2.StageLoop(s.sentryCtx, s.chainConfig, s.chainDB, s.stagedSync, s.sentriesClient.Hd, s.notifications, s.sentriesClient.UpdateHead, s.waitForStageLoopStop, s.config.Sync.LoopThrottle)

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
	<-s.waitForStageLoopStop
	if s.config.Miner.Enabled {
		<-s.waitForMiningStop
	}
	for _, sentryServer := range s.sentryServers {
		sentryServer.Close()
	}
	if s.txPool2DB != nil {
		s.txPool2DB.Close()
	}
	if s.agg != nil {
		s.agg.Close()
	}
	s.chainDB.Close()
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

func (s *Ethereum) SentryControlServer() *sentry.MultiClient {
	return s.sentriesClient
}

// RemoveContents is like os.RemoveAll, but preserve dir itself
func RemoveContents(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
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

func l1ContractAddressCheck(ctx context.Context, cfg *ethconfig.Zk, l1BlockSyncer *syncer.L1Syncer) (bool, error) {
	l1AddrRollup, err := l1BlockSyncer.CallRollupManager(ctx, &cfg.AddressZkevm)
	if err != nil {
		return false, err
	}
	if l1AddrRollup != cfg.AddressRollup {
		log.Warn("L1 contract address check failed (AddressRollup)", "expected", cfg.AddressRollup, "actual", l1AddrRollup)
		return false, nil
	}

	l1AddrAdmin, err := l1BlockSyncer.CallAdmin(ctx, &cfg.AddressZkevm)
	if err != nil {
		return false, err
	}
	if l1AddrAdmin != cfg.AddressAdmin {
		log.Warn("L1 contract address check failed (AddressAdmin)", "expected", cfg.AddressAdmin, "actual", l1AddrAdmin)
		return false, nil
	}

	l1AddrGerManager, err := l1BlockSyncer.CallGlobalExitRootManager(ctx, &cfg.AddressZkevm)
	if err != nil {
		return false, err
	}
	if l1AddrGerManager != cfg.AddressGerManager {
		log.Warn("L1 contract address check failed (AddressGerManager)", "expected", cfg.AddressGerManager, "actual", l1AddrGerManager)
		return false, nil
	}

	l1AddrSequencer, err := l1BlockSyncer.CallTrustedSequencer(ctx, &cfg.AddressZkevm)
	if err != nil {
		return false, err
	}
	if l1AddrSequencer != cfg.AddressSequencer {
		log.Warn("L1 contract address check failed (AddressSequencer)", "expected", cfg.AddressSequencer, "actual", l1AddrSequencer)
		return false, nil
	}
	return true, nil
}
