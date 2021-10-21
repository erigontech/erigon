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
	"math/big"
	"os"
	"path"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentry"
	txpool_proto "github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	txpool2 "github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/erigon-lib/txpool/txpooluitl"
	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/ethutils"
	"github.com/ledgerwatch/erigon/eth/fetcher"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/stages/txpropagate"
	"github.com/ledgerwatch/erigon/turbo/txpool"
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
	logger log.Logger

	// Handlers
	txPool *core.TxPool

	// DB interfaces
	chainDB    kv.RwDB
	privateAPI *grpc.Server

	engine consensus.Engine

	gasPrice  *uint256.Int
	etherbase common.Address

	networkID uint64

	torrentClient *snapshotsync.Client

	lock              sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	chainConfig       *params.ChainConfig
	genesisHash       common.Hash
	quitMining        chan struct{}
	miningSealingQuit chan struct{}
	pendingBlocks     chan *types.Block
	minedBlocks       chan *types.Block

	// downloader fields
	downloadCtx     context.Context
	downloadCancel  context.CancelFunc
	downloadServer  *download.ControlServerImpl
	sentryServers   []*download.SentryServerImpl
	txPoolP2PServer *txpool.P2PServer
	sentries        []direct.SentryClient
	stagedSync      *stagedsync.Sync

	notifications *stagedsync.Notifications

	waitForStageLoopStop chan struct{}
	waitForMiningStop    chan struct{}

	txPool2DB               kv.RwDB
	txPool2                 *txpool2.TxPool
	newTxs2                 chan txpool2.Hashes
	txPool2Fetch            *txpool2.Fetch
	txPool2Send             *txpool2.Send
	txPool2GrpcServer       *txpool2.GrpcServer
	notifyMiningAboutNewTxs chan struct{}
}

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(stack *node.Node, config *ethconfig.Config, logger log.Logger) (*Ethereum, error) {
	if config.Miner.GasPrice == nil || config.Miner.GasPrice.Cmp(common.Big0) <= 0 {
		log.Warn("Sanitizing invalid miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.Defaults.Miner.GasPrice)
		config.Miner.GasPrice = new(big.Int).Set(ethconfig.Defaults.Miner.GasPrice)
	}

	tmpdir := path.Join(stack.Config().DataDir, etl.TmpDirName)
	if err := os.RemoveAll(tmpdir); err != nil { // clean it on startup
		return nil, fmt.Errorf("clean tmp dir: %s, %w", tmpdir, err)
	}

	// Assemble the Ethereum object
	chainKv, err := node.OpenDatabase(stack.Config(), logger, kv.ChainDB)
	if err != nil {
		return nil, err
	}

	var torrentClient *snapshotsync.Client
	config.Snapshot.Dir = stack.Config().ResolvePath("snapshots")
	if config.Snapshot.Enabled {
		var peerID string
		if err = chainKv.View(context.Background(), func(tx kv.Tx) error {
			v, err := tx.GetOne(kv.BittorrentInfo, []byte(kv.BittorrentPeerID))
			if err != nil {
				return err
			}
			peerID = string(v)
			return nil
		}); err != nil {
			log.Error("Get bittorrent peer", "err", err)
		}
		torrentClient, err = snapshotsync.New(config.Snapshot.Dir, config.Snapshot.Seeding, peerID)
		if err != nil {
			return nil, err
		}
		if len(peerID) == 0 {
			log.Info("Generate new bittorent peerID", "id", common.Bytes2Hex(torrentClient.PeerID()))
			if err = chainKv.Update(context.Background(), func(tx kv.RwTx) error {
				return torrentClient.SavePeerID(tx)
			}); err != nil {
				log.Error("Bittorrent peerID haven't saved", "err", err)
			}
		}

		chainKv, err = snapshotsync.WrapSnapshots(chainKv, config.Snapshot.Dir)
		if err != nil {
			return nil, err
		}
		err = snapshotsync.SnapshotSeeding(chainKv, torrentClient, "headers", config.Snapshot.Dir)
		if err != nil {
			return nil, err
		}
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
	types.SetHeaderSealFlag(chainConfig.IsHeaderWithSeal())
	log.Info("Initialised chain configuration", "config", chainConfig)

	ctx, ctxCancel := context.WithCancel(context.Background())
	kvRPC := remotedbserver.NewKvServer(ctx, chainKv)
	backend := &Ethereum{
		downloadCtx:          ctx,
		downloadCancel:       ctxCancel,
		config:               config,
		logger:               logger,
		chainDB:              chainKv,
		networkID:            config.NetworkID,
		etherbase:            config.Miner.Etherbase,
		torrentClient:        torrentClient,
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
	} else {
		consensusConfig = &config.Ethash
	}

	backend.engine = ethconfig.CreateConsensusEngine(chainConfig, logger, consensusConfig, config.Miner.Notify, config.Miner.Noverify)

	log.Info("Initialising Ethereum protocol", "network", config.NetworkID)

	if err := chainKv.Update(context.Background(), func(tx kv.RwTx) error {
		if err := prune.SetIfNotExist(tx, config.Prune); err != nil {
			return err
		}

		if err = stagedsync.UpdateMetrics(tx); err != nil {
			return err
		}

		pm, err := prune.Get(tx)
		if err != nil {
			return err
		}
		if config.Prune.Initialised {
			// If storage mode is not explicitly specified, we take whatever is in the database
			if !reflect.DeepEqual(pm, config.Prune) {
				return errors.New("not allowed change of --prune flag, last time you used: " + pm.String())
			}
		} else {
			config.Prune = pm
		}
		log.Info("Effective", "prune", config.Prune.String())

		return nil
	}); err != nil {
		return nil, err
	}

	if config.TxPool.Journal != "" {
		config.TxPool.Journal = stack.ResolvePath(config.TxPool.Journal)
	}

	backend.txPool = core.NewTxPool(config.TxPool, chainConfig, chainKv)

	// setting notifier to support streaming events to rpc daemon
	var mg *snapshotsync.SnapshotMigrator
	if config.Snapshot.Enabled {
		currentSnapshotBlock, currentInfohash, err := snapshotsync.GetSnapshotInfo(chainKv)
		if err != nil {
			return nil, err
		}
		mg = snapshotsync.NewMigrator(config.Snapshot.Dir, currentSnapshotBlock, currentInfohash)
		err = mg.RemoveNonCurrentSnapshots()
		if err != nil {
			log.Error("Remove non current snapshot", "err", err)
		}
	}

	if len(stack.Config().P2P.SentryAddr) > 0 {
		for _, addr := range stack.Config().P2P.SentryAddr {
			sentryClient, err := download.GrpcSentryClient(backend.downloadCtx, addr)
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
		cfg66.NodeDatabase = path.Join(stack.Config().DataDir, "nodes", "eth66")
		server66 := download.NewSentryServer(backend.downloadCtx, d66, readNodeInfo, &cfg66, eth.ETH66)
		backend.sentryServers = append(backend.sentryServers, server66)
		backend.sentries = []direct.SentryClient{direct.NewSentryClientDirect(eth.ETH66, server66)}
		cfg65 := stack.Config().P2P
		cfg65.NodeDatabase = path.Join(stack.Config().DataDir, "nodes", "eth65")
		d65, err := setupDiscovery(backend.config.EthDiscoveryURLs)
		if err != nil {
			return nil, err
		}
		cfg65.ListenAddr = cfg65.ListenAddr65
		server65 := download.NewSentryServer(backend.downloadCtx, d65, readNodeInfo, &cfg65, eth.ETH65)
		backend.sentryServers = append(backend.sentryServers, server65)
		backend.sentries = append(backend.sentries, direct.NewSentryClientDirect(eth.ETH65, server65))
		go func() {
			logEvery := time.NewTicker(120 * time.Second)
			defer logEvery.Stop()

			var logItems []interface{}

			for {
				select {
				case <-backend.downloadCtx.Done():
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
	backend.downloadServer, err = download.NewControlServer(chainKv, stack.Config().NodeName(), chainConfig, genesis.Hash(), backend.engine, backend.config.NetworkID, backend.sentries, config.BlockDownloaderWindow)
	if err != nil {
		return nil, err
	}
	config.BodyDownloadTimeoutSeconds = 30

	var txPoolRPC txpool_proto.TxpoolServer
	var miningRPC txpool_proto.MiningServer
	if config.TxPool.V2 {
		cfg := txpool2.DefaultConfig
		cfg.DBDir = path.Join(stack.Config().DataDir, "txpool")
		cfg.PendingSubPoolLimit = int(config.TxPool.GlobalSlots)
		cfg.BaseFeeSubPoolLimit = int(config.TxPool.GlobalBaseFeeQueue)
		cfg.QueuedSubPoolLimit = int(config.TxPool.GlobalQueue)
		cfg.MinFeeCap = config.TxPool.PriceLimit
		cfg.AccountSlots = config.TxPool.AccountSlots
		cfg.LogEvery = 1 * time.Minute     //5 * time.Minute
		cfg.CommitEvery = 60 * time.Second //5 * time.Minute

		//cacheConfig := kvcache.DefaultCoherentCacheConfig
		//cacheConfig.MetricsLabel = "txpool"

		stateDiffClient := direct.NewStateDiffClientDirect(kvRPC)
		backend.newTxs2 = make(chan txpool2.Hashes, 1024)
		//defer close(newTxs)
		backend.txPool2DB, backend.txPool2, backend.txPool2Fetch, backend.txPool2Send, backend.txPool2GrpcServer, err = txpooluitl.AllComponents(
			ctx, cfg, kvcache.NewDummy(), backend.newTxs2, backend.chainDB, backend.sentries, stateDiffClient,
		)
		if err != nil {
			return nil, err
		}
		txPoolRPC = backend.txPool2GrpcServer
	} else {
		backend.txPoolP2PServer, err = txpool.NewP2PServer(backend.downloadCtx, backend.sentries, backend.txPool)
		if err != nil {
			return nil, err
		}

		fetchTx := func(peerID string, hashes []common.Hash) error {
			backend.txPoolP2PServer.SendTxsRequest(context.TODO(), peerID, hashes)
			return nil
		}

		backend.txPoolP2PServer.TxFetcher = fetcher.NewTxFetcher(backend.txPool.Has, backend.txPool.AddRemotes, fetchTx)
		txPoolRPC = privateapi.NewTxPoolServer(ctx, backend.txPool)
	}

	backend.notifyMiningAboutNewTxs = make(chan struct{}, 1)
	backend.quitMining = make(chan struct{})
	backend.miningSealingQuit = make(chan struct{})
	backend.pendingBlocks = make(chan *types.Block, 1)
	backend.minedBlocks = make(chan *types.Block, 1)

	miner := stagedsync.NewMiningState(&config.Miner)
	backend.pendingBlocks = miner.PendingResultCh
	backend.minedBlocks = miner.MiningResultCh

	mining := stagedsync.New(
		stagedsync.MiningStages(backend.downloadCtx,
			stagedsync.StageMiningCreateBlockCfg(backend.chainDB, miner, *backend.chainConfig, backend.engine, backend.txPool, backend.txPool2, backend.txPool2DB, tmpdir),
			stagedsync.StageMiningExecCfg(backend.chainDB, miner, backend.notifications.Events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir),
			stagedsync.StageHashStateCfg(backend.chainDB, tmpdir),
			stagedsync.StageTrieCfg(backend.chainDB, false, true, tmpdir),
			stagedsync.StageMiningFinishCfg(backend.chainDB, *backend.chainConfig, backend.engine, miner, backend.miningSealingQuit),
		), stagedsync.MiningUnwindOrder, stagedsync.MiningPruneOrder)

	var ethashApi *ethash.API
	if casted, ok := backend.engine.(*ethash.Ethash); ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}

	ethBackendRPC := privateapi.NewEthBackendServer(ctx, backend, backend.notifications.Events)
	miningRPC = privateapi.NewMiningServer(ctx, backend, ethashApi)
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
			txPoolRPC,
			miningRPC,
			stack.Config().PrivateApiAddr,
			stack.Config().PrivateApiRateLimit,
			creds)
		if err != nil {
			return nil, err
		}
	}

	if !config.TxPool.Disable {
		if config.TxPool.V2 {
			backend.txPool2Fetch.ConnectCore()
			backend.txPool2Fetch.ConnectSentries()
			go txpool2.MainLoop(backend.downloadCtx,
				backend.txPool2DB, backend.chainDB,
				backend.txPool2, backend.newTxs2, backend.txPool2Send, backend.txPool2GrpcServer.NewSlotsStreams,
				func() {
					select {
					case backend.notifyMiningAboutNewTxs <- struct{}{}:
					default:
					}
				})
		} else {
			go txpropagate.BroadcastPendingTxsToNetwork(backend.downloadCtx, backend.txPool, backend.txPoolP2PServer.RecentPeers, backend.downloadServer)
			go func() {
				newTransactions := make(chan core.NewTxsEvent, 128)
				sub := backend.txPool.SubscribeNewTxsEvent(newTransactions)
				defer sub.Unsubscribe()
				defer close(newTransactions)
				for {
					select {
					case <-ctx.Done():
						return
					case <-newTransactions:
						select {
						case backend.notifyMiningAboutNewTxs <- struct{}{}:
						default:
						}
					}
				}
			}()
		}

		// start pool on non-mainnet immediately
		if backend.chainConfig.ChainID.Uint64() != params.MainnetChainConfig.ChainID.Uint64() && !backend.config.TxPool.Disable {
			var execution uint64
			var hh *types.Header
			if err := chainKv.View(ctx, func(tx kv.Tx) error {
				execution, err = stages.GetStageProgress(tx, stages.Execution)
				if err != nil {
					return err
				}
				hh = rawdb.ReadCurrentHeader(tx)
				return nil
			}); err != nil {
				return nil, err
			}

			if backend.config.TxPool.V2 {
				if hh != nil {
					if err := backend.txPool2DB.View(context.Background(), func(tx kv.Tx) error {
						pendingBaseFee := misc.CalcBaseFee(chainConfig, hh)
						return backend.txPool2.OnNewBlock(context.Background(), &remote.StateChangeBatch{
							PendingBlockBaseFee: pendingBaseFee.Uint64(),
							DatabaseViewID:      tx.ViewID(),
							ChangeBatch: []*remote.StateChange{
								{BlockHeight: hh.Number.Uint64(), BlockHash: gointerfaces.ConvertHashToH256(hh.Hash())},
							},
						}, txpool2.TxSlots{}, txpool2.TxSlots{}, tx)
					}); err != nil {
						return nil, err
					}
				}
			} else {
				if hh != nil {
					if err := backend.txPool.Start(hh.GasLimit, execution); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	go func() {
		defer debug.LogPanic()
		for {
			select {
			case b := <-backend.minedBlocks:
				//p2p
				//backend.downloadServer.BroadcastNewBlock(context.Background(), b, b.Difficulty())
				//rpcdaemon
				if err := miningRPC.(*privateapi.MiningServer).BroadcastMinedBlock(b); err != nil {
					log.Error("txpool rpc mined block broadcast", "err", err)
				}
				if err := backend.downloadServer.Hd.AddMinedBlock(b); err != nil {
					log.Error("add mined block to header downloader", "err", err)
				}
				if err := backend.downloadServer.Bd.AddMinedBlock(b); err != nil {
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

	backend.stagedSync, err = stages2.NewStagedSync(
		backend.downloadCtx,
		backend.logger,
		backend.chainDB,
		stack.Config().P2P,
		*config,
		backend.downloadServer,
		tmpdir,
		backend.txPool,
		backend.txPoolP2PServer,

		torrentClient, mg, backend.notifications.Accumulator,
	)
	if err != nil {
		return nil, err
	}

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
	s.txPool.SetGasPrice(gasPrice)

	// Configure the local mining address
	eb, err := s.Etherbase()
	if err != nil {
		log.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %w", err)
	}
	if clique, ok := s.engine.(*clique.Clique); ok {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %w", err)
		}

		clique.Authorize(eb, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
			return crypto.Sign(crypto.Keccak256(message), cfg.SigKey)
		})
	}

	if s.chainConfig.ChainID.Uint64() > 10 {
		go func() {
			skipCycleEvery := time.NewTicker(4 * time.Second)
			defer skipCycleEvery.Stop()
			for range skipCycleEvery.C {
				select {
				case s.downloadServer.Hd.SkipCycleHack <- struct{}{}:
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
		reply, err := sc.PeerCount(ctx, &sentry.PeerCountRequest{})
		if err != nil {
			log.Warn("sentry", "err", err)
			return 0, nil
		}
		sentryPc += reply.Count
	}

	return sentryPc, nil
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
			download.RecvMessageLoop(s.downloadCtx, s.sentries[i], s.downloadServer, nil)
		}(i)
		go func(i int) {
			download.RecvUploadMessageLoop(s.downloadCtx, s.sentries[i], s.downloadServer, nil)
		}(i)
		go func(i int) {
			download.RecvUploadHeadersMessageLoop(s.downloadCtx, s.sentries[i], s.downloadServer, nil)
		}(i)
	}

	go stages2.StageLoop(s.downloadCtx, s.chainDB, s.stagedSync, s.downloadServer.Hd, s.notifications, s.downloadServer.UpdateHead, s.waitForStageLoopStop, s.config.SyncLoopThrottle)

	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	// Stop all the peer-related stuff first.
	s.downloadCancel()
	if s.txPoolP2PServer != nil {
		s.txPoolP2PServer.TxFetcher.Stop()
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
	if s.txPool != nil {
		s.txPool.Stop()
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
	if s.config.TxPool.V2 {
		s.txPool2DB.Close()
	}
	return nil
}
