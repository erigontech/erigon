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
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"math/big"
	"os"
	"path"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/turbo-geth/cmd/headers/download"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/clique"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
	"github.com/ledgerwatch/turbo-geth/eth/ethutils"
	"github.com/ledgerwatch/turbo-geth/eth/fetcher"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	proto_sentry "github.com/ledgerwatch/turbo-geth/gointerfaces/sentry"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config contains the configuration options of the ETH protocol.
// Deprecated: use ethconfig.Config instead.
type Config = ethconfig.Config

// Ethereum implements the Ethereum full node service.
type Ethereum struct {
	config *ethconfig.Config

	// Handlers
	txPool *core.TxPool

	handler           *handler
	ethDialCandidates enode.Iterator

	// DB interfaces
	chainDB    ethdb.Database // Same as chainDb, but different interface
	chainKV    ethdb.RwKV     // Same as chainDb, but different interface
	privateAPI *grpc.Server

	engine consensus.Engine

	gasPrice  *uint256.Int
	etherbase common.Address

	networkID uint64

	p2pServer *p2p.Server

	torrentClient *snapshotsync.Client

	lock          sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	events        *remotedbserver.Events
	chainConfig   *params.ChainConfig
	genesisHash   common.Hash
	quitMining    chan struct{}
	pendingBlocks chan *types.Block
	minedBlocks   chan *types.Block

	// downloader v2 fields
	downloadV2Ctx    context.Context
	downloadV2Cancel context.CancelFunc
	downloadServer   *download.ControlServerImpl
	sentryServer     *download.SentryServerImpl
	txPoolP2PServer  *eth.TxPoolServer
	sentries         []proto_sentry.SentryClient
	stagedSync2      *stagedsync.StagedSync
}

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(stack *node.Node, config *ethconfig.Config, gitCommit string) (*Ethereum, error) {
	if config.Miner.GasPrice == nil || config.Miner.GasPrice.Cmp(common.Big0) <= 0 {
		log.Warn("Sanitizing invalid miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.Defaults.Miner.GasPrice)
		config.Miner.GasPrice = new(big.Int).Set(ethconfig.Defaults.Miner.GasPrice)
	}

	tmpdir := path.Join(stack.Config().DataDir, etl.TmpDirName)
	if err := os.RemoveAll(tmpdir); err != nil { // clean it on startup
		return nil, fmt.Errorf("clean tmp dir: %s, %w", tmpdir, err)
	}

	// Assemble the Ethereum object
	var chainDb ethdb.Database
	var err error
	chainDb, err = stack.OpenDatabaseWithFreezer("chaindata", stack.Config().DataDir)
	if err != nil {
		return nil, err
	}

	var torrentClient *snapshotsync.Client
	snapshotsDir := stack.Config().ResolvePath("snapshots")
	if config.SnapshotLayout {
		v, err := chainDb.Get(dbutils.BittorrentInfoBucket, []byte(dbutils.BittorrentPeerID))
		if err != nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
			log.Error("Get bittorrent peer", "err", err)
		}
		torrentClient, err = snapshotsync.New(snapshotsDir, config.SnapshotSeeding, string(v))
		if err != nil {
			return nil, err
		}
		if len(v) == 0 {
			log.Info("Generate new bittorent peerID", "id", common.Bytes2Hex(torrentClient.PeerID()))
			err = torrentClient.SavePeerID(chainDb)
			if err != nil {
				log.Error("Bittorrent peerID haven't saved", "err", err)
			}
		}

		err = snapshotsync.WrapSnapshots(chainDb, snapshotsDir)
		if err != nil {
			return nil, err
		}
		err = snapshotsync.SnapshotSeeding(chainDb, torrentClient, "headers", snapshotsDir)
		if err != nil {
			return nil, err
		}
	}

	chainConfig, genesisHash, genesisErr := core.SetupGenesisBlock(chainDb, config.Genesis, config.StorageMode.History, false /* overwrite */)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	backend := &Ethereum{
		config:        config,
		chainDB:       chainDb,
		chainKV:       chainDb.(ethdb.HasRwKV).RwKV(),
		networkID:     config.NetworkID,
		etherbase:     config.Miner.Etherbase,
		p2pServer:     stack.Server(),
		torrentClient: torrentClient,
		chainConfig:   chainConfig,
		genesisHash:   genesisHash,
	}
	backend.gasPrice, _ = uint256.FromBig(config.Miner.GasPrice)

	var consensusConfig interface{}

	if chainConfig.Clique != nil {
		consensusConfig = &config.Clique
	} else {
		consensusConfig = &config.Ethash
	}

	backend.engine = ethconfig.CreateConsensusEngine(chainConfig, consensusConfig, config.Miner.Notify, config.Miner.Noverify)

	log.Info("Initialising Ethereum protocol", "network", config.NetworkID)

	err = ethdb.SetStorageModeIfNotExist(chainDb, config.StorageMode)
	if err != nil {
		return nil, err
	}

	sm, err := ethdb.GetStorageModeFromDB(chainDb)
	if err != nil {
		return nil, err
	}
	if config.StorageMode.Initialised {
		// If storage mode is not explicitely specified, we take whatever is in the database
		if !reflect.DeepEqual(sm, config.StorageMode) {
			return nil, errors.New("mode is " + config.StorageMode.ToString() + " original mode is " + sm.ToString())
		}
	}

	if sm.Pruning && !backend.config.EnableDownloadV2 {
		log.Info("Pruning is on, switching to new downloader")
		backend.config.EnableDownloadV2 = true
	}

	if err = stagedsync.UpdateMetrics(chainDb); err != nil {
		return nil, err
	}

	vmConfig := BlockchainRuntimeConfig(config)
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())

	if config.TxPool.Journal != "" {
		config.TxPool.Journal = stack.ResolvePath(config.TxPool.Journal)
	}

	backend.txPool = core.NewTxPool(config.TxPool, chainConfig, chainDb, txCacher)

	stagedSync := config.StagedSync

	// setting notifier to support streaming events to rpc daemon
	backend.events = remotedbserver.NewEvents()
	var mg *snapshotsync.SnapshotMigrator
	if config.SnapshotLayout {
		currentSnapshotBlock, currentInfohash, err := snapshotsync.GetSnapshotInfo(chainDb)
		if err != nil {
			return nil, err
		}
		mg = snapshotsync.NewMigrator(snapshotsDir, currentSnapshotBlock, currentInfohash)
		err = mg.RemoveNonCurrentSnapshots()
		if err != nil {
			log.Error("Remove non current snapshot", "err", err)
		}
	}
	if stagedSync == nil {
		// if there is not stagedsync, we create one with the custom notifier
		if config.SnapshotLayout {
			stagedSync = stagedsync.New(stagedsync.WithSnapshotsStages(), stagedsync.UnwindOrderWithSnapshots(), stagedsync.OptionalParameters{Notifier: backend.events, SnapshotDir: snapshotsDir, TorrnetClient: torrentClient, SnapshotMigrator: mg})
		} else {
			stagedSync = stagedsync.New(stagedsync.DefaultStages(), stagedsync.DefaultUnwindOrder(), stagedsync.OptionalParameters{Notifier: backend.events})
		}
	} else {
		// otherwise we add one if needed
		if stagedSync.Notifier == nil {
			stagedSync.Notifier = backend.events
		}
		if config.SnapshotLayout {
			stagedSync.SetTorrentParams(torrentClient, snapshotsDir, mg)
			log.Info("Set torrent params", "snapshotsDir", snapshotsDir)
		}
	}

	mining := stagedsync.New(stagedsync.MiningStages(), stagedsync.MiningUnwindOrder(), stagedsync.OptionalParameters{})

	var ethashApi *ethash.API
	if casted, ok := backend.engine.(*ethash.Ethash); ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}

	kvRPC := remotedbserver.NewKvServer(backend.chainKV)
	ethBackendRPC := remotedbserver.NewEthBackendServer(backend, backend.events, gitCommit)
	txPoolRPC := remotedbserver.NewTxPoolServer(context.Background(), backend.txPool)
	miningRPC := remotedbserver.NewMiningServer(context.Background(), backend, ethashApi)

	if stack.Config().PrivateApiAddr != "" {

		if stack.Config().TLSConnection {
			// load peer cert/key, ca cert
			var creds credentials.TransportCredentials

			if stack.Config().TLSCACert != "" {
				var peerCert tls.Certificate
				var caCert []byte
				peerCert, err = tls.LoadX509KeyPair(stack.Config().TLSCertFile, stack.Config().TLSKeyFile)
				if err != nil {
					log.Error("load peer cert/key error:%v", err)
					return nil, err
				}
				caCert, err = ioutil.ReadFile(stack.Config().TLSCACert)
				if err != nil {
					log.Error("read ca cert file error:%v", err)
					return nil, err
				}
				caCertPool := x509.NewCertPool()
				caCertPool.AppendCertsFromPEM(caCert)
				creds = credentials.NewTLS(&tls.Config{
					Certificates: []tls.Certificate{peerCert},
					ClientCAs:    caCertPool,
					ClientAuth:   tls.RequireAndVerifyClientCert,
					MinVersion:   tls.VersionTLS12,
				})
			} else {
				creds, err = credentials.NewServerTLSFromFile(stack.Config().TLSCertFile, stack.Config().TLSKeyFile)
			}

			if err != nil {
				return nil, err
			}
			backend.privateAPI, err = remotedbserver.StartGrpc(
				kvRPC,
				ethBackendRPC,
				txPoolRPC,
				miningRPC,
				stack.Config().PrivateApiAddr,
				stack.Config().PrivateApiRateLimit,
				&creds)
			if err != nil {
				return nil, err
			}
		} else {
			backend.privateAPI, err = remotedbserver.StartGrpc(
				kvRPC,
				ethBackendRPC,
				txPoolRPC,
				miningRPC,
				stack.Config().PrivateApiAddr,
				stack.Config().PrivateApiRateLimit,
				nil)
			if err != nil {
				return nil, err
			}
		}
	}

	checkpoint := config.Checkpoint
	if backend.config.EnableDownloadV2 {
		backend.downloadV2Ctx, backend.downloadV2Cancel = context.WithCancel(context.Background())
		if len(stack.Config().P2P.SentryAddr) > 0 {
			for _, addr := range stack.Config().P2P.SentryAddr {
				sentry, err := download.GrpcSentryClient(backend.downloadV2Ctx, addr)
				if err != nil {
					return nil, err
				}
				backend.sentries = append(backend.sentries, sentry)
			}
		} else {
			backend.sentryServer = download.NewSentryServer(backend.downloadV2Ctx, stack.Config().DataDir)
			sentry := &download.SentryClientDirect{}
			backend.sentryServer.P2pServer = backend.p2pServer
			sentry.SetServer(backend.sentryServer)
			backend.sentries = []proto_sentry.SentryClient{sentry}
		}
		blockDownloaderWindow := 65536
		backend.downloadServer, err = download.NewControlServer(chainDb, stack.Config().NodeName(), chainConfig, genesisHash, backend.engine, backend.config.NetworkID, backend.sentries, blockDownloaderWindow)
		if err != nil {
			return nil, err
		}
		if err = download.SetSentryStatus(backend.downloadV2Ctx, backend.sentries, backend.downloadServer); err != nil {
			return nil, err
		}
		backend.txPoolP2PServer, err = eth.NewTxPoolServer(backend.downloadV2Ctx, backend.sentries, backend.txPool)
		if err != nil {
			return nil, err
		}

		fetchTx := func(peerID string, hashes []common.Hash) error {
			backend.txPoolP2PServer.SendTxsRequest(context.TODO(), peerID, hashes)
			return nil
		}

		backend.txPoolP2PServer.TxFetcher = fetcher.NewTxFetcher(backend.txPool.Has, backend.txPool.AddRemotes, fetchTx)
		bodyDownloadTimeoutSeconds := 30 // TODO: convert to duration, make configurable

		backend.stagedSync2, err = download.NewStagedSync(
			backend.downloadV2Ctx,
			backend.chainKV,
			sm,
			config.BatchSize,
			bodyDownloadTimeoutSeconds,
			backend.downloadServer,
			tmpdir,
			backend.txPool,
			backend.txPoolP2PServer,
		)
		if err != nil {
			return nil, err
		}

	} else {
		genesisBlock, _ := rawdb.ReadBlockByNumberDeprecated(chainDb, 0)
		if genesisBlock == nil {
			return nil, core.ErrNoGenesis
		}

		if backend.handler, err = newHandler(&handlerConfig{
			Database:    chainDb,
			ChainConfig: chainConfig,
			genesis:     genesisBlock,
			vmConfig:    &vmConfig,
			engine:      backend.engine,
			TxPool:      backend.txPool,
			Network:     config.NetworkID,
			Checkpoint:  checkpoint,

			Whitelist: config.Whitelist,
		}); err != nil {
			return nil, err
		}

		backend.handler.SetTmpDir(tmpdir)
		backend.handler.SetBatchSize(config.BatchSize)
		backend.handler.SetStagedSync(stagedSync)
	}

	go SendPendingTxsToRpcDaemon(backend.txPool, backend.events)

	backend.quitMining = make(chan struct{})
	backend.pendingBlocks = make(chan *types.Block, 1)
	backend.minedBlocks = make(chan *types.Block, 1)
	go func() {
		for {
			select {
			case b := <-backend.minedBlocks:
				// todo: broadcast p2p
				if err := miningRPC.BroadcastMinedBlock(b); err != nil {
					log.Error("txpool rpc mined block broadcast", "err", err)
				}
			case b := <-backend.pendingBlocks:
				if err := miningRPC.BroadcastMinedBlock(b); err != nil {
					log.Error("txpool rpc mined block broadcast", "err", err)
				}
			case <-backend.quitMining:
				return
			}
		}
	}()

	if err := backend.StartMining(backend.chainKV, backend.pendingBlocks, backend.minedBlocks, mining, backend.config.Miner, backend.gasPrice, tmpdir, backend.quitMining); err != nil {
		return nil, err
	}

	//eth.APIBackend = &EthAPIBackend{stack.Config().ExtRPCEnabled(), stack.Config().AllowUnprotectedTxs, eth, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.Miner.GasPrice
	}
	//eth.APIBackend.gpo = gasprice.NewOracle(eth.APIBackend, gpoParams)
	backend.ethDialCandidates, err = setupDiscovery(backend.config.EthDiscoveryURLs)
	if err != nil {
		return nil, err
	}

	backend.ethDialCandidates, err = setupDiscovery(backend.config.EthDiscoveryURLs)
	if err != nil {
		return nil, err
	}

	// Register the backend on the node
	stack.RegisterAPIs(backend.APIs())
	if backend.config.P2PEnabled {
		stack.RegisterProtocols(backend.Protocols())
	}

	stack.RegisterLifecycle(backend)
	return backend, nil
}

func SendPendingTxsToRpcDaemon(txPool *core.TxPool, notifier *remotedbserver.Events) {
	if notifier == nil {
		return
	}

	txsCh := make(chan core.NewTxsEvent, txChanSize)
	txsSub := txPool.SubscribeNewTxsEvent(txsCh)
	defer txsSub.Unsubscribe()

	for {
		select {
		case e := <-txsCh:
			notifier.OnNewPendingTxs(e.Txs)
		case <-txsSub.Err():
			return
		}
	}
}

func BlockchainRuntimeConfig(config *ethconfig.Config) vm.Config {
	var (
		vmConfig = vm.Config{
			EnablePreimageRecording: config.EnablePreimageRecording,
			NoReceipts:              !config.StorageMode.Receipts,
		}
	)
	return vmConfig
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
func (s *Ethereum) StartMining(kv ethdb.RwKV, pendingBlocksCh chan *types.Block, minedBlocksCh chan *types.Block, mining *stagedsync.StagedSync, cfg params.MiningConfig, gasPrice *uint256.Int, tmpdir string, quitCh chan struct{}) error {
	if !cfg.Enabled {
		return nil
	}

	s.txPool.SetGasPrice(gasPrice)

	// Configure the local mining address
	eb, err := s.Etherbase()
	if err != nil {
		log.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %v", err)
	}
	if clique, ok := s.engine.(*clique.Clique); ok {
		if cfg.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %v", err)
		}

		clique.Authorize(eb, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
			return crypto.Sign(message, cfg.SigKey)
		})
	}

	if s.chainConfig.ChainID.Uint64() != params.MainnetChainConfig.ChainID.Uint64() {
		// If mining is started, we can disable the transaction rejection mechanism
		// introduced to speed sync times.
		if s.config.EnableDownloadV2 {

		} else {
			atomic.StoreUint32(&s.handler.acceptTxs, 1)
		}

		tx, err := kv.BeginRo(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		execution, _ := stages.GetStageProgress(tx, stages.Execution)
		hh := rawdb.ReadCurrentHeader(tx)
		tx.Rollback()
		if hh != nil {
			if err := s.txPool.Start(hh.GasLimit, execution); err != nil {
				return err
			}
		}
	}

	go func() {
		newTransactions := make(chan core.NewTxsEvent, txChanSize)
		sub := s.txPool.SubscribeNewTxsEvent(newTransactions)
		defer sub.Unsubscribe()
		defer close(newTransactions)

		var works bool
		var hasWork bool
		errc := make(chan error, 1)

		for {
			select {
			case <-newTransactions:
				hasWork = true
			case err := <-errc:
				works = false
				hasWork = false
				if err != nil {
					log.Warn("mining", "err", err)
				}
			case <-sub.Err():
				return
			case <-quitCh:
				return
			}

			if !works && hasWork {
				works = true
				go func() { errc <- s.miningStep(kv, pendingBlocksCh, minedBlocksCh, mining, cfg, tmpdir, quitCh) }()
			}
		}
	}()

	return nil
}

func (s *Ethereum) miningStep(kv ethdb.RwKV, pendingBlockCh chan *types.Block, minedBlockCh chan *types.Block, mining *stagedsync.StagedSync, cfg params.MiningConfig, tmpdir string, quitCh chan struct{}) error {
	sealCancel := make(chan struct{})
	tx, err := kv.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	miningState, err := mining.Prepare(
		nil,
		s.chainConfig,
		s.engine,
		&vm.Config{},
		nil,
		tx,
		"",
		ethdb.DefaultStorageMode,
		tmpdir,
		0,
		quitCh,
		nil,
		s.txPool,
		false,
		stagedsync.StageMiningCfg(cfg, true, pendingBlockCh, minedBlockCh, sealCancel),
	)
	if err != nil {
		return err
	}
	if err = miningState.Run(nil, tx); err != nil {
		return err
	}
	tx.Rollback()
	return nil
}

func (s *Ethereum) IsMining() bool { return s.config.Miner.Enabled }

func (s *Ethereum) TxPool() *core.TxPool        { return s.txPool }
func (s *Ethereum) ChainKV() ethdb.RwKV         { return s.chainKV }
func (s *Ethereum) NetVersion() (uint64, error) { return s.networkID, nil }

// Protocols returns all the currently configured
// network protocols to start.
func (s *Ethereum) Protocols() []p2p.Protocol {
	var headHeight uint64
	_ = s.chainKV.View(context.Background(), func(tx ethdb.Tx) error {
		headHeight, _ = stages.GetStageProgress(tx, stages.Finish)
		return nil
	})
	var readNodeInfo = func() *eth.NodeInfo {
		var res *eth.NodeInfo
		_ = s.chainKV.View(context.Background(), func(tx ethdb.Tx) error {
			res = eth.ReadNodeInfo(tx, s.chainConfig, s.genesisHash, s.networkID)
			return nil
		})

		return res
	}
	if s.config.EnableDownloadV2 {
		return download.MakeProtocols(
			s.downloadV2Ctx,
			readNodeInfo,
			s.ethDialCandidates,
			&s.sentryServer.Peers,
			s.sentryServer.GetStatus,
			s.sentryServer.ReceiveCh,
			s.sentryServer.ReceiveUploadCh,
			s.sentryServer.ReceiveTxCh)
	} else {
		return eth.MakeProtocols((*ethHandler)(s.handler), readNodeInfo, s.ethDialCandidates, s.chainConfig, s.genesisHash, headHeight)
	}
}

// Start implements node.Lifecycle, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *Ethereum) Start() error {
	eth.StartENRUpdater(s.chainConfig, s.genesisHash, s.events, s.p2pServer.LocalNode())

	// Figure out a max peers count based on the server limits
	maxPeers := s.p2pServer.MaxPeers
	if s.config.EnableDownloadV2 {
		go download.RecvMessage(s.downloadV2Ctx, s.sentries[0], s.downloadServer.HandleInboundMessage)
		go download.RecvUploadMessage(s.downloadV2Ctx, s.sentries[0], s.downloadServer.HandleInboundMessage)
		go download.Loop(s.downloadV2Ctx, s.chainDB, s.stagedSync2, s.downloadServer, s.events)
	} else {
		// Start the networking layer and the light server if requested
		s.handler.Start(maxPeers)
	}
	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	// Stop all the peer-related stuff first.
	if s.config.EnableDownloadV2 {
		s.downloadV2Cancel()
		s.txPoolP2PServer.TxFetcher.Stop()
		s.txPool.Stop()
	} else {
		s.handler.Stop()
	}
	if s.quitMining != nil {
		close(s.quitMining)
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

	//s.miner.Stop()
	s.engine.Close()
	if s.txPool != nil {
		s.txPool.Stop()
	}
	return nil
}
