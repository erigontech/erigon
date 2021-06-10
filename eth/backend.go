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
	"sync"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/clique"
	"github.com/ledgerwatch/erigon/consensus/ethash"
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
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/remote"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/txpool"
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

	// DB interfaces
	chainKV    ethdb.RwKV
	privateAPI *grpc.Server

	engine consensus.Engine

	gasPrice  *uint256.Int
	etherbase common.Address

	networkID uint64

	p2pServer *p2p.Server

	torrentClient *snapshotsync.Client

	lock              sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	events            *remotedbserver.Events
	chainConfig       *params.ChainConfig
	genesisHash       common.Hash
	quitMining        chan struct{}
	miningSealingQuit chan struct{}
	pendingBlocks     chan *types.Block
	minedBlocks       chan *types.Block

	// downloader v2 fields
	downloadV2Ctx        context.Context
	downloadV2Cancel     context.CancelFunc
	downloadServer       *download.ControlServerImpl
	sentryServers        []*download.SentryServerImpl
	txPoolP2PServer      *txpool.P2PServer
	sentries             []remote.SentryClient
	stagedSync2          *stagedsync.StagedSync
	waitForStageLoopStop chan struct{}
	waitForMiningStop    chan struct{}
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
	chainDb, err = stack.OpenDatabase(ethdb.Chain, stack.Config().DataDir)
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

		err = snapshotsync.WrapSnapshots(chainDb, snapshotsDir, stack.Config().MDBX)
		if err != nil {
			return nil, err
		}
		err = snapshotsync.SnapshotSeeding(chainDb, torrentClient, "headers", snapshotsDir)
		if err != nil {
			return nil, err
		}
	}

	chainConfig, genesis, genesisErr := core.CommitGenesisBlock(chainDb.RwKV(), config.Genesis, config.StorageMode.History)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	backend := &Ethereum{
		config:               config,
		chainKV:              chainDb.(ethdb.HasRwKV).RwKV(),
		networkID:            config.NetworkID,
		etherbase:            config.Miner.Etherbase,
		p2pServer:            stack.Server(),
		torrentClient:        torrentClient,
		chainConfig:          chainConfig,
		genesisHash:          genesis.Hash(),
		waitForStageLoopStop: make(chan struct{}),
		waitForMiningStop:    make(chan struct{}),
		sentries:             []remote.SentryClient{},
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

	if err := chainDb.RwKV().Update(context.Background(), func(tx ethdb.RwTx) error {
		if err := ethdb.SetStorageModeIfNotExist(tx, config.StorageMode); err != nil {
			return err
		}

		if err = stagedsync.UpdateMetrics(tx); err != nil {
			return err
		}

		sm, err := ethdb.GetStorageModeFromDB(tx)
		if err != nil {
			return err
		}
		if config.StorageMode.Initialised {
			// If storage mode is not explicitly specified, we take whatever is in the database
			if !reflect.DeepEqual(sm, config.StorageMode) {
				return errors.New("mode is " + config.StorageMode.ToString() + " original mode is " + sm.ToString())
			}
		} else {
			config.StorageMode = sm
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if config.TxPool.Journal != "" {
		config.TxPool.Journal = stack.ResolvePath(config.TxPool.Journal)
	}

	backend.txPool = core.NewTxPool(config.TxPool, chainConfig, chainDb.RwKV())

	// setting notifier to support streaming events to rpc daemon
	backend.events = remotedbserver.NewEvents()
	var mg *snapshotsync.SnapshotMigrator
	if config.SnapshotLayout {
		currentSnapshotBlock, currentInfohash, err := snapshotsync.GetSnapshotInfo(chainDb.RwKV())
		if err != nil {
			return nil, err
		}
		mg = snapshotsync.NewMigrator(snapshotsDir, currentSnapshotBlock, currentInfohash, stack.Config().MDBX)
		err = mg.RemoveNonCurrentSnapshots()
		if err != nil {
			log.Error("Remove non current snapshot", "err", err)
		}
	}

	backend.quitMining = make(chan struct{})
	backend.miningSealingQuit = make(chan struct{})
	backend.pendingBlocks = make(chan *types.Block, 1)
	backend.minedBlocks = make(chan *types.Block, 1)

	mining := stagedsync.New(
		stagedsync.MiningStages(
			stagedsync.StageMiningCreateBlockCfg(backend.chainKV, backend.config.Miner, *backend.chainConfig, backend.engine, backend.txPool, tmpdir),
			stagedsync.StageMiningExecCfg(backend.chainKV, backend.config.Miner, backend.events, *backend.chainConfig, backend.engine, &vm.Config{}, tmpdir),
			stagedsync.StageHashStateCfg(backend.chainKV, tmpdir),
			stagedsync.StageTrieCfg(backend.chainKV, false, true, tmpdir),
			stagedsync.StageMiningFinishCfg(backend.chainKV, *backend.chainConfig, backend.engine, backend.pendingBlocks, backend.minedBlocks, backend.miningSealingQuit),
		), stagedsync.MiningUnwindOrder(), stagedsync.OptionalParameters{})

	var ethashApi *ethash.API
	if casted, ok := backend.engine.(*ethash.Ethash); ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}

	kvRPC := remotedbserver.NewKvServer(backend.chainKV, stack.Config().MDBX)
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
		var readNodeInfo = func() *eth.NodeInfo {
			var res *eth.NodeInfo
			_ = backend.chainKV.View(context.Background(), func(tx ethdb.Tx) error {
				res = eth.ReadNodeInfo(tx, backend.chainConfig, backend.genesisHash, backend.networkID)
				return nil
			})

			return res
		}

		d66, err := setupDiscovery(backend.config.EthDiscoveryURLs)
		if err != nil {
			return nil, err
		}

		server66 := download.NewSentryServer(backend.downloadV2Ctx, stack.Config().P2P.Name,
			path.Join(stack.Config().DataDir, "erigon", "nodekey"),
			path.Join(stack.Config().DataDir, "nodes", "eth66"),
			stack.Config().P2P.ListenAddr, d66, readNodeInfo, eth.ETH66)
		backend.sentryServers = append(backend.sentryServers, server66)
		backend.sentries = []remote.SentryClient{remote.NewSentryClientDirect(eth.ETH66, server66)}
		if stack.Config().P2P.Eth65Enabled {
			d65, err := setupDiscovery(backend.config.EthDiscoveryURLs)
			if err != nil {
				return nil, err
			}
			server65 := download.NewSentryServer(backend.downloadV2Ctx, stack.Config().P2P.Name,
				path.Join(stack.Config().DataDir, "erigon", "nodekey"),
				path.Join(stack.Config().DataDir, "nodes", "eth65"),
				stack.Config().P2P.ListenAddr65, d65, readNodeInfo, eth.ETH65)
			backend.sentryServers = append(backend.sentryServers, server65)
			backend.sentries = append(backend.sentries, remote.NewSentryClientDirect(eth.ETH65, server65))
		}
	}
	blockDownloaderWindow := 65536
	backend.downloadServer, err = download.NewControlServer(chainDb.RwKV(), stack.Config().NodeName(), chainConfig, genesis.Hash(), backend.engine, backend.config.NetworkID, backend.sentries, blockDownloaderWindow)
	if err != nil {
		return nil, err
	}
	backend.txPoolP2PServer, err = txpool.NewP2PServer(backend.downloadV2Ctx, backend.sentries, backend.txPool)
	if err != nil {
		return nil, err
	}

	fetchTx := func(peerID string, hashes []common.Hash) error {
		backend.txPoolP2PServer.SendTxsRequest(context.TODO(), peerID, hashes)
		return nil
	}

	backend.txPoolP2PServer.TxFetcher = fetcher.NewTxFetcher(backend.txPool.Has, backend.txPool.AddRemotes, fetchTx)
	bodyDownloadTimeoutSeconds := 30 // TODO: convert to duration, make configurable

	backend.stagedSync2, err = stages2.NewStagedSync2(
		backend.downloadV2Ctx,
		backend.chainKV,
		config.StorageMode,
		config.BatchSize,
		bodyDownloadTimeoutSeconds,
		backend.downloadServer,
		tmpdir,
		snapshotsDir,
		backend.txPool,
		backend.txPoolP2PServer,
	)
	if err != nil {
		return nil, err
	}

	if config.SnapshotLayout {
		backend.stagedSync2.SetTorrentParams(torrentClient, snapshotsDir, mg)
		log.Info("Set torrent params", "snapshotsDir", snapshotsDir)
	}

	go SendPendingTxsToRpcDaemon(backend.txPool, backend.events)

	go func() {
		for {
			select {
			case b := <-backend.minedBlocks:
				// todo: broadcast p2p
				if err := miningRPC.BroadcastMinedBlock(b); err != nil {
					log.Error("txpool rpc mined block broadcast", "err", err)
				}
			case b := <-backend.pendingBlocks:
				if err := miningRPC.BroadcastPendingBlock(b); err != nil {
					log.Error("txpool rpc pending block broadcast", "err", err)
				}
			case <-backend.quitMining:
				return
			}
		}
	}()

	if err := backend.StartMining(context.Background(), backend.chainKV, mining, backend.config.Miner, backend.gasPrice, backend.quitMining); err != nil {
		return nil, err
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

const txChanSize int = 4096

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
func (s *Ethereum) StartMining(ctx context.Context, kv ethdb.RwKV, mining *stagedsync.StagedSync, cfg params.MiningConfig, gasPrice *uint256.Int, quitCh chan struct{}) error {
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
		defer close(s.waitForMiningStop)
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
				if errors.Is(err, common.ErrStopped) {
					return
				}
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
				go func() { errc <- stages2.MiningStep(ctx, kv, mining) }()
			}
		}
	}()

	return nil
}

func (s *Ethereum) IsMining() bool { return s.config.Miner.Enabled }

func (s *Ethereum) TxPool() *core.TxPool        { return s.txPool }
func (s *Ethereum) ChainKV() ethdb.RwKV         { return s.chainKV }
func (s *Ethereum) NetVersion() (uint64, error) { return s.networkID, nil }

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
		go download.RecvMessageLoop(s.downloadV2Ctx, s.sentries[i], s.downloadServer, nil)
		go download.RecvUploadMessageLoop(s.downloadV2Ctx, s.sentries[i], s.downloadServer, nil)
	}

	go Loop(s.downloadV2Ctx, s.chainKV, s.stagedSync2, s.downloadServer, s.events, s.config.StateStream, s.waitForStageLoopStop)
	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	// Stop all the peer-related stuff first.
	s.downloadV2Cancel()
	s.txPoolP2PServer.TxFetcher.Stop()
	s.txPool.Stop()
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
	<-s.waitForStageLoopStop
	if s.config.Miner.Enabled {
		<-s.waitForMiningStop
	}
	return nil
}

//Deprecated - use stages.StageLoop
func Loop(ctx context.Context, db ethdb.RwKV, sync *stagedsync.StagedSync, controlServer *download.ControlServerImpl, notifier stagedsync.ChainEventNotifier, stateStream bool, waitForDone chan struct{}) {
	stages2.StageLoop(
		ctx,
		db,
		sync,
		controlServer.Hd,
		controlServer.ChainConfig,
		notifier,
		stateStream,
		controlServer.UpdateHead,
		waitForDone,
	)
}
