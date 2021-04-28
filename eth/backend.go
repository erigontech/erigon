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
	"github.com/ledgerwatch/turbo-geth/event"
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

	lock        sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	events      *remotedbserver.Events
	chainConfig *params.ChainConfig
	genesisHash common.Hash
	quitMining  chan struct{}

	// downloader v2 fields
	downloadV2Ctx    context.Context
	downloadV2Cancel context.CancelFunc
	downloadServer   *download.ControlServerImpl
	sentryServer     *download.SentryServerImpl
	txPoolServer     *download.TxPoolServer
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
	snapshotsDir:= stack.Config().ResolvePath("snapshots")
	v,err:=chainDb.Get(dbutils.BittorrentInfoBucket, []byte(dbutils.BittorrentPeerID))
	if err!=nil && !errors.Is(err, ethdb.ErrKeyNotFound) {
		log.Error("Get bittorrent peer","err", err)
	}
	torrentClient, err = snapshotsync.New(snapshotsDir, config.SnapshotSeeding, string(v))
	if err != nil {
		return nil, err
	}
	if len(v)==0 {
		log.Info("Generate new bittorent peerID", "id", common.Bytes2Hex(torrentClient.PeerID()))
		err = torrentClient.SavePeerID(chainDb)
		if err!=nil {
			log.Error("Bittorrent peerID haven't saved","err", err)
		}
	}

	if config.SnapshotLayout {
		/*
		0) Если скачивание начато, то продолжить/начать качать
		1) Понять, какие сейчас снепшоты есть и рабочие
		2) Подключиться к ним
		3) Обернуть в них дб
		4) Поставить сид
		 */
		err = snapshotsync.WrapSnapshots(chainDb, snapshotsDir)
		if err!=nil {
			return nil, err
		}
		err= snapshotsync.SnapshotSeeding(chainDb, torrentClient, "headers", snapshotsDir)
		if err!=nil {
			return nil, err
		}
	}

	chainConfig, genesisHash, genesisErr := core.SetupGenesisBlockWithOverride(chainDb, config.Genesis, config.OverrideBerlin, config.StorageMode.History, false /* overwrite */)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	eth := &Ethereum{
		config:        config,
		chainDB:       chainDb,
		chainKV:       chainDb.(ethdb.HasRwKV).RwKV(),
		engine:        ethconfig.CreateConsensusEngine(chainConfig, &config.Ethash, config.Miner.Notify, config.Miner.Noverify),
		networkID:     config.NetworkID,
		etherbase:     config.Miner.Etherbase,
		p2pServer:     stack.Server(),
		torrentClient: torrentClient,
		chainConfig:   chainConfig,
		genesisHash:   genesisHash,
	}
	eth.gasPrice, _ = uint256.FromBig(config.Miner.GasPrice)

	var consensusConfig interface{}

	if chainConfig.Clique != nil {
		consensusConfig = &config.Clique
	} else {
		consensusConfig = &config.Ethash
	}

	if !eth.config.EnableDownloadV2 {
		eth.engine = ethconfig.CreateConsensusEngine(chainConfig, consensusConfig, config.Miner.Notify, config.Miner.Noverify)
	}

	log.Info("Initialising Ethereum protocol", "network", config.NetworkID)

	err = ethdb.SetStorageModeIfNotExist(chainDb, config.StorageMode)
	if err != nil {
		return nil, err
	}

	sm, err := ethdb.GetStorageModeFromDB(chainDb)
	if err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(sm, config.StorageMode) {
		return nil, errors.New("mode is " + config.StorageMode.ToString() + " original mode is " + sm.ToString())
	}

	if err = stagedsync.UpdateMetrics(chainDb); err != nil {
		return nil, err
	}

	vmConfig, _ := BlockchainRuntimeConfig(config)
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())

	if config.TxPool.Journal != "" {
		config.TxPool.Journal, err = stack.ResolvePath(config.TxPool.Journal)
		if err != nil {
			return nil, err
		}
	}

	eth.txPool = core.NewTxPool(config.TxPool, chainConfig, chainDb, txCacher)

	stagedSync := config.StagedSync

	// setting notifier to support streaming events to rpc daemon
	eth.events = remotedbserver.NewEvents()
	var mg *snapshotsync.SnapshotMigrator
	if config.SnapshotLayout {
		fmt.Println("SnapshotLayout", config.SnapshotLayout, stagedSync == nil, config.SnapshotMode)
		currentSnapshotBlock, currentInfohash,err:=snapshotsync.GetSnapshotInfo(chainDb)
		if err!=nil {
			return nil, err
		}
		mg=snapshotsync.NewMigrator(snapshotsDir, currentSnapshotBlock, currentInfohash)
		err = mg.RemoveNonCurrentSnapshots(chainDb)
		if err!=nil {
			log.Error("Remove non current snapshot", "err", err)
		}
	}
	if stagedSync == nil {
		// if there is not stagedsync, we create one with the custom notifier
		if config.SnapshotLayout {
			stagedSync = stagedsync.New(stagedsync.WithSnapshotsStages(), stagedsync.UnwindOrderWithSnapshots(), stagedsync.OptionalParameters{Notifier: eth.events, SnapshotDir: snapshotsDir, TorrnetClient: torrentClient, SnapshotMigrator:mg})
		} else {
			stagedSync = stagedsync.New(stagedsync.DefaultStages(), stagedsync.DefaultUnwindOrder(), stagedsync.OptionalParameters{Notifier: eth.events})
		}
	} else {
		// otherwise we add one if needed
		if stagedSync.Notifier == nil {
			stagedSync.Notifier = eth.events
		}
		if config.SnapshotLayout {
			stagedSync.SetTorrentParams(torrentClient, snapshotsDir, mg)
			log.Info("Set torrent params", "snapshotsDir", snapshotsDir)
		}
	}

	mining := stagedsync.New(stagedsync.MiningStages(), stagedsync.MiningUnwindOrder(), stagedsync.OptionalParameters{})

	var ethashApi *ethash.API
	if casted, ok := eth.engine.(*ethash.Ethash); ok {
		ethashApi = casted.APIs(nil)[1].Service.(*ethash.API)
	}
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
			eth.privateAPI, err = remotedbserver.StartGrpc(
				chainDb.(ethdb.HasRwKV).RwKV(),
				eth,
				ethashApi,
				stack.Config().PrivateApiAddr,
				stack.Config().PrivateApiRateLimit,
				&creds,
				eth.events,
				gitCommit)
			if err != nil {
				return nil, err
			}
		} else {
			eth.privateAPI, err = remotedbserver.StartGrpc(
				chainDb.(ethdb.HasRwKV).RwKV(),
				eth,
				ethashApi,
				stack.Config().PrivateApiAddr,
				stack.Config().PrivateApiRateLimit,
				nil,
				eth.events,
				gitCommit)
			if err != nil {
				return nil, err
			}
		}
	}

	checkpoint := config.Checkpoint
	if eth.config.EnableDownloadV2 {
		eth.sentryServer = download.NewSentryServer(context.Background())
		sentry := &download.SentryClientDirect{}
		eth.sentryServer.P2pServer = eth.p2pServer
		sentry.SetServer(eth.sentryServer)
		eth.sentries = []proto_sentry.SentryClient{sentry}
		blockDownloaderWindow := 65536
		eth.downloadV2Ctx, eth.downloadV2Cancel = context.WithCancel(context.Background())
		eth.downloadServer, err = download.NewControlServer(chainDb, stack.Config().NodeName(), chainConfig, genesisHash, eth.engine, eth.config.NetworkID, eth.sentries, blockDownloaderWindow)
		if err != nil {
			return nil, err
		}
		if err = download.SetSentryStatus(eth.downloadV2Ctx, sentry, eth.downloadServer); err != nil {
			return nil, err
		}
		eth.txPoolServer, err = download.NewTxPoolServer(eth.sentries, eth.txPool)
		if err != nil {
			return nil, err
		}

		fetchTx := func(peerID string, hashes []common.Hash) error {
			eth.txPoolServer.SendTxsRequest(context.TODO(), peerID, hashes)
			return nil
		}

		eth.txPoolServer.TxFetcher = fetcher.NewTxFetcher(eth.txPool.Has, eth.txPool.AddRemotes, fetchTx)
		bodyDownloadTimeoutSeconds := 30 // TODO: convert to duration, make configurable

		eth.stagedSync2, err = download.NewStagedSync(
			eth.downloadV2Ctx,
			eth.chainDB,
			config.BatchSize,
			bodyDownloadTimeoutSeconds,
			eth.downloadServer,
			tmpdir,
			eth.txPool,
		)
		if err != nil {
			return nil, err
		}

	} else {
		genesisBlock, _ := rawdb.ReadBlockByNumber(chainDb, 0)
		if genesisBlock == nil {
			return nil, core.ErrNoGenesis
		}

		if eth.handler, err = newHandler(&handlerConfig{
			Database:    chainDb,
			ChainConfig: chainConfig,
			genesis:     genesisBlock,
			vmConfig:    &vmConfig,
			engine:      eth.engine,
			TxPool:      eth.txPool,
			Network:     config.NetworkID,
			Checkpoint:  checkpoint,

			Whitelist: config.Whitelist,
		}); err != nil {
			return nil, err
		}

		eth.handler.SetTmpDir(tmpdir)
		eth.handler.SetBatchSize(config.BatchSize)
		eth.handler.SetStagedSync(stagedSync)
	}

	go SendPendingTxsToRpcDaemon(eth.txPool, eth.events)

	if err := eth.StartMining(mining, tmpdir); err != nil {
		return nil, err
	}

	//eth.APIBackend = &EthAPIBackend{stack.Config().ExtRPCEnabled(), stack.Config().AllowUnprotectedTxs, eth, nil}
	gpoParams := config.GPO
	if gpoParams.Default == nil {
		gpoParams.Default = config.Miner.GasPrice
	}
	//eth.APIBackend.gpo = gasprice.NewOracle(eth.APIBackend, gpoParams)
	eth.ethDialCandidates, err = setupDiscovery(eth.config.EthDiscoveryURLs)
	if err != nil {
		return nil, err
	}

	eth.ethDialCandidates, err = setupDiscovery(eth.config.EthDiscoveryURLs)
	if err != nil {
		return nil, err
	}

	// Register the backend on the node
	stack.RegisterAPIs(eth.APIs())
	stack.RegisterProtocols(eth.Protocols())
	stack.RegisterLifecycle(eth)
	// Check for unclean shutdown
	return eth, nil
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

func BlockchainRuntimeConfig(config *ethconfig.Config) (vm.Config, *core.CacheConfig) {
	var (
		vmConfig = vm.Config{
			EnablePreimageRecording: config.EnablePreimageRecording,
			NoReceipts:              !config.StorageMode.Receipts,
		}
		cacheConfig = &core.CacheConfig{
			Pruning:             config.Pruning,
			BlocksBeforePruning: config.BlocksBeforePruning,
			BlocksToPrune:       config.BlocksToPrune,
			PruneTimeout:        config.PruningTimeout,
			DownloadOnly:        config.DownloadOnly,
			NoHistory:           !config.StorageMode.History,
		}
	)
	return vmConfig, cacheConfig
}

// func makeExtraData(extra []byte) []byte {
// 	if len(extra) == 0 {
// 		// create default extradata
// 		extra, _ = rlp.EncodeToBytes([]interface{}{
// 			uint(params.VersionMajor<<16 | params.VersionMinor<<8 | params.VersionMicro),
// 			"turbo-geth",
// 			runtime.GOOS,
// 		})
// 	}
// 	if uint64(len(extra)) > params.MaximumExtraDataSize {
// 		log.Warn("Miner extra data exceed limit", "extra", hexutil.Bytes(extra), "limit", params.MaximumExtraDataSize)
// 		extra = nil
// 	}
// 	return extra
// }

func (s *Ethereum) APIs() []rpc.API {
	return []rpc.API{}
}

/*
// APIs return the collection of RPC services the ethereum package offers.
// NOTE, some of these services probably need to be moved to somewhere else.
func (s *Ethereum) APIs() []rpc.API {
	if s.APIBackend == nil {
		return []rpc.API{}
	}
	apis := ethapi.GetAPIs(s.APIBackend)

	// Append any APIs exposed explicitly by the consensus engine
	//apis = append(apis, s.engine.APIs(s.BlockChain())...)

	// Append all the local APIs and return
	return append(apis, []rpc.API{
		//{
		//	Namespace: "eth",
		//	Version:   "1.0",
		//	Service:   NewPublicEthereumAPI(s),
		//	Public:    true,
		//},
		//{
		//	Namespace: "eth",
		//	Version:   "1.0",
		//	Service:   NewPublicMinerAPI(s),
		//	Public:    true,
		//},
		//{
		//	Namespace: "eth",
		//	Version:   "1.0",
		//	Service:   downloader.NewPublicDownloaderAPI(s.handler.downloader, s.eventMux),
		//	Public:    true,
		//},
		//{
		//	Namespace: "miner",
		//	Version:   "1.0",
		//	Service:   NewPrivateMinerAPI(s),
		//	Public:    false,
		//},
		//{
		//	Namespace: "eth",
		//	Version:   "1.0",
		//	Service:   filters.NewPublicFilterAPI(s.APIBackend, 5*time.Minute),
		//	Public:    true,
		//},
		//{
		//	Namespace: "admin",
		//	Version:   "1.0",
		//	Service:   NewPrivateAdminAPI(s),
		//},
		//{
		//	Namespace: "debug",
		//	Version:   "1.0",
		//	Service:   NewPublicDebugAPI(s),
		//	Public:    true,
		//}, {
		//	Namespace: "debug",
		//	Version:   "1.0",
		//	Service:   NewPrivateDebugAPI(s),
		//},
		{
			Namespace: "net",
			Version:   "1.0",
			Service:   s.netRPCService,
			Public:    true,
		},
	}...)
}
*/

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
func (s *Ethereum) StartMining(mining *stagedsync.StagedSync, tmpdir string) error {
	if !s.config.Miner.Enabled {
		return nil
	}

	s.lock.Lock()
	price := s.gasPrice
	s.quitMining = make(chan struct{})
	s.lock.Unlock()
	s.txPool.SetGasPrice(price)

	// Configure the local mining address
	eb, err := s.Etherbase()
	if err != nil {
		log.Error("Cannot start mining without etherbase", "err", err)
		return fmt.Errorf("etherbase missing: %v", err)
	}
	if clique, ok := s.engine.(*clique.Clique); ok {
		if s.config.Miner.SigKey == nil {
			log.Error("Etherbase account unavailable locally", "err", err)
			return fmt.Errorf("signer missing: %v", err)
		}

		clique.Authorize(eb, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
			return crypto.Sign(message, s.config.Miner.SigKey)
		})
	}

	if s.chainConfig.ChainID.Uint64() != params.MainnetChainConfig.ChainID.Uint64() {
		// If mining is started, we can disable the transaction rejection mechanism
		// introduced to speed sync times.
		if s.config.EnableDownloadV2 {

		} else {
			atomic.StoreUint32(&s.handler.acceptTxs, 1)
		}

		tx, err := s.chainKV.BeginRo(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
		hh := rawdb.ReadCurrentHeader(tx)
		if hh != nil {
			execution, _ := stages.GetStageProgress(tx, stages.Execution)
			if err := s.txPool.Start(hh.GasLimit, execution); err != nil {
				return err
			}
		}
	}
	txsChMining := make(chan core.NewTxsEvent, txChanSize)
	txsSubMining := s.txPool.SubscribeNewTxsEvent(txsChMining)

	s.quitMining = make(chan struct{})
	go func() {
		defer txsSubMining.Unsubscribe()
		defer close(txsChMining)
		s.miningLoop(txsChMining, txsSubMining, mining, tmpdir, s.quitMining)
	}()

	return nil
}

func (s *Ethereum) miningLoop(newTransactions chan core.NewTxsEvent, sub event.Subscription, mining *stagedsync.StagedSync, tmpdir string, quitCh chan struct{}) {
	var works bool
	var hasWork bool
	errc := make(chan error, 1)
	resultCh := make(chan *types.Block, 1)

	for {
		select {
		case <-newTransactions:
			hasWork = true
		case minedBlock := <-resultCh:
			works = false
			// TODO: send mined block to sentry
			_ = minedBlock
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
			go func() { errc <- s.miningStep(resultCh, mining, tmpdir, quitCh) }()
		}
	}
}

func (s *Ethereum) miningStep(resultCh chan *types.Block, mining *stagedsync.StagedSync, tmpdir string, quitCh chan struct{}) error {
	tx, err := s.chainKV.BeginRw(context.Background())
	if err != nil {
		return err
	}
	defer tx.Rollback()
	txdb := ethdb.NewRwTxDb(tx)
	sealCancel := make(chan struct{})
	miningState, err := mining.Prepare(
		nil,
		s.chainConfig,
		s.engine,
		&vm.Config{},
		nil,
		txdb,
		"",
		ethdb.DefaultStorageMode,
		tmpdir,
		0,
		quitCh,
		nil,
		s.txPool,
		false,
		stagedsync.StageMiningCfg(s.config.Miner, true, resultCh, sealCancel),
		stagedsync.StageSendersCfg(s.chainConfig),
	)
	if err != nil {
		return err
	}
	if err = miningState.Run(txdb, txdb); err != nil {
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
			&s.sentryServer.PeerHeightMap,
			&s.sentryServer.PeerTimeMap,
			&s.sentryServer.PeerRwMap,
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
		go download.RecvTxMessage(s.downloadV2Ctx, s.sentries[0], s.txPoolServer.HandleInboundMessage)
		go download.Loop(s.downloadV2Ctx, s.chainDB, s.stagedSync2, s.downloadServer)
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
		s.txPoolServer.TxFetcher.Stop()
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
