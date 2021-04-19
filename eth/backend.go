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
	"crypto/ecdsa"
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
	ethereum "github.com/ledgerwatch/turbo-geth"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/clique"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/bloombits"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/crypto"
	"github.com/ledgerwatch/turbo-geth/eth/downloader"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
	"github.com/ledgerwatch/turbo-geth/eth/ethutils"
	"github.com/ledgerwatch/turbo-geth/eth/protocols/eth"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/p2p"
	"github.com/ledgerwatch/turbo-geth/p2p/enode"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync/bittorrent"
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
	txPool             *core.TxPool
	blockchain         *core.BlockChain
	handler            *handler
	ethDialCandidates  enode.Iterator
	snapDialCandidates enode.Iterator

	// DB interfaces
	chainKV    ethdb.RwKV // Same as chainDb, but different interface
	privateAPI *grpc.Server

	engine consensus.Engine

	bloomRequests chan chan *bloombits.Retrieval // Channel receiving bloom data retrieval requests

	//APIBackend *EthAPIBackend

	gasPrice  *uint256.Int
	etherbase common.Address
	signer    *ecdsa.PrivateKey

	networkID uint64
	//netRPCService *ethapi.PublicNetAPI

	p2pServer *p2p.Server

	torrentClient *bittorrent.Client

	lock        sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	events      *remotedbserver.Events
	chainConfig *params.ChainConfig
	genesisHash common.Hash
}

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(stack *node.Node, config *ethconfig.Config) (*Ethereum, error) {
	if config.Miner.GasPrice == nil || config.Miner.GasPrice.Cmp(common.Big0) <= 0 {
		log.Warn("Sanitizing invalid miner gas price", "provided", config.Miner.GasPrice, "updated", ethconfig.Defaults.Miner.GasPrice)
		config.Miner.GasPrice = new(big.Int).Set(ethconfig.Defaults.Miner.GasPrice)
	}

	tmpdir := path.Join(stack.Config().DataDir, etl.TmpDirName)

	// Assemble the Ethereum object
	var chainDb ethdb.Database
	var err error
	if config.EnableDebugProtocol {
		if err = os.RemoveAll("simulator"); err != nil {
			return nil, fmt.Errorf("removing simulator db: %w", err)
		}
		chainDb = ethdb.MustOpen("simulator")
	} else {
		chainDb, err = stack.OpenDatabaseWithFreezer("chaindata", tmpdir)
		if err != nil {
			return nil, err
		}
	}

	chainConfig, genesisHash, genesisErr := core.SetupGenesisBlockWithOverride(chainDb, config.Genesis, config.OverrideBerlin, config.StorageMode.History, false /* overwrite */)

	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		return nil, genesisErr
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	var torrentClient *bittorrent.Client
	if config.SnapshotMode != (snapshotsync.SnapshotMode{}) && config.NetworkID == params.MainnetChainConfig.ChainID.Uint64() {
		if config.ExternalSnapshotDownloaderAddr != "" {
			cli, cl, innerErr := snapshotsync.NewClient(config.ExternalSnapshotDownloaderAddr)
			if innerErr != nil {
				return nil, innerErr
			}
			defer cl() //nolint

			_, innerErr = cli.Download(context.Background(), &snapshotsync.DownloadSnapshotRequest{
				NetworkId: config.NetworkID,
				Type:      config.SnapshotMode.ToSnapshotTypes(),
			})
			if innerErr != nil {
				return nil, innerErr
			}

			waitDownload := func() (map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo, error) {
				snapshotReadinessCheck := func(mp map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo, tp snapshotsync.SnapshotType) bool {
					if mp[tp].Readiness != int32(100) {
						log.Info("Downloading", "snapshot", tp, "%", mp[tp].Readiness)
						return false
					}
					return true
				}
				for {
					mp := make(map[snapshotsync.SnapshotType]*snapshotsync.SnapshotsInfo)
					snapshots, err1 := cli.Snapshots(context.Background(), &snapshotsync.SnapshotsRequest{NetworkId: config.NetworkID})
					if err1 != nil {
						return nil, err1
					}
					for i := range snapshots.Info {
						if mp[snapshots.Info[i].Type].SnapshotBlock < snapshots.Info[i].SnapshotBlock && snapshots.Info[i] != nil {
							mp[snapshots.Info[i].Type] = snapshots.Info[i]
						}
					}

					downloaded := true
					if config.SnapshotMode.Headers {
						if !snapshotReadinessCheck(mp, snapshotsync.SnapshotType_headers) {
							downloaded = false
						}
					}
					if config.SnapshotMode.Bodies {
						if !snapshotReadinessCheck(mp, snapshotsync.SnapshotType_bodies) {
							downloaded = false
						}
					}
					if config.SnapshotMode.State {
						if !snapshotReadinessCheck(mp, snapshotsync.SnapshotType_state) {
							downloaded = false
						}
					}
					if config.SnapshotMode.Receipts {
						if !snapshotReadinessCheck(mp, snapshotsync.SnapshotType_receipts) {
							downloaded = false
						}
					}
					if downloaded {
						return mp, nil
					}
					time.Sleep(time.Second * 10)
				}
			}
			downloadedSnapshots, innerErr := waitDownload()
			if innerErr != nil {
				return nil, innerErr
			}
			snapshotKV := chainDb.(ethdb.HasRwKV).RwKV()

			snapshotKV, innerErr = snapshotsync.WrapBySnapshotsFromDownloader(snapshotKV, downloadedSnapshots)
			if innerErr != nil {
				return nil, innerErr
			}
			chainDb.(ethdb.HasRwKV).SetRwKV(snapshotKV)

			innerErr = snapshotsync.PostProcessing(chainDb, config.SnapshotMode, downloadedSnapshots)
			if innerErr != nil {
				return nil, innerErr
			}
		} else {
			var dbPath string
			dbPath, err = stack.Config().ResolvePath("snapshots")
			if err != nil {
				return nil, err
			}
			torrentClient, err = bittorrent.New(dbPath, config.SnapshotSeeding)
			if err != nil {
				return nil, err
			}

			err = torrentClient.Load(chainDb)
			if err != nil {
				return nil, err
			}
			err = torrentClient.AddSnapshotsTorrents(context.Background(), chainDb, config.NetworkID, config.SnapshotMode)
			if err == nil {
				torrentClient.Download()
				snapshotKV := chainDb.(ethdb.HasRwKV).RwKV()
				mp, innerErr := torrentClient.GetSnapshots(chainDb, config.NetworkID)
				if innerErr != nil {
					return nil, innerErr
				}

				snapshotKV, innerErr = snapshotsync.WrapBySnapshotsFromDownloader(snapshotKV, mp)
				if innerErr != nil {
					return nil, innerErr
				}
				chainDb.(ethdb.HasRwKV).SetRwKV(snapshotKV)
				tx, err := chainDb.Begin(context.Background(), ethdb.RW)
				if err != nil {
					return nil, err
				}
				defer tx.Rollback()
				innerErr = snapshotsync.PostProcessing(chainDb, config.SnapshotMode, mp)
				if err = tx.Commit(); err != nil {
					return nil, err
				}
				if innerErr != nil {
					return nil, innerErr
				}
			} else {
				log.Error("There was an error in snapshot init. Swithing to regular sync", "err", err)
			}
		}
	}

	eth := &Ethereum{
		config:        config,
		chainKV:       chainDb.(ethdb.HasRwKV).RwKV(),
		engine:        ethconfig.CreateConsensusEngine(chainConfig, &config.Ethash, config.Miner.Notify, config.Miner.Noverify, chainDb),
		networkID:     config.NetworkID,
		etherbase:     config.Miner.Etherbase,
		bloomRequests: make(chan chan *bloombits.Retrieval),
		p2pServer:     stack.Server(),
		torrentClient: torrentClient,
		chainConfig:   chainConfig,
		genesisHash:   genesisHash,
	}
	eth.gasPrice, _ = uint256.FromBig(config.Miner.GasPrice)

	log.Info("Initialising Ethereum protocol", "network", config.NetworkID)

	bcVersion := rawdb.ReadDatabaseVersion(chainDb)
	var dbVer = "<nil>"
	if bcVersion != nil {
		dbVer = fmt.Sprintf("%d", *bcVersion)
	}

	if !config.SkipBcVersionCheck {
		if bcVersion != nil && *bcVersion > core.BlockChainVersion {
			return nil, fmt.Errorf("database version is v%d, Geth %s only supports v%d", *bcVersion, params.VersionWithMeta, core.BlockChainVersion)
		} else if bcVersion == nil || *bcVersion < core.BlockChainVersion {
			log.Warn("Upgrade blockchain database version", "from", dbVer, "to", core.BlockChainVersion)
			if err2 := rawdb.WriteDatabaseVersion(chainDb, core.BlockChainVersion); err2 != nil {
				return nil, err2
			}
		}
	}

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

	vmConfig, cacheConfig := BlockchainRuntimeConfig(config)
	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	eth.blockchain, err = core.NewBlockChain(chainDb, cacheConfig, chainConfig, eth.engine, vmConfig, eth.shouldPreserve, txCacher)
	if err != nil {
		return nil, err
	}

	eth.blockchain.EnableReceipts(config.StorageMode.Receipts)
	eth.blockchain.EnableTxLookupIndex(config.StorageMode.TxIndex)

	// Rewind the chain in case of an incompatible config upgrade.
	if compat, ok := genesisErr.(*params.ConfigCompatError); ok {
		log.Warn("Rewinding chain to upgrade configuration", "err", compat)
		eth.blockchain.SetHead(compat.RewindTo)
		err = rawdb.WriteChainConfig(chainDb, genesisHash, chainConfig)
		if err != nil {
			return nil, err
		}
	}

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
	if stagedSync == nil {
		// if there is not stagedsync, we create one with the custom notifier
		stagedSync = stagedsync.New(stagedsync.DefaultStages(), stagedsync.DefaultUnwindOrder(), stagedsync.OptionalParameters{Notifier: eth.events})
	} else {
		// otherwise we add one if needed
		if stagedSync.Notifier == nil {
			stagedSync.Notifier = eth.events
		}
	}

	mining := stagedsync.New(stagedsync.MiningStages(), stagedsync.MiningUnwindOrder(), stagedsync.OptionalParameters{})

	var ethashApi *ethash.API
	if casted, ok := eth.Engine().(*ethash.Ethash); ok {
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
			eth.privateAPI, err = remotedbserver.StartGrpc(chainDb.(ethdb.HasRwKV).RwKV(), eth, ethashApi, stack.Config().PrivateApiAddr, stack.Config().PrivateApiRateLimit, &creds, eth.events)
			if err != nil {
				return nil, err
			}
		} else {
			eth.privateAPI, err = remotedbserver.StartGrpc(chainDb.(ethdb.HasRwKV).RwKV(), eth, ethashApi, stack.Config().PrivateApiAddr, stack.Config().PrivateApiRateLimit, nil, eth.events)
			if err != nil {
				return nil, err
			}
		}
	}

	checkpoint := config.Checkpoint
	if eth.handler, err = newHandler(&handlerConfig{
		Database:    chainDb,
		Chain:       eth.blockchain,
		ChainConfig: eth.blockchain.Config(),
		genesis:     eth.blockchain.Genesis(),
		vmConfig:    eth.blockchain.GetVMConfig(),
		engine:      eth.blockchain.Engine(),
		TxPool:      eth.txPool,
		Network:     config.NetworkID,
		Checkpoint:  checkpoint,

		Whitelist: config.Whitelist,
		Mining:    &config.Miner,
	}); err != nil {
		return nil, err
	}
	eth.snapDialCandidates, _ = setupDiscovery(eth.config.SnapDiscoveryURLs) //nolint:staticcheck
	eth.handler.SetTmpDir(tmpdir)
	eth.handler.SetBatchSize(config.CacheSize, config.BatchSize)
	eth.handler.SetStagedSync(stagedSync)
	eth.handler.SetMining(mining)

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
			ArchiveSyncInterval: uint64(config.ArchiveSyncInterval),
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
func (s *Ethereum) isLocalBlock(block *types.Block) bool {
	s.lock.RLock()
	etherbase := s.etherbase
	s.lock.RUnlock()
	return ethutils.IsLocalBlock(s.engine, etherbase, s.config.TxPool.Locals, block.Header())
}

// shouldPreserve checks whether we should preserve the given block
// during the chain reorg depending on whether the author of block
// is a local account.
func (s *Ethereum) shouldPreserve(block *types.Block) bool {
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

// SetEtherbase sets the mining reward address.
func (s *Ethereum) SetEtherbase(etherbase common.Address) {
	s.lock.Lock()
	s.etherbase = etherbase
	s.lock.Unlock()
}

// StartMining starts the miner with the given number of CPU threads. If mining
// is already running, this method adjust the number of threads allowed to use
// and updates the minimum price required by the transaction pool.
func (s *Ethereum) StartMining(threads int) error {
	// Update the thread count within the consensus engine
	type threaded interface {
		SetThreads(threads int)
	}
	if th, ok := s.engine.(threaded); ok {
		log.Info("Updated mining threads", "threads", threads)
		if threads == 0 {
			threads = -1 // Disable the miner from within
		}
		th.SetThreads(threads)
	}
	// If the miner was not running, initialize it
	if !s.IsMining() {
		// Propagate the initial price point to the transaction pool
		s.lock.RLock()
		price := s.gasPrice
		s.lock.RUnlock()
		s.txPool.SetGasPrice(price)

		// Configure the local mining address
		eb, err := s.Etherbase()
		if err != nil {
			log.Error("Cannot start mining without etherbase", "err", err)
			return fmt.Errorf("etherbase missing: %v", err)
		}
		if clique, ok := s.engine.(*clique.Clique); ok {
			if s.signer == nil {
				log.Error("Etherbase account unavailable locally", "err", err)
				return fmt.Errorf("signer missing: %v", err)
			}

			clique.Authorize(eb, func(_ common.Address, mimeType string, message []byte) ([]byte, error) {
				return crypto.Sign(message, s.signer)
			})
		}
		// If mining is started, we can disable the transaction rejection mechanism
		// introduced to speed sync times.
		atomic.StoreUint32(&s.handler.acceptTxs, 1)
	}
	return nil
}

// StopMining terminates the miner, both at the consensus engine level as well as
// at the block creation level.
func (s *Ethereum) StopMining() {
	// Update the thread count within the consensus engine
	type threaded interface {
		SetThreads(threads int)
	}
	if th, ok := s.engine.(threaded); ok {
		th.SetThreads(-1)
	}
}

func (s *Ethereum) IsMining() bool { return s.config.Miner.Enabled }

func (s *Ethereum) BlockChain() *core.BlockChain       { return s.blockchain }
func (s *Ethereum) TxPool() *core.TxPool               { return s.txPool }
func (s *Ethereum) Engine() consensus.Engine           { return s.engine }
func (s *Ethereum) ChainKV() ethdb.RwKV                { return s.chainKV }
func (s *Ethereum) IsListening() bool                  { return true } // Always listening
func (s *Ethereum) Downloader() *downloader.Downloader { return s.handler.downloader }
func (s *Ethereum) NetVersion() (uint64, error)        { return s.networkID, nil }
func (s *Ethereum) SyncProgress() ethereum.SyncProgress {
	return s.handler.downloader.Progress()
}
func (s *Ethereum) Synced() bool      { return atomic.LoadUint32(&s.handler.acceptTxs) == 1 }
func (s *Ethereum) ArchiveMode() bool { return !s.config.Pruning }

// Protocols returns all the currently configured
// network protocols to start.
func (s *Ethereum) Protocols() []p2p.Protocol {
	var headHeight uint64
	_ = s.chainKV.View(context.Background(), func(tx ethdb.Tx) error {
		headHeight, _ = stages.GetStageProgress(tx, stages.Finish)
		return nil
	})
	protos := eth.MakeProtocols((*ethHandler)(s.handler), s.networkID, s.ethDialCandidates, s.chainConfig, s.genesisHash, headHeight)
	return protos
}

// Start implements node.Lifecycle, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *Ethereum) Start() error {
	eth.StartENRUpdater(s.chainConfig, s.genesisHash, s.events, s.p2pServer.LocalNode())

	// Figure out a max peers count based on the server limits
	maxPeers := s.p2pServer.MaxPeers
	// Start the networking layer and the light server if requested
	s.handler.Start(maxPeers)
	return nil
}

// Stop implements node.Service, terminating all internal goroutines used by the
// Ethereum protocol.
func (s *Ethereum) Stop() error {
	// Stop all the peer-related stuff first.
	s.handler.Stop()
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
	s.blockchain.Stop()
	s.engine.Close()
	if s.txPool != nil {
		s.txPool.Stop()
	}
	return nil
}
