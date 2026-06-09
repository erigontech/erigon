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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format/getters"
	executionclient "github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/validator/devvalidator"
	"github.com/erigontech/erigon/cmd/caplin/caplin1"
	rpcdaemoncli "github.com/erigontech/erigon/cmd/rpcdaemon/cli"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/disk"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/event"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/db/kv/kvcfg"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/db/kv/remotedbserver"
	"github.com/erigontech/erigon/db/kv/temporal"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/diagnostics/diaglib"
	"github.com/erigontech/erigon/diagnostics/mem"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/chain"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
	"github.com/erigontech/erigon/execution/engineapi"
	"github.com/erigontech/erigon/execution/engineapi/engine_block_downloader"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/chainreader"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/protocol/rules/ethash"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/state/genesiswrite"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node"
	manifestexchange "github.com/erigontech/erigon/node/components/manifest_exchange"
	sentrycomp "github.com/erigontech/erigon/node/components/sentry"
	"github.com/erigontech/erigon/node/components/snapshotauth"
	storagecomp "github.com/erigontech/erigon/node/components/storage"
	"github.com/erigontech/erigon/node/components/storage/flow"
	snapshotinv "github.com/erigontech/erigon/node/components/storage/snapshot"
	"github.com/erigontech/erigon/node/components/storage/views"
	"github.com/erigontech/erigon/node/direct"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/ethstats"
	"github.com/erigontech/erigon/node/gointerfaces/grpcutil"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon/node/gointerfaces/sentryproto"
	"github.com/erigontech/erigon/node/gointerfaces/txpoolproto"
	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
	"github.com/erigontech/erigon/node/nodebuilder"
	privateapi2 "github.com/erigontech/erigon/node/privateapi"
	"github.com/erigontech/erigon/node/rulesconfig"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/enr"
	"github.com/erigontech/erigon/p2p/forkid"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/heimdall/poshttp"
	polygonsync "github.com/erigontech/erigon/polygon/sync"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/contracts"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/mcp"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/txnprovider"
	"github.com/erigontech/erigon/txnprovider/shutter"
	"github.com/erigontech/erigon/txnprovider/txpool"
	"github.com/erigontech/erigon/txnprovider/txpool/txpoolcfg"

	_ "github.com/erigontech/erigon/polygon/chain" // Register Polygon chains
)

// Config contains the configuration options of the ETH protocol.
//
// Deprecated: use ethconfig.Config instead.
type Config = ethconfig.Config

// Ethereum implements the Ethereum full node service.
type Ethereum struct {
	config *ethconfig.Config

	// DB interfaces
	chainDB    kv.TemporalRwDB
	privateAPI *grpc.Server

	engine rules.Engine

	etherbase common.Address

	networkID uint64

	lock         sync.RWMutex // Protects the variadic fields (e.g. gas price and etherbase)
	chainConfig  *chain.Config
	apiList      []rpc.API
	genesisBlock *types.Block
	genesisHash  common.Hash

	execModule *execmodule.ExecModule

	ethBackendRPC       *privateapi2.EthBackendServer
	ethRpcClient        rpchelper.ApiBackend
	engineBackendRPC    *engineapi.EngineServer
	miningRPC           *privateapi2.MiningServer
	miningRpcClient     txpoolproto.MiningClient
	stateDiffClient     *direct.StateDiffClientDirect
	rpcFilters          *rpchelper.Filters
	rpcDaemonStateCache kvcache.Cache
	mcpRPC              *mcp.ErigonMCPServer

	miningSealingQuit   chan struct{}
	pendingBlocks       chan *types.Block
	minedBlocks         chan *types.Block
	minedBlockObservers *event.Observers[*types.Block]

	sentryCtx    context.Context
	sentryCancel context.CancelFunc
	// sentryProvider owns the full P2P stack: sentry servers, multi-sentry
	// client, status-data provider, and the execution-P2P layer. Read
	// directly via sentryProvider.{Servers, Client, StatusDataProvider,
	// ExecutionP2P*} rather than via cached fields on Ethereum.
	sentryProvider *sentrycomp.Provider

	stagedSync         *stagedsync.Sync
	pipelineStagedSync *stagedsync.Sync
	syncStages         []*stagedsync.Stage
	syncUnwindOrder    stagedsync.UnwindOrder
	syncPruneOrder     stagedsync.PruneOrder

	downloaderClient downloader.Client

	notifications *shards.Notifications

	unsubscribeEthstat func()

	txPool                    *txpool.TxPool
	txPoolGrpcServer          txpoolproto.TxpoolServer
	txPoolRpcClient           txpoolproto.TxpoolClient
	shutterPool               *shutter.Pool
	blockBuilderNotifyNewTxns chan struct{}
	components                *nodebuilder.Builder

	blockSnapshots *freezeblocks.RoSnapshots
	blockReader    services.FullBlockReader
	blockWriter    *blockio.BlockWriter
	kvRPC          *remotedbserver.KvServer
	logger         log.Logger

	sentinel sentinelproto.SentinelClient

	polygonSyncService *polygonsync.Service
	polygonBridge      *bridge.Service
	heimdallService    *heimdall.Service
	stopNode           func() error
	bgComponentsEg     errgroup.Group
	readAheader        *exec.BlockReadAheader
	kzgWarmupDone      chan struct{}
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
	return nil
}

// New creates a new Ethereum object (including the
// initialisation of the common Ethereum object)
func New(ctx context.Context, stack *node.Node, config *ethconfig.Config, logger log.Logger, tracer *tracers.Tracer) (*Ethereum, error) {
	var kzgWarmupDone chan struct{}
	if config.WarmupKzgCtxOnInit {
		kzgWarmupDone = make(chan struct{})
		go func() {
			defer close(kzgWarmupDone)
			t := time.Now()
			kzg.InitKZGCtx()
			logger.Info("KZG crypto context ready", "took", time.Since(t))
		}()
	}

	dirs := stack.Config().Dirs

	// Startup auto-recovery of staged adoption. A policy=auto node that
	// crashed between writing a batch's ready-marker and finishing the
	// cutover left a marked, validated batch under <datadir>/temp.
	// Complete the cutover here — before RemoveContents wipes temp and
	// before any snapshot file is opened. policy=stage/warn deliberately
	// leave marked batches for the operator's `erigon snapshots adopt`.
	if pol, perr := snapshotsync.ParseAdoptionPolicy(config.Snapshot.AdoptionPolicy); perr == nil && pol == snapshotsync.AdoptionAuto {
		recovered, rerr := snapshotinv.RecoverStagedAdoptions(dirs.Snap, dirs.Tmp, false, logger)
		if rerr != nil {
			logger.Warn("staged-adoption startup recovery failed", "err", rerr)
		}
		for _, b := range recovered {
			logger.Info("staged-adoption startup recovery: cut over", "batch", b.Name, "files", len(b.Files))
		}
	}

	tmpdir := dirs.Tmp
	if err := RemoveContents(tmpdir); err != nil { // clean it on startup
		return nil, fmt.Errorf("clean tmp dir: %s, %w", tmpdir, err)
	}

	// Assemble the Ethereum object
	rawChainDB, err := node.OpenDatabase(ctx, stack.Config(), dbcfg.ChainDB, "", false, logger)
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

		// Apply config-driven global state for the commitment and trie subsystems.
		// These globals are read by 80+ call sites; the config fields are the source of truth.
		if config.KeepExecutionProofs {
			statecfg.EnableHistoricalCommitment()
		}
		if config.ExperimentalConcurrentCommitment {
			statecfg.ExperimentalConcurrentCommitment = true
		}

		if err = stages.UpdateMetrics(tx); err != nil {
			return err
		}

		config.Prune, err = prune.EnsureNotChanged(tx, config.Prune)
		if err != nil {
			return err
		}

		// Persist the snapshot-flow mode flags. Omitting one on
		// restart must not silently downgrade the datadir — e.g.
		// dropping --snap.lifecycle-driven-by-storage previously left
		// Provider.Inventory nil and the new mode-B snapshot-trim
		// path a silent no-op (live-rig 2026-06-02). Flipping a
		// persisted value via CLI is refused at startup.
		type snapModeFlag struct {
			name string
			key  kvcfg.ConfigKey
			ptr  *bool
		}
		snapModes := []snapModeFlag{
			{"--snap.lifecycle-driven-by-storage", kvcfg.SnapLifecycleDrivenByStorage, &config.Snapshot.LifecycleDrivenByStorage},
			{"--snap.p2p-manifest", kvcfg.SnapP2PManifest, &config.Snapshot.P2PManifest},
			{"--snap.bootstrap-from-preverified", kvcfg.SnapBootstrapFromPreverified, &config.Snapshot.BootstrapFromPreverified},
		}
		for _, f := range snapModes {
			notChanged, enabled, terr := f.key.EnsureNotChanged(tx, *f.ptr)
			if terr != nil {
				return terr
			}
			if !notChanged {
				return fmt.Errorf("%s changed since the datadir was created (datadir=%v, cli=%v); changing this flag mid-life is prohibited — reset the datadir or restart without the flag to use the persisted value", f.name, enabled, *f.ptr)
			}
			*f.ptr = enabled
		}

		// Persist the trust-root universe fingerprint. The effective
		// trust spec = compiled-in chain default, optionally
		// overridden by --snapshot.trust-roots. Hashing the parsed
		// root set (rather than the spec string) tolerates harmless
		// reordering / whitespace changes while still rejecting
		// authority rotation. An empty / "any" spec persists the
		// fingerprint of an empty root set; flipping to a populated
		// spec later still trips this gate, which is intended (going
		// from trust-everyone to a specific universe is a real
		// trust-posture change).
		// config.Genesis (and its chain config) is nil for some dev/test node
		// setups; an absent chain name resolves to an empty embedded trust spec,
		// i.e. the fingerprint of an empty root set — the same as an "any" spec.
		var mxChainName string
		if config.Genesis != nil && config.Genesis.Config != nil {
			mxChainName = config.Genesis.Config.ChainName
		}
		trustSpec := snapcfg.GetEmbeddedTrustRoots(mxChainName)
		if override := strings.TrimSpace(config.Snapshot.TrustRoots); override != "" {
			trustSpec = override
		}
		var trustRoots []snapshotauth.TrustRoot
		if trustSpec != "" && !strings.EqualFold(trustSpec, "any") {
			trustRoots, err = snapshotauth.ParseTrustRoots(trustSpec)
			if err != nil {
				return fmt.Errorf("parsing snapshot trust roots: %w", err)
			}
		}
		fp := snapshotauth.TrustRootsFingerprint(trustRoots)
		stored, err := tx.GetOne(kv.DatabaseInfo, kvcfg.SnapTrustFingerprint)
		if err != nil {
			return err
		}
		if stored == nil {
			if err := tx.Put(kv.DatabaseInfo, kvcfg.SnapTrustFingerprint, fp[:]); err != nil {
				return err
			}
		} else if !bytes.Equal(stored, fp[:]) {
			return fmt.Errorf("snapshot trust root universe changed since the datadir was created (stored fingerprint %x, current %x); changing trust roots mid-life is prohibited — reset the datadir to rotate", stored, fp)
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
		etherbase:                 config.Builder.Etherbase,
		blockBuilderNotifyNewTxns: make(chan struct{}, 1),
		miningSealingQuit:         make(chan struct{}),
		minedBlocks:               make(chan *types.Block, 1),
		minedBlockObservers:       event.NewObservers[*types.Block](),
		logger:                    logger,
		readAheader:               exec.NewBlockReadAheader(),
		kzgWarmupDone:             kzgWarmupDone,
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
		chainConfig, genesis, genesisErr = genesiswrite.WriteGenesisBlock(tx, genesisSpec, config.Snapshot.ChainName, config.OverrideOsakaTime, config.OverrideAmsterdamTime, config.KeepStoredChainConfig, dirs, logger)
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

	setDefaultMinerGasLimit(config, chainConfig)
	setBorDefaultTxPoolPriceLimit(&config.TxPool, chainConfig, logger)

	logger.Info("Initialised chain configuration", "config", chainConfig, "genesis", genesis.Hash())
	if dbg.OnlyCreateDB {
		logger.Info("done")
		os.Exit(1)
	}

	segmentsBuildLimiter := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))

	// Check if we have an already initialized chain and fall back to
	// that if so. Otherwise we need to generate a new genesis spec.
	blockReader, blockWriter, allSnapshots, allBorSnapshots, bridgeStore, heimdallStore, temporalDb, err := SetUpBlockReader(ctx, rawChainDB, config.Dirs, config, chainConfig, stack.Config().Http.DBReadConcurrency, logger, segmentsBuildLimiter)
	if err != nil {
		return nil, err
	}
	backend.blockSnapshots, backend.blockReader, backend.blockWriter = allSnapshots, blockReader, blockWriter
	backend.chainDB = temporalDb

	// Snapshot inventory — the metadata registry the storage component
	// uses to model file lifecycle (Declared → Downloading → Downloaded
	// → Indexing → Indexed → Validating → Advertisable). Created here
	// because both storage.Provider.Initialize and the SetAwaiter calls
	// below need a shared reference. See
	// docs/plans/20260501-storage-lifecycle-spec.md.
	inv := snapshotinv.NewInventory()

	// Initialize extracted components.
	//
	// Downloader is initialized here; OnFilesChange file-change callbacks
	// are registered inside storage.Provider.Initialize (see BuildStorage
	// call further below), not in backend.go.
	backend.components = nodebuilder.New()
	if err := backend.components.BuildDownloader(ctx, config.Downloader, config.Snapshot, config.Dirs, logger, stack.Config().DebugMux); err != nil {
		return nil, err
	}
	backend.downloaderClient = backend.components.Downloader.Client

	// KV RPC + Notifications stay in backend.go — Notifications is an
	// execution-layer concern that will move to the execution component.
	kvRPC := remotedbserver.NewKvServer(ctx, temporalDb, allSnapshots, allBorSnapshots, temporalDb.Debug(), logger)
	backend.notifications = shards.NewNotifications(kvRPC)
	backend.kvRPC = kvRPC

	// Storage component: owns DB references, file-change callbacks, block retire.
	// Aggregator + IndexWorkers feed productionIndexBuilder when the lifecycle
	// driver is active (LifecycleDrivenByStorage). The structural cast through
	// kv.TemporalRwDB.Agg() avoids importing temporal/state at this layer.
	var agg *state.Aggregator
	if dbWithAgg, ok := temporalDb.(interface{ Agg() any }); ok {
		if a, ok := dbWithAgg.Agg().(*state.Aggregator); ok {
			agg = a
		}
	}
	// Pass the aggregator through the StateAggregator interface, but keep
	// the Deps field a true nil interface when there is no aggregator —
	// a typed-nil *state.Aggregator boxed into the interface would defeat
	// the storage Provider's `Aggregator != nil` guards.
	var aggDep storagecomp.StateAggregator
	if agg != nil {
		aggDep = agg
	}
	if err := backend.components.BuildStorage(storagecomp.Deps{
		Ctx:              ctx,
		ChainDB:          temporalDb,
		BlockReader:      blockReader,
		BlockWriter:      blockWriter,
		AllSnapshots:     allSnapshots,
		AllBorSnapshots:  allBorSnapshots,
		BridgeStore:      bridgeStore,
		HeimdallStore:    heimdallStore,
		ChainConfig:      chainConfig,
		Genesis:          genesis,
		Config:           config,
		DBEventNotifier:  backend.notifications.Events,
		DownloaderClient: backend.downloaderClient,
		RepublishChainToml: func() error {
			// Re-publish chain.toml after retire/merge produces fresh
			// snapshot files (called from OnFilesChange). Downloader
			// emits V1 + V2 internally when SetInventory has been
			// called; the ENR ends pointing at the V2 info-hash.
			// nil-safe on consumer-only nodes.
			if backend.components == nil || backend.components.Downloader == nil ||
				backend.components.Downloader.Downloader == nil {
				return nil
			}
			return backend.components.Downloader.Downloader.PublishLocalChainToml()
		},
		Inventory:    inv,
		Aggregator:   aggDep,
		IndexWorkers: estimate.IndexSnapshot.Workers(),
		// PostIndexedSeed: invoked by the orchestrator after every
		// phase-1 file reaches LifecycleIndexed AND snapshots.OpenFolder
		// + aggregator.OpenFolder have run, but BEFORE
		// InitialStateReady fires. Populates kv.HeaderTD + canonical
		// hash pointers from frozen headers via FillDBFromSnapshots.
		// Without this, Caplin's BlockCollector.Flush errors with
		// "parent's total difficulty not found" for the snapshot tip
		// because no other code path seeds TD for blocks in the
		// snapshot range. See
		// docs/plans/20260518-storage-owns-post-download-pipeline.md.
		PostIndexedSeed: func(seedCtx context.Context) error {
			return temporalDb.Update(seedCtx, func(tx kv.RwTx) error {
				return rawdbreset.FillDBFromSnapshots("storage.postIndexed", seedCtx, tx, dirs, blockReader, logger)
			})
		},
		SegmentsBuildLimiter: segmentsBuildLimiter,
		Logger:               logger,
	}); err != nil {
		return nil, err
	}

	// canonicalView + canonicalGenesis are assigned below in the
	// manifest_exchange block when a genesis is pinned. The producer
	// self-check closure and the adoption trigger (set just below)
	// close over them and read them at publish time, long after
	// assignment, so the forward reference is safe.
	var canonicalView *snapshotsync.CanonicalView
	var canonicalGenesis snapcfg.PreverifiedItems

	// Hand the storage component's inventory to the downloader so
	// PublishLocalChainToml emits V2 sidecars alongside V1. Required
	// for consumer-side manifest_exchange to fetch a usable manifest.
	if backend.components.Downloader != nil && backend.components.Downloader.Downloader != nil &&
		backend.components.Storage != nil {
		backend.components.Downloader.Downloader.SetInventory(backend.components.Storage.Inventory)

		// Wire the producer-side self-check
		// (docs/plans/20260515-three-layer-snapshot-distribution.md):
		// every RollingV2Publisher.Publish flattens its outgoing
		// ChainTomlV2 and runs snapshotsync.CheckOwnAdvertisement
		// against the currently-loaded canonical chain.toml. Failure
		// returns an error from Publish; the call site in
		// db/downloader/downloader.go logs at Warn and the node
		// continues running (this generation is skipped; next
		// inventory-change publish retries). Lives here because
		// db/snapshotsync imports db/downloader and the reverse
		// would be a cycle.
		chainName := config.Genesis.Config.ChainName

		// Phase 7c minority-detection trigger. The producer self-check
		// runs on every RollingV2Publisher.Publish; when it finds this
		// node advertising a non-canonical hash for a quorum-promoted
		// file it hands the verdict to triggerAdoption, which runs
		// staged adoption out of band (a network fetch + validation +
		// cutover must not block the publish path). adoptionRunning is
		// a single-flight guard — only one adoption in flight; a failed
		// run resets it so the next publish retries.
		adoptionPolicy, apErr := snapshotsync.ParseAdoptionPolicy(config.Snapshot.AdoptionPolicy)
		if apErr != nil {
			return nil, fmt.Errorf("invalid --snapshot.adoption-policy: %w", apErr)
		}
		var adoptionRunning atomic.Bool
		triggerAdoption := func(verdict *snapshotsync.MinorityVerdict) {
			if !adoptionRunning.CompareAndSwap(false, true) {
				return // an adoption is already in flight
			}
			version := 0
			if canonicalView != nil {
				version = canonicalView.Version()
			}
			go func() {
				defer adoptionRunning.Store(false)
				res, err := backend.components.Storage.RunStagedAdoption(backend.sentryCtx, storagecomp.AdoptionRequest{
					Verdict:          verdict,
					Policy:           adoptionPolicy,
					CanonicalVersion: fmt.Sprint(version),
					PruneMode:        config.Prune,
					Downloader:       backend.components.Downloader,
				})
				if err != nil {
					logger.Error("staged adoption failed", "err", err)
					return
				}
				logger.Info("staged adoption", "outcome", res.Outcome, "reason", res.Reason)
			}()
		}

		// Adoption grace window. A minority verdict must persist for
		// config.Snapshot.AdoptionGrace before staging is triggered, so a
		// transient swarm disagreement — a brief quorum flap, or this
		// node's own fresh publish not yet observed by peers — settles
		// instead of kicking off a network fetch + cutover. A self-check
		// that comes back clean within the window cancels the pending
		// trigger. Grace 0 → triggerAdoption fires on first detection.
		adoptionGate := snapshotsync.NewAdoptionGraceGate(config.Snapshot.AdoptionGrace, triggerAdoption)

		backend.components.Downloader.Downloader.SetManifestSelfCheck(func(m *downloader.ChainTomlV2) error {
			// Check against the live canonical quorum view when a
			// genesis is pinned; otherwise fall back to the static
			// embedded preverified set treated as genesis (legacy
			// mode — every mismatch is a fatal divergence).
			genesis := canonicalGenesis
			var canonicals []snapcfg.PreverifiedItems
			if canonicalView != nil {
				canonicals = []snapcfg.PreverifiedItems{canonicalView.Canonical()}
			} else {
				cfg, known := snapcfg.KnownCfg(chainName)
				if !known || cfg == nil {
					return nil // no canonical loaded → defensive no-op
				}
				genesis = cfg.Preverified.Items
				canonicals = []snapcfg.PreverifiedItems{cfg.Preverified.Items}
			}
			verdict, err := snapshotsync.CheckOwnAdvertisement(
				downloader.ChainTomlV2ToItems(m), genesis, canonicals)
			if err != nil {
				return err
			}
			if verdict != nil {
				logger.Warn("publisher self-check: minority entries detected",
					"count", len(verdict.Adopt), "policy", adoptionPolicy)
				if adoptionGate.Arm(verdict) {
					logger.Info("publisher self-check: deferring adoption for grace window",
						"grace", config.Snapshot.AdoptionGrace)
				}
			} else if adoptionGate.Clear() {
				logger.Info("publisher self-check: minority cleared within grace window — adoption cancelled")
			}
			return nil
		})

		// Wire the producer-side two-UCAN authentication
		// (docs/plans/20260520-chaintoml-ucan-flow-spec.md). The
		// Authority UCAN — loaded from --snapshot.delegation, the
		// default <datadir>/snapshot.ucan, or a self-signed bootstrap
		// generated on first run — roots this node's publish authority
		// and is paired with every V2 generation. The Content UCAN is
		// minted per generation: a self-issued attestation binding
		// chain.v2:hash:<sha256(toml)> to the operator's secp256k1 key
		// (the same key the ENR is keyed with), parented to the
		// Authority UCAN. Together they replace the interim .sig
		// sidecar; consumers running with TrustConfig verify the chain
		// to a configured trust root. Empty delegation bytes →
		// V2-only publication (no AuthorityUCANHash, no Content UCAN).
		delegationBytes, derr := snapshotauth.LoadOrGenerateDelegation(
			config.Snapshot.DelegationPath, dirs.DataDir, stack.Config().P2P.PrivateKey, logger)
		if derr != nil {
			return nil, fmt.Errorf("loading snapshot delegation: %w", derr)
		}
		if len(delegationBytes) > 0 {
			backend.components.Downloader.Downloader.SetDelegationSource(func() ([]byte, error) {
				return delegationBytes, nil
			})
			if signKey := stack.Config().P2P.PrivateKey; signKey != nil {
				backend.components.Downloader.Downloader.SetContentUCANMinter(func(tomlBytes []byte) ([]byte, error) {
					return snapshotauth.MintContentUCAN(tomlBytes, signKey, delegationBytes, time.Now())
				})
			}
		}

		// Wire chain identity (fork-spec.md § Identification): every V2
		// manifest generated by this node carries its genesis fork +
		// activated continuous fork schedule. Consumers use these to
		// run an EIP-2124 compatibility early-reject before quorum.
		genesisFork, forks := downloader.BuildChainIdentity(chainConfig, backend.genesisHash, genesis.Time())
		backend.components.Downloader.Downloader.SetChainIdentity(genesisFork, forks)

		// Fork-publisher post-cut-only filter: on a fork chain
		// (chainConfig.Parent != ""), every chain.v2 generation drops
		// pre-cut entries before the manifest is written. Pre-cut
		// files we hold are parent-canonicity (transported via raw BT
		// info-hash discovery on the parent's manifest); listing them
		// in the fork's manifest would impersonate parent canonicity
		// under the fork's trust root. See
		// memory/fork-trust-root-model-2026-05-24 for the rule. Empty
		// stepToBlock is the safe default — state files conservatively
		// classify as straddle and drop, which is correct for a fork
		// publisher whose first retire produces fresh fork-lineage
		// state files.
		if chainConfig.Parent != "" && chainConfig.CutBlock > 0 {
			backend.components.Downloader.Downloader.SetForkCutBlock(chainConfig.CutBlock, nil)
		}
	}

	// Wire wait-on-miss into the read handles. Awaiter is satisfied
	// structurally by *snapshot.Inventory (single method,
	// WaitForReady). RoSnapshots and Aggregator consult it on
	// read-miss so declared-but-not-local files block on ctx instead
	// of hard-failing. See
	// docs/plans/20260501-readhandle-integration.md.
	//
	// Structural double-cast accommodates kv.TemporalRwDB being an
	// interface — the concrete *temporal.DB exposes Agg() any, which
	// returns *state.Aggregator. We avoid importing temporal/state
	// here by reaching for SetAwaiter via the structural interface
	// db/state.Aggregator implements.
	allSnapshots.SetAwaiter(inv)
	if dbWithAgg, ok := temporalDb.(interface{ Agg() any }); ok {
		if agg, ok := dbWithAgg.Agg().(interface {
			SetAwaiter(views.Awaiter)
		}); ok {
			agg.SetAwaiter(inv)
		}
	}

	p2pConfig := stack.Config().P2P

	// Sentry component: owns the P2P stack (servers, direct sentry clients, or
	// external gRPC connections per --sentry.api.addr). The Provider's Configure +
	// Initialize replaces the ~80-line if-external-else-local block that used to
	// live here; see node/components/sentry/provider.go.
	backend.sentryProvider = &sentrycomp.Provider{}
	backend.sentryProvider.Configure(sentrycomp.Config{
		SentryCtx:         backend.sentryCtx,
		P2P:               p2pConfig,
		ChainDB:           backend.chainDB,
		ChainConfig:       backend.chainConfig,
		GenesisHash:       backend.genesisHash,
		NetworkID:         backend.networkID,
		Genesis:           genesis,
		BlockReader:       blockReader,
		EthDiscoveryURLs:  backend.config.EthDiscoveryURLs,
		ChainName:         config.Snapshot.ChainName,
		NodesDir:          stack.Config().Dirs.Nodes,
		EnableWitProtocol: stack.Config().P2P.EnableWitProtocol,
		Events:            backend.notifications.Events,
		Logger:            logger,
		Disable:           stack.Config().DisableSentry,
	})
	if err := backend.sentryProvider.Initialize(ctx); err != nil {
		return nil, err
	}

	// Shared event bus. When storage is running its orchestrator (i.e.
	// LifecycleDrivenByStorage), bind sentry + downloader + manifest_exchange
	// to the same bus so the chain
	//   sentry PeerEvent → manifest_exchange (fetches peer chain.toml.v2)
	//     → flow.PeerManifestReceived → orchestrator → InitialStateReady
	// closes on real peer events.
	if bus := backend.components.Storage.Bus(); bus != nil {
		if err := backend.sentryProvider.BindBus(bus); err != nil {
			return nil, fmt.Errorf("sentry BindBus: %w", err)
		}
		if _, err := backend.sentryProvider.EnablePeerEventAutoWire(ctx); err != nil {
			return nil, fmt.Errorf("sentry EnablePeerEventAutoWire: %w", err)
		}
		if backend.components.Downloader != nil {
			if err := backend.components.Downloader.BindBus(ctx, bus); err != nil {
				return nil, fmt.Errorf("downloader BindBus: %w", err)
			}

			// First-publish gate
			// (docs/plans/20260522-publisher-startup-preflight.md): hold
			// the chain.v2 advertisement until the initial file set is
			// both fully downloaded (flow.InitialDownloadsComplete) AND
			// has settled through the validator chain — infohash check
			// included — to Advertisable or quarantine
			// (flow.InitialValidationComplete). Until then
			// PublishLocalChainTomlV2 is a no-op; on the conjunction the
			// gate opens and the first known-good generation is
			// published. Only wired here, under the orchestrator-running
			// branch — the only configuration where those events fire.
			if dl := backend.components.Downloader.Downloader; dl != nil {
				dl.EnableV2PublishGate()
				firstPublishReady := flow.FirstPublishGateChannel(bus)
				go func() {
					select {
					case <-ctx.Done():
						return
					case <-firstPublishReady:
					}
					dl.OpenV2PublishGate()
					if err := dl.PublishLocalChainToml(); err != nil {
						logger.Warn("[snapshot-flow] first gated chain.toml publish failed", "err", err)
					}
				}()
			}

			mx := &manifestexchange.Provider{}

			// Consumer-side canonical validation + on-disk cache per
			// docs/plans/20260515-three-layer-snapshot-distribution.md.
			// The validator filters each received peer manifest to its
			// canonical-matching subset before it reaches the
			// orchestrator; the cache persists validated manifests as
			// chain.<peer_id>.toml so the node survives restarts and
			// can re-seed peers' manifests for peers that have gone
			// offline.
			mxChainName := config.Genesis.Config.ChainName

			// Layer 1 canonical quorum view
			// (docs/plans/20260520-chaintoml-ucan-flow-spec.md). When a
			// genesis v0 has been pinned into the datadir by
			// `erigon snapshots genesis`, the consumer-side filter runs
			// against a live view: genesis ∪ entries quorum-promoted
			// from trust-verified peer advertisements. Absent a pinned
			// genesis the filter uses the static embedded preverified
			// set, unchanged.
			genesisItems, genesisPinned, gerr := snapcfg.LoadPinnedGenesis(dirs.Snap)
			if gerr != nil {
				return nil, fmt.Errorf("loading pinned canonical genesis: %w", gerr)
			}
			if genesisPinned {
				canonicalGenesis = genesisItems
				qc := snapcfg.QuorumConfigFor(mxChainName)
				if config.Snapshot.QuorumFloor > 0 {
					qc.QFloor = config.Snapshot.QuorumFloor
				}
				canonicalView = snapshotsync.NewCanonicalView(genesisItems, qc)

				// Restore quorum progress across restarts: the view's
				// observations and promotions are persisted to the
				// snapshot dir and reloaded here so a restart does not
				// reset the canonical view to bare genesis.
				viewStatePath := filepath.Join(dirs.Snap, snapshotsync.CanonicalViewStateFile)
				if data, rerr := os.ReadFile(viewStatePath); rerr == nil {
					if rerr := canonicalView.RestoreState(data); rerr != nil {
						logger.Warn("manifest_exchange: discarding unreadable canonical view state", "err", rerr)
					}
				}
				logger.Info("manifest_exchange: canonical quorum view enabled",
					"genesis_entries", len(genesisItems), "quorum_floor", qc.QFloor,
					"restored_version", canonicalView.Version())

				view := canonicalView
				go func() {
					t := time.NewTicker(2 * time.Minute)
					defer t.Stop()
					flush := func() {
						data, merr := view.MarshalState()
						if merr != nil {
							logger.Warn("manifest_exchange: marshal canonical view state", "err", merr)
							return
						}
						if werr := os.WriteFile(viewStatePath, data, 0o644); werr != nil {
							logger.Warn("manifest_exchange: persist canonical view state", "err", werr)
						}
					}
					for {
						select {
						case <-ctx.Done():
							flush()
							return
						case <-t.C:
							flush()
						}
					}
				}()

				// CanonicalHeadRewound → demote orphaned entries. The
				// consumer-side canonical view subordinates to consensus:
				// when storage emits a rewind, the block-axis predicate
				// drops every promoted block file whose range includes
				// any block above ToBlock, and the observation record is
				// dropped so a stale re-Observe cannot re-promote it.
				// State-domain files are deferred (their step-axis
				// ranges need a step→block binding); the predicate is
				// composable via snapshotsync.Or so a future state-axis
				// predicate can stack here. See
				// docs/plans/20260522-canonical-layer-revision.md §5.
				bus.Subscribe(func(e flow.CanonicalHeadRewound) {
					dropped := view.Demote(snapshotsync.DemoteByRewindPredicate(e.ToBlock))
					if dropped > 0 {
						logger.Info("[canonical-view] demoted on rewind",
							"toBlock", e.ToBlock, "dropped", dropped)
					}
				})
			}

			mx.SetCanonicalValidator(func(issuer []byte, adv *downloader.ChainTomlV2) *downloader.ChainTomlV2 {
				advItems := downloader.ChainTomlV2ToItems(adv)
				var canon snapcfg.PreverifiedItems
				if canonicalView != nil {
					if len(issuer) > 0 {
						// Quorum accumulates only on non-transitional entries —
						// jagged-step files from a fork-from non-aligned cut
						// (PendingReplacement=true) must not lock the swarm
						// onto a shape that a subsequent retire will supersede.
						canonicalView.Observe(issuer, downloader.ChainTomlV2ToCanonicalItems(adv), time.Now())
					}
					canon = canonicalView.Canonical()
				} else {
					cfg, known := snapcfg.KnownCfg(mxChainName)
					if !known || cfg == nil {
						return adv // no canonical loaded → pass through (defensive)
					}
					canon = cfg.Preverified.Items
				}
				validItems := snapshotsync.ValidateAdvertisement(
					advItems,
					[]snapcfg.PreverifiedItems{canon},
				)
				if len(validItems) == 0 {
					return nil // every entry filtered → reject wholesale
				}
				// Reconstruct a ChainTomlV2 carrying only the validated
				// (name, hash) entries. The orchestrator only needs the
				// flat sets for fetch planning; the structured Coverage
				// fields are recomputed downstream if needed.
				out := &downloader.ChainTomlV2{
					Version:           downloader.ChainTomlV2Version,
					AuthorityUCANHash: adv.AuthorityUCANHash,
				}
				// Preserve the original section boundaries by checking
				// each name against the original ChainTomlV2.
				validSet := make(map[string]string, len(validItems))
				for _, it := range validItems {
					validSet[it.Name] = it.Hash
				}
				for _, b := range adv.Blocks {
					if vh, ok := validSet[b.Name]; ok && vh == b.Hash {
						out.Blocks = append(out.Blocks, b)
					}
				}
				for name, h := range adv.Meta {
					if vh, ok := validSet[name]; ok && vh == h {
						if out.Meta == nil {
							out.Meta = map[string]string{}
						}
						out.Meta[name] = h
					}
				}
				for name, h := range adv.Salt {
					if vh, ok := validSet[name]; ok && vh == h {
						if out.Salt == nil {
							out.Salt = map[string]string{}
						}
						out.Salt[name] = h
					}
				}
				for _, f := range adv.Caplin {
					if vh, ok := validSet[f.Name]; ok && vh == f.Hash {
						out.Caplin = append(out.Caplin, f)
					}
				}
				for domain, dm := range adv.Domains {
					if dm == nil {
						continue
					}
					var keep []downloader.DomainFileEntry
					for _, f := range dm.Files {
						if vh, ok := validSet[f.Name]; ok && vh == f.Hash {
							keep = append(keep, f)
						}
					}
					if len(keep) > 0 {
						if out.Domains == nil {
							out.Domains = map[string]*downloader.DomainManifest{}
						}
						out.Domains[domain] = &downloader.DomainManifest{
							Coverage: dm.Coverage,
							Files:    keep,
						}
					}
				}
				return out
			})
			mx.SetCacheDir(filepath.Join(stack.Config().Dirs.Snap, "peer-manifests"))

			// Consumer-side fork-ID early-reject (fork-spec.md
			// § Identification). A peer manifest whose declared chain
			// identity is incompatible with this node's is dropped
			// before any UCAN or canonical-quorum work. Phase 1 of
			// docs/plans/20260522-fork-identification-impl.md.
			{
				localGenesisFork, _ := downloader.BuildChainIdentity(chainConfig, backend.genesisHash, genesis.Time())
				localH, localT := forkid.GatherForks(chainConfig, genesis.Time())
				mx.SetForkIDFilter(manifestexchange.BuildForkIDFilter(
					localGenesisFork, backend.genesisHash, localH, localT))
			}

			// Consumer-side UCAN trust gate
			// (docs/plans/20260520-chaintoml-ucan-flow-spec.md). Trust
			// roots come from the compiled-in per-chain default
			// (snapcfg.GetEmbeddedTrustRoots), overridden by
			// --snapshot.trust-roots. A blank effective spec — or the
			// explicit "any" opt-out — leaves the gate inert (SetTrust
			// never called → trust every peer), preserving pre-UCAN
			// behaviour; every chain currently ships blank. A non-blank
			// spec roots gateOnUCAN's verify chain at the configured
			// authorities.
			trustSpec := snapcfg.GetEmbeddedTrustRoots(mxChainName)
			if override := strings.TrimSpace(config.Snapshot.TrustRoots); override != "" {
				trustSpec = override
			}
			if trustSpec != "" && !strings.EqualFold(trustSpec, "any") {
				roots, terr := snapshotauth.ParseTrustRoots(trustSpec)
				if terr != nil {
					return nil, fmt.Errorf("parsing snapshot trust roots: %w", terr)
				}
				if len(roots) > 0 {
					if terr := mx.SetTrust(&manifestexchange.TrustConfig{
						Verifier:             snapshotauth.NewVerifier(roots),
						Fetcher:              backend.components.Downloader,
						RequiredCapabilities: []string{string(snapshotauth.CapAdvertise), string(snapshotauth.CapServe)},
					}); terr != nil {
						return nil, fmt.Errorf("configuring manifest_exchange trust gate: %w", terr)
					}
					logger.Info("manifest_exchange: UCAN trust gate enabled", "trust_roots", len(roots))
				}
			}

			if err := mx.BindBus(ctx, bus, backend.components.Downloader, logger); err != nil {
				return nil, fmt.Errorf("manifest_exchange BindBus: %w", err)
			}
			// Re-fire PeerConnected via sentry whenever the legacy
			// chain.toml discovery loop finds a peer with chain-toml
			// ENR. The original observer-fired PeerConnected may have
			// happened before the peer's ENR propagated the chain-toml
			// entry; this gives manifest_exchange a second look with a
			// known-populated ENR.
			if dl := backend.components.Downloader.Downloader; dl != nil {
				dl.SetOnPeerWithChainTomlDiscovered(func(peer *downloader.ChainTomlPeer) {
					if peer == nil || peer.Node == nil {
						return
					}
					backend.sentryProvider.PublishPeerConnected(peer.Node)
				})
			}
		}

		// All bus subscribers are now wired: sentry, downloader,
		// manifest_exchange, plus the orchestrator from
		// storage.Initialize. Now safe to fire the synthetic
		// bootstrap-from-preverified manifest — the orchestrator
		// will publish DownloadRequested events and the downloader's
		// SubscribeAsync handler will pick them up. Firing earlier
		// (e.g. from inside storage.Initialize) loses the events
		// because the downloader subscriber doesn't exist yet, and
		// the wedge symptom is indistinguishable from the original
		// no-V2-peer-bootstrap gap this is meant to close.
		backend.components.Storage.BootstrapFromPreverified()

		// Shadow-fork follower path: if the running chain.Config carries
		// a non-empty Parent, publish ForkBootstrapRequired so the
		// manifest_exchange subscriber fetches the parent's V2 manifest
		// by ParentManifestHash. No-op for root chains.
		backend.components.Storage.EmitForkBootstrap()
	}

	// Wire chain.toml ENR updater and P2P discovery after sentry servers are created.
	// Only applies in local downloader mode; remote-downloader mode has nil Downloader.
	// Gated behind --snap.p2p-manifest so default syncs stay on the pre-v2 path and
	// avoid the torrent-name collision between a locally-seeded chain.toml and the
	// downloaded chain.toml.torrent in AddTorrentsFromDisk (see #20615).
	if backend.components.Downloader.Downloader != nil && len(backend.sentryProvider.Servers) > 0 && backend.config.Snapshot.P2PManifest {
		dl := backend.components.Downloader.Downloader
		dl.SetENRUpdater(func(ct enr.ChainToml) {
			// Resolve the torrent port at update time rather than capture-time:
			// the torrent client may not be listening yet when the updater
			// closure is built. Only set the "bt" ENR key if we have a valid
			// port in [1..65535]; otherwise peers would dial an unusable port.
			torrentPort := dl.TorrentPort()
			validPort := torrentPort > 0 && torrentPort <= 65535

			for _, srv := range backend.sentryProvider.Servers {
				if p2p := srv.GetP2PServer(); p2p != nil {
					p2p.LocalNode().Set(ct)
					if validPort {
						p2p.LocalNode().Set(enr.BT(torrentPort))
					}
					// Keep the downloader's notion of our own external
					// IP fresh so the peer-sidecar fetch can recognise
					// same-host peers (see fetchPeerSidecar).
					if selfIP := p2p.LocalNode().Node().IP(); selfIP != nil {
						dl.SetSelfIP(selfIP)
					}
					// The ENR fingerprint (node ID) is constant for the
					// node's lifetime; the rolling publisher uses it to
					// name per-node advertisements chain.v2.<enr-fp>.*.
					dl.SetSelfENRFingerprint(downloader.ENRFingerprint([32]byte(p2p.LocalNode().Node().ID())))
				}
			}
			if !validPort {
				logger.Debug("[chaintoml] skipping bt ENR entry (no valid torrent port yet)", "torrentPort", torrentPort)
			}
		})

		sentryServers := backend.sentryProvider.Servers
		dl.SetNodeSourceFn(func() downloader.NodeSource {
			var sources []downloader.NodeSource
			for _, srv := range sentryServers {
				p2pSrv := srv.GetP2PServer()
				if p2pSrv == nil {
					continue
				}
				// Keep the downloader's notion of our own external IP
				// fresh here too — the enr-updater path only fires when
				// PublishLocalChainToml has something to publish, which
				// it doesn't on a fresh consumer datadir, so without
				// this the same-host loopback fallback never engages.
				if selfIP := p2pSrv.LocalNode().Node().IP(); selfIP != nil {
					dl.SetSelfIP(selfIP)
				}
				dl.SetSelfENRFingerprint(downloader.ENRFingerprint([32]byte(p2pSrv.LocalNode().Node().ID())))
				dv5 := p2pSrv.DiscV5()
				// Add directly connected devp2p peers FIRST — resolved peers take
				// priority over discv5 routing table entries which may have stale ENRs.
				srv := p2pSrv // capture for closure
				peersFn := func() []*enode.Node {
					peers := srv.Peers()
					nodes := make([]*enode.Node, len(peers))
					for i, p := range peers {
						nodes[i] = p.Node()
					}
					return nodes
				}
				if dv5 != nil {
					sources = append(sources, &downloader.ResolvingPeerNodeSource{
						PeersFn:  peersFn,
						Resolver: dv5,
					})
				} else {
					sources = append(sources, &downloader.PeerNodeSource{
						PeersFn: peersFn,
					})
				}
				// Add discv5 routing table (deduped by CompositeNodeSource).
				if dv5 != nil {
					sources = append(sources, dv5)
				}
			}
			if len(sources) == 0 {
				return nil
			}
			return &downloader.CompositeNodeSource{Sources: sources}
		})

		// Re-publish chain.toml ENR entry after a delay to let P2P servers start.
		// P2P servers start lazily on SetStatus(), so the ENR updater callback
		// needs the P2P server to be running before it can set ENR entries.
		go func() {
			select {
			case <-ctx.Done():
				return
			case <-time.After(30 * time.Second):
			}
			if pubErr := dl.PublishLocalChainToml(); pubErr != nil {
				logger.Debug("[chaintoml] no existing chain.toml to re-publish", "err", pubErr)
			} else {
				logger.Info("[chaintoml] re-published existing chain.toml ENR entry")
			}
		}()

		// Activate the downloader's background goroutines (chain.toml
		// discovery loop if P2PManifest is set, torrent peer manager
		// unconditionally). The Provider gates internally on its
		// snapshotCfg + nodeSourceFn — all the per-deployment setters
		// (SetInventory / SetChainIdentity / SetForkCutBlock /
		// SetDelegationSource / SetContentUCANMinter /
		// SetManifestSelfCheck / SetNodeSource etc.) ran above this
		// block. Surface the ManifestReady channel afterwards so
		// stage_snapshots's gate at stage_snapshots.go:190 receives
		// the same channel close that the bus-bridge in
		// node/components/downloader/bus.go promotes to
		// flow.ManifestDiscoveryComplete.
		if err := backend.components.Downloader.Activate(ctx); err != nil {
			return nil, fmt.Errorf("downloader.Activate: %w", err)
		}
		if backend.config.Snapshot.P2PManifest {
			backend.config.Snapshot.ManifestReady = dl.ManifestReady()
		}
	}

	// setup periodic logging and prometheus updates
	go mem.LogMemStats(ctx, logger)
	go disk.UpdateDiskStats(ctx, logger)
	go kv.CollectTableSizesPeriodically(ctx, backend.chainDB, dbcfg.ChainDB, logger)

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
	var rulesConfig any

	if chainConfig.Aura != nil {
		rulesConfig = &config.Aura
	} else if chainConfig.Bor != nil {
		rulesConfig = chainConfig.Bor
	} else {
		rulesConfig = &config.Ethash
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
			heimdallClient = heimdall.NewIdleClient(config.Builder)
			bridgeClient = bridge.NewIdleClient()
		}
		borConfig := rulesConfig.(*borcfg.BorConfig)

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
			Store:       heimdallStore,
			ChainConfig: chainConfig,
			BorConfig:   borConfig,
			Client:      heimdallClient,
			Logger:      logger,
		})

		bridgeRPC = bridge.NewBackendServer(ctx, polygonBridge)
		heimdallRPC = heimdall.NewBackendServer(ctx, heimdallService)

		backend.polygonBridge = polygonBridge
		backend.heimdallService = heimdallService
	}

	backend.engine = rulesconfig.CreateRulesEngine(ctx, stack.Config(), chainConfig, rulesConfig, false /* noVerify */, config.WithoutHeimdall, blockReader, false /* readonly */, logger, polygonBridge, heimdallService)

	// ForkValidator is created later, after pipelineStagedSync and PipelineExecutor are ready.

	// StatusDataProvider, sentry multiplexer, and the execution-P2P layer
	// (message listener, peer tracker, publisher) are built inside the
	// sentry Provider's Initialize. Consumers read directly off the Provider.
	statusDataProvider := backend.sentryProvider.StatusDataProvider

	// The BackwardBlockDownloader is an execution-side consumer of the
	// execution-P2P layer, not part of it — it stays in backend.go because
	// its lifetime and tmpdir wiring belong to the execution module.
	var executionFetcher execp2p.Fetcher
	executionFetcher = execp2p.NewFetcher(logger, backend.sentryProvider.ExecutionP2PMessageListener, backend.sentryProvider.ExecutionP2PMessageSender)
	executionFetcher = execp2p.NewPenalizingFetcher(logger, executionFetcher, backend.sentryProvider.ExecutionP2PPeerPenalizer)
	executionFetcher = execp2p.NewTrackingFetcher(executionFetcher, backend.sentryProvider.ExecutionP2PPeerTracker)
	bbd := execp2p.NewBackwardBlockDownloader(logger, executionFetcher, backend.sentryProvider.ExecutionP2PPeerPenalizer, backend.sentryProvider.ExecutionP2PPeerTracker, tmpdir)

	// MultiClient is the late-binding half of the Sentry Provider — it needs
	// the consensus engine which is only available after polygon + engine
	// construction above.
	if err := backend.sentryProvider.BuildMultiClient(sentrycomp.MultiClientDeps{
		Dirs:        stack.Config().Dirs,
		Engine:      backend.engine,
		LogPeerInfo: stack.Config().SentryLogPeerInfo,
	}); err != nil {
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

	backend.stateDiffClient = direct.NewStateDiffClientDirect(backend.kvRPC)

	// Start the eth/71 BAL downloader (EIP-8159) only on chains that activate
	// EIP-7928. collectMissingBALs already short-circuits on the first pre-
	// Amsterdam header (BlockAccessListHash==nil), but skipping the whole
	// goroutine on chains that never reach Amsterdam is cheaper and clearer.
	if chainConfig.AmsterdamTime != nil {
		// Always-on once gated, negotiation-driven: if no peer advertises eth/71
		// this is a silent no-op per scan pass. When eth/71 peers connect, the
		// downloader backfills missing BALs into rawdb so subsequent stage_exec
		// runs can skip local BAL regeneration.
		go sentry_multi_client.NewBALDownloader(backend.sentryProvider.Client, backend.chainDB, logger).Run(backend.sentryCtx)
	}

	var txnProvider txnprovider.TxnProvider
	var blobGetter txpool.BlobGetter
	if config.TxPool.Disable {
		backend.txPoolGrpcServer = &txpool.GrpcDisabled{}
	} else {
		sentries := backend.sentryProvider.Client.Sentries()
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
			kvcache.NewSimple(),
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
		blobGetter = backend.txPool
	}

	execmoduleCache := &execmodule.Cache{}
	execmoduleCache.SetPublishedSD(backend.notifications.Events.LatestSD)
	httpRpcCfg := stack.Config().Http
	httpRpcCfg.StateCache.LocalCache = execmoduleCache
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
		backend.notifications.Events,
	)
	backend.ethRpcClient = ethRpcClient
	backend.txPoolRpcClient = txPoolRpcClient
	backend.miningRpcClient = miningRpcClient
	backend.rpcDaemonStateCache = rpcDaemonStateCache
	backend.rpcFilters = rpcFilters

	baseApi := jsonrpc.NewBaseApi(
		backend.rpcFilters,
		backend.rpcDaemonStateCache,
		blockReader,
		httpRpcCfg.WithDatadir,
		httpRpcCfg.EvmCallTimeout,
		backend.engine,
		httpRpcCfg.Dirs,
		backend.polygonBridge,
		httpRpcCfg.BlockRangeLimit,
		httpRpcCfg.GetLogsMaxResults,
	)
	ethApiConfig := &jsonrpc.EthApiConfig{
		GasCap:                      httpRpcCfg.Gascap,
		FeeCap:                      httpRpcCfg.Feecap,
		ReturnDataLimit:             httpRpcCfg.ReturnDataLimit,
		AllowUnprotectedTxs:         httpRpcCfg.AllowUnprotectedTxs,
		MaxGetProofRewindBlockCount: httpRpcCfg.MaxGetProofRewindBlockCount,
		SubscribeLogsChannelSize:    httpRpcCfg.WebsocketSubscribeLogsChannelSize,
		RpcTxSyncDefaultTimeout:     httpRpcCfg.RpcTxSyncDefaultTimeout,
		RpcTxSyncMaxTimeout:         httpRpcCfg.RpcTxSyncMaxTimeout,
	}
	ethApi := jsonrpc.NewEthAPI(
		baseApi,
		backend.chainDB,
		backend.ethRpcClient,
		backend.txPoolRpcClient,
		backend.miningRpcClient,
		ethApiConfig,
		logger,
	)

	erigonApi := jsonrpc.NewErigonAPI(baseApi, backend.chainDB, backend.ethRpcClient)

	otsApi := jsonrpc.NewOtterscanAPI(baseApi, backend.chainDB, stack.Config().Http.OtsMaxPageSize)

	mcpServer := mcp.NewErigonMCPServer(ethApi, erigonApi, otsApi, config.Dirs.Log)

	if config.MCPAddress != "" {
		go func() {
			logger.Info("serve MCP on", "addr", config.MCPAddress)
			mcpErr := mcpServer.ServeSSE(config.MCPAddress)
			if mcpErr != nil {
				logger.Error("mcpServer.ServeSSE", "err", err)
				return
			}
		}()
	}

	if config.Shutter.Enabled {
		if config.TxPool.Disable {
			panic("can't enable shutter pool when devp2p txpool is disabled")
		}
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
			backend.chainConfig,
			baseTxnProvider,
			contractBackend,
			backend.stateDiffClient,
			currentBlockNumReader,
		)
		txnProvider = backend.shutterPool
	}

	blkBuilder := builder.NewBuilder(
		backend.sentryCtx,
		backend.chainDB,
		&config.Builder,
		backend.chainConfig,
		backend.engine,
		backend.blockReader,
		stagedsync.StageExecuteBlocksCfg(
			backend.chainDB,
			config.Prune,
			config.BatchSize,
			chainConfig,
			backend.engine,
			&vm.Config{},
			backend.notifications,
			config.StateStream,
			false, /*badBlockHalt*/
			dirs,
			blockReader,
			config.Genesis,
			config.Sync,
			config.ExperimentalBAL,
			backend.readAheader,
		),
		backend.notifications.Events,
		&vm.Config{},
		tmpdir,
		txnProvider,
		backend.miningSealingQuit,
		latestBlockBuiltStore,
		backend.notifications.Events.LatestSD,
		logger,
	)
	backend.pendingBlocks = blkBuilder.PendingBlockCh()

	blockRetire := backend.components.Storage.BlockRetire
	var creds credentials.TransportCredentials
	if stack.Config().PrivateApiAddr != "" {
		if stack.Config().TLSConnection {
			creds, err = grpcutil.TLS(stack.Config().TLSCACert, stack.Config().TLSCertFile, stack.Config().TLSKeyFile)
			if err != nil {
				return nil, err
			}
		}
		backend.privateAPI, err = privateapi2.StartGrpc(
			backend.kvRPC,
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
			case <-ctx.Done():
				logger.Debug("[mined blocks listener] ctx done")
				return
			case b := <-backend.minedBlocks:
				backend.minedBlockObservers.Notify(b)

				//p2p
				//backend.sentryProvider.Client.BroadcastNewBlock(context.Background(), b, b.Difficulty())
				//rpcdaemon
				if err := backend.miningRPC.BroadcastMinedBlock(b); err != nil {
					logger.Error("txpool rpc mined block broadcast", "err", err)
				}

			case b := <-backend.pendingBlocks:
				if err := backend.miningRPC.BroadcastPendingBlock(b); err != nil {
					logger.Error("txpool rpc pending block broadcast", "err", err)
				}
			}
		}
	}()

	// This adds completed snapshots on disk after sync, so that we don't unnecessarily report
	// incomplete torrents. There's still the issue of having torrents not in the preverified set:
	// snapshots not in that set could cause issues. That's an unsolved issue and probably requires
	// always resetting before resuming/starting a sync.
	//
	// Gated on LifecycleDrivenByStorage: when storage owns the
	// import lifecycle, disk-discovery is the lifecycle driver's
	// job (wire E in node/components/storage/lifecycle/driver.go),
	// and seeding setup happens via OnFilesChange in
	// storage.Provider. Calling AddTorrentsFromDisk in that mode
	// duplicates work the storage component now owns and causes
	// the chain.toml/chain.toml.torrent collision (#20615 +
	// completion plan §5e). Per the architectural rule: the
	// downloader takes its file list from the storage component,
	// not from disk.
	var afterSnapshotDownload func(ctx context.Context) error
	if backend.components.Downloader != nil && backend.components.Downloader.Downloader != nil && !backend.config.Snapshot.LifecycleDrivenByStorage {
		afterSnapshotDownload = func(ctx context.Context) (err error) {
			incomplete, err := backend.components.Downloader.Downloader.AddTorrentsFromDisk(ctx)
			if err != nil {
				err = fmt.Errorf("adding torrents from disk: %w", err)
				return
			}
			if incomplete != 0 {
				// Sync just completed, so incomplete snapshots are unexpected. They may be
				// aberrations from torrents not in the preverified set; see comment above.
				backend.logger.Warn("Downloader detected incomplete snapshots after sync", "count", incomplete)
			}
			return
		}
	}

	config.Snapshot.InitialStateReady = backend.components.Storage.InitialStateReady
	// Bridge block-retirement signals onto the storage component's event
	// bus. Stage_snapshots calls these inside its retire orchestration
	// with the [fromBlock, toBlock] range; subscribers on the storage bus
	// see flow.RetirementStarted / flow.RetirementDone carrying the
	// range info that the legacy shards.Events fan-out elides. Safe to
	// invoke unconditionally — the legacy fan-out fires regardless.
	if bus := backend.components.Storage.Bus(); bus != nil {
		config.Snapshot.PublishRetirementStart = func(fromBlock, toBlock uint64) {
			bus.Publish(flow.RetirementStarted{FromBlock: fromBlock, ToBlock: toBlock})
		}
		config.Snapshot.PublishRetirementDone = func(fromBlock, toBlock uint64) {
			bus.Publish(flow.RetirementDone{FromBlock: fromBlock, ToBlock: toBlock})
		}
	}
	backend.syncStages = stageloop.NewDefaultStages(backend.sentryCtx, backend.chainDB, config, backend.sentryProvider.Client, backend.notifications, backend.downloaderClient,
		blockReader, blockRetire, tracer, afterSnapshotDownload, backend.readAheader)
	backend.syncUnwindOrder = stagedsync.DefaultUnwindOrder
	backend.syncPruneOrder = stagedsync.DefaultPruneOrder

	backend.stagedSync = stagedsync.New(config.Sync, backend.syncStages, backend.syncUnwindOrder, backend.syncPruneOrder, logger, stages.ModeApplyingBlocks)

	pipelineStages := stageloop.NewPipelineStages(ctx, backend.chainDB, config, backend.sentryProvider.Client, backend.notifications, backend.downloaderClient, blockReader, blockRetire, tracer, afterSnapshotDownload, backend.readAheader)
	backend.pipelineStagedSync = stagedsync.New(config.Sync, pipelineStages, stagedsync.PipelineUnwindOrder, stagedsync.PipelinePruneOrder, logger, stages.ModeApplyingBlocks)

	validationNotifications := shards.NewNotifications(nil)
	validationSync := stageloop.NewInMemoryExecution(backend.sentryCtx, backend.chainDB, config, backend.sentryProvider.Client,
		validationNotifications, blockReader, blockWriter, logger, backend.readAheader)
	dispatcher := execmodule.NewDispatcher(chainConfig, backend.notifications.Events, backend.notifications.StateChangesConsumer, logger)
	pipelineExecutor := execmodule.NewPipelineExecutor(backend.pipelineStagedSync, backend.chainDB, blockReader, chainConfig, backend.engine, validationSync, validationNotifications, dispatcher, logger)
	// V2 mode: wire the orchestrator's InitialStateReady so
	// ProcessFrozenBlocks waits for storage's postIndexed (which holds
	// its own MDBX RW tx during FillDBFromSnapshots) before opening
	// the framework's RW tx. Without this, the two writers deadlock.
	if backend.components.Storage != nil && backend.components.Storage.InitialStateReady != nil {
		pipelineExecutor.SetInitialStateReady(backend.components.Storage.InitialStateReady)
	}

	hook := stageloop.NewHook(backend.sentryCtx, backend.notifications, backend.stagedSync, backend.chainConfig, backend.logger, dispatcher, backend.sentryProvider.Client.SetStatus, statusDataProvider, backend.sentryProvider.ExecutionP2PPublisher)

	// for polygon, we only need to download snapshots on start so that all driver components are correctly initialised before any block execution begins
	onlySnapDownloadOnStart := chainConfig.Bor != nil
	accum := &execmodule.Accumulation{
		Accumulator:    backend.notifications.Accumulator,
		RecentReceipts: backend.notifications.RecentReceipts,
	}
	backend.execModule = execmodule.NewExecModule(
		ctx,
		blockReader,
		backend.chainDB,
		pipelineExecutor,
		currentBlockNumber,
		chainConfig,
		blkBuilder.Build,
		hook,
		accum,
		execmoduleCache,
		logger,
		backend.engine,
		config.Sync,
		config.FcuBackgroundPrune,
		config.FcuBackgroundCommit,
		onlySnapDownloadOnStart,
		backend.readAheader,
		backend.stopNode,
		newProviderUnwinderAdapter(backend.components.Storage),
	)
	backend.execModule.SetPublishedSD(backend.notifications.Events.LatestSD)

	var executionEngine executionclient.ExecutionEngine

	executionEngine, err = executionclient.NewExecutionClientDirect(chainreader.NewChainReaderEth1(chainConfig, backend.execModule, config.FcuTimeout), txPoolRpcClient)
	if err != nil {
		return nil, err
	}

	engineBackendRPC := engineapi.NewEngineServer(
		logger,
		chainConfig,
		backend.execModule,
		engine_block_downloader.NewEngineBlockDownloader(
			ctx,
			logger,
			backend.execModule,
			blockReader,
			backend.chainDB,
			chainConfig,
			config.Sync,
			bbd,
		),
		config.InternalCL && !config.CaplinConfig.EnableEngineAPI, // If the chain supports the engine API, then we should not make the server fail.
		config.InternalCL, // Suppress "no CL" warning when any embedded CL is active.
		config.Builder.EnabledPOS,
		!config.PolygonPosSingleSlotFinality,
		backend.txPoolRpcClient,
		blobGetter,
		config.FcuTimeout,
		config.MaxReorgDepth,
	)
	backend.engineBackendRPC = engineBackendRPC
	// If we choose not to run a consensus layer, run our embedded.
	if config.InternalCL && (clparams.EmbeddedSupported(config.NetworkID) || config.CaplinConfig.IsDevnet()) {
		config.CaplinConfig.NetworkId = clparams.NetworkType(config.NetworkID)
		config.CaplinConfig.LoopBlockLimit = uint64(config.LoopBlockLimit)
		if config.CaplinConfig.EnableEngineAPI {
			executionEngine, err = executionclient.NewExecutionClientEngineLocal(
				engineBackendRPC,
				chainreader.NewChainReaderEth1(chainConfig, backend.execModule, config.FcuTimeout),
				txPoolRpcClient,
				nil, // beaconCfg: local mode uses chainRW which returns properly versioned blocks
			)
			if err != nil {
				logger.Error("failed to create execution client", "err", err)
				return nil, err
			}
		}
		go func() {
			eth1Getter := getters.NewExecutionSnapshotReader(ctx, blockReader, backend.chainDB)
			// Pass storage's BlockHeadersReady channel so Caplin waits
			// for the tip *-headers.seg to be readable before starting
			// its clstages loop — see provider.watchTipHeaderForOpenSegments.
			// Nil-safe: when storage isn't running its orchestrator the
			// channel is nil and Caplin starts immediately (legacy path).
			// localBlockTipFn: query the storage component's Inventory
			// for the highest contiguous Local block, so Caplin's
			// canonical-block-tip stop bound reflects what's actually
			// on disk rather than what preverified.toml advertises.
			// Nil-safe: Inventory is nil for non-storage-driven
			// configurations; RunCaplinService falls back to
			// DeriveManifestTips in that case.
			var localBlockTipFn func() uint64
			if backend.components.Storage != nil && backend.components.Storage.Inventory != nil {
				inv := backend.components.Storage.Inventory
				localBlockTipFn = func() uint64 {
					view := inv.View()
					defer view.Close()
					return view.LocalBlockTip()
				}
			}
			if err := caplin1.RunCaplinService(ctx, executionEngine, config.CaplinConfig, dirs, eth1Getter, backend.downloaderClient, creds, segmentsBuildLimiter, backend.components.Storage.BlockHeadersReady, localBlockTipFn); err != nil {
				if !errors.Is(err, context.Canceled) {
					logger.Error("could not start caplin", "err", err)
				}
			}
			ctxCancel()
		}()
		// Start embedded dev validator if configured.
		if config.CaplinConfig.DevValidatorSeed != "" {
			go func() {
				beaconAddr := config.CaplinConfig.BeaconAPIRouter.Address
				if beaconAddr == "" {
					beaconAddr = "127.0.0.1:5555"
				}
				beaconURL := fmt.Sprintf("http://%s", beaconAddr)
				validatorCount := config.CaplinConfig.DevValidatorCount
				if validatorCount == 0 {
					validatorCount = 64
				}
				// Load the beacon config from the custom config path.
				beaconCfg, _, err := clparams.CustomConfig(config.CaplinConfig.CustomConfigPath)
				if err != nil {
					logger.Error("[dev-validator] failed to load beacon config", "err", err)
					return
				}
				svc, err := devvalidator.NewService(beaconURL, config.CaplinConfig.DevValidatorSeed, validatorCount, &beaconCfg, logger)
				if err != nil {
					logger.Error("[dev-validator] failed to create service", "err", err)
					return
				}
				svc.Start(ctx)
			}()
		}
	}

	if chainConfig.Bor != nil {
		backend.polygonSyncService = polygonsync.NewService(
			config,
			logger,
			chainConfig,
			backend.sentryProvider.Multiplexer,
			p2pConfig.MaxPeers,
			statusDataProvider,
			backend.execModule,
			config.LoopBlockLimit,
			polygonBridge,
			heimdallService,
			backend.notifications,
			backend.engineBackendRPC,
			backend,
			config.Dirs.Tmp,
		)

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

	if !dbg.NoBackgroundMaintenance() {
		go func() {
			if err := temporalDb.Debug().MergeLoop(ctx); err != nil {
				logger.Error("snapashot merge loop error", "err", err)
			}
		}()
	}

	return backend, nil
}

func (s *Ethereum) Init(stack *node.Node, config *ethconfig.Config, chainConfig *chain.Config) error {
	blockReader := s.blockReader
	ctx := s.sentryCtx
	chainKv := s.chainDB
	var err error
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

	// start HTTP API
	httpRpcCfg := stack.Config().Http
	if config.Ethstats != "" {
		var headCh chan [][]byte
		headCh, s.unsubscribeEthstat = s.notifications.Events.AddHeaderSubscription()
		if err := ethstats.New(stack, s.sentryProvider.Servers, chainKv, s.blockReader, config.Ethstats, s.networkID, ctx.Done(), headCh, s.txPoolRpcClient); err != nil {
			return err
		}
	}

	var testingEntry *rpc.API
	if slices.Contains(httpRpcCfg.API, "testing") {
		entry := engineapi.NewTestingRPCEntry(s.engineBackendRPC, s.logger, s.chainDB)
		testingEntry = &entry
	}
	s.apiList = jsonrpc.APIList(chainKv, s.ethRpcClient, s.txPoolRpcClient, s.miningRpcClient, s.rpcFilters, s.rpcDaemonStateCache, blockReader, &httpRpcCfg, s.engine, s.logger, s.polygonBridge, s.heimdallService, testingEntry)

	s.bgComponentsEg.Go(func() error {
		err := rpcdaemoncli.StartRpcServer(ctx, &httpRpcCfg, s.apiList, s.logger)
		if err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("cli.StartRpcServer error", "err", err)
		}
		return err
	})

	if chainConfig.Bor == nil || config.PolygonPosSingleSlotFinality {
		s.bgComponentsEg.Go(func() error {
			defer s.logger.Debug("[EngineServer] goroutine terminated")
			err := s.engineBackendRPC.Start(ctx, &httpRpcCfg, s.chainDB, s.blockReader, s.rpcFilters, s.rpcDaemonStateCache, s.engine, s.ethRpcClient, s.miningRpcClient, s.notifications.Events)
			if err != nil && !errors.Is(err, context.Canceled) {
				s.logger.Error("[EngineServer] background goroutine failed", "err", err)
			}
			return err
		})
	}

	// Register the backend on the node
	stack.RegisterLifecycle(s)
	return nil
}

func (s *Ethereum) APIs() []rpc.API {
	return s.apiList
}

func (s *Ethereum) StateDiffClient() *direct.StateDiffClientDirect {
	return s.stateDiffClient
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

func (s *Ethereum) IsMining() bool { return false }

func (s *Ethereum) RegisterMinedBlockObserver(callback func(msg *types.Block)) event.UnregisterFunc {
	return s.minedBlockObservers.Register(callback)
}

func (s *Ethereum) ChainKV() kv.RwDB            { return s.chainDB }
func (s *Ethereum) NetVersion() (uint64, error) { return s.networkID, nil }
func (s *Ethereum) NetPeerCount() (uint64, error) {
	var sentryPc uint64 = 0

	s.logger.Trace("sentry", "peer count", sentryPc)
	for _, sc := range s.sentryProvider.Client.Sentries() {
		ctx := context.Background()
		reply, err := sc.PeerCount(ctx, &sentryproto.PeerCountRequest{})
		if err != nil {
			s.logger.Warn("sentry", "err", err)
			return 0, nil
		}
		sentryPc += reply.Count
	}

	return sentryPc, nil
}

func (s *Ethereum) NodesInfo(limit int) (*remoteproto.NodesInfoReply, error) {
	sentries := s.sentryProvider.Client.Sentries()
	if limit == 0 || limit > len(sentries) {
		limit = len(sentries)
	}

	// Sentries that share a single p2p.Server return identical NodeInfo
	// (same Node ID, same enode). Dedup by Enode so admin_nodeInfo doesn't
	// list the same node N times. `limit` caps the number of *unique* nodes
	// returned — keep scanning the rest of the sentry list past duplicates
	// so a hybrid setup with both shared-Server and external sentries can
	// still fill the cap.
	seenEnodes := make(map[string]struct{}, limit)
	nodes := make([]*typesproto.NodeInfoReply, 0, limit)
	for _, sc := range sentries {
		if len(nodes) >= limit {
			break
		}

		nodeInfo, err := sc.NodeInfo(context.Background(), nil)
		if err != nil {
			s.logger.Error("sentry nodeInfo", "err", err)
			continue
		}
		if nodeInfo == nil || nodeInfo.Enode == "" {
			continue
		}
		if _, dup := seenEnodes[nodeInfo.Enode]; dup {
			continue
		}
		seenEnodes[nodeInfo.Enode] = struct{}{}

		nodes = append(nodes, nodeInfo)
	}

	nodesInfo := &remoteproto.NodesInfoReply{NodesInfo: nodes}
	slices.SortFunc(nodesInfo.NodesInfo, remoteproto.NodeInfoReplyCmp)

	return nodesInfo, nil
}

func SetUpBlockReader(ctx context.Context, db kv.RwDB, dirs datadir.Dirs, snConfig *ethconfig.Config, chainConfig *chain.Config, dbReadConcurrency int, logger log.Logger, blockSnapBuildSema *semaphore.Weighted) (*freezeblocks.BlockReader, *blockio.BlockWriter, *freezeblocks.RoSnapshots, *heimdall.RoSnapshots, bridge.Store, heimdall.Store, kv.TemporalRwDB, error) {
	snConfig.Snapshot.ChainName = chainConfig.ChainName
	allSnapshots := freezeblocks.NewRoSnapshots(snConfig.Snapshot, dirs.Snap, logger)

	var allBorSnapshots *heimdall.RoSnapshots
	var bridgeStore bridge.Store
	var heimdallStore heimdall.Store

	if chainConfig.Bor != nil {
		allBorSnapshots = heimdall.NewRoSnapshots(snConfig.Snapshot, dirs.Snap, logger)
		bridgeStore = bridge.NewSnapshotStore(bridge.NewMdbxStore(dirs.DataDir, logger, false, int64(dbReadConcurrency)), allBorSnapshots, chainConfig.Bor)
		heimdallStore = heimdall.NewSnapshotStore(heimdall.NewMdbxStore(logger, dirs.DataDir, false, int64(dbReadConcurrency)), allBorSnapshots)
	}
	blockReader := freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)

	_, knownSnapCfg := snapcfg.KnownCfg(chainConfig.ChainName)
	createNewSaltFileIfNeeded := snConfig.Snapshot.NoDownloader || snConfig.Snapshot.DisableDownloadE3 || !knownSnapCfg
	if _, err := snaptype.LoadSalt(dirs.Snap, createNewSaltFileIfNeeded, logger); err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}

	erigonDBSettings, err := state.ResolveErigonDBSettings(dirs, logger, snConfig.Snapshot.NoDownloader)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, err
	}
	aggOpts := state.New(dirs).Logger(logger).SanityOldNaming().GenSaltIfNeed(createNewSaltFileIfNeeded).WithErigonDBSettings(erigonDBSettings)
	if snConfig.ErigondbDomainStepsInFrozenFile != nil {
		v := *snConfig.ErigondbDomainStepsInFrozenFile
		stepsStr := "Inf"
		if v != config3.UnboundedDomainMerge {
			stepsStr = fmt.Sprintf("%d", v)
		}
		logger.Info("domain merge cap overridden", "steps_in_frozen_file", stepsStr)
		aggOpts = aggOpts.ErigondbDomainStepsInFrozenFile(v)
	}
	agg, err := aggOpts.Open(ctx, db)
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

func (s *Ethereum) Peers(ctx context.Context) (*remoteproto.PeersReply, error) {
	var reply remoteproto.PeersReply
	for _, sentryClient := range s.sentryProvider.Client.Sentries() {
		peers, err := sentryClient.Peers(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.Peers error: %w", err)
		}
		reply.Peers = append(reply.Peers, peers.Peers...)
	}

	return &reply, nil
}

func (s *Ethereum) AddPeer(ctx context.Context, req *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error) {
	for _, sentryClient := range s.sentryProvider.Client.Sentries() {
		_, err := sentryClient.AddPeer(ctx, &sentryproto.AddPeerRequest{Url: req.Url})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.AddPeers error: %w", err)
		}
	}
	return &remoteproto.AddPeerReply{Success: true}, nil
}

func (s *Ethereum) RemovePeer(ctx context.Context, req *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error) {
	for _, sentryClient := range s.sentryProvider.Client.Sentries() {
		_, err := sentryClient.RemovePeer(ctx, &sentryproto.RemovePeerRequest{Url: req.Url})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.RemovePeers error: %w", err)
		}
	}
	return &remoteproto.RemovePeerReply{Success: true}, nil
}

func (s *Ethereum) AddTrustedPeer(ctx context.Context, req *remoteproto.AddPeerRequest) (*remoteproto.AddPeerReply, error) {
	for _, sentryClient := range s.sentryProvider.Client.Sentries() {
		_, err := sentryClient.AddTrustedPeer(ctx, &sentryproto.AddPeerRequest{Url: req.Url})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.AddTrustedPeer error: %w", err)
		}
	}
	return &remoteproto.AddPeerReply{Success: true}, nil
}

func (s *Ethereum) RemoveTrustedPeer(ctx context.Context, req *remoteproto.RemovePeerRequest) (*remoteproto.RemovePeerReply, error) {
	for _, sentryClient := range s.sentryProvider.Client.Sentries() {
		_, err := sentryClient.RemoveTrustedPeer(ctx, &sentryproto.RemovePeerRequest{Url: req.Url})
		if err != nil {
			return nil, fmt.Errorf("ethereum backend MultiClient.RemoveTrustedPeer error: %w", err)
		}
	}
	return &remoteproto.RemovePeerReply{Success: true}, nil
}

func (s *Ethereum) SetHead(ctx context.Context, targetBlock uint64) error {
	return s.execModule.SetHead(ctx, targetBlock)
}

// Protocols returns all the currently configured
// network protocols to start.
func (s *Ethereum) Protocols() []p2p.Protocol {
	protocols := make([]p2p.Protocol, 0, len(s.sentryProvider.Servers))
	for i := range s.sentryProvider.Servers {
		protocols = append(protocols, s.sentryProvider.Servers[i].Protocols...)
	}
	return protocols
}

// Start implements node.Lifecycle, starting all internal goroutines needed by the
// Ethereum protocol implementation.
func (s *Ethereum) Start() error {
	// Sentry Provider launches all sentry-owned background goroutines:
	// MultiClient stream loops, StatusDataProvider refresh loop,
	// execution-P2P layer (message listener, peer tracker, publisher),
	// and the peer-count logger. See node/components/sentry/provider.go.
	if err := s.sentryProvider.Start(s.sentryCtx); err != nil {
		return err
	}

	stageLoopDispatcher := execmodule.NewDispatcher(s.chainConfig, s.notifications.Events, s.notifications.StateChangesConsumer, s.logger)
	hook := stageloop.NewHook(s.sentryCtx, s.notifications, s.stagedSync, s.chainConfig, s.logger, stageLoopDispatcher, s.sentryProvider.Client.SetStatus, s.sentryProvider.StatusDataProvider, s.sentryProvider.ExecutionP2PPublisher)

	currentTDProvider := func() *uint256.Int {
		currentTD, err := readCurrentTotalDifficulty(s.sentryCtx, s.chainDB, s.blockReader)
		if err != nil {
			panic(err)
		}
		return currentTD
	}

	if chainspec.IsChainPoS(s.chainConfig, currentTDProvider) {
		diaglib.Send(diaglib.SyncStageList{StagesList: diaglib.InitStagesFromList(s.pipelineStagedSync.StagesIdsList())})
		go s.execModule.Start(s.sentryCtx, hook)
	} else if s.chainConfig.Bor != nil {
		diaglib.Send(diaglib.SyncStageList{StagesList: diaglib.InitStagesFromList(s.stagedSync.StagesIdsList())})
		s.bgComponentsEg.Go(func() error {
			defer s.logger.Info("[polygon.sync] exeuction server start goroutine completed")
			s.execModule.Start(s.sentryCtx, hook)
			return nil
		})
		s.bgComponentsEg.Go(func() error {
			defer s.logger.Info("[polygon.sync] goroutine terminated")
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
	if s.components != nil && s.components.Downloader != nil {
		s.components.Downloader.Close()
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
	_ = s.engine.Close()
	if err := s.sentryProvider.Close(); err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("sentry component close", "err", err)
	}

	// Wait for background goroutines to release DB transactions before closing DB.
	// Background components (txpool, sentry loops, p2p) hold read transactions that
	// must be rolled back before chainDB.Close() can complete.
	if err := s.bgComponentsEg.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error("background component error", "err", err)
	}

	// Wait for any in-flight updateForkChoice goroutine to finish. These are
	// fire-and-forget goroutines that hold DB read transactions; without this
	// wait, chainDB.Close() can hang in waitTxsAllDoneOnClose.
	if s.execModule != nil {
		waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		s.execModule.WaitIdle(waitCtx)
		waitCancel()
	}

	// Wait for any in-flight read-ahead warmup goroutines that hold DB read
	// transactions. The read-aheader spawns fire-and-forget goroutines via
	// AddHeaderAndBody during ValidateChain.
	{
		warmCtx, warmCancel := context.WithTimeout(context.Background(), 5*time.Second)
		s.readAheader.WaitForWarmup(warmCtx)
		warmCancel()
	}

	// Wait for the KZG warmup goroutine (spawned in New) to finish. It logs
	// on completion; without this wait a slow warmup can log after the owning
	// scope is gone — in tests that panics the runner ("Log in goroutine
	// after <test> has completed"). InitKZGCtx is bounded (a one-time trusted
	// setup load), so the timeout is a safety valve only.
	if s.kzgWarmupDone != nil {
		select {
		case <-s.kzgWarmupDone:
		case <-time.After(30 * time.Second):
			s.logger.Warn("KZG warmup goroutine still running at shutdown")
		}
	}

	s.chainDB.Close()

	if s.config.Downloader != nil {
		_ = s.config.Downloader.CloseTorrentLogFile()
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
	return s.sentryProvider.Client
}

func (s *Ethereum) BlockIO() (services.FullBlockReader, *blockio.BlockWriter) {
	return s.blockReader, s.blockWriter
}

func (s *Ethereum) TxpoolServer() txpoolproto.TxpoolServer {
	return s.txPoolGrpcServer
}

func (s *Ethereum) ExecutionModule() *execmodule.ExecModule {
	return s.execModule
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

func readCurrentTotalDifficulty(ctx context.Context, db kv.RwDB, blockReader services.FullBlockReader) (*uint256.Int, error) {
	var currentTD *uint256.Int
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

func (s *Ethereum) Sentinel() sentinelproto.SentinelClient {
	return s.sentinel
}

func (s *Ethereum) DataDir() string {
	return s.config.Dirs.DataDir
}

func setDefaultMinerGasLimit(config *ethconfig.Config, chainConfig *chain.Config) {
	if config.Builder.GasLimit == nil {
		gasLimit := ethconfig.DefaultBlockGasLimitByChain(chainConfig)
		config.Builder.GasLimit = &gasLimit
	}
}

// setBorDefaultTxPoolPriceLimit enforces MinFeeCap to be equal to BorDefaultTxPoolPriceLimit (25gwei by default)
func setBorDefaultTxPoolPriceLimit(config *txpoolcfg.Config, chainConfig *chain.Config, logger log.Logger) {
	if chainConfig.Bor != nil && config.MinFeeCap != txpoolcfg.BorDefaultTxPoolPriceLimit {
		logger.Warn("Sanitizing invalid bor min fee cap", "provided", config.MinFeeCap, "updated", txpoolcfg.BorDefaultTxPoolPriceLimit)
		config.MinFeeCap = txpoolcfg.BorDefaultTxPoolPriceLimit
	}
	_ = config.MinFeeCap
}
