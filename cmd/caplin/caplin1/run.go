// Copyright 2024 The Erigon Authors
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

package caplin1

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path"
	"time"

	"github.com/spf13/afero"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc/credentials"

	"github.com/erigontech/erigon/cl/aggregation"
	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/beacon"
	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/handler"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	peerdasstate "github.com/erigontech/erigon/cl/das/state"
	clp2p "github.com/erigontech/erigon/cl/p2p"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/persistence/state/historical_states_reader"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/fork_graph"
	"github.com/erigontech/erigon/cl/phase1/forkchoice/public_keys_registry"
	"github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/cl/phase1/network/registry"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/phase1/stages"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel"
	"github.com/erigontech/erigon/cl/sentinel/service"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/cl/validator/attestation_producer"
	"github.com/erigontech/erigon/cl/validator/committee_subscription"
	"github.com/erigontech/erigon/cl/validator/sync_contribution_pool"
	"github.com/erigontech/erigon/cl/validator/validator_params"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/version"
	"github.com/erigontech/erigon/node/ethconfig"
	p2pnat "github.com/erigontech/erigon/p2p/nat"
)

func OpenCaplinDatabase(ctx context.Context,
	beaconConfig *clparams.BeaconChainConfig,
	ethClock eth_clock.EthereumClock,
	dbPath string,
	blobDir string,
	engine execution_client.ExecutionEngine,
	wipeout bool,
	blobPruneDistance uint64,
) (kv.RwDB, blob_storage.BlobStorage, error) {
	dataDirIndexer := path.Join(dbPath, "beacon_indicies")
	blobDbPath := path.Join(blobDir, "chaindata")

	if wipeout {
		dir.RemoveAll(dataDirIndexer)
		dir.RemoveAll(blobDbPath)
	}

	os.MkdirAll(dbPath, 0700)
	os.MkdirAll(dataDirIndexer, 0700)

	db := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dbPath).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { //TODO: move Caplin tables to own tables cofig
			return kv.ChaindataTablesCfg
		}).MustOpen()
	blobDB := mdbx.New(dbcfg.CaplinDB, log.New()).Path(blobDbPath).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg {
			return kv.ChaindataTablesCfg
		}).MustOpen()

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	{ // start ticking forkChoice
		go func() {
			<-ctx.Done()
			db.Close()
			blobDB.Close() // close blob database here
		}()
	}
	return db, blob_storage.NewBlobStore(blobDB, afero.NewBasePathFs(afero.NewOsFs(), blobDir), blobPruneDistance, beaconConfig, ethClock), nil
}

func OpenCaplinIndexDb(ctx context.Context, dbPath string) (kv.RwDB, error) {
	dataDirIndexer := path.Join(dbPath, "beacon_indicies")

	os.MkdirAll(dataDirIndexer, 0700)

	return mdbx.New(dbcfg.CaplinDB, log.New()).Path(dbPath).
		WithTableCfg(func(defaultBuckets kv.TableCfg) kv.TableCfg { //TODO: move Caplin tables to own tables cofig
			return kv.ChaindataTablesCfg
		}).Open(ctx)
}

// upgradeGenesisState applies sequential fork upgrades to bring a genesis state from
// actualVersion to targetVersion. This is needed when the genesis tool produces SSZ at
// an older fork (e.g. Fulu) but the beacon config expects a newer fork at epoch 0 (e.g. GLOAS).
func upgradeGenesisState(s *state.CachingBeaconState, from, to clparams.StateVersion) error {
	for v := from + 1; v <= to; v++ {
		switch v {
		case clparams.AltairVersion:
			if err := s.UpgradeToAltair(); err != nil {
				return err
			}
		case clparams.BellatrixVersion:
			if err := s.UpgradeToBellatrix(); err != nil {
				return err
			}
		case clparams.CapellaVersion:
			if err := s.UpgradeToCapella(); err != nil {
				return err
			}
		case clparams.DenebVersion:
			if err := s.UpgradeToDeneb(); err != nil {
				return err
			}
		case clparams.ElectraVersion:
			if err := s.UpgradeToElectra(); err != nil {
				return err
			}
		case clparams.FuluVersion:
			if err := s.UpgradeToFulu(); err != nil {
				return err
			}
		case clparams.GloasVersion:
			if err := s.UpgradeToGloas(); err != nil {
				return err
			}
		}
	}
	return nil
}

func RunCaplinService(ctx context.Context, engine execution_client.ExecutionEngine, config clparams.CaplinConfig,
	dirs datadir.Dirs, eth1Getter snapshot_format.ExecutionBlockReaderByNumber,
	snDownloader downloader.Client, creds credentials.TransportCredentials, snBuildSema *semaphore.Weighted) error {

	var networkConfig *clparams.NetworkConfig
	var beaconConfig *clparams.BeaconChainConfig

	var err error

	var genesisState *state.CachingBeaconState
	var genesisDb genesisdb.GenesisDB

	if config.IsDevnet() {
		config.NetworkId = clparams.CustomNetwork // Force custom network
		if config.HaveInvalidDevnetParams() {
			return errors.New("devnet config and genesis state paths must be set together")
		}
		customBeaconCfg, customNetworkCfg, err := clparams.CustomConfig(config.CustomConfigPath)
		if err != nil {
			return err
		}
		beaconConfig = &customBeaconCfg
		networkConfig = &customNetworkCfg
		genesisDb = genesisdb.NewGenesisDB(beaconConfig, dirs.CaplinGenesis)
		stateBytes, err := os.ReadFile(config.CustomGenesisStatePath)
		if err != nil {
			return fmt.Errorf("could not read provided genesis state file: %s", err)
		}
		genesisState = state.New(beaconConfig)

		// Detect actual state version from fork.current_version embedded in the SSZ bytes.
		// SSZ layout: genesis_time(8) + genesis_validators_root(32) + slot(8) + fork.previous_version(4) → offset 52.
		// External genesis tools (e.g. ethpandaops) may not yet support the latest fork, so the
		// SSZ could be at an older version (e.g. Fulu) even when the config expects GLOAS at epoch 0.
		targetVersion := beaconConfig.GetCurrentStateVersion(beaconConfig.GenesisEpoch)
		actualVersion := targetVersion
		const forkCurrentVersionOffset = 52
		if len(stateBytes) >= forkCurrentVersionOffset+4 {
			var forkVersion common.Bytes4
			copy(forkVersion[:], stateBytes[forkCurrentVersionOffset:forkCurrentVersionOffset+4])
			if entry, ok := beaconConfig.ForkVersionSchedule[forkVersion]; ok {
				actualVersion = entry.StateVersion
			}
		}

		if err := genesisState.DecodeSSZ(stateBytes, int(actualVersion)); err != nil {
			return fmt.Errorf("could not decode genesis state (detected version %s): %s", actualVersion, err)
		}

		// If the genesis SSZ is at an older fork version than expected, apply sequential upgrades.
		if actualVersion < targetVersion {
			log.Info("[Caplin] Upgrading genesis state to target fork", "from", actualVersion, "to", targetVersion)
			if err := upgradeGenesisState(genesisState, actualVersion, targetVersion); err != nil {
				return fmt.Errorf("could not upgrade genesis state from %s to %s: %s", actualVersion, targetVersion, err)
			}
		}
	} else {
		networkConfig, beaconConfig = clparams.GetConfigsByNetwork(config.NetworkId)
		genesisDb = genesisdb.NewGenesisDB(beaconConfig, dirs.CaplinGenesis)

		isGenesisDBInitialized, err := genesisDb.IsInitialized()
		if err != nil {
			return err
		}

		// If genesis state is provided and is hardcoded, use it
		if initial_state.IsGenesisStateSupported(config.NetworkId) && !isGenesisDBInitialized {
			genesisState, err = initial_state.GetGenesisState(config.NetworkId)
			if err != nil {
				return err
			}
		}
	}

	// init the current beacon config for global access
	clparams.InitGlobalStaticConfig(beaconConfig, &config)

	if config.NetworkId == clparams.CustomNetwork {
		config.NetworkId = clparams.NetworkType(beaconConfig.DepositNetworkID)
	}

	if len(config.BootstrapNodes) > 0 {
		networkConfig.BootNodes = config.BootstrapNodes
	}

	if len(config.StaticPeers) > 0 {
		networkConfig.StaticPeers = config.StaticPeers
	}
	if genesisState != nil {
		genesisDb.Initialize(genesisState)
	} else {
		genesisState, err = genesisDb.ReadGenesisState()
		if err != nil {
			return err
		}
	}

	state, err := checkpoint_sync.ReadOrFetchLatestBeaconState(ctx, dirs, beaconConfig, config, genesisDb)
	if err != nil {
		return err
	}
	ethClock := eth_clock.NewEthereumClock(state.GenesisTime(), state.GenesisValidatorsRoot(), beaconConfig)

	pruneBlobDistance := uint64(128600)
	if config.ArchiveBlobs || config.BlobPruningDisabled {
		pruneBlobDistance = math.MaxUint64
	}

	indexDB, blobStorage, err := OpenCaplinDatabase(ctx, beaconConfig, ethClock, dirs.CaplinIndexing, dirs.CaplinBlobs, engine, false, pruneBlobDistance)
	if err != nil {
		return err
	}

	// Write the genesis beacon block to the index DB before the sentinel and fork
	// choice are created. This ensures the genesis block is available when peers
	// ask for it via beacon_blocks_by_root (otherwise we return empty and get banned).
	// Previously this only ran for dev mode, but Kurtosis devnets also start from
	// genesis and need the block written early.
	if state != nil && state.Slot() == 0 {
		if err := writeDevGenesisBeaconBlock(ctx, state, beaconConfig, indexDB); err != nil {
			return fmt.Errorf("write genesis beacon block: %w", err)
		}
	}

	caplinOptions := []CaplinOption{}
	if config.BeaconAPIRouter.Builder {
		if config.RelayUrlExist() {
			caplinOptions = append(caplinOptions, WithBuilder(config.MevRelayUrl, beaconConfig))
		} else {
			log.Warn("builder api enable but relay url not set. Skipping builder mode")
			config.BeaconAPIRouter.Builder = false
		}
	}
	log.Info("Starting caplin")

	if eth1Getter != nil {
		eth1Getter.SetBeaconChainConfig(beaconConfig)
	}
	if engineWithCfg, ok := engine.(*execution_client.ExecutionClientEngine); ok {
		engineWithCfg.SetBeaconChainConfig(beaconConfig)
	}

	ctx, cn := context.WithCancel(ctx)
	defer cn()

	option := &option{}
	for _, opt := range caplinOptions {
		opt(option)
	}

	logger := log.New("app", "caplin")

	config.NetworkId = clparams.CustomNetwork // Force custom network
	freezeCfg := ethconfig.Defaults.Snapshot
	freezeCfg.ChainName = beaconConfig.ConfigName
	csn := freezeblocks.NewCaplinSnapshots(freezeCfg, beaconConfig, dirs, logger)
	rcsn := freezeblocks.NewBeaconSnapshotReader(csn, eth1Getter, beaconConfig)

	epbsPool := pool.NewEpbsPool()
	pool := pool.NewOperationsPool(beaconConfig)
	attestationProducer := attestation_producer.New(ctx, beaconConfig)

	caplinFcuPath := path.Join(dirs.Tmp, "caplin-forkchoice")
	dir.RemoveAll(caplinFcuPath)
	err = os.MkdirAll(caplinFcuPath, 0o755)
	if err != nil {
		return err
	}
	fcuFs := afero.NewBasePathFs(afero.NewOsFs(), caplinFcuPath)
	syncedDataManager := synced_data.NewSyncedDataManager(beaconConfig, true)

	syncContributionPool := sync_contribution_pool.NewSyncContributionPool(beaconConfig)
	emitters := beaconevents.NewEventEmitter()
	aggregationPool := aggregation.NewAggregationPool(ctx, beaconConfig, networkConfig, ethClock)
	doLMDSampling := len(state.GetActiveValidatorsIndices(state.Slot()/beaconConfig.SlotsPerEpoch)) >= 20_000

	// create the public keys registry
	pksRegistry := public_keys_registry.NewHeadViewPublicKeysRegistry(syncedDataManager)
	validatorParameters := validator_params.NewValidatorParams()
	forkChoice, err := forkchoice.NewForkChoiceStore(
		ethClock, state, engine, pool, fork_graph.NewForkGraphDisk(state, syncedDataManager, fcuFs, config.BeaconAPIRouter, emitters),
		emitters, syncedDataManager, blobStorage, pksRegistry, validatorParameters, doLMDSampling, indexDB)
	if err != nil {
		logger.Error("Could not create forkchoice", "err", err)
		return err
	}
	bls.SetEnabledCaching(true)

	forkDigest, err := ethClock.CurrentForkDigest()
	if err != nil {
		return err
	}
	activeIndicies := state.GetActiveValidatorsIndices(state.Slot() / beaconConfig.SlotsPerEpoch)

	// Parse --caplin.nat into a NAT interface so both P2P managers advertise the correct
	// external IP in discv5 ENR and libp2p multiaddrs (needed for Docker/NAT deployments).
	var caplinNAT p2pnat.Interface
	if config.CaplinNAT != "" {
		var natErr error
		caplinNAT, natErr = p2pnat.Parse(config.CaplinNAT)
		if natErr != nil {
			return fmt.Errorf("invalid --caplin.nat option %q: %w", config.CaplinNAT, natErr)
		}
	}

	p2p, err := clp2p.NewP2Pmanager(ctx, &clp2p.P2PConfig{
		IpAddr:         config.CaplinDiscoveryAddr,
		Port:           int(config.CaplinDiscoveryPort),
		TCPPort:        uint(config.CaplinDiscoveryTCPPort),
		EnableUPnP:     config.EnableUPnP,
		NAT:            caplinNAT,
		LocalDiscovery: config.LocalDiscovery,
		//MaxInboundTrafficPerPeer:     config.MaxInboundTrafficPerPeer,
		//MaxOutboundTrafficPerPeer:    config.MaxOutboundTrafficPerPeer,
		//AdaptableTrafficRequirements: config.AdptableTrafficRequirements,
		NetworkConfig:      networkConfig,
		BeaconConfig:       beaconConfig,
		TmpDir:             dirs.Tmp,
		DataDir:            dirs.DataDir,
		SubscribeAllTopics: config.SubscribeAllTopics,
		//EnableBlocks:                 true,
		//ActiveIndicies: uint64(len(activeIndicies)),
		MaxPeerCount: config.MaxPeerCount,
	}, logger, ethClock)
	if err != nil {
		return err
	}
	peerDasState := peerdasstate.NewPeerDasState(beaconConfig, networkConfig)
	columnStorage := blob_storage.NewDataColumnStore(afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinColumnData), pruneBlobDistance, beaconConfig, ethClock, emitters)
	sentinel, localNode, err := service.StartSentinelService(&sentinel.SentinelConfig{
		P2PConfig: clp2p.P2PConfig{
			IpAddr:             config.CaplinDiscoveryAddr,
			Port:               int(config.CaplinDiscoveryPort),
			TCPPort:            uint(config.CaplinDiscoveryTCPPort),
			EnableUPnP:         config.EnableUPnP,
			NAT:                caplinNAT,
			LocalDiscovery:     config.LocalDiscovery,
			NetworkConfig:      networkConfig,
			BeaconConfig:       beaconConfig,
			TmpDir:             dirs.Tmp,
			DataDir:            dirs.DataDir,
			MaxPeerCount:       config.MaxPeerCount,
			SubscribeAllTopics: config.SubscribeAllTopics,
		},
		MaxInboundTrafficPerPeer:     config.MaxInboundTrafficPerPeer,
		MaxOutboundTrafficPerPeer:    config.MaxOutboundTrafficPerPeer,
		AdaptableTrafficRequirements: config.AdptableTrafficRequirements,
		SubscribeAllTopics:           config.SubscribeAllTopics,
		EnableBlocks:                 true,
		ActiveIndicies:               uint64(len(activeIndicies)),
	}, rcsn, blobStorage, indexDB, &service.ServerConfig{
		Network: "tcp",
		Addr:    fmt.Sprintf("%s:%d", config.SentinelAddr, config.SentinelPort),
		Creds:   creds,
		InitialStatus: func() *cltypes.Status {
			// Use the fork choice anchor root (actual genesis block root) instead of
			// state.FinalizedCheckpoint().Root which is 0x00..00 at genesis.
			// IMPORTANT: HeadSlot must be 0 (matching the genesis anchor slot) so that
			// head_root and head_slot are consistent. Setting head_slot to the clock
			// slot while head_root is the genesis root causes Lighthouse to penalize
			// us for head_root/head_slot mismatch (-20 score, eventually banned).
			anchorRoot := forkChoice.AnchorRoot()
			return &cltypes.Status{
				ForkDigest:     forkDigest,
				FinalizedRoot:  anchorRoot,
				FinalizedEpoch: forkChoice.FinalizedCheckpoint().Epoch,
				HeadSlot:       0,
				HeadRoot:       anchorRoot,
			}
		}(),
	}, ethClock, forkChoice, columnStorage, peerDasState, p2p, logger)
	if err != nil {
		return err
	}
	// Create the gossip manager
	gossipManager := gossip.NewGossipManager(ctx, p2p, beaconConfig, networkConfig, ethClock, config.SubscribeAllTopics, uint64(len(activeIndicies)), config.MaxInboundTrafficPerPeer, config.MaxOutboundTrafficPerPeer, config.AdptableTrafficRequirements)

	peerDasState.SetLocalNodeID(localNode)
	beaconRpc := rpc.NewBeaconRpcP2P(ctx, sentinel, beaconConfig, ethClock, state)
	gossipManager.SetPeerBanner(beaconRpc)
	peerDas := das.NewPeerDas(ctx, beaconRpc, beaconConfig, &config, columnStorage, blobStorage, sentinel, localNode.ID(), ethClock, peerDasState, gossipManager, rcsn, indexDB)
	forkChoice.InitPeerDas(peerDas)   // hack init
	peerDas.SetForkChoice(forkChoice) // [New in Gloas:EIP7732] Set forkChoice for GLOAS kzg_commitments lookup
	committeeSub := committee_subscription.NewCommitteeSubscribeManagement(ctx, beaconConfig, networkConfig, ethClock, aggregationPool, syncedDataManager, gossipManager)
	batchSignatureVerifier := services.NewBatchSignatureVerifier(ctx, sentinel)
	// Define gossip services
	blockService := services.NewBlockService(ctx, indexDB, forkChoice, syncedDataManager, ethClock, beaconConfig, emitters)
	blobService := services.NewBlobSidecarService(ctx, beaconConfig, forkChoice, syncedDataManager, ethClock, emitters, false)
	dataColumnSidecarService := services.NewDataColumnSidecarService(ctx, beaconConfig, ethClock, forkChoice, syncedDataManager, columnStorage, emitters)
	syncCommitteeMessagesService := services.NewSyncCommitteeMessagesService(beaconConfig, ethClock, syncedDataManager, syncContributionPool, batchSignatureVerifier, false)
	attestationService := services.NewAttestationService(ctx, forkChoice, committeeSub, ethClock, syncedDataManager, beaconConfig, networkConfig, emitters, batchSignatureVerifier)
	syncContributionService := services.NewSyncContributionService(syncedDataManager, beaconConfig, syncContributionPool, ethClock, emitters, batchSignatureVerifier, validatorParameters, false)
	aggregateAndProofService := services.NewAggregateAndProofService(ctx, syncedDataManager, forkChoice, beaconConfig, pool, false, batchSignatureVerifier, validatorParameters)
	voluntaryExitService := services.NewVoluntaryExitService(pool, emitters, syncedDataManager, beaconConfig, ethClock, batchSignatureVerifier)
	blsToExecutionChangeService := services.NewBLSToExecutionChangeService(pool, emitters, syncedDataManager, beaconConfig, batchSignatureVerifier)
	proposerSlashingService := services.NewProposerSlashingService(pool, syncedDataManager, beaconConfig, ethClock, emitters)
	attesterSlashingService := services.NewAttesterSlashingService(forkChoice)
	executionPayloadService := services.NewExecutionPayloadService(ctx, forkChoice, beaconConfig, emitters)
	payloadAttestationService := services.NewPayloadAttestationService(ctx, forkChoice, ethClock, networkConfig, emitters)
	proposerPreferencesService := services.NewProposerPreferencesService(syncedDataManager, forkChoice, ethClock, beaconConfig, epbsPool)
	executionPayloadBidService := services.NewExecutionPayloadBidService(ctx, syncedDataManager, forkChoice, ethClock, beaconConfig, epbsPool, emitters)
	registry.RegisterGossipServices(
		gossipManager,
		forkChoice,
		ethClock,
		blockService,
		attesterSlashingService,
		blobService,
		dataColumnSidecarService,
		syncCommitteeMessagesService,
		syncContributionService,
		aggregateAndProofService,
		attestationService,
		voluntaryExitService,
		blsToExecutionChangeService,
		proposerSlashingService,
		executionPayloadService,
		payloadAttestationService,
		proposerPreferencesService,
		executionPayloadBidService,
	)

	{
		go batchSignatureVerifier.Start()
	}

	{ // start ticking forkChoice
		go func() {
			tickInterval := time.NewTicker(2 * time.Millisecond)
			for {
				select {
				case <-tickInterval.C:
					forkChoice.OnTick(uint64(time.Now().Unix()))
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	logger.Info("Started Ethereum 2.0 Gossip Service")

	{ // start logging peers
		go func() {
			logIntervalPeers := time.NewTicker(1 * time.Minute)
			for {
				select {
				case <-logIntervalPeers.C:
					if peerCount, err := beaconRpc.Peers(); err == nil {
						logger.Info("Caplin P2P", "peers_count", peerCount)
					} else {
						logger.Error("Caplin P2P", "err", err)
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	tx, err := indexDB.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := beacon_indicies.WriteHighestFinalized(tx, 0); err != nil {
		return err
	}

	vTables := state_accessors.NewStaticValidatorTable()
	// Read the current table
	if config.ArchiveStates {
		if err := state_accessors.ReadValidatorsTable(tx, vTables); err != nil {
			return err
		}
	}
	stateSnapshots := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: beaconConfig.ConfigName}, beaconConfig, dirs, snapshotsync.MakeCaplinStateSnapshotsTypes(indexDB), logger)
	antiq := antiquary.NewAntiquary(ctx, blobStorage, genesisState, vTables, beaconConfig, dirs, snDownloader, indexDB, stateSnapshots, csn, rcsn, syncedDataManager, logger, config.ArchiveStates, config.ArchiveBlocks, config.ArchiveBlobs, config.SnapshotGenerationEnabled, snBuildSema)
	// Create the antiquary
	go func() {
		keepGoing := true
		for keepGoing {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
				if err := antiq.Loop(); err != nil {
					logger.Debug("Antiquary failed", "err", err)
				} else {
					keepGoing = false
				}
			}
		}
	}()

	if err := tx.Commit(); err != nil {
		return err
	}

	statesReader := historical_states_reader.NewHistoricalStatesReader(beaconConfig, rcsn, vTables, genesisState, stateSnapshots, syncedDataManager)
	if config.BeaconAPIRouter.Active {
		apiHandler := handler.NewApiHandler(
			logger,
			networkConfig,
			ethClock,
			beaconConfig,
			indexDB,
			forkChoice,
			pool,
			rcsn,
			syncedDataManager,
			statesReader,
			sentinel,
			version.GitTag,
			&config.BeaconAPIRouter,
			emitters,
			blobStorage,
			columnStorage,
			csn,
			validatorParameters,
			attestationProducer,
			engine,
			syncContributionPool,
			committeeSub,
			aggregationPool,
			syncCommitteeMessagesService,
			syncContributionService,
			aggregateAndProofService,
			attestationService,
			voluntaryExitService,
			blsToExecutionChangeService,
			proposerSlashingService,
			option.builderClient,
			stateSnapshots,
			gossipManager,
			true,
			peerDas,
			epbsPool,
			executionPayloadBidService,
			payloadAttestationService,
			proposerPreferencesService,
		)
		go beacon.ListenAndServe(&beacon.LayeredBeaconHandler{
			ArchiveApi: apiHandler,
		}, config.BeaconAPIRouter)
		log.Info("Beacon API started", "addr", config.BeaconAPIRouter.Address)
	}

	stageCfg := stages.ClStagesCfg(
		ctx,
		beaconRpc,
		antiq,
		ethClock,
		beaconConfig,
		state,
		engine,
		//gossipManager,
		forkChoice,
		indexDB,
		csn,
		rcsn,
		dirs,
		config,
		syncedDataManager,
		emitters,
		blobStorage,
		attestationProducer,
		peerDas,
	)
	sync := stages.ConsensusClStages(ctx, stageCfg)

	logger.Info("[Caplin] starting clstages loop")
	err = sync.StartWithStage(ctx, "DownloadHistoricalBlocks", logger, stageCfg)
	logger.Info("[Caplin] exiting clstages loop")
	if err != nil {
		return err
	}
	return err
}
