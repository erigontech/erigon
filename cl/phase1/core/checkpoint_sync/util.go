package checkpoint_sync

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
)

// ReadOrFetchLatestBeaconState reads the latest beacon state from disk or fetches it from the network.
// If remote checkpoint sync fails, it falls back to the local finalized state on disk.
// If no local finalized state is available, it returns an error.
func ReadOrFetchLatestBeaconState(ctx context.Context, dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig, genesisDB genesisdb.GenesisDB) (*state.CachingBeaconState, error) {
	var syncer CheckpointSyncer
	// Allow remote checkpoint sync for devnets when the user explicitly provides a checkpoint sync URL.
	hasCustomCheckpointURL := len(clparams.ConfigurableCheckpointsURLs) > 0
	remoteSync := !caplinConfig.DisabledCheckpointSync && (!caplinConfig.IsDevnet() || hasCustomCheckpointURL)

	if remoteSync {
		// Prefer resuming from our own most-recently-finalized state (local, reorg-immune) so the
		// node comes up at a finalized anchor at/below the EL head — no network fetch, no EL backfill.
		if localFinalized := tryResumeFromLocalFinalizedState(dirs, beaconCfg, caplinConfig, genesisDB); localFinalized != nil {
			return localFinalized, nil
		}

		syncer := NewRemoteCheckpointSync(beaconCfg, caplinConfig.NetworkId)
		st, err := syncer.GetLatestBeaconState(ctx)
		if err == nil {
			return st, nil
		}
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		log.Warn("[Checkpoint Sync] Remote checkpoint sync failed, attempting to read local finalized state", "err", err)

		// Fallback: read the local finalized state as a last resort, but still reject a
		// foreign-network file — a remote outage must not let a different network's state anchor
		// the node. Staleness is tolerated here: a same-network finalized anchor beats failing to
		// start when remote is unreachable.
		localState, localErr := readSameNetworkFinalizedState(dirs, beaconCfg, genesisDB)
		if localErr == nil {
			log.Info("[Checkpoint Sync] Successfully loaded local finalized state", "slot", localState.Slot())
			return localState, nil
		}
		log.Error("[Checkpoint Sync] No usable local finalized state available either", "err", localErr)
		return nil, fmt.Errorf("remote checkpoint sync failed: %w, and no usable local finalized state: %w", err, localErr)
	}

	// Non-remote sync path (disabled checkpoint sync or devnet)
	aferoFs := afero.NewOsFs()
	genesisState, err := genesisDB.ReadGenesisState()
	if err != nil {
		return nil, fmt.Errorf("could not read genesis state: %w", err)
	}
	syncer = NewLocalCheckpointSyncer(genesisState, afero.NewBasePathFs(aferoFs, dirs.CaplinLatest))
	return syncer.GetLatestBeaconState(ctx)
}

// ReadLocalFinalizedState reads the node's own most-recently-finalized state directly from disk.
func ReadLocalFinalizedState(dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig) (*state.CachingBeaconState, error) {
	return readLocalStateFile(dirs, beaconCfg, clparams.LatestFinalizedStateFileName, "finalized")
}

// readSameNetworkFinalizedState reads the local finalized state, returning it only when its
// GenesisValidatorsRoot matches the configured genesis so a foreign-network file can never anchor
// the node. Unlike the primary resume path it does not apply the staleness horizon.
func readSameNetworkFinalizedState(dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig, genesisDB genesisdb.GenesisDB) (*state.CachingBeaconState, error) {
	localState, err := ReadLocalFinalizedState(dirs, beaconCfg)
	if err != nil {
		return nil, err
	}
	genesisState, err := genesisDB.ReadGenesisState()
	if err != nil {
		return nil, fmt.Errorf("could not read genesis state to validate local finalized state: %w", err)
	}
	if localState.GenesisValidatorsRoot() != genesisState.GenesisValidatorsRoot() {
		return nil, errors.New("local finalized state is from a different network")
	}
	return localState, nil
}

// tryResumeFromLocalFinalizedState returns the node's own most-recently-finalized state when it is a
// safe local resume anchor, or nil to fall through to remote checkpoint sync. It is safe when the file
// is present, its GenesisValidatorsRoot matches the configured genesis (same network), and it is within
// the data-availability resume horizon. Any failure is logged and yields nil (non-fatal fall-through).
func tryResumeFromLocalFinalizedState(dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig, genesisDB genesisdb.GenesisDB) *state.CachingBeaconState {
	localFinalized, err := ReadLocalFinalizedState(dirs, beaconCfg)
	if err != nil {
		log.Info("[Checkpoint Sync] No local finalized state to resume from; using remote", "reason", "absent", "err", err)
		return nil
	}

	genesisState, err := genesisDB.ReadGenesisState()
	if err != nil {
		log.Warn("[Checkpoint Sync] Could not read genesis state to validate local finalized state; using remote", "err", err)
		return nil
	}
	if localFinalized.GenesisValidatorsRoot() != genesisState.GenesisValidatorsRoot() {
		log.Info("[Checkpoint Sync] Local finalized state is from a different network; using remote", "reason", "gvr-mismatch",
			"local", localFinalized.GenesisValidatorsRoot(), "want", genesisState.GenesisValidatorsRoot())
		return nil
	}

	genesisTime := localFinalized.GenesisTime()
	secondsPerSlot := beaconCfg.SecondsPerSlot
	nowUnix := uint64(time.Now().Unix())
	localSlot := localFinalized.Slot()
	anchorEpoch := localSlot / beaconCfg.SlotsPerEpoch
	needsSidecars := caplinConfig.ArchiveBlobs || caplinConfig.ImmediateBlobsBackfilling
	horizonSlots := resolveResumeHorizonSlots(beaconCfg, anchorEpoch, needsSidecars)
	if !stateWithinResumeHorizon(localSlot, genesisTime, nowUnix, secondsPerSlot, horizonSlots) {
		log.Info("[Checkpoint Sync] Local finalized state is too stale to resume from; using remote", "reason", "stale",
			"slot", localSlot, "horizonSlots", horizonSlots)
		return nil
	}

	log.Info("[Checkpoint Sync] Resuming from local finalized state", "slot", localSlot)
	return localFinalized
}

func readLocalStateFile(dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig, fileName, kind string) (*state.CachingBeaconState, error) {
	statePath := filepath.Join(dirs.CaplinLatest, fileName)
	snappyEncoded, err := afero.ReadFile(afero.NewOsFs(), statePath)
	if err != nil {
		return nil, fmt.Errorf("could not read local %s state file: %w", kind, err)
	}
	decompressed, err := utils.DecompressSnappy(snappyEncoded, false)
	if err != nil {
		return nil, fmt.Errorf("local %s state is corrupt: %w", kind, err)
	}
	slot, err := utils.ExtractSlotFromSerializedBeaconState(decompressed)
	if err != nil {
		return nil, fmt.Errorf("could not extract slot from local %s state: %w", kind, err)
	}
	bs := state.New(beaconCfg)
	epoch := slot / beaconCfg.SlotsPerEpoch
	if err := bs.DecodeSSZ(decompressed, int(beaconCfg.GetCurrentStateVersion(epoch))); err != nil {
		return nil, fmt.Errorf("could not decode local %s state: %w", kind, err)
	}
	return bs, nil
}

// resolveResumeHorizonSlots returns the maximum staleness, in slots, a locally-finalized state
// may have and still be resumed from. The bound is what peers will still serve for the catch-up,
// NOT weak-subjectivity: we resume from our own finalized (reorg-immune) state.
//
// Forward sync fetches sidecars only when the node archives blobs or backfills them immediately;
// such a node is bound by the anchor fork's sidecar retention — blob sidecars pre-Fulu,
// data-column sidecars Fulu+. Every other node fetches blocks alone, whose serve-range is
// MIN_EPOCHS_FOR_BLOCK_REQUESTS and is far wider.
func resolveResumeHorizonSlots(beaconCfg *clparams.BeaconChainConfig, anchorEpoch uint64, needsSidecars bool) uint64 {
	boundEpochs := beaconCfg.MinEpochsForBlockRequests()
	if needsSidecars {
		boundEpochs = beaconCfg.MinEpochsForBlobSidecarsRequests
		if beaconCfg.GetCurrentStateVersion(anchorEpoch).AfterOrEqual(clparams.FuluVersion) {
			boundEpochs = beaconCfg.MinEpochsForDataColumnSidecarsRequests
		}
	}
	return boundEpochs * beaconCfg.SlotsPerEpoch
}

// stateWithinResumeHorizon reports whether a locally-finalized state at localSlot is recent
// enough to resume from, given horizonSlots (see resolveResumeHorizonSlots). When the current
// slot can't be derived (unset secondsPerSlot, or now before genesis) or the local state is
// at/ahead of the current slot, resume is allowed.
func stateWithinResumeHorizon(localSlot, genesisTime, nowUnix, secondsPerSlot, horizonSlots uint64) bool {
	if secondsPerSlot == 0 || nowUnix < genesisTime {
		return true
	}
	currentSlot := (nowUnix - genesisTime) / secondsPerSlot
	if localSlot >= currentSlot {
		return true
	}
	return currentSlot-localSlot <= horizonSlots
}

// FetchFinalizedEnvelope fetches the finalized execution payload envelope from the checkpoint sync endpoint.
func FetchFinalizedEnvelope(ctx context.Context, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig) *cltypes.SignedExecutionPayloadEnvelope {
	hasCustomCheckpointURL := len(clparams.ConfigurableCheckpointsURLs) > 0
	remoteSync := !caplinConfig.DisabledCheckpointSync && (!caplinConfig.IsDevnet() || hasCustomCheckpointURL)
	if !remoteSync {
		return nil
	}

	syncer := NewRemoteCheckpointSync(beaconCfg, caplinConfig.NetworkId).(*RemoteCheckpointSync)
	envelope, err := syncer.FetchFinalizedEnvelope(ctx)
	if err != nil {
		log.Warn("[Checkpoint Sync] Could not fetch finalized envelope (non-fatal)", "err", err)
		return nil
	}
	return envelope
}
