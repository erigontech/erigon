package checkpoint_sync

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
)

// ReadOrFetchLatestBeaconState reads the latest beacon state from disk or fetches it from the network.
// If remote checkpoint sync fails, it falls back to the local head state on disk.
// If no local head state is available, it returns an error.
func ReadOrFetchLatestBeaconState(ctx context.Context, dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig, genesisDB genesisdb.GenesisDB) (*state.CachingBeaconState, error) {
	remoteSync := !caplinConfig.DisabledCheckpointSync && !caplinConfig.IsDevnet()

	if remoteSync {
		syncer := NewRemoteCheckpointSync(beaconCfg, caplinConfig.NetworkId)
		st, err := syncer.GetLatestBeaconState(ctx)
		if err == nil {
			return st, nil
		}
		log.Warn("[Checkpoint Sync] Remote checkpoint sync failed, attempting to read local head state", "err", err)

		// Fallback: try to read the local head state from disk
		localState, localErr := ReadLocalHeadState(dirs, beaconCfg)
		if localErr == nil {
			log.Info("[Checkpoint Sync] Successfully loaded local head state", "slot", localState.Slot())
			return localState, nil
		}
		log.Error("[Checkpoint Sync] No local head state available either", "err", localErr)
		return nil, fmt.Errorf("remote checkpoint sync failed: %w, and no local head state: %w", err, localErr)
	}

	// Non-remote sync path (disabled checkpoint sync or devnet)
	aferoFs := afero.NewOsFs()
	genesisState, err := genesisDB.ReadGenesisState()
	if err != nil {
		return nil, fmt.Errorf("could not read genesis state: %w", err)
	}
	syncer := NewLocalCheckpointSyncer(genesisState, afero.NewBasePathFs(aferoFs, dirs.CaplinLatest))
	return syncer.GetLatestBeaconState(ctx)
}

// ReadLocalHeadState reads the head state directly from disk without falling back to genesis.
func ReadLocalHeadState(dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig) (*state.CachingBeaconState, error) {
	statePath := filepath.Join(dirs.CaplinLatest, clparams.LatestStateFileName)
	snappyEncoded, err := afero.ReadFile(afero.NewOsFs(), statePath)
	if err != nil {
		return nil, fmt.Errorf("could not read local head state file: %w", err)
	}
	decompressed, err := utils.DecompressSnappy(snappyEncoded, false)
	if err != nil {
		return nil, fmt.Errorf("local head state is corrupt: %w", err)
	}
	slot, err := utils.ExtractSlotFromSerializedBeaconState(decompressed)
	if err != nil {
		return nil, fmt.Errorf("could not extract slot from local head state: %w", err)
	}
	bs := state.New(beaconCfg)
	epoch := slot / beaconCfg.SlotsPerEpoch
	if err := bs.DecodeSSZ(decompressed, int(beaconCfg.GetCurrentStateVersion(epoch))); err != nil {
		return nil, fmt.Errorf("could not decode local head state: %w", err)
	}
	return bs, nil
}
