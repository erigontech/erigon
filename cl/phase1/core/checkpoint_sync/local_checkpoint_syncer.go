package checkpoint_sync

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/spf13/afero"
)

type LocalCheckpointSyncer struct {
	genesisState *state.CachingBeaconState
	dir          afero.Fs
}

// The local checkpoint syncer, loads a checkpoint from the local disk or uses the genesis state.
func NewLocalCheckpointSyncer(genesisState *state.CachingBeaconState, dir afero.Fs) CheckpointSyncer {
	return &LocalCheckpointSyncer{
		genesisState: genesisState,
		dir:          dir,
	}

}

func (l *LocalCheckpointSyncer) GetLatestBeaconState(ctx context.Context) (*state.CachingBeaconState, error) {
	// Open file {latestStateSubDir}/{fileName}
	snappyEncoded, err := afero.ReadFile(l.dir, clparams.LatestStateFileName)
	if err != nil {
		log.Warn("Could not read local state, starting sync from genesis.")
		return l.genesisState.Copy()
	}
	decompressedSnappy, err := utils.DecompressSnappy(snappyEncoded, false)
	if err != nil {
		return nil, fmt.Errorf("local state is corrupt: %s", err)
	}

	beaconCfg := l.genesisState.BeaconConfig()
	bs := state.New(beaconCfg)
	slot, err := utils.ExtractSlotFromSerializedBeaconState(decompressedSnappy)
	if err != nil {
		return nil, fmt.Errorf("could not deserialize state slot: %s", err)
	}
	if err := bs.DecodeSSZ(decompressedSnappy, int(beaconCfg.GetCurrentStateVersion(slot/beaconCfg.SlotsPerEpoch))); err != nil {
		return nil, fmt.Errorf("could not deserialize state: %s", err)
	}
	return bs, nil
}
