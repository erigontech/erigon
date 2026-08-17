package checkpoint_sync

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common/log/v3"
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
	// Resume from the node's own most-recently-finalized state (reorg-immune), or genesis when absent.
	snappyEncoded, err := afero.ReadFile(l.dir, clparams.LatestFinalizedStateFileName)
	if err != nil {
		log.Warn("Could not read local finalized state, starting sync from genesis.", "file", clparams.LatestFinalizedStateFileName, "err", err)
		return l.genesisState.Copy()
	}
	decompressedSnappy, err := utils.DecompressSnappy(snappyEncoded, false)
	if err != nil {
		return nil, fmt.Errorf("local state is corrupt: %w", err)
	}

	beaconCfg := l.genesisState.BeaconConfig()
	bs := state.New(beaconCfg)
	slot, err := utils.ExtractSlotFromSerializedBeaconState(decompressedSnappy)
	if err != nil {
		return nil, fmt.Errorf("could not deserialize state slot: %w", err)
	}
	if err := bs.DecodeSSZ(decompressedSnappy, int(beaconCfg.GetCurrentStateVersion(slot/beaconCfg.SlotsPerEpoch))); err != nil {
		return nil, fmt.Errorf("could not deserialize state: %w", err)
	}
	// Same-network gate as the remote-sync resume paths: a file left by another chain must never
	// anchor the node. Staleness is not gated here — there is no remote to fall back to, and a stale
	// same-network finalized anchor beats replaying from genesis.
	if bs.GenesisValidatorsRoot() != l.genesisState.GenesisValidatorsRoot() {
		log.Warn("Local finalized state is from a different network, starting sync from genesis.",
			"local", bs.GenesisValidatorsRoot(), "want", l.genesisState.GenesisValidatorsRoot())
		return l.genesisState.Copy()
	}
	return bs, nil
}
