package checkpoint_sync

import (
	"context"

	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/spf13/afero"
)

// ReadOrFetchLatestBeaconState reads the latest beacon state from disk or fetches it from the network.
func ReadOrFetchLatestBeaconState(ctx context.Context, dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig) (*state.CachingBeaconState, error) {
	var syncer CheckpointSyncer
	remoteSync := !caplinConfig.DisabledCheckpointSync

	if !initial_state.IsGenesisStateSupported(caplinConfig.NetworkId) && !remoteSync {
		log.Warn("Local checkpoint sync is not supported for this network, falling back to remote sync")
		remoteSync = true
	}
	if remoteSync {
		syncer = NewRemoteCheckpointSync(beaconCfg, caplinConfig.NetworkId)
	} else {
		aferoFs := afero.NewOsFs()

		genesisState, err := initial_state.GetGenesisState(caplinConfig.NetworkId)
		if err != nil {
			return nil, err
		}
		syncer = NewLocalCheckpointSyncer(genesisState, afero.NewBasePathFs(aferoFs, dirs.CaplinLatest))
	}
	return syncer.GetLatestBeaconState(ctx)
}
