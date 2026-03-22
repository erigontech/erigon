package checkpoint_sync

import (
	"context"
	"fmt"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/db/datadir"
)

// ReadOrFetchLatestBeaconState reads the latest beacon state from disk or fetches it from the network.
func ReadOrFetchLatestBeaconState(ctx context.Context, dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig, genesisDB genesisdb.GenesisDB) (*state.CachingBeaconState, error) {
	var syncer CheckpointSyncer
	// Allow remote checkpoint sync for devnets when the user explicitly provides a checkpoint sync URL.
	hasCustomCheckpointURL := len(clparams.ConfigurableCheckpointsURLs) > 0
	remoteSync := !caplinConfig.DisabledCheckpointSync && (!caplinConfig.IsDevnet() || hasCustomCheckpointURL)

	if remoteSync {
		syncer = NewRemoteCheckpointSync(beaconCfg, caplinConfig.NetworkId)
	} else {
		aferoFs := afero.NewOsFs()

		genesisState, err := genesisDB.ReadGenesisState()
		if err != nil {
			return nil, fmt.Errorf("could not read genesis state: %w", err)
		}
		syncer = NewLocalCheckpointSyncer(genesisState, afero.NewBasePathFs(aferoFs, dirs.CaplinLatest))
	}
	return syncer.GetLatestBeaconState(ctx)
}
