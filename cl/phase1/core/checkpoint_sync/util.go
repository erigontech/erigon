package checkpoint_sync

import (
	"context"
	"fmt"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
)

// maxLocalCheckpointAge is the maximum age (in slots) of a local checkpoint to be considered valid
// for reuse instead of fetching from remote. 14400 slots = ~48 hours at 12s/slot.
const maxLocalCheckpointAge = 14400

// ReadOrFetchLatestBeaconState reads the latest beacon state from disk or fetches it from the network.
// When remote sync is enabled, it first checks for a local checkpoint. If the local checkpoint exists
// and is within maxLocalCheckpointAge slots of the current time, it will be used instead of fetching
// from the network.
func ReadOrFetchLatestBeaconState(ctx context.Context, dirs datadir.Dirs, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig, genesisDB genesisdb.GenesisDB, ethClock eth_clock.EthereumClock) (*state.CachingBeaconState, error) {
	aferoFs := afero.NewOsFs()
	caplinLatestFs := afero.NewBasePathFs(aferoFs, dirs.CaplinLatest)

	genesisState, err := genesisDB.ReadGenesisState()
	if err != nil {
		return nil, fmt.Errorf("could not read genesis state: %w", err)
	}

	remoteSync := !caplinConfig.DisabledCheckpointSync && !caplinConfig.IsDevnet()

	// Try to load local checkpoint first
	localSyncer := NewLocalCheckpointSyncer(genesisState, caplinLatestFs)
	localState, err := localSyncer.GetLatestBeaconState(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not read local checkpoint: %w", err)
	}

	localSlot := localState.Slot()
	currentSlot := ethClock.GetCurrentSlot()
	isLocalRecent := currentSlot >= localSlot && (currentSlot-localSlot) <= maxLocalCheckpointAge

	if remoteSync {
		// If local checkpoint is recent enough, use it
		if localSlot > 0 && isLocalRecent {
			log.Info("[Checkpoint Sync] Using recent local checkpoint", "localSlot", localSlot, "currentSlot", currentSlot, "age", currentSlot-localSlot)
			return localState, nil
		}

		if localSlot > 0 {
			log.Info("[Checkpoint Sync] Local checkpoint too old, fetching from network", "localSlot", localSlot, "currentSlot", currentSlot, "age", currentSlot-localSlot)
		}

		// Fetch from remote
		syncer := NewRemoteCheckpointSync(beaconCfg, caplinConfig.NetworkId)
		return syncer.GetLatestBeaconState(ctx)
	}

	// Local sync mode (devnet or checkpoint sync disabled)
	return localState, nil
}
