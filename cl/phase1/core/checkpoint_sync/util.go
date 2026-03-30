package checkpoint_sync

import (
	"context"
	"fmt"

	"github.com/spf13/afero"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/genesisdb"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common/log/v3"
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

// FetchFinalizedBlock fetches the finalized beacon block from the checkpoint sync endpoint.
// [New in Gloas:EIP7732] Returns nil (not error) if the block cannot be fetched or if
// using local checkpoint sync (genesis/restart). The anchor block is best-effort.
func FetchFinalizedBlock(ctx context.Context, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig) *cltypes.SignedBeaconBlock {
	hasCustomCheckpointURL := len(clparams.ConfigurableCheckpointsURLs) > 0
	remoteSync := !caplinConfig.DisabledCheckpointSync && (!caplinConfig.IsDevnet() || hasCustomCheckpointURL)
	if !remoteSync {
		return nil
	}

	syncer := NewRemoteCheckpointSync(beaconCfg, caplinConfig.NetworkId).(*RemoteCheckpointSync)
	block, err := syncer.FetchFinalizedBlock(ctx)
	if err != nil {
		log.Warn("[Checkpoint Sync] Could not fetch finalized block (non-fatal)", "err", err)
		return nil
	}
	return block
}

// FetchFinalizedEnvelope fetches the finalized execution payload envelope from the checkpoint sync endpoint.
// [New in Gloas:EIP7732] Returns nil (not error) if the envelope cannot be fetched, if the
// finalized block was EMPTY, or if using local checkpoint sync.
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
