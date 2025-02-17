package checkpoint_sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
)

// RemoteCheckpointSync is a CheckpointSyncer that fetches the checkpoint state from a remote endpoint.
type RemoteCheckpointSync struct {
	beaconConfig *clparams.BeaconChainConfig
	net          clparams.NetworkType
}

func NewRemoteCheckpointSync(beaconConfig *clparams.BeaconChainConfig, net clparams.NetworkType) CheckpointSyncer {
	return &RemoteCheckpointSync{
		beaconConfig: beaconConfig,
		net:          net,
	}
}

func (r *RemoteCheckpointSync) GetLatestBeaconState(ctx context.Context) (*state.CachingBeaconState, error) {
	uris := clparams.GetAllCheckpointSyncEndpoints(r.net)
	if len(uris) == 0 {
		return nil, errors.New("no uris for checkpoint sync")
	}

	fetchBeaconState := func(uri string) (*state.CachingBeaconState, error) {
		log.Info("[Checkpoint Sync] Requesting beacon state", "uri", uri)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Accept", "application/octet-stream")
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync request failed %s", err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer func() {
			err = resp.Body.Close()
		}()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("checkpoint sync failed, bad status code %d", resp.StatusCode)
		}
		marshaled, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync read failed %s", err)
		}

		slot, err := utils.ExtractSlotFromSerializedBeaconState(marshaled)
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync read failed %s", err)
		}

		epoch := slot / r.beaconConfig.SlotsPerEpoch
		beaconState := state.New(r.beaconConfig)
		err = beaconState.DecodeSSZ(marshaled, int(r.beaconConfig.GetCurrentStateVersion(epoch)))
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync decode failed %s", err)
		}
		return beaconState, nil
	}

	// Try all uris until one succeeds
	var err error
	var beaconState *state.CachingBeaconState
	for _, uri := range uris {
		beaconState, err = fetchBeaconState(uri)
		if err == nil {
			return beaconState, nil
		}
		log.Warn("[Checkpoint Sync] Failed to fetch beacon state", "uri", uri, "err", err)
	}
	return nil, err

}
