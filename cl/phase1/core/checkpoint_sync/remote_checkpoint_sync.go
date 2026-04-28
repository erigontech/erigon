package checkpoint_sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common/log/v3"
)

const CheckpointHttpTimeout = 60 * time.Second

// RemoteCheckpointSync is a CheckpointSyncer that fetches the checkpoint state from a remote endpoint.
type RemoteCheckpointSync struct {
	beaconConfig *clparams.BeaconChainConfig
	net          clparams.NetworkType
	timeout      time.Duration
}

func NewRemoteCheckpointSync(beaconConfig *clparams.BeaconChainConfig, net clparams.NetworkType) CheckpointSyncer {
	return &RemoteCheckpointSync{
		beaconConfig: beaconConfig,
		net:          net,
		timeout:      CheckpointHttpTimeout,
	}
}

func (r *RemoteCheckpointSync) GetLatestBeaconState(ctx context.Context) (*state.CachingBeaconState, error) {
	uris := clparams.GetAllCheckpointSyncEndpoints(r.net)
	if len(uris) == 0 {
		return nil, errors.New("no uris for checkpoint sync")
	}

	fetchBeaconState := func(uri string) (*state.CachingBeaconState, error) {
		ctxWithTimeout, cancel := context.WithTimeout(ctx, r.timeout)
		defer cancel()

		log.Info("[Checkpoint Sync] Requesting beacon state", "uri", uri)
		req, err := http.NewRequestWithContext(ctxWithTimeout, http.MethodGet, uri, nil)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Accept", "application/octet-stream")
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
		log.Info("[Checkpoint Sync] Beacon state retrieved", "slot", slot)
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

func (r *RemoteCheckpointSync) GetHeadExecutionBlockNumber(ctx context.Context) (uint64, bool, error) {
	uris := clparams.GetAllCheckpointSyncEndpoints(r.net)
	if len(uris) == 0 {
		return 0, false, errors.New("no uris for checkpoint sync")
	}

	var lastErr error
	for _, checkpointURI := range uris {
		blockURI, err := headBlockURI(checkpointURI)
		if err != nil {
			lastErr = err
			continue
		}
		blockNumber, err := r.fetchHeadExecutionBlockNumber(ctx, blockURI)
		if err == nil {
			return blockNumber, true, nil
		}
		lastErr = err
		log.Debug("[Checkpoint Sync] Failed to fetch head beacon block", "uri", blockURI, "err", err)
	}
	return 0, false, lastErr
}

func (r *RemoteCheckpointSync) fetchHeadExecutionBlockNumber(ctx context.Context, uri string) (uint64, error) {
	ctxWithTimeout, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctxWithTimeout, http.MethodGet, uri, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() {
		err = resp.Body.Close()
	}()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("head beacon block request failed, bad status code %d", resp.StatusCode)
	}

	var response struct {
		Data struct {
			Message struct {
				Body struct {
					ExecutionPayload struct {
						BlockNumber string `json:"block_number"`
					} `json:"execution_payload"`
					ExecutionPayloadHeader struct {
						BlockNumber string `json:"block_number"`
					} `json:"execution_payload_header"`
				} `json:"body"`
			} `json:"message"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return 0, err
	}
	if response.Data.Message.Body.ExecutionPayload.BlockNumber != "" {
		return strconv.ParseUint(response.Data.Message.Body.ExecutionPayload.BlockNumber, 10, 64)
	}
	if response.Data.Message.Body.ExecutionPayloadHeader.BlockNumber != "" {
		return strconv.ParseUint(response.Data.Message.Body.ExecutionPayloadHeader.BlockNumber, 10, 64)
	}
	return 0, errors.New("head beacon block has no execution payload block number")
}

func headBlockURI(checkpointURI string) (string, error) {
	u, err := url.Parse(checkpointURI)
	if err != nil {
		return "", err
	}
	ethAPIPath := "/eth/v2/"
	idx := strings.Index(u.Path, ethAPIPath)
	if idx < 0 {
		return "", fmt.Errorf("checkpoint URI has no %s path: %s", ethAPIPath, checkpointURI)
	}
	u.Path = strings.TrimRight(u.Path[:idx], "/") + "/eth/v2/beacon/blocks/head"
	u.RawQuery = ""
	u.Fragment = ""
	return u.String(), nil
}
