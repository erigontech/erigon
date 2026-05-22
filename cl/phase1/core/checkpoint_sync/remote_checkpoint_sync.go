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
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	CheckpointHttpTimeout = 60 * time.Second

	// beaconStatePath is the standard Beacon API path for fetching the finalized state.
	// Users commonly provide just the checkpoint base URL (e.g. https://checkpoint-sync.example.io)
	// without this suffix, which results in fetching an HTML page instead of SSZ data.
	beaconStatePath = "/eth/v2/debug/beacon/states/finalized"
)

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
		uri = normalizeCheckpointURL(uri)

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

		ct := resp.Header.Get("Content-Type")
		if ct != "" && !strings.HasPrefix(ct, "application/octet-stream") {
			return nil, fmt.Errorf("checkpoint sync returned unexpected content-type %q (expected application/octet-stream); verify the URL includes the beacon state API path", ct)
		}

		marshaled, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync read failed: %w", err)
		}

		slot, err := utils.ExtractSlotFromSerializedBeaconState(marshaled)
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync read failed: %w", err)
		}

		epoch := slot / r.beaconConfig.SlotsPerEpoch
		stateVersion := r.beaconConfig.GetCurrentStateVersion(epoch)

		// Prefer the Eth-Consensus-Version header from the checkpoint server when
		// available. This is more reliable than deriving the version from the slot
		// when running against devnets whose custom config may not be fully loaded.
		if hdr := resp.Header.Get("Eth-Consensus-Version"); hdr != "" {
			if hdrVersion := cycleDetectVersion(hdr); hdrVersion != stateVersion {
				log.Info("[Checkpoint Sync] Overriding config-derived version with Eth-Consensus-Version header",
					"configVersion", stateVersion, "headerVersion", hdrVersion)
				stateVersion = hdrVersion
			}
		}

		log.Info("[Checkpoint Sync] Decoding beacon state",
			"slot", slot, "epoch", epoch,
			"stateVersion", stateVersion)
		beaconState := state.New(r.beaconConfig)
		err = beaconState.DecodeSSZ(marshaled, int(stateVersion))
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
		if errors.Is(err, context.Canceled) {
			return nil, err
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

// FetchFinalizedEnvelope fetches the finalized execution payload envelope from the checkpoint sync endpoint.
// [New in Gloas:EIP7732] The anchor envelope is needed so that the fork graph knows whether the
// finalized block was FULL (had its payload executed) or EMPTY.
func (r *RemoteCheckpointSync) FetchFinalizedEnvelope(ctx context.Context) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	uris := clparams.GetAllCheckpointSyncEndpoints(r.net)
	for _, uri := range uris {
		env, err := r.fetchEnvelope(ctx, uri)
		if err != nil {
			log.Debug("[Checkpoint Sync] Failed to fetch finalized envelope", "uri", uri, "err", err)
			continue
		}
		return env, nil
	}
	return nil, nil // Non-fatal: anchor envelope is best-effort
}

func (r *RemoteCheckpointSync) fetchEnvelope(ctx context.Context, stateURI string) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	stateURI = normalizeCheckpointURL(stateURI)

	// Derive the envelope URL from the state URL.
	// State:    .../eth/v2/debug/beacon/states/{state_id}
	// Envelope: .../eth/v1/beacon/execution_payload_envelope/{state_id}
	idx := strings.Index(stateURI, "/eth/")
	if idx < 0 {
		return nil, fmt.Errorf("cannot derive envelope URL from %s", stateURI)
	}
	stateId := stateURI[strings.LastIndex(stateURI, "/")+1:]
	envelopeURI := stateURI[:idx] + "/eth/v1/beacon/execution_payload_envelope/" + stateId

	ctxWithTimeout, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	log.Info("[Checkpoint Sync] Requesting finalized envelope", "uri", envelopeURI)
	req, err := http.NewRequestWithContext(ctxWithTimeout, http.MethodGet, envelopeURI, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// 404 means the finalized block was EMPTY (no envelope exists) — this is normal
		log.Info("[Checkpoint Sync] Finalized envelope not found (block may be EMPTY)")
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("finalized envelope fetch failed, status %d", resp.StatusCode)
	}

	marshaled, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("finalized envelope read failed: %w", err)
	}

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(r.beaconConfig),
	}
	if err := envelope.DecodeSSZ(marshaled, int(clparams.GloasVersion)); err != nil {
		return nil, fmt.Errorf("finalized envelope decode failed: %w", err)
	}
	log.Info("[Checkpoint Sync] Finalized envelope retrieved", "beaconBlockRoot", envelope.Message.BeaconBlockRoot)
	return envelope, nil
}

// normalizeCheckpointURL ensures the URL includes the beacon state API path.
// Users often provide just the base URL (e.g. https://checkpoint-sync.example.io)
// which serves an HTML landing page instead of SSZ data.
func normalizeCheckpointURL(uri string) string {
	if !strings.Contains(uri, "/eth/") {
		uri = strings.TrimRight(uri, "/") + beaconStatePath
	}
	return uri
}

// cycleDetectVersion maps the Eth-Consensus-Version header to a clparams version.
func cycleDetectVersion(header string) clparams.StateVersion {
	switch strings.ToLower(header) {
	case "phase0":
		return clparams.Phase0Version
	case "altair":
		return clparams.AltairVersion
	case "bellatrix":
		return clparams.BellatrixVersion
	case "capella":
		return clparams.CapellaVersion
	case "deneb":
		return clparams.DenebVersion
	case "electra":
		return clparams.ElectraVersion
	case "fulu":
		return clparams.FuluVersion
	case "gloas", "glamsterdam":
		return clparams.GloasVersion
	default:
		return clparams.GloasVersion // Assume latest
	}
}
