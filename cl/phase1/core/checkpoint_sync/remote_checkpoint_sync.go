package checkpoint_sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
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
			return nil, fmt.Errorf("checkpoint sync read failed: %w", err)
		}

		slot, err := utils.ExtractSlotFromSerializedBeaconState(marshaled)
		if err != nil {
			return nil, fmt.Errorf("checkpoint sync read failed: %w", err)
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
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		log.Warn("[Checkpoint Sync] Failed to fetch beacon state", "uri", uri, "err", err)
	}
	return nil, err
}

// FetchFinalizedBlock fetches the finalized beacon block from the checkpoint sync endpoint.
// [New in Gloas:EIP7732] The anchor block is needed so that the fork graph can determine
// whether the first forward-sync block was built on a FULL or EMPTY parent.
func (r *RemoteCheckpointSync) FetchFinalizedBlock(ctx context.Context) (*cltypes.SignedBeaconBlock, error) {
	uris := clparams.GetAllCheckpointSyncEndpoints(r.net)
	for _, uri := range uris {
		block, err := r.fetchBlock(ctx, uri)
		if err != nil {
			log.Debug("[Checkpoint Sync] Failed to fetch finalized block", "uri", uri, "err", err)
			continue
		}
		return block, nil
	}
	return nil, nil // Non-fatal: anchor block is best-effort for GLOAS
}

func (r *RemoteCheckpointSync) fetchBlock(ctx context.Context, stateURI string) (*cltypes.SignedBeaconBlock, error) {
	// Derive the block URL from the state URL.
	// State: .../eth/v2/debug/beacon/states/finalized
	// Block: .../eth/v2/beacon/blocks/finalized
	idx := strings.Index(stateURI, "/eth/")
	if idx < 0 {
		return nil, fmt.Errorf("cannot derive block URL from %s", stateURI)
	}
	blockURI := stateURI[:idx] + "/eth/v2/beacon/blocks/finalized"

	ctxWithTimeout, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	log.Info("[Checkpoint Sync] Requesting finalized block", "uri", blockURI)
	req, err := http.NewRequestWithContext(ctxWithTimeout, http.MethodGet, blockURI, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("finalized block fetch failed, status %d", resp.StatusCode)
	}

	marshaled, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("finalized block read failed: %w", err)
	}

	version := cycleDetectVersion(resp.Header.Get("Eth-Consensus-Version"))

	block := cltypes.NewSignedBeaconBlock(r.beaconConfig, version)
	if err := block.DecodeSSZ(marshaled, int(version)); err != nil {
		return nil, fmt.Errorf("finalized block decode failed: %w", err)
	}
	log.Info("[Checkpoint Sync] Finalized block retrieved", "slot", block.Block.Slot)
	return block, nil
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
	// Derive the envelope URL from the state URL.
	// State:    .../eth/v2/debug/beacon/states/finalized
	// Envelope: .../eth/v1/beacon/execution_payload_envelope/finalized
	idx := strings.Index(stateURI, "/eth/")
	if idx < 0 {
		return nil, fmt.Errorf("cannot derive envelope URL from %s", stateURI)
	}
	envelopeURI := stateURI[:idx] + "/eth/v1/beacon/execution_payload_envelope/finalized"

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
	log.Info("[Checkpoint Sync] Finalized envelope retrieved", "slot", envelope.Message.Slot)
	return envelope, nil
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
