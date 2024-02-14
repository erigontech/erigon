package validatorapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/beacon/building"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
)

var errNotImplemented = errors.New("not implemented")

func (v *ValidatorApiHandler) PostEthV1ValidatorPrepareBeaconProposer(w http.ResponseWriter, r *http.Request) (*int, error) {
	var req []building.PrepareBeaconProposer
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	for _, x := range req {
		v.state.SetFeeRecipient(x.ValidatorIndex, x.FeeRecipient)
	}
	return nil, nil
}

func (v *ValidatorApiHandler) PostEthV1ValidatorContributionAndProofs(w http.ResponseWriter, r *http.Request) (*int, error) {
	var req []*cltypes.ContributionAndProof
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV1ValidatorSyncCommitteeSubscriptions(w http.ResponseWriter, r *http.Request) (*int, error) {
	var req []building.SyncCommitteeSubscription
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV1ValidatorBeaconCommitteeSubscriptions(w http.ResponseWriter, r *http.Request) (*int, error) {
	var req []building.BeaconCommitteeSubscription
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV1ValidatorAggregateAndProofs(w http.ResponseWriter, r *http.Request) (*int, error) {
	var req []cltypes.SignedAggregateAndProof
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV1BeaconPoolSyncCommittees(w http.ResponseWriter, r *http.Request) (*int, error) {
	var req []*solid.SyncCommittee
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV1BeaconPoolAttestations(w http.ResponseWriter, r *http.Request) (*int, error) {
	var req []*solid.Attestation
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV1BeaconBlocks(w http.ResponseWriter, r *http.Request) (*int, error) {
	ethConsensusVersion := r.Header.Get("Eth-Consensus-Version")
	var req cltypes.SignedBeaconBlock
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	_ = ethConsensusVersion
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV2BeaconBlocks(w http.ResponseWriter, r *http.Request) (*int, error) {
	broadcastValidation := r.URL.Query().Get("broadcast_validation")
	if broadcastValidation == "" {
		broadcastValidation = "gossip"
	}
	ethConsensusVersion := r.Header.Get("Eth-Consensus-Version")
	if ethConsensusVersion == "" {
		return nil, beaconhttp.NewEndpointError(400, errors.New("no eth consensus version set"))
	}
	var req cltypes.SignedBeaconBlock
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	_, _ = broadcastValidation, ethConsensusVersion
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV1BeaconBlindedBlocks(w http.ResponseWriter, r *http.Request) (*int, error) {
	ethConsensusVersion := r.Header.Get("Eth-Consensus-Version")
	var req cltypes.SignedBlindedBeaconBlock
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	_ = ethConsensusVersion
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}

func (v *ValidatorApiHandler) PostEthV2BeaconBlindedBlocks(w http.ResponseWriter, r *http.Request) (*int, error) {
	broadcastValidation := r.URL.Query().Get("broadcast_validation")
	if broadcastValidation == "" {
		broadcastValidation = "gossip"
	}
	ethConsensusVersion := r.Header.Get("Eth-Consensus-Version")
	if ethConsensusVersion == "" {
		return nil, beaconhttp.NewEndpointError(400, errors.New("no eth consensus version set"))
	}
	var req cltypes.SignedBlindedBeaconBlock
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}
	// TODO: this endpoint
	_, _ = broadcastValidation, ethConsensusVersion
	return nil, beaconhttp.NewEndpointError(404, errNotImplemented)
}
