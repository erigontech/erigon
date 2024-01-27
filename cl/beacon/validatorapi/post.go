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

func (v *ValidatorApiHandler) PostEthV1ValidatorBeaconCommitteeSubscriptions(w http.ResponseWriter, r *http.Request) (*string, error) {
	var req []building.BeaconCommitteeSubscription
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(400, fmt.Errorf("invalid request: %w", err))
	}

	// the empty list i believe is a valid request, and it will just do nothing
	if len(req) == 0 {
		return nil, nil
	}

	/*
	   	  After beacon node receives this request,
	   		search using discv5 for peers related to this subnet and replace current peers with those ones if necessary.
	   		If validator is_aggregator, beacon node must:

	       1. announce subnet topic subscription on gossipsub
	       2. aggregate attestations received on that subnet
	*/

	// grab the head state to calculate committee counts with
	s, err := v.privateGetStateFromStateId("head")
	if err != nil {
		return nil, err
	}

	//calculate gossip_subnets
	var subnets []uint64
	for _, x := range req {
		committees_per_slot := s.CommitteeCount(uint64(x.Slot) / v.BeaconChainCfg.SlotsPerEpoch)
		subnet := v.BeaconChainCfg.ComputeSubnetForAttestation(committees_per_slot, uint64(x.Slot), uint64(x.CommitteeIndex))
		subnets = append(subnets, subnet)
	}

	// beacon_attestation_{compute_subnet_for_attestation(committees_per_slot, attestation.data.slot, attestation.data.index)}
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
