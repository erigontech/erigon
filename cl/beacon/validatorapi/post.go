package validatorapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/sentinel"
	"github.com/ledgerwatch/erigon/cl/beacon/beaconhttp"
	"github.com/ledgerwatch/erigon/cl/beacon/building"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/log/v3"
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

	l := log.New()

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

	digest, err := fork.ComputeForkDigest(v.BeaconChainCfg, v.GenesisCfg)
	if err != nil {
		l.Error("[Gossip] Failed to calculate fork choice", "err", err)
	}

	//calculate gossip_subnets
	topics := make([]string, 0, len(req))
	for _, x := range req {
		// skip ones we are aggregator for
		if !x.IsAggregator {
			continue
		}
		committees_per_slot := s.CommitteeCount(uint64(x.Slot) / v.BeaconChainCfg.SlotsPerEpoch)
		subnet := v.BeaconChainCfg.ComputeSubnetForAttestation(committees_per_slot, uint64(x.Slot), uint64(x.CommitteeIndex))
		topicString := fmt.Sprintf("/eth2/%x/beacon_attestation_%d/%s", digest, subnet, "ssz_snappy")
		topics = append(topics, topicString)
	}

	filterString := "/eth2/*/beacon_attestation_*/ssz_snappy"
	subscriptionData := &sentinel.SubscriptionData{
		Filter: &filterString,
		Topics: topics, // TODO: this should be done in advance in the sentinel and recalculated every once in a while
	}
	subDuration := time.Duration(v.BeaconChainCfg.SecondsPerSlot*v.BeaconChainCfg.SlotsPerEpoch*v.BeaconChainCfg.EpochsPerSyncCommitteePeriod) * time.Second
	ctx, cn := context.WithTimeout(r.Context(), subDuration)
	sub, err := v.Sentinel.SubscribeGossip(ctx, subscriptionData)
	if err != nil {
		cn()
		return nil, err
	}
	go func() {
		l.Info("[beacon api] started aggregating gossip", "subscription", subscriptionData)
		defer func() {
			l.Info("[beacon api] aggregation gossip subscriber closed")
			cn()
		}()
		for {
			dat, err := sub.Recv()
			if err != nil {
				return
			}
			split := strings.Split(strings.Trim(dat.Name, "/"), "/")
			if len(split) < 4 {
				continue
			}
			decimalString := strings.TrimPrefix(split[2], "beacon_attestation_")
			// TODO: validate subnet topic int so we dont repeat work perhaps? not sure.
			// possible optimization for cpu usage down the line
			subnetTopicInt, err := strconv.ParseUint(decimalString, 10, 64)
			if err != nil {
				continue
			}
			l = l.New("subnetTopic", subnetTopicInt)
			a := &solid.Attestation{}
			err = a.DecodeSSZ(dat.Data, 0)
			if err != nil {
				l.Info("[beacon api] received invalid attestation", "error", err)
				continue
			}
			//
			err = v.Machine.ProcessAttestations(s, solid.NewDynamicListSSZFromList[*solid.Attestation]([]*solid.Attestation{a}, 1))
			if err != nil {
				l.Info("[beacon api] attestation failed validation", "error", err)
				continue
			}
			v.state.AddAttestation(subnetTopicInt, a)
			l.Info("[beacon api] added attestation", "signature", common.Bytes96(a.Signature()))
		}
	}()
	return nil, nil
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
