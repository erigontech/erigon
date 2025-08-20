// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package handler

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"

	sentinel "github.com/erigontech/erigon-lib/gointerfaces/sentinelproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
)

func (a *ApiHandler) GetEthV1BeaconPoolVoluntaryExits(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(a.operationsPool.VoluntaryExitsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolAttesterSlashings(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(a.operationsPool.AttesterSlashingsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolProposerSlashings(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(a.operationsPool.ProposerSlashingsPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolBLSExecutionChanges(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	return newBeaconResponse(a.operationsPool.BLSToExecutionChangesPool.Raw()), nil
}

func (a *ApiHandler) GetEthV1BeaconPoolAttestations(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	committeeIndex, err := beaconhttp.Uint64FromQueryParams(r, "committee_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	atts := a.operationsPool.AttestationsPool.Raw()
	if slot == nil && committeeIndex == nil {
		return newBeaconResponse(atts), nil
	}
	ret := make([]any, 0, len(atts))
	for i := range atts {
		if slot != nil && atts[i].Data.Slot != *slot {
			continue
		}
		cIndex := atts[i].Data.CommitteeIndex
		if committeeIndex != nil && cIndex != *committeeIndex {
			continue
		}
		ret = append(ret, atts[i])
	}

	return newBeaconResponse(ret), nil
}

func (a *ApiHandler) GetEthV2BeaconPoolAttestations(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	committeeIndex, err := beaconhttp.Uint64FromQueryParams(r, "committee_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	atts := a.operationsPool.AttestationsPool.Raw()
	if slot == nil && committeeIndex == nil {
		return newBeaconResponse(atts), nil
	}
	ret := make([]any, 0, len(atts))
	for i := range atts {
		if slot != nil && atts[i].Data.Slot != *slot {
			continue
		}
		if committeeIndex != nil {
			indices := atts[i].CommitteeBits.GetOnIndices()
			if len(indices) != 1 || uint64(indices[0]) != *committeeIndex {
				continue
			}
		}
		ret = append(ret, atts[i])
	}

	return newBeaconResponse(ret), nil
}

func (a *ApiHandler) PostEthV1BeaconPoolAttestations(w http.ResponseWriter, r *http.Request) {
	req := []*solid.Attestation{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	failures := []poolingFailure{}
	for i, attestation := range req {
		if a.syncedData.Syncing() {
			beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("head state not available")).WriteTo(w)
			return
		}
		var (
			slot                      = attestation.Data.Slot
			cIndex                    = attestation.Data.CommitteeIndex
			committeeCountPerSlot     = a.syncedData.CommitteeCount(slot / a.beaconChainCfg.SlotsPerEpoch)
			attestationWithGossipData = &services.AttestationForGossip{
				Attestation:      attestation,
				ImmediateProcess: true, // we want to process attestation immediately
			}
		)
		subnet := subnets.ComputeSubnetForAttestation(committeeCountPerSlot, slot, cIndex, a.beaconChainCfg.SlotsPerEpoch, a.netConfig.AttestationSubnetCount)
		encodedSSZ, err := attestation.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}

		if err := a.attestationService.ProcessMessage(r.Context(), &subnet, attestationWithGossipData); err != nil && !errors.Is(err, services.ErrIgnore) {
			log.Warn("[Beacon REST] failed to process attestation in attestation service", "err", err)
			failures = append(failures, poolingFailure{
				Index:   i,
				Message: err.Error(),
			})
			continue
		}
		if a.sentinel != nil {
			if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
				Data:     encodedSSZ,
				Name:     gossip.TopicNamePrefixBeaconAttestation,
				SubnetId: &subnet,
			}); err != nil {
				a.logger.Debug("[Beacon REST] failed to publish attestation to gossip", "err", err)
			}
		}
	}
	if len(failures) > 0 {
		errResp := poolingError{
			Code:     http.StatusBadRequest,
			Message:  "some failures",
			Failures: failures,
		}
		w.WriteHeader(http.StatusBadRequest)
		if err := json.NewEncoder(w).Encode(errResp); err != nil {
			log.Warn("failed to encode response", "err", err)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV2BeaconPoolAttestations(w http.ResponseWriter, r *http.Request) {
	v := r.Header.Get("Eth-Consensus-Version")
	if v == "" {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("missing version header")).WriteTo(w)
		return
	}
	clVersion, err := clparams.StringToClVersion(v)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	if clVersion < clparams.ElectraVersion {
		a.PostEthV1BeaconPoolAttestations(w, r)
		return
	}

	req := []*solid.SingleAttestation{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	failures := []poolingFailure{}
	for i, attestation := range req {
		if a.syncedData.Syncing() {
			beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("head state not available")).WriteTo(w)
			return
		}
		var (
			slot                      = attestation.AttestationData().Slot
			cIndex                    = attestation.CommitteeIndex
			committeeCountPerSlot     = a.syncedData.CommitteeCount(slot / a.beaconChainCfg.SlotsPerEpoch)
			attestationWithGossipData = &services.AttestationForGossip{
				SingleAttestation: attestation,
				ImmediateProcess:  true, // we want to process attestation immediately
			}
		)
		subnet := subnets.ComputeSubnetForAttestation(committeeCountPerSlot, slot, cIndex, a.beaconChainCfg.SlotsPerEpoch, a.netConfig.AttestationSubnetCount)
		encodedSSZ, err := attestation.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}

		if err := a.attestationService.ProcessMessage(r.Context(), &subnet, attestationWithGossipData); err != nil && !errors.Is(err, services.ErrIgnore) {
			log.Warn("[Beacon REST] failed to process attestation in attestation service", "err", err)
			failures = append(failures, poolingFailure{
				Index:   i,
				Message: err.Error(),
			})
			continue
		}
		if a.sentinel != nil {
			if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
				Data:     encodedSSZ,
				Name:     gossip.TopicNamePrefixBeaconAttestation,
				SubnetId: &subnet,
			}); err != nil {
				a.logger.Debug("[Beacon REST] failed to publish attestation to gossip", "err", err)
			}
		}
	}
	if len(failures) > 0 {
		errResp := poolingError{
			Code:     http.StatusBadRequest,
			Message:  "some failures",
			Failures: failures,
		}
		w.WriteHeader(http.StatusBadRequest)
		if err := json.NewEncoder(w).Encode(errResp); err != nil {
			log.Warn("failed to encode response", "err", err)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV1BeaconPoolVoluntaryExits(w http.ResponseWriter, r *http.Request) {
	req := cltypes.SignedVoluntaryExit{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	encodedSSZ, err := req.EncodeSSZ(nil)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
		return
	}

	if err := a.voluntaryExitService.ProcessMessage(r.Context(), nil, &services.SignedVoluntaryExitForGossip{
		SignedVoluntaryExit:   &req,
		ImmediateVerification: true,
	}); err != nil && !errors.Is(err, services.ErrIgnore) {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	a.operationsPool.VoluntaryExitsPool.Insert(req.VoluntaryExit.ValidatorIndex, &req)
	if a.sentinel != nil {
		if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
			Data: encodedSSZ,
			Name: gossip.TopicNameVoluntaryExit,
		}); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish voluntary exit to gossip", "err", err)
		}
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV1BeaconPoolAttesterSlashings(w http.ResponseWriter, r *http.Request) {
	clVersion := a.beaconChainCfg.GetCurrentStateVersion(a.ethClock.GetCurrentEpoch())

	req := cltypes.NewAttesterSlashing(clVersion)
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	if err := a.forkchoiceStore.OnAttesterSlashing(req, false); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	// Broadcast to gossip
	if a.sentinel != nil {
		encodedSSZ, err := req.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}
		if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
			Data: encodedSSZ,
			Name: gossip.TopicNameAttesterSlashing,
		}); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish attester slashing to gossip", "err", err)
		}
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV1BeaconPoolProposerSlashings(w http.ResponseWriter, r *http.Request) {
	req := cltypes.ProposerSlashing{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	if err := a.proposerSlashingService.ProcessMessage(r.Context(), nil, &req); err != nil && !errors.Is(err, services.ErrIgnore) {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	// Broadcast to gossip
	if a.sentinel != nil {
		encodedSSZ, err := req.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}
		if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
			Data: encodedSSZ,
			Name: gossip.TopicNameProposerSlashing,
		}); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish proposer slashing to gossip", "err", err)
		}
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

type poolingFailure struct {
	Index   int    `json:"index"`
	Message string `json:"message"`
}

type poolingError struct {
	Code     int              `json:"code"`
	Message  string           `json:"message"`
	Failures []poolingFailure `json:"failures,omitempty"`
}

func (a *ApiHandler) PostEthV1BeaconPoolBlsToExecutionChanges(w http.ResponseWriter, r *http.Request) {
	req := []*cltypes.SignedBLSToExecutionChange{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	failures := []poolingFailure{}
	for _, v := range req {
		encodedSSZ, err := v.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}

		if err := a.blsToExecutionChangeService.ProcessMessage(r.Context(), nil, &services.SignedBLSToExecutionChangeForGossip{
			SignedBLSToExecutionChange: v,
		}); err != nil && !errors.Is(err, services.ErrIgnore) {
			failures = append(failures, poolingFailure{Index: len(failures), Message: err.Error()})
			continue
		}
		if a.sentinel != nil {
			if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
				Data: encodedSSZ,
				Name: gossip.TopicNameBlsToExecutionChange,
			}); err != nil {
				a.logger.Debug("[Beacon REST] failed to publish bls-to-execution-change to gossip", "err", err)
			}
		}
	}

	if len(failures) > 0 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(poolingError{Code: http.StatusBadRequest, Message: "some failures", Failures: failures})
		return
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

func (a *ApiHandler) PostEthV1ValidatorAggregatesAndProof(w http.ResponseWriter, r *http.Request) {
	req := []*cltypes.SignedAggregateAndProof{}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	failures := []poolingFailure{}
	for _, v := range req {
		encodedSSZ, err := v.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			log.Warn("[Beacon REST] failed to encode aggregate and proof", "err", err)
			return
		}

		// for this service we are not publishing gossipData as the service does it internally, we just pass that data as a parameter.
		if err := a.aggregateAndProofsService.ProcessMessage(r.Context(), nil, &services.SignedAggregateAndProofForGossip{
			SignedAggregateAndProof: v,
			ImmediateProcess:        true, // we want to process aggregate and proof immediately
		}); err != nil && !errors.Is(err, services.ErrIgnore) {
			log.Warn("[Beacon REST] failed to process bls-change", "err", err)
			failures = append(failures, poolingFailure{Index: len(failures), Message: err.Error()})
			continue
		}

		if a.sentinel != nil {
			if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
				Data: encodedSSZ,
				Name: gossip.TopicNameBeaconAggregateAndProof,
			}); err != nil {
				a.logger.Debug("[Beacon REST] failed to publish aggregate and proof to gossip", "err", err)
			}
		}
	}

	if len(failures) > 0 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(poolingError{Code: http.StatusBadRequest, Message: "some failures", Failures: failures})
		return
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

// PostEthV1BeaconPoolSyncCommittees is a handler for POST /eth/v1/beacon/pool/sync_committees.
// it receives a list of sync committee messages and adds them to the sync committee pool.
func (a *ApiHandler) PostEthV1BeaconPoolSyncCommittees(w http.ResponseWriter, r *http.Request) {
	msgs := []*cltypes.SyncCommitteeMessage{}
	if err := json.NewDecoder(r.Body).Decode(&msgs); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	var err error

	failures := []poolingFailure{}
	for idx, v := range msgs {
		var publishingSubnets []uint64
		if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
			publishingSubnets, err = subnets.ComputeSubnetsForSyncCommittee(headState, v.ValidatorIndex)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			failures = append(failures, poolingFailure{Index: idx, Message: err.Error()})
			continue
		}

		for _, subnet := range publishingSubnets {

			var syncCommitteeMessageWithGossipData services.SyncCommitteeMessageForGossip
			syncCommitteeMessageWithGossipData.SyncCommitteeMessage = v
			syncCommitteeMessageWithGossipData.ImmediateVerification = true

			encodedSSZ, err := syncCommitteeMessageWithGossipData.SyncCommitteeMessage.EncodeSSZ(nil)
			if err != nil {
				beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
				return
			}

			subnetId := subnet

			if err = a.syncCommitteeMessagesService.ProcessMessage(r.Context(), &subnet, &syncCommitteeMessageWithGossipData); err != nil && !errors.Is(err, services.ErrIgnore) {
				log.Warn("[Beacon REST] failed to process attestation in syncCommittee service", "err", err)
				failures = append(failures, poolingFailure{Index: idx, Message: err.Error()})
				break
			}
			if a.sentinel != nil {
				if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
					Data:     encodedSSZ,
					Name:     gossip.TopicNamePrefixSyncCommittee,
					SubnetId: &subnetId,
				}); err != nil {
					a.logger.Debug("[Beacon REST] failed to publish sync committee message to gossip", "err", err)
				}
			}
		}
	}
	if len(failures) > 0 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(poolingError{Code: http.StatusBadRequest, Message: "some failures", Failures: failures})
		return
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}

// PostEthV1ValidatorContributionsAndProofs is a handler for POST /eth/v1/validator/contributions_and_proofs.
// it receives a list of signed contributions and proofs and adds them to the sync committee pool.
func (a *ApiHandler) PostEthV1ValidatorContributionsAndProofs(w http.ResponseWriter, r *http.Request) {
	msgs := []*cltypes.SignedContributionAndProof{}
	if err := json.NewDecoder(r.Body).Decode(&msgs); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	failures := []poolingFailure{}
	for idx, v := range msgs {
		if bytes.Equal(v.Message.Contribution.AggregationBits, make([]byte, len(v.Message.Contribution.AggregationBits))) {
			continue // skip empty contributions
		}

		var signedContributionAndProofWithGossipData services.SignedContributionAndProofForGossip
		signedContributionAndProofWithGossipData.SignedContributionAndProof = v
		signedContributionAndProofWithGossipData.ImmediateVerification = true

		encodedSSZ, err := signedContributionAndProofWithGossipData.SignedContributionAndProof.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			log.Warn("[Beacon REST] failed to encode aggregate and proof", "err", err)
			return
		}

		if err = a.syncContributionAndProofsService.ProcessMessage(r.Context(), nil, &signedContributionAndProofWithGossipData); err != nil && !errors.Is(err, services.ErrIgnore) {
			log.Warn("[Beacon REST] failed to process sync contribution", "err", err)
			failures = append(failures, poolingFailure{Index: idx, Message: err.Error()})
			continue
		}
		if a.sentinel != nil {
			if _, err := a.sentinel.PublishGossip(r.Context(), &sentinel.GossipData{
				Data: encodedSSZ,
				Name: gossip.TopicNameSyncCommitteeContributionAndProof,
			}); err != nil {
				a.logger.Debug("[Beacon REST] failed to publish sync committee contribution to gossip", "err", err)
			}
		}
	}

	if len(failures) > 0 {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(poolingError{Code: http.StatusBadRequest, Message: "some failures", Failures: failures})
		return
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}
