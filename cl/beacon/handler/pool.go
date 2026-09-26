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
	"math"
	"net/http"
	"time"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	networkgossip "github.com/erigontech/erigon/cl/phase1/network/gossip"
	"github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/phase1/network/subnets"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common/log/v3"
)

// syncCommitteeMessageExpiry returns the latest wall-clock time a
// sync-committee message for the given slot is still worth publishing: the
// slot's end, plus the protocol's maximum gossip clock disparity allowance.
// Mirrors the exact inclusive boundary the consensus spec's gossip
// validation uses (reject only once now exceeds this instant), which the
// coarser, whole-slot-rounding IsSlotCurrentSlotWithMaximumClockDisparity
// does not preserve.
func syncCommitteeMessageExpiry(clock eth_clock.EthereumClock, cfg *clparams.NetworkConfig, slot uint64) time.Time {
	nextSlot := slot
	if slot != math.MaxUint64 {
		nextSlot = slot + 1
	}
	return clock.GetSlotTime(nextSlot).Add(time.Duration(cfg.MaximumGossipClockDisparity))
}

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
		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameBeaconAttestation(subnet), encodedSSZ); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish attestation to gossip", "err", err)
			failures = append(failures, poolingFailure{
				Index:   i,
				Message: err.Error(),
			})
			continue
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
	log.Debug("[Beacon REST] posting attestations")
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

		if err := a.attestationService.ProcessMessage(r.Context(), &subnet, attestationWithGossipData); errors.Is(err, services.ErrIgnore) {
			log.Debug("[Beacon REST] ignored attestation in attestation service", "err", err, "slot", slot, "committeeIndex", cIndex)
		} else if err != nil {
			log.Warn("[Beacon REST] failed to process attestation in attestation service", "err", err)
			failures = append(failures, poolingFailure{
				Index:   i,
				Message: err.Error(),
			})
			continue
		}
		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameBeaconAttestation(subnet), encodedSSZ); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish attestation to gossip", "err", err)
			failures = append(failures, poolingFailure{
				Index:   i,
				Message: err.Error(),
			})
			continue
		}
		log.Debug("[Beacon REST] published attestation to gossip", "slot", slot, "committeeIndex", cIndex)
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
	if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameVoluntaryExit, encodedSSZ); err != nil {
		a.logger.Debug("[Beacon REST] failed to publish voluntary exit to gossip", "err", err)
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
	if err := a.forkchoiceStore.OnAttesterSlashing(req, false); err != nil && !errors.Is(err, forkchoice.ErrIgnore) {
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
		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameAttesterSlashing, encodedSSZ); err != nil {
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
		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameProposerSlashing, encodedSSZ); err != nil {
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

// writePoolingFailures reports partial failures as 400 with the failure list.
// logger is nil on handlers built by struct literal, which the package tests do.
func (a *ApiHandler) writePoolingFailures(w http.ResponseWriter, failures []poolingFailure) {
	w.WriteHeader(http.StatusBadRequest)
	if err := json.NewEncoder(w).Encode(poolingError{Code: http.StatusBadRequest, Message: "some failures", Failures: failures}); err != nil && a.logger != nil {
		a.logger.Debug("[Beacon REST] failed to encode pooling error", "err", err)
	}
}

func (a *ApiHandler) PostEthV1BeaconPoolBlsToExecutionChanges(w http.ResponseWriter, r *http.Request) {
	req := []*cltypes.SignedBLSToExecutionChange{}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}
	failures := []poolingFailure{}
	for idx, v := range req {
		encodedSSZ, err := v.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}

		if err := a.blsToExecutionChangeService.ProcessMessage(r.Context(), nil, &services.SignedBLSToExecutionChangeForGossip{
			SignedBLSToExecutionChange: v,
		}); err != nil && !errors.Is(err, services.ErrIgnore) {
			failures = append(failures, poolingFailure{Index: idx, Message: err.Error()})
			continue
		}

		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameBlsToExecutionChange, encodedSSZ); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish bls-to-execution-change to gossip", "err", err)
		}
	}

	if len(failures) > 0 {
		a.writePoolingFailures(w, failures)
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
	for idx, v := range req {
		if v == nil || v.Message == nil || v.Message.Aggregate == nil || v.Message.Aggregate.Data == nil || v.Message.Aggregate.AggregationBits == nil {
			failures = append(failures, poolingFailure{Index: idx, Message: "invalid aggregate and proof"})
			continue
		}
		epoch := v.Message.Aggregate.Data.Slot / a.beaconChainCfg.SlotsPerEpoch
		version := a.beaconChainCfg.GetCurrentStateVersion(epoch)
		if version >= clparams.ElectraVersion && v.Message.Aggregate.CommitteeBits == nil {
			failures = append(failures, poolingFailure{Index: idx, Message: "invalid aggregate and proof: missing committee bits"})
			continue
		}
		v.SetVersion(version)
		if err := v.Message.Aggregate.ValidateForConfig(a.beaconChainCfg, version); err != nil {
			failures = append(failures, poolingFailure{Index: idx, Message: err.Error()})
			continue
		}
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
		}); errors.Is(err, services.ErrIgnore) {
			log.Debug("[Beacon REST] aggregate ignored", "err", err, "slot", v.Message.Aggregate.Data.Slot)
		} else if err != nil {
			log.Warn("[Beacon REST] failed to process aggregate", "err", err)
			failures = append(failures, poolingFailure{Index: idx, Message: err.Error()})
			continue
		}
		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameBeaconAggregateAndProof, encodedSSZ); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish aggregate and proof to gossip", "err", err)
		}
	}

	if len(failures) > 0 {
		a.writePoolingFailures(w, failures)
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
	var admissionErr error
	for idx, v := range msgs {
		var publishingSubnets []uint64
		if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
			publishingSubnets, err = subnets.ComputeSubnetsForSyncCommittee(headState, v.ValidatorIndex)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			if errors.Is(err, synced_data.ErrNotSynced) {
				beaconhttp.WrapEndpointError(err).WriteTo(w)
				return
			}
			failures = append(failures, poolingFailure{Index: idx, Message: err.Error()})
			continue
		}

		expiry := syncCommitteeMessageExpiry(a.ethClock, a.netConfig, v.Slot)

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
			// Published in the background so the gossip validation/publish
			// pipeline's latency isn't added to this request's response time.
			// A non-nil return means the message was never admitted to the
			// queue - a known failure, not an unknowable later network one -
			// so it is surfaced below rather than swallowed behind a 200.
			// ErrPublishJobExpired is excluded: a message whose window has
			// already closed (e.g. an ordinary stale slot, which
			// ProcessMessage above already exempted from failures via
			// ErrIgnore) is expected, not a server-side fault.
			if pubErr := a.gossipManager.PublishBackground(
				gossip.TopicNameSyncCommittee(int(subnetId)), encodedSSZ, expiry,
				"validatorIndex", v.ValidatorIndex, "subnet", subnetId, "slot", v.Slot,
			); pubErr != nil && !errors.Is(pubErr, networkgossip.ErrPublishJobExpired) && admissionErr == nil {
				admissionErr = pubErr
			}
		}
	}
	if len(failures) > 0 {
		// Validation failures take precedence over admission failures in the
		// response: the indexed 400 detail is more actionable, and admission
		// failures are still logged and counted regardless of which response
		// is written.
		a.writePoolingFailures(w, failures)
		return
	}
	if admissionErr != nil {
		beaconhttp.NewEndpointError(http.StatusInternalServerError, admissionErr).WriteTo(w)
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

		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameSyncCommitteeContributionAndProof, encodedSSZ); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish sync committee contribution to gossip", "err", err)
		}
	}

	if len(failures) > 0 {
		a.writePoolingFailures(w, failures)
		return
	}
	// Only write 200
	w.WriteHeader(http.StatusOK)
}
