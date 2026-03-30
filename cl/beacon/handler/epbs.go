// Copyright 2026 The Erigon Authors
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
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/common"
)

// ---- PTC Duties ----

// ptcDutyResponse represents a single PTC duty assignment.
type ptcDutyResponse struct {
	Pubkey         common.Bytes48 `json:"pubkey"`
	ValidatorIndex uint64         `json:"validator_index,string"`
	Slot           uint64         `json:"slot,string"`
}

// PostEthV1ValidatorDutiesPtc returns PTC duties for the given epoch.
// POST /eth/v1/validator/duties/ptc/{epoch}
// [New in Gloas:EIP7732]
func (a *ApiHandler) PostEthV1ValidatorDutiesPtc(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	epoch, err := beaconhttp.EpochFromRequest(r)
	if err != nil {
		return nil, err
	}

	// PTC duties only available from GLOAS fork onwards
	if epoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("PTC duties not available before GLOAS fork (epoch %d)", a.beaconChainCfg.GloasForkEpoch))
	}

	// Parse request body for validator indices
	var validatorIndices []uint64
	if err := json.NewDecoder(r.Body).Decode(&validatorIndices); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("invalid request body: %w", err))
	}

	// PTC duties restricted to current epoch only (consensus-specs PR #586)
	var duties []ptcDutyResponse
	if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		currentEpoch := state.Epoch(s)
		if epoch != currentEpoch {
			return beaconhttp.NewEndpointError(http.StatusBadRequest,
				fmt.Errorf("PTC duties only available for current epoch %d, requested %d", currentEpoch, epoch))
		}

		// Build a lookup set for requested validators
		requestedSet := make(map[uint64]struct{}, len(validatorIndices))
		for _, idx := range validatorIndices {
			requestedSet[idx] = struct{}{}
		}

		// Get PTC for each slot in the epoch
		startSlot := epoch * a.beaconChainCfg.SlotsPerEpoch
		endSlot := startSlot + a.beaconChainCfg.SlotsPerEpoch
		for slot := startSlot; slot < endSlot; slot++ {
			ptc, err := s.GetPTC(slot)
			if err != nil {
				return err
			}
			for _, validatorIndex := range ptc {
				if _, ok := requestedSet[validatorIndex]; !ok {
					continue
				}
				pk, err := s.ValidatorPublicKey(int(validatorIndex))
				if err != nil {
					return err
				}
				duties = append(duties, ptcDutyResponse{
					Pubkey:         pk,
					ValidatorIndex: validatorIndex,
					Slot:           slot,
				})
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return newBeaconResponse(duties), nil
}

// ---- Payload Attestation Data ----

// payloadAttestationDataResponse matches the PayloadAttestationData spec type.
type payloadAttestationDataResponse struct {
	BeaconBlockRoot   common.Hash `json:"beacon_block_root"`
	Slot              uint64      `json:"slot,string"`
	PayloadPresent    bool        `json:"payload_present"`
	BlobDataAvailable bool        `json:"blob_data_available"`
}

// GetEthV1ValidatorPayloadAttestationData returns PayloadAttestationData for PTC validators.
// GET /eth/v1/validator/payload_attestation_data/{slot}
// [New in Gloas:EIP7732]
func (a *ApiHandler) GetEthV1ValidatorPayloadAttestationData(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slotStr, err := beaconhttp.StringFromRequest(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	slot, err := strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("invalid slot: %w", err))
	}

	// Must be GLOAS epoch
	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	if epoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("payload attestation data not available before GLOAS fork"))
	}

	// Get the beacon block root for this slot from fork choice
	headRoot, headSlot, _, err := a.getHead()
	if err != nil {
		return nil, err
	}

	// The PTC attests to the current slot's block
	if slot != headSlot {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound,
			fmt.Errorf("payload attestation data only available for head slot %d, requested %d", headSlot, slot))
	}

	// Check payload status: has the execution payload envelope been received?
	payloadPresent := a.forkchoiceStore.HasEnvelope(headRoot)

	// TODO: check blob data availability separately when data availability tracking is available
	blobDataAvailable := payloadPresent

	return newBeaconResponse(payloadAttestationDataResponse{
		BeaconBlockRoot:   headRoot,
		Slot:              slot,
		PayloadPresent:    payloadPresent,
		BlobDataAvailable: blobDataAvailable,
	}), nil
}

// ---- Payload Attestation Pool ----

// GetEthV1BeaconPoolPayloadAttestations returns payload attestations from the pool.
// GET /eth/v1/beacon/pool/payload_attestations
// [New in Gloas:EIP7732]
func (a *ApiHandler) GetEthV1BeaconPoolPayloadAttestations(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	if a.epbsPool == nil {
		return newBeaconResponse([]any{}), nil
	}

	var results []*cltypes.PayloadAttestationMessage
	for _, key := range a.epbsPool.PayloadAttestations.Keys() {
		if slot != nil && key.Slot != *slot {
			continue
		}
		msg, ok := a.epbsPool.PayloadAttestations.Get(key)
		if !ok || msg == nil {
			continue
		}
		results = append(results, msg)
	}
	if results == nil {
		results = make([]*cltypes.PayloadAttestationMessage, 0)
	}
	return newBeaconResponse(results), nil
}

// PostEthV1BeaconPoolPayloadAttestations submits a PayloadAttestationMessage.
// POST /eth/v1/beacon/pool/payload_attestations
// [New in Gloas:EIP7732]
func (a *ApiHandler) PostEthV1BeaconPoolPayloadAttestations(w http.ResponseWriter, r *http.Request) {
	req := &cltypes.PayloadAttestationMessage{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	// Validate via PayloadAttestationService (handles dedup, clock disparity, pending queue,
	// and delegates to forkchoice.OnPayloadAttestationMessage for signature + PTC checks)
	if a.payloadAttestationService != nil {
		if err := a.payloadAttestationService.ProcessMessage(r.Context(), nil, req); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
	}

	// Store in pool for GET endpoint serving
	if a.epbsPool != nil && req.Data != nil {
		a.epbsPool.PayloadAttestations.Add(pool.PayloadAttestationKey{
			Slot:           req.Data.Slot,
			ValidatorIndex: req.ValidatorIndex,
		}, req)
	}

	// Broadcast to gossip
	if a.sentinel != nil {
		encodedSSZ, err := req.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}
		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNamePayloadAttestation, encodedSSZ); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish payload attestation to gossip", "err", err)
		}
	}
	w.WriteHeader(http.StatusOK)
}

// ---- Execution Payload Envelope ----

// GetEthV1BeaconExecutionPayloadEnvelope returns the SignedExecutionPayloadEnvelope for a block.
// GET /eth/v1/beacon/execution_payload_envelope/{block_id}
// [New in Gloas:EIP7732]
func (a *ApiHandler) GetEthV1BeaconExecutionPayloadEnvelope(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	blockId, err := beaconhttp.BlockIdFromRequest(r)
	if err != nil {
		return nil, err
	}

	blockRoot, err := a.blockRootFromBlockId(blockId)
	if err != nil {
		return nil, err
	}

	// Check if the envelope exists
	if !a.forkchoiceStore.HasEnvelope(blockRoot) {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound,
			fmt.Errorf("execution payload envelope not found for block %v", blockRoot))
	}

	envelope, err := a.forkchoiceStore.ReadEnvelopeFromDisk(blockRoot)
	if err != nil {
		return nil, err
	}
	if envelope == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound,
			fmt.Errorf("execution payload envelope not found for block %v", blockRoot))
	}

	epoch := envelope.Message.Slot / a.beaconChainCfg.SlotsPerEpoch
	return newBeaconResponse(envelope).WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)), nil
}

// ---- Execution Payload Bid ----

// PostEthV1BeaconExecutionPayloadBid publishes a SignedExecutionPayloadBid.
// POST /eth/v1/beacon/execution_payload_bid
// [New in Gloas:EIP7732]
func (a *ApiHandler) PostEthV1BeaconExecutionPayloadBid(w http.ResponseWriter, r *http.Request) {
	req := &cltypes.SignedExecutionPayloadBid{}
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	// Validate via the bid service (checks signature, slot timing, proposer preferences, etc.)
	if a.executionPayloadBidService != nil {
		if err := a.executionPayloadBidService.ProcessMessage(r.Context(), nil, req); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
	}

	// Broadcast to gossip
	if a.sentinel != nil {
		encodedSSZ, err := req.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}
		if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameExecutionPayloadBid, encodedSSZ); err != nil {
			a.logger.Debug("[Beacon REST] failed to publish execution payload bid to gossip", "err", err)
		}
	}
	w.WriteHeader(http.StatusOK)
}

// GetEthV1ValidatorExecutionPayloadBid returns the highest bid for a given slot and builder index.
// GET /eth/v1/validator/execution_payload_bid/{slot}/{builder_index}
// [New in Gloas:EIP7732]
func (a *ApiHandler) GetEthV1ValidatorExecutionPayloadBid(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slotStr, err := beaconhttp.StringFromRequest(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	slot, err := strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("invalid slot: %w", err))
	}
	builderIndexStr, err := beaconhttp.StringFromRequest(r, "builder_index")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	builderIndex, err := strconv.ParseUint(builderIndexStr, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("invalid builder_index: %w", err))
	}

	// Must be GLOAS epoch
	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	if epoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("execution payload bids not available before GLOAS fork"))
	}

	if a.epbsPool == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable,
			fmt.Errorf("EPBS pool not available"))
	}

	// Scan the highest bids cache for a matching (slot, builder_index).
	// The cache is keyed by (slot, parentBlockHash, parentBlockRoot), so we iterate all keys
	// and find the highest-value bid matching the requested slot+builder.
	var bestBid *cltypes.SignedExecutionPayloadBid
	for _, key := range a.epbsPool.HighestBids.Keys() {
		if key.Slot != slot {
			continue
		}
		bid, ok := a.epbsPool.HighestBids.Get(key)
		if !ok || bid == nil || bid.Message == nil {
			continue
		}
		if bid.Message.BuilderIndex != builderIndex {
			continue
		}
		if bestBid == nil || bid.Message.Value > bestBid.Message.Value {
			bestBid = bid
		}
	}

	if bestBid == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound,
			fmt.Errorf("no bid found for slot %d builder %d", slot, builderIndex))
	}

	return newBeaconResponse(bestBid).WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)), nil
}

// ---- Helpers ----

// blockRootFromBlockId resolves a block_id to a block root hash.
func (a *ApiHandler) blockRootFromBlockId(blockId *beaconhttp.SegmentID) (common.Hash, error) {
	switch {
	case blockId.Head():
		root, _, _, err := a.getHead()
		return root, err
	case blockId.Finalized():
		// Get finalized root from fork choice
		var root common.Hash
		err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			root = s.FinalizedCheckpoint().Root
			return nil
		})
		return root, err
	case blockId.Justified():
		var root common.Hash
		err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
			root = s.CurrentJustifiedCheckpoint().Root
			return nil
		})
		return root, err
	case blockId.Genesis():
		return common.Hash{}, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("genesis block has no execution payload envelope"))
	default:
		root := blockId.GetRoot()
		if root == nil {
			slot := blockId.GetSlot()
			if slot == nil {
				return common.Hash{}, beaconhttp.NewEndpointError(http.StatusBadRequest,
					fmt.Errorf("invalid block_id"))
			}
			// Slot-based lookup: get block root at slot from state
			var blockRoot common.Hash
			err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
				var err error
				blockRoot, err = s.GetBlockRootAtSlot(*slot)
				return err
			})
			return blockRoot, err
		}
		return *root, nil
	}
}
