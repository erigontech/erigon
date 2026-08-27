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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"slices"
	"strconv"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/beacon/beaconhttp"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	peerdasutils "github.com/erigontech/erigon/cl/das/utils"
	"github.com/erigontech/erigon/cl/gossip"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	clservices "github.com/erigontech/erigon/cl/phase1/network/services"
	"github.com/erigontech/erigon/cl/pool"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	execparams "github.com/erigontech/erigon/execution/protocol/params"
)

const (
	maxProposerPreferencesRequestItems     = 2048
	maxEpbsJSONSize                        = 1 << 20
	maxExecutionPayloadEnvelopeRequestSize = int64(execparams.MaxRlpBlockSize) * 4
)

func maxSignedExecutionPayloadBidSSZSize() int64 {
	emptyBidSize := (&cltypes.SignedExecutionPayloadBid{Message: &cltypes.ExecutionPayloadBid{}}).EncodingSizeSSZ()
	return int64(emptyBidSize + cltypes.MaxBlobsCommittmentsPerBlock*length.Bytes48)
}

func maxPayloadAttestationMessagesSSZSize(cfg *clparams.BeaconChainConfig) int64 {
	msgSize := (&cltypes.PayloadAttestationMessage{Data: new(cltypes.PayloadAttestationData)}).EncodingSizeSSZ()
	return int64(cfg.PtcSize) * int64(msgSize)
}

func requestContentType(r *http.Request) (string, error) {
	contentTypeHeader := r.Header.Get("Content-Type")
	if contentTypeHeader == "" {
		return "application/json", nil
	}
	contentType, _, err := mime.ParseMediaType(contentTypeHeader)
	if err != nil {
		return "", fmt.Errorf("unsupported content type: %s", contentTypeHeader)
	}
	if contentType == "" {
		return "application/json", nil
	}
	return contentType, nil
}

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

	if epochSlotOverflows(epoch, a.beaconChainCfg.SlotsPerEpoch) {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("epoch %d overflows slot computation", epoch))
	}

	// PTC duties only available from GLOAS fork onwards
	if epoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("PTC duties not available before GLOAS fork (epoch %d)", a.beaconChainCfg.GloasForkEpoch))
	}

	// Parse request body for validator indices (string-encoded per Beacon API spec)
	var idxsStr []string
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEpbsJSONSize)).Decode(&idxsStr); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("invalid request body: %w", err))
	}
	validatorIndices := make([]uint64, 0, len(idxsStr))
	for _, s := range idxsStr {
		idx, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
				fmt.Errorf("invalid validator index %q: %w", s, err))
		}
		validatorIndices = append(validatorIndices, idx)
	}

	// PTC duties available for current and next epoch (beacon-APIs PR #592)
	duties := make([]ptcDutyResponse, 0)
	if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		currentEpoch := state.Epoch(s)
		if epoch < currentEpoch || epoch > currentEpoch+1 {
			return beaconhttp.NewEndpointError(http.StatusBadRequest,
				fmt.Errorf("PTC duties only available for current epoch %d and next epoch %d, requested %d", currentEpoch, currentEpoch+1, epoch))
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

	// PTC duties use the same dependent_root as proposer duties (start of epoch shuffling)
	dependentRoot, err := a.getDependentRoot(epoch, false)
	if err != nil {
		return nil, err
	}

	return newBeaconResponse(duties).
		WithOptimistic(a.forkchoiceStore.IsHeadOptimistic()).
		With("dependent_root", dependentRoot), nil
}

// ---- Payload Attestation Data ----

// GetEthV1ValidatorPayloadAttestationData returns PayloadAttestationData for PTC validators.
// [New in Gloas:EIP7732]
func (a *ApiHandler) GetEthV1ValidatorPayloadAttestationData(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	if a.syncedData.Syncing() {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("beacon node is syncing"))
	}
	slotStr, err := beaconhttp.StringFromRequest(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	if slotStr == "" {
		slotValues, ok := r.URL.Query()["slot"]
		if !ok || len(slotValues) != 1 || slotValues[0] == "" {
			return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("slot query parameter is required exactly once"))
		}
		slotStr = slotValues[0]
	}
	slot, err := strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("invalid slot: %w", err))
	}
	// Must be GLOAS epoch
	if a.beaconChainCfg.SlotsPerEpoch == 0 {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("slots per epoch is zero"))
	}
	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	if epoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("payload attestation data not available before GLOAS fork"))
	}

	// Get the beacon block root for this slot from fork choice
	headRoot, headSlot, statusCode, err := a.getSelectedHead()
	if err != nil {
		return nil, beaconhttp.NewEndpointError(statusCode, err)
	}

	// The PTC attests to the current slot's block
	if slot != headSlot {
		return beaconhttp.NewNoContentResponse(), nil
	}

	// Check payload status: has the execution payload envelope been received?
	payloadPresent := a.forkchoiceStore.HasEnvelope(headRoot)

	// Check blob data availability independently via PeerDAS.
	// blob_data_available is true when the envelope exists AND either:
	// (a) the block has no blob commitments (trivially available), or
	// (b) all local custody columns are present per PeerDAS.
	blobDataAvailable := a.forkchoiceStore.IsBlobDataAvailable(slot, headRoot)

	return newBeaconResponse(&cltypes.PayloadAttestationData{
		BeaconBlockRoot:   headRoot,
		Slot:              slot,
		PayloadPresent:    payloadPresent,
		BlobDataAvailable: blobDataAvailable,
	}).WithVersion(clparams.GloasVersion), nil
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

	results := solid.NewStaticListSSZ[*cltypes.PayloadAttestation](
		int(a.beaconChainCfg.MaxPayloadAttestations),
		cltypes.PayloadAttestationSSZSizeWithPtcSize(a.beaconChainCfg.PtcSize),
	)

	if a.epbsPool == nil {
		return newBeaconResponse(results).WithVersion(clparams.GloasVersion), nil
	}

	var messages []*cltypes.PayloadAttestationMessage
	for _, key := range a.epbsPool.PayloadAttestations.Keys() {
		if slot != nil && key.Slot != *slot {
			continue
		}
		msg, ok := a.epbsPool.PayloadAttestations.Get(key)
		if !ok || msg == nil {
			continue
		}
		messages = append(messages, msg)
	}
	if len(messages) == 0 {
		return newBeaconResponse(results).WithVersion(clparams.GloasVersion), nil
	}

	ptcProvider := payloadAttestationPTCMap{}
	if err := a.syncedData.ViewHeadState(func(s *state.CachingBeaconState) error {
		ptcProvider = snapshotPayloadAttestationPTCs(s, messages)
		return nil
	}); err != nil {
		a.logger.Debug("[Beacon REST] failed to aggregate payload attestations", "err", err)
		return newBeaconResponse(results).WithVersion(clparams.GloasVersion), nil
	}
	aggregated, err := aggregatePayloadAttestationMessages(a.beaconChainCfg, ptcProvider, messages)
	if err != nil {
		a.logger.Debug("[Beacon REST] failed to aggregate payload attestations", "err", err)
		return newBeaconResponse(results).WithVersion(clparams.GloasVersion), nil
	}
	results = aggregated

	return newBeaconResponse(results).WithVersion(clparams.GloasVersion), nil
}

func snapshotPayloadAttestationPTCs(
	ptcProvider payloadAttestationPTCProvider,
	messages []*cltypes.PayloadAttestationMessage,
) payloadAttestationPTCMap {
	slots := map[uint64]struct{}{}
	for _, msg := range messages {
		if msg == nil || msg.Data == nil {
			continue
		}
		slots[msg.Data.Slot] = struct{}{}
	}
	out := make(payloadAttestationPTCMap, len(slots))
	for slot := range slots {
		ptc, err := ptcProvider.GetPTC(slot)
		if err != nil {
			continue
		}
		out[slot] = append([]uint64(nil), ptc...)
	}
	return out
}

func aggregatePayloadAttestationMessages(
	cfg *clparams.BeaconChainConfig,
	ptcProvider payloadAttestationPTCProvider,
	messages []*cltypes.PayloadAttestationMessage,
) (*solid.ListSSZ[*cltypes.PayloadAttestation], error) {
	result := solid.NewStaticListSSZ[*cltypes.PayloadAttestation](
		int(cfg.MaxPayloadAttestations),
		cltypes.PayloadAttestationSSZSizeWithPtcSize(cfg.PtcSize),
	)

	type dataKey struct {
		BeaconBlockRoot   common.Hash
		Slot              uint64
		PayloadPresent    bool
		BlobDataAvailable bool
	}
	type payloadAttestationGroup struct {
		data *cltypes.PayloadAttestationData
		sigs map[int][]byte
	}

	ptcBySlot := make(map[uint64]map[uint64][]int)
	groups := make(map[dataKey]*payloadAttestationGroup)
	for _, msg := range messages {
		if msg == nil || msg.Data == nil {
			continue
		}
		validatorToPTCPositions, ok := ptcBySlot[msg.Data.Slot]
		if !ok {
			ptc, err := ptcProvider.GetPTC(msg.Data.Slot)
			if err != nil {
				ptcBySlot[msg.Data.Slot] = nil
				continue
			}
			validatorToPTCPositions = payloadAttestationPTCPositions(ptc)
			ptcBySlot[msg.Data.Slot] = validatorToPTCPositions
		}
		if validatorToPTCPositions == nil {
			continue
		}
		ptcPositions, ok := validatorToPTCPositions[msg.ValidatorIndex]
		if !ok {
			continue
		}
		key := dataKey{
			BeaconBlockRoot:   msg.Data.BeaconBlockRoot,
			Slot:              msg.Data.Slot,
			PayloadPresent:    msg.Data.PayloadPresent,
			BlobDataAvailable: msg.Data.BlobDataAvailable,
		}
		group, ok := groups[key]
		if !ok {
			group = &payloadAttestationGroup{
				data: msg.Data.Clone().(*cltypes.PayloadAttestationData),
				sigs: make(map[int][]byte),
			}
			groups[key] = group
		}
		for _, ptcIndex := range ptcPositions {
			if _, exists := group.sigs[ptcIndex]; !exists {
				signature := make([]byte, len(msg.Signature))
				copy(signature, msg.Signature[:])
				group.sigs[ptcIndex] = signature
			}
		}
	}

	type candidate struct {
		attestation *cltypes.PayloadAttestation
		weight      int
	}
	candidates := make([]candidate, 0, len(groups))
	for _, group := range groups {
		bits := solid.NewBitVector(int(cfg.PtcSize))
		signatures := make([][]byte, 0, len(group.sigs))
		for ptcIndex, signature := range group.sigs {
			if err := bits.SetBitAt(ptcIndex, true); err != nil {
				return nil, err
			}
			signatures = append(signatures, signature)
		}
		if len(signatures) == 0 {
			continue
		}
		aggregatedSignature, err := bls.AggregateSignatures(signatures)
		if err != nil {
			continue
		}
		var signature common.Bytes96
		copy(signature[:], aggregatedSignature)
		candidates = append(candidates, candidate{
			attestation: &cltypes.PayloadAttestation{
				AggregationBits: bits,
				Data:            group.data,
				Signature:       signature,
			},
			weight: len(group.sigs),
		})
	}

	slices.SortFunc(candidates, func(a, b candidate) int {
		if a.weight != b.weight {
			return cmp.Compare(b.weight, a.weight)
		}
		left := a.attestation.Data
		right := b.attestation.Data
		if left.Slot != right.Slot {
			return cmp.Compare(left.Slot, right.Slot)
		}
		if c := bytes.Compare(left.BeaconBlockRoot[:], right.BeaconBlockRoot[:]); c != 0 {
			return c
		}
		if left.PayloadPresent != right.PayloadPresent {
			if left.PayloadPresent {
				return -1
			}
			return 1
		}
		if left.BlobDataAvailable != right.BlobDataAvailable {
			if left.BlobDataAvailable {
				return -1
			}
			return 1
		}
		return 0
	})
	for i := 0; i < len(candidates) && result.Len() < int(cfg.MaxPayloadAttestations); i++ {
		result.Append(candidates[i].attestation)
	}
	return result, nil
}

type payloadAttestationPTCProvider interface {
	GetPTC(slot uint64) ([]uint64, error)
}

type payloadAttestationPTCMap map[uint64][]uint64

func (m payloadAttestationPTCMap) GetPTC(slot uint64) ([]uint64, error) {
	ptc, ok := m[slot]
	if !ok {
		return nil, fmt.Errorf("ptc unavailable for slot %d", slot)
	}
	return ptc, nil
}

func payloadAttestationPTCPositions(ptc []uint64) map[uint64][]int {
	positions := make(map[uint64][]int, len(ptc))
	for i, validatorIndex := range ptc {
		positions[validatorIndex] = append(positions[validatorIndex], i)
	}
	return positions
}

// PostEthV1BeaconPoolPayloadAttestations submits an array of PayloadAttestationMessages.
// POST /eth/v1/beacon/pool/payload_attestations
// Accepts application/json or application/octet-stream (SSZ).
// [New in Gloas:EIP7732]
func (a *ApiHandler) PostEthV1BeaconPoolPayloadAttestations(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Eth-Consensus-Version") != clparams.GloasVersion.String() {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Gloas Eth-Consensus-Version header is required")).WriteTo(w)
		return
	}
	var req []*cltypes.PayloadAttestationMessage

	contentType, err := requestContentType(r)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, err).WriteTo(w)
		return
	}

	switch contentType {
	case "application/octet-stream":
		octets, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPayloadAttestationMessagesSSZSize(a.beaconChainCfg)))
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		// Each PayloadAttestationMessage is fixed-size SSZ.
		// Lighthouse flat-maps messages into a single byte slice.
		msgSize := (&cltypes.PayloadAttestationMessage{
			Data: new(cltypes.PayloadAttestationData),
		}).EncodingSizeSSZ()
		if len(octets)%msgSize != 0 {
			beaconhttp.NewEndpointError(http.StatusBadRequest,
				fmt.Errorf("SSZ body length %d is not a multiple of PayloadAttestationMessage size %d", len(octets), msgSize)).WriteTo(w)
			return
		}
		count := len(octets) / msgSize
		req = make([]*cltypes.PayloadAttestationMessage, 0, count)
		for i := range count {
			msg := &cltypes.PayloadAttestationMessage{}
			if err := msg.DecodeSSZStrict(octets[i*msgSize:(i+1)*msgSize], int(clparams.GloasVersion)); err != nil {
				beaconhttp.NewEndpointError(http.StatusBadRequest,
					fmt.Errorf("failed to decode SSZ PayloadAttestationMessage at index %d: %w", i, err)).WriteTo(w)
				return
			}
			req = append(req, msg)
		}
	case "application/json":
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEpbsJSONSize))
		if err := decoder.Decode(&req); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("request body contains trailing data")).WriteTo(w)
			return
		}
		if uint64(len(req)) > a.beaconChainCfg.PtcSize {
			beaconhttp.NewEndpointError(http.StatusBadRequest,
				fmt.Errorf("payload attestation count %d exceeds %d", len(req), a.beaconChainCfg.PtcSize)).WriteTo(w)
			return
		}
	default:
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", r.Header.Get("Content-Type"))).WriteTo(w)
		return
	}

	failures := []poolingFailure{}
	for i, msg := range req {
		if msg == nil || msg.Data == nil {
			failures = append(failures, poolingFailure{
				Index:   i,
				Message: "missing payload attestation message data",
			})
			continue
		}

		// Validate via PayloadAttestationService (handles dedup, clock disparity, pending queue,
		// and delegates to forkchoice.OnPayloadAttestationMessage for signature + PTC checks)
		if a.payloadAttestationService == nil {
			failures = append(failures, poolingFailure{Index: i, Message: "payload attestation validation unavailable"})
			continue
		}
		encodedSSZ, err := msg.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}
		publishFailed := false
		if err := a.payloadAttestationService.ProcessRESTMessage(r.Context(), msg, func() error {
			err := a.publishGossip(r.Context(), gossip.TopicNamePayloadAttestation, encodedSSZ)
			publishFailed = err != nil
			return err
		}); err != nil {
			if publishFailed {
				beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
				return
			}
			if errors.Is(err, clservices.ErrAttestationDuplicate) {
				continue
			}
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
			a.logger.Warn("failed to encode response", "err", err)
		}
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ---- Proposer Preferences Pool ----

// GetEthV1BeaconPoolProposerPreferences returns proposer preferences from the pool.
// GET /eth/v1/beacon/pool/proposer_preferences
// [New in Gloas:EIP7732]
func (a *ApiHandler) GetEthV1BeaconPoolProposerPreferences(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slot, err := beaconhttp.Uint64FromQueryParams(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}

	if a.epbsPool == nil {
		return newBeaconResponse([]any{}), nil
	}

	var results []*cltypes.SignedProposerPreferences
	for _, key := range a.epbsPool.ProposerPreferences.Keys() {
		if slot != nil && key.Slot != *slot {
			continue
		}
		msg, ok := a.epbsPool.ProposerPreferences.Get(key)
		if !ok || msg == nil {
			continue
		}
		results = append(results, msg)
	}
	if results == nil {
		results = make([]*cltypes.SignedProposerPreferences, 0)
	}
	return newBeaconResponse(results), nil
}

// PostEthV1BeaconPoolProposerPreferences submits a SignedProposerPreferences message.
// POST /eth/v1/beacon/pool/proposer_preferences
// Accepts application/json or application/octet-stream (SSZ).
// [New in Gloas:EIP7732]
func (a *ApiHandler) PostEthV1BeaconPoolProposerPreferences(w http.ResponseWriter, r *http.Request) {
	reqs, ok := decodeProposerPreferencesRequest(w, r, false)
	if !ok {
		return
	}
	a.postProposerPreferences(w, r, reqs)
}

// PostEthV1ValidatorProposerPreferences submits proposer preferences for validators.
// POST /eth/v1/validator/proposer_preferences
// [New in Gloas:EIP7732]
func (a *ApiHandler) PostEthV1ValidatorProposerPreferences(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Eth-Consensus-Version") != clparams.GloasVersion.String() {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Gloas Eth-Consensus-Version header is required")).WriteTo(w)
		return
	}
	reqs, ok := decodeProposerPreferencesRequest(w, r, true)
	if !ok {
		return
	}
	a.postProposerPreferences(w, r, reqs)
}

func decodeProposerPreferencesRequest(w http.ResponseWriter, r *http.Request, canonical bool) ([]*cltypes.SignedProposerPreferences, bool) {
	contentType, err := requestContentType(r)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, err).WriteTo(w)
		return nil, false
	}
	msgSize := (&cltypes.SignedProposerPreferences{Message: new(cltypes.ProposerPreferences)}).EncodingSizeSSZ()
	maxBodySize := int64(maxProposerPreferencesRequestItems * msgSize * 4)

	switch contentType {
	case "application/octet-stream":
		octets, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(maxProposerPreferencesRequestItems*msgSize)))
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return nil, false
		}
		if len(octets) == 0 || len(octets)%msgSize != 0 {
			beaconhttp.NewEndpointError(http.StatusBadRequest,
				fmt.Errorf("SSZ body length %d is not a multiple of SignedProposerPreferences size %d", len(octets), msgSize)).WriteTo(w)
			return nil, false
		}
		reqs := make([]*cltypes.SignedProposerPreferences, 0, len(octets)/msgSize)
		for i := 0; i < len(octets)/msgSize; i++ {
			req := &cltypes.SignedProposerPreferences{}
			if err := req.DecodeSSZ(octets[i*msgSize:(i+1)*msgSize], int(clparams.GloasVersion)); err != nil {
				beaconhttp.NewEndpointError(http.StatusBadRequest,
					fmt.Errorf("failed to decode SSZ SignedProposerPreferences at index %d: %w", i, err)).WriteTo(w)
				return nil, false
			}
			reqs = append(reqs, req)
		}
		return reqs, true
	case "application/json":
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodySize))
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return nil, false
		}
		var reqs []*cltypes.SignedProposerPreferences
		if err := json.Unmarshal(body, &reqs); err == nil {
			if len(reqs) > maxProposerPreferencesRequestItems {
				beaconhttp.NewEndpointError(http.StatusBadRequest,
					fmt.Errorf("proposer preferences count %d exceeds %d", len(reqs), maxProposerPreferencesRequestItems)).WriteTo(w)
				return nil, false
			}
			return reqs, true
		}
		if canonical {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("proposer preferences request must be a JSON array")).WriteTo(w)
			return nil, false
		}
		req := &cltypes.SignedProposerPreferences{}
		if err := json.Unmarshal(body, req); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return nil, false
		}
		return []*cltypes.SignedProposerPreferences{req}, true
	default:
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", r.Header.Get("Content-Type"))).WriteTo(w)
		return nil, false
	}
}

func (a *ApiHandler) postProposerPreferences(w http.ResponseWriter, r *http.Request, reqs []*cltypes.SignedProposerPreferences) {
	if len(reqs) == 0 {
		beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("empty proposer preferences request")).WriteTo(w)
		return
	}
	failures := make([]poolingFailure, 0)
	for i, req := range reqs {
		if req == nil || req.Message == nil {
			failures = append(failures, poolingFailure{Index: i, Message: "missing message in signed proposer preferences"})
			continue
		}

		if a.proposerPreferencesService != nil {
			if err := a.proposerPreferencesService.ProcessMessage(r.Context(), nil, req); err != nil {
				failures = append(failures, poolingFailure{Index: i, Message: err.Error()})
				continue
			}
		}

		if a.epbsPool != nil {
			a.epbsPool.ProposerPreferences.Add(pool.ProposerPreferencesKey{
				Slot:          req.Message.ProposalSlot,
				DependentRoot: req.Message.DependentRoot,
			}, req)
		}

		if a.sentinel != nil {
			encodedSSZ, err := req.EncodeSSZ(nil)
			if err != nil {
				beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
				return
			}
			if err := a.gossipManager.Publish(r.Context(), gossip.TopicNameProposerPreferences, encodedSSZ); err != nil {
				a.logger.Debug("[Beacon REST] failed to publish proposer preferences to gossip", "err", err)
			}
		}
	}
	if len(failures) != 0 {
		a.writePoolingFailures(w, failures)
		return
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

	block, ok := a.forkchoiceStore.GetBlock(blockRoot)
	if !ok || block == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound,
			fmt.Errorf("block not found for block root %v", blockRoot))
	}
	if a.beaconChainCfg.SlotsPerEpoch == 0 {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("slots per epoch is zero"))
	}
	slot := block.Block.Slot
	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	finalized := a.forkchoiceStore.FinalizedCheckpoint()
	finalizedSlot := finalized.Epoch * a.beaconChainCfg.SlotsPerEpoch
	isFinalized := slot <= finalizedSlot && a.forkchoiceStore.Ancestor(finalized.Root, slot).Root == blockRoot
	return newBeaconResponse(envelope).
		WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)).
		WithOptimistic(a.forkchoiceStore.IsRootOptimistic(blockRoot)).
		WithFinalized(isFinalized), nil
}

// PostEthV1BeaconExecutionPayloadEnvelope publishes a SignedExecutionPayloadEnvelope.
// POST /eth/v1/beacon/execution_payload_envelope
// Accepts application/json or application/octet-stream (SSZ).
// The envelope is processed through forkchoice and broadcast on gossip.
// [New in Gloas:EIP7732]
func (a *ApiHandler) PostEthV1BeaconExecutionPayloadEnvelope(w http.ResponseWriter, r *http.Request) {
	a.postEthV1BeaconExecutionPayloadEnvelope(w, r, true)
}

func (a *ApiHandler) postEthV1BeaconExecutionPayloadEnvelopeLegacy(w http.ResponseWriter, r *http.Request) {
	a.postEthV1BeaconExecutionPayloadEnvelope(w, r, false)
}

func (a *ApiHandler) postEthV1BeaconExecutionPayloadEnvelope(w http.ResponseWriter, r *http.Request, canonical bool) {
	contentType, err := requestContentType(r)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, err).WriteTo(w)
		return
	}
	blobDataIncluded := false
	validation := BlockPublishingValidationGossip
	if canonical {
		if r.Header.Get("Eth-Consensus-Version") != clparams.GloasVersion.String() {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Gloas Eth-Consensus-Version header is required")).WriteTo(w)
			return
		}
		value := r.Header.Get("Eth-Blob-Data-Included")
		if value == "" {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Eth-Blob-Data-Included header is required")).WriteTo(w)
			return
		}
		blobDataIncluded, err = strconv.ParseBool(value)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid Eth-Blob-Data-Included: %w", err)).WriteTo(w)
			return
		}
		validation, err = a.parseBlockPublishingValidation(r, 2)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
	}
	signedEnvelope, contents, err := a.decodeExecutionPayloadEnvelopeRequest(w, r, contentType, blobDataIncluded)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
		return
	}

	if signedEnvelope.Message == nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("missing message in signed envelope")).WriteTo(w)
		return
	}
	if validation == BlockPublishingValidationConsensusAndEquivocation {
		block, ok := a.forkchoiceStore.GetBlock(signedEnvelope.Message.BeaconBlockRoot)
		if !ok || block == nil || block.Block == nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("execution payload envelope block is unavailable")).WriteTo(w)
			return
		}
		if a.forkchoiceStore.HasBlockEquivocation(block.Block.Slot, block.Block.ProposerIndex, signedEnvelope.Message.BeaconBlockRoot) {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("execution payload envelope block has an equivocation")).WriteTo(w)
			return
		}
	}
	if validation == BlockPublishingValidationGossip {
		if err := a.forkchoiceStore.ValidateExecutionPayloadEnvelope(signedEnvelope); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
	}
	contentsIntegrationFailed := false
	if contents != nil {
		if err := a.validateAndStoreExecutionPayloadEnvelopeContents(r.Context(), contents); err != nil {
			if !errors.Is(err, errExecutionPayloadEnvelopeIntegration) {
				beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
				return
			}
			contentsIntegrationFailed = true
		}
	}

	status := http.StatusOK
	if contentsIntegrationFailed {
		status = http.StatusAccepted
	}
	gossipValidated := false
	emitGossipEvent := false
	emitIntegrationEvents := false
	if err := a.forkchoiceStore.OnExecutionPayload(r.Context(), signedEnvelope, canonical, true); err != nil {
		if canonical && !blobDataIncluded && errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		if canonical && validation != BlockPublishingValidationGossip {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		switch {
		case errors.Is(err, forkchoice.ErrIgnore):
			persisted, _ := a.forkchoiceStore.ReadEnvelopeFromDisk(signedEnvelope.Message.BeaconBlockRoot)
			if !signedExecutionPayloadEnvelopesEqual(persisted, signedEnvelope) {
				if canonical {
					beaconhttp.NewEndpointError(http.StatusServiceUnavailable, err).WriteTo(w)
					return
				}
				status = http.StatusAccepted
				break
			}
			a.logger.Debug("[Beacon REST] OnExecutionPayload queued or ignored", "err", err)
			if !contentsIntegrationFailed {
				status = http.StatusOK
			}
			gossipValidated = true
		case errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable):
			a.logger.Debug("[Beacon REST] OnExecutionPayload queued or ignored", "err", err)
			status = http.StatusAccepted
			gossipValidated = true
			emitGossipEvent = true
		case canonical && validation == BlockPublishingValidationGossip:
			status = http.StatusAccepted
			gossipValidated = true
			emitGossipEvent = true
		case canonical:
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		default:
			beaconhttp.WrapEndpointError(err).WriteTo(w)
			return
		}
	} else {
		gossipValidated = true
		emitGossipEvent = true
		emitIntegrationEvents = true
	}
	if gossipValidated && (canonical || a.sentinel != nil) && validation == BlockPublishingValidationConsensusAndEquivocation {
		block, ok := a.forkchoiceStore.GetBlock(signedEnvelope.Message.BeaconBlockRoot)
		if !ok || block == nil || block.Block == nil || a.forkchoiceStore.HasBlockEquivocation(block.Block.Slot, block.Block.ProposerIndex, signedEnvelope.Message.BeaconBlockRoot) {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("execution payload envelope block has an equivocation")).WriteTo(w)
			return
		}
	}
	if emitGossipEvent && (canonical || a.sentinel != nil) && a.emitters != nil && signedEnvelope.Message.Payload != nil {
		block, ok := a.forkchoiceStore.GetBlock(signedEnvelope.Message.BeaconBlockRoot)
		if ok && block != nil && block.Block != nil {
			a.emitters.Operation().SendExecutionPayloadGossip(&beaconevents.ExecutionPayloadGossipData{
				Slot: block.Block.Slot, BuilderIndex: signedEnvelope.Message.BuilderIndex,
				BlockHash: signedEnvelope.Message.Payload.BlockHash, BlockRoot: signedEnvelope.Message.BeaconBlockRoot,
			})
		}
	}
	if status == http.StatusOK && emitIntegrationEvents && a.emitters != nil {
		block, ok := a.forkchoiceStore.GetBlock(signedEnvelope.Message.BeaconBlockRoot)
		if ok && block != nil && block.Block != nil && signedEnvelope.Message.Payload != nil {
			a.emitters.Operation().SendExecutionPayload(&beaconevents.ExecutionPayloadData{
				Slot: block.Block.Slot, BuilderIndex: signedEnvelope.Message.BuilderIndex,
				BlockHash: signedEnvelope.Message.Payload.BlockHash, BlockRoot: signedEnvelope.Message.BeaconBlockRoot,
				ExecutionOptimistic: a.forkchoiceStore.IsRootOptimistic(signedEnvelope.Message.BeaconBlockRoot),
			})
			a.emitters.Operation().SendExecutionPayloadAvailable(&beaconevents.ExecutionPayloadAvailableData{
				Slot: block.Block.Slot, BlockRoot: signedEnvelope.Message.BeaconBlockRoot,
			})
			a.emitFullHeadV2(block, signedEnvelope.Message.BeaconBlockRoot)
		}
	}

	if gossipValidated && (canonical || a.sentinel != nil) {
		encodedSSZ, err := signedEnvelope.EncodeSSZ(nil)
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}
		if err := a.publishGossip(r.Context(), gossip.TopicNameExecutionPayload, encodedSSZ); err != nil {
			beaconhttp.NewEndpointError(http.StatusInternalServerError, err).WriteTo(w)
			return
		}
	}

	w.WriteHeader(status)
}

func signedExecutionPayloadEnvelopesEqual(left, right *cltypes.SignedExecutionPayloadEnvelope) bool {
	if left == nil || right == nil {
		return false
	}
	leftSSZ, leftErr := left.EncodeSSZ(nil)
	rightSSZ, rightErr := right.EncodeSSZ(nil)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftSSZ, rightSSZ)
}

func (a *ApiHandler) emitFullHeadV2(block *cltypes.SignedBeaconBlock, blockRoot common.Hash) {
	headRoot, headSlot, err := a.forkchoiceStore.GetHead(nil)
	if err != nil || headRoot != blockRoot || a.beaconChainCfg.SlotsPerEpoch == 0 {
		return
	}
	payloadStatus := a.forkchoiceStore.GetHeadPayloadStatus()
	if payloadStatus != cltypes.PayloadStatusFull {
		return
	}
	optimistic := a.forkchoiceStore.IsRootOptimistic(blockRoot)
	headState, err := a.forkchoiceStore.GetStateAtBlockRoot(blockRoot, true)
	if err != nil || headState == nil {
		return
	}
	event, err := beaconevents.BuildHeadV2Data(a.beaconChainCfg, headState, headSlot, headRoot, block.Block.StateRoot, "full", optimistic)
	if err != nil {
		return
	}
	a.emitters.WithHeadEventLock(func() {
		currentRoot, currentSlot, err := a.forkchoiceStore.GetHead(nil)
		if err != nil || currentRoot != headRoot || currentSlot != headSlot ||
			a.forkchoiceStore.GetHeadPayloadStatus() != payloadStatus ||
			a.forkchoiceStore.IsRootOptimistic(currentRoot) != optimistic {
			return
		}
		a.emitters.State().SendHeadV2(event)
	})
}

func (a *ApiHandler) decodeExecutionPayloadEnvelopeRequest(w http.ResponseWriter, r *http.Request, contentType string, blobDataIncluded bool) (*cltypes.SignedExecutionPayloadEnvelope, *cltypes.SignedExecutionPayloadEnvelopeContents, error) {
	signedEnvelope := &cltypes.SignedExecutionPayloadEnvelope{Message: cltypes.NewExecutionPayloadEnvelope(a.beaconChainCfg)}
	var contents *cltypes.SignedExecutionPayloadEnvelopeContents
	if blobDataIncluded {
		contents = newSignedExecutionPayloadEnvelopeContentsForDecoding(a.beaconChainCfg, a.ethClock.GetCurrentSlot())
	}
	switch contentType {
	case "application/json":
		target := any(signedEnvelope)
		if contents != nil {
			target = contents
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxExecutionPayloadEnvelopeRequestSize))
		if err := decoder.Decode(target); err != nil {
			return nil, nil, err
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, nil, errors.New("request body contains trailing data")
		}
	case "application/octet-stream":
		octets, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxExecutionPayloadEnvelopeRequestSize))
		if err != nil {
			return nil, nil, err
		}
		if contents != nil {
			if err := contents.DecodeSSZStrict(octets, int(clparams.GloasVersion)); err != nil {
				return nil, nil, err
			}
		} else if err := signedEnvelope.DecodeSSZStrict(octets, int(clparams.GloasVersion)); err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, fmt.Errorf("unsupported content type: %s", r.Header.Get("Content-Type"))
	}
	if contents != nil {
		signedEnvelope = contents.SignedExecutionPayloadEnvelope
	}
	return signedEnvelope, contents, nil
}

func newSignedExecutionPayloadEnvelopeContentsForDecoding(cfg *clparams.BeaconChainConfig, currentSlot uint64) *cltypes.SignedExecutionPayloadEnvelopeContents {
	contents := cltypes.NewSignedExecutionPayloadEnvelopeContents(cfg, currentSlot)
	maxBlobs := int(min(cfg.MaxBlobsPerBlockUpperBound(), uint64(cltypes.MaxBlobsCommittmentsPerBlock)))
	contents.KZGProofs = solid.NewStaticListSSZ[*cltypes.KZGProof](maxBlobs*int(cfg.NumberOfColumns), cltypes.BYTES_KZG_PROOF)
	contents.Blobs = solid.NewStaticListSSZ[*cltypes.Blob](maxBlobs, int(cltypes.BYTES_PER_BLOB))
	return contents
}

func (a *ApiHandler) validateAndStoreExecutionPayloadEnvelopeContents(ctx context.Context, contents *cltypes.SignedExecutionPayloadEnvelopeContents) error {
	if contents == nil || contents.SignedExecutionPayloadEnvelope == nil {
		return errors.New("execution payload envelope contents has nil envelope")
	}
	if err := a.forkchoiceStore.ValidateExecutionPayloadEnvelope(contents.SignedExecutionPayloadEnvelope); err != nil {
		return err
	}
	return a.storeExecutionPayloadEnvelopeContents(ctx, contents)
}

var errExecutionPayloadEnvelopeIntegration = errors.New("execution payload envelope integration failed")

func (a *ApiHandler) storeExecutionPayloadEnvelopeContents(ctx context.Context, contents *cltypes.SignedExecutionPayloadEnvelopeContents) error {
	if contents == nil || contents.SignedExecutionPayloadEnvelope == nil || contents.SignedExecutionPayloadEnvelope.Message == nil {
		return errors.New("execution payload envelope contents has nil envelope")
	}
	envelope := contents.SignedExecutionPayloadEnvelope.Message
	block, ok := a.forkchoiceStore.GetBlock(envelope.BeaconBlockRoot)
	if !ok || block == nil || block.Block == nil || block.Block.Body == nil {
		return errors.New("execution payload envelope references an unknown block")
	}
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	if bid == nil || bid.Message == nil {
		return errors.New("execution payload envelope block has no bid")
	}
	commitments := &bid.Message.BlobKzgCommitments
	if contents.Blobs == nil || contents.KZGProofs == nil || contents.Blobs.Len() != commitments.Len() ||
		contents.KZGProofs.Len() != commitments.Len()*int(a.beaconChainCfg.NumberOfColumns) {
		return errors.New("execution payload envelope blob, proof, and commitment counts do not match")
	}
	cellsAndProofs := make([]peerdasutils.CellsAndKZGProofs, 0, commitments.Len())
	bundles := make([]BlobBundle, 0, commitments.Len())
	for i := 0; i < commitments.Len(); i++ {
		blob := contents.Blobs.Get(i)
		commitment := commitments.Get(i)
		if blob == nil || commitment == nil {
			return fmt.Errorf("execution payload envelope blob %d is nil", i)
		}
		bundle := BlobBundle{Blob: blob, Commitment: common.Bytes48(*commitment), KzgProofs: make([]common.Bytes48, a.beaconChainCfg.NumberOfColumns)}
		cells, err := das.ComputeCells(blob)
		if err != nil {
			return err
		}
		proofs := make([]cltypes.KZGProof, a.beaconChainCfg.NumberOfColumns)
		for j := range proofs {
			proof := contents.KZGProofs.Get(i*int(a.beaconChainCfg.NumberOfColumns) + j)
			if proof == nil {
				return fmt.Errorf("execution payload envelope proof %d is nil", j)
			}
			proofs[j] = *proof
			bundle.KzgProofs[j] = common.Bytes48(*proof)
		}
		bundles = append(bundles, bundle)
		cellsAndProofs = append(cellsAndProofs, peerdasutils.CellsAndKZGProofs{Blobs: cells, Proofs: proofs})
	}
	columns, err := peerdasutils.GetDataColumnSidecarsGloas(block.Block.Slot, envelope.BeaconBlockRoot, cellsAndProofs)
	if err != nil {
		return err
	}
	for _, column := range columns {
		if !das.VerifyDataColumnSidecarKZGProofsWithCommitments(column, commitments) {
			return fmt.Errorf("execution payload envelope column %d has invalid KZG proof", column.Index)
		}
	}
	if len(columns) != 0 && a.columnStorage == nil {
		return fmt.Errorf("%w: data column storage unavailable", errExecutionPayloadEnvelopeIntegration)
	}
	for _, column := range columns {
		if err := a.columnStorage.WriteColumnSidecars(ctx, envelope.BeaconBlockRoot, int64(column.Index), column); err != nil {
			return fmt.Errorf("%w: %w", errExecutionPayloadEnvelopeIntegration, err)
		}
	}
	for _, bundle := range bundles {
		a.blobBundles.Add(bundle.Commitment, bundle)
	}
	for _, column := range columns {
		if a.sentinel != nil {
			encoded, err := column.EncodeSSZ(nil)
			if err != nil {
				return err
			}
			if err := a.gossipManager.Publish(ctx, gossip.TopicNameDataColumnSidecar(das.ComputeSubnetForDataColumnSidecar(column.Index)), encoded); err != nil {
				a.logger.Debug("failed to publish execution payload data column", "err", err)
			}
		}
	}
	return nil
}

// ---- Execution Payload Bid ----

// PostEthV1BeaconExecutionPayloadBid publishes a SignedExecutionPayloadBid.
// POST /eth/v1/beacon/execution_payload_bid
// [New in Gloas:EIP7732]
func (a *ApiHandler) PostEthV1BeaconExecutionPayloadBid(w http.ResponseWriter, r *http.Request) {
	a.postEthV1BeaconExecutionPayloadBid(w, r, true)
}

func (a *ApiHandler) postEthV1BeaconExecutionPayloadBidLegacy(w http.ResponseWriter, r *http.Request) {
	a.postEthV1BeaconExecutionPayloadBid(w, r, false)
}

func (a *ApiHandler) postEthV1BeaconExecutionPayloadBid(w http.ResponseWriter, r *http.Request, canonical bool) {
	if canonical && r.Header.Get("Eth-Consensus-Version") != clparams.GloasVersion.String() {
		beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("Gloas Eth-Consensus-Version header is required")).WriteTo(w)
		return
	}
	req := new(cltypes.SignedExecutionPayloadBid)
	contentType, err := requestContentType(r)
	if err != nil {
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType, err).WriteTo(w)
		return
	}
	switch contentType {
	case "application/json":
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxEpbsJSONSize))
		if err := decoder.Decode(req); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			beaconhttp.NewEndpointError(http.StatusBadRequest, errors.New("request body contains trailing data")).WriteTo(w)
			return
		}
	case "application/octet-stream":
		octets, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxSignedExecutionPayloadBidSSZSize()))
		if err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
		if err := req.DecodeSSZStrict(octets, int(clparams.GloasVersion)); err != nil {
			beaconhttp.NewEndpointError(http.StatusBadRequest, err).WriteTo(w)
			return
		}
	default:
		beaconhttp.NewEndpointError(http.StatusUnsupportedMediaType,
			fmt.Errorf("unsupported content type: %s", r.Header.Get("Content-Type"))).WriteTo(w)
		return
	}
	if req.Message == nil {
		beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("missing message in signed execution payload bid")).WriteTo(w)
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

	if a.beaconChainCfg.SlotsPerEpoch == 0 {
		return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, errors.New("slots per epoch is zero"))
	}
	// Must be GLOAS epoch
	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	if epoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("execution payload bids not available before GLOAS fork"))
	}
	currentSlot := a.ethClock.GetCurrentSlot()
	if slot < currentSlot || slot-currentSlot > 1 {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("execution payload bid slot %d is not current or next", slot))
	}
	registered := false
	if a.syncedData != nil {
		if err := a.syncedData.ViewHeadState(func(headState *state.CachingBeaconState) error {
			builders := headState.GetBuilders()
			registered = builders != nil && builderIndex < uint64(builders.Len()) && builders.Get(int(builderIndex)) != nil
			return nil
		}); err != nil {
			return nil, beaconhttp.NewEndpointError(http.StatusServiceUnavailable, err)
		}
	}
	if !registered {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("builder index %d is not registered", builderIndex))
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

	return newBeaconResponse(bestBid.Message).WithVersion(clparams.GloasVersion), nil
}

// ---- Validator Execution Payload Envelope ----

// GetEthV1ValidatorExecutionPayloadEnvelope returns the unsigned ExecutionPayloadEnvelope
// for a given slot and builder index. Used by the validator client to retrieve the
// self-build envelope for signing after block production.
// GET /eth/v1/validator/execution_payload_envelope/{slot}/{builder_index}
// [New in Gloas:EIP7732]
func (a *ApiHandler) GetEthV1ValidatorExecutionPayloadEnvelope(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
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
			fmt.Errorf("execution payload envelopes not available before GLOAS fork"))
	}

	envelope, ok := a.selfBuildEnvelopeForSlot(slot, func(envelope *cltypes.ExecutionPayloadEnvelope) bool {
		return envelope.BuilderIndex == builderIndex
	})
	if !ok || envelope == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound,
			fmt.Errorf("no execution payload envelope found for slot %d with builder_index %d", slot, builderIndex))
	}

	return newBeaconResponse(envelope).WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)), nil
}

// GetEthV1ValidatorExecutionPayloadEnvelopeBySlot returns the unsigned ExecutionPayloadEnvelope for a slot.
// GET /eth/v1/validator/execution_payload_envelopes/{slot}
// [New in Gloas:EIP7732]
func (a *ApiHandler) GetEthV1ValidatorExecutionPayloadEnvelopeBySlot(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slotStr, err := beaconhttp.StringFromRequest(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	slot, err := strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("invalid slot: %w", err))
	}

	epoch := slot / a.beaconChainCfg.SlotsPerEpoch
	if epoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest,
			fmt.Errorf("execution payload envelopes not available before GLOAS fork"))
	}

	envelope, ok := a.selfBuildEnvelopeForSlot(slot, nil)
	if !ok || envelope == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound,
			fmt.Errorf("no execution payload envelope found for slot %d", slot))
	}

	return newBeaconResponse(envelope).WithVersion(a.beaconChainCfg.GetCurrentStateVersion(epoch)), nil
}

func (a *ApiHandler) GetEthV1ValidatorExecutionPayloadEnvelopeByBlockRoot(w http.ResponseWriter, r *http.Request) (*beaconhttp.BeaconResponse, error) {
	slotStr, err := beaconhttp.StringFromRequest(r, "slot")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	slot, err := strconv.ParseUint(slotStr, 10, 64)
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid slot: %w", err))
	}
	rootStr, err := beaconhttp.StringFromRequest(r, "beacon_block_root")
	if err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, err)
	}
	var root common.Hash
	if err := root.UnmarshalText([]byte(rootStr)); err != nil {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("invalid beacon_block_root: %w", err))
	}
	if slot != a.ethClock.GetCurrentSlot() {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("execution payload envelope is only retained for the current slot"))
	}
	if slot/a.beaconChainCfg.SlotsPerEpoch < a.beaconChainCfg.GloasForkEpoch {
		return nil, beaconhttp.NewEndpointError(http.StatusBadRequest, fmt.Errorf("execution payload envelopes not available before GLOAS fork"))
	}
	envelope, ok := a.selfBuildEnvelopes.Get(selfBuildEnvelopeKey{Slot: slot, BeaconBlockRoot: root})
	if !ok || envelope == nil {
		return nil, beaconhttp.NewEndpointError(http.StatusNotFound, fmt.Errorf("no execution payload envelope found for slot %d and block root %s", slot, root))
	}
	return newBeaconResponse(envelope).WithVersion(clparams.GloasVersion), nil
}

func (a *ApiHandler) selfBuildEnvelopeForSlot(slot uint64, accept func(*cltypes.ExecutionPayloadEnvelope) bool) (*cltypes.ExecutionPayloadEnvelope, bool) {
	keys := a.selfBuildEnvelopes.Keys()
	for _, key := range slices.Backward(keys) {
		if key.Slot != slot {
			continue
		}
		envelope, ok := a.selfBuildEnvelopes.Get(key)
		if ok && envelope != nil && (accept == nil || accept(envelope)) {
			return envelope, true
		}
	}
	return nil, false
}

// ---- Helpers ----

// blockRootFromBlockId resolves a block_id to a block root hash.
func (a *ApiHandler) blockRootFromBlockId(blockId *beaconhttp.SegmentID) (common.Hash, error) {
	switch {
	case blockId.Head():
		root, _, _, err := a.getSelectedHead()
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
