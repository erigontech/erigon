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

package handlers

import (
	"errors"
	"slices"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// executionPayloadEnvelopesByRangeHandler handles the ExecutionPayloadEnvelopesByRange v1 req/resp protocol.
// [New in Gloas:EIP7732]
func (c *ConsensusHandlers) executionPayloadEnvelopesByRangeHandler(s network.Stream) error {
	curEpoch := c.ethClock.GetCurrentEpoch()
	if curEpoch < c.beaconConfig.GloasForkEpoch {
		return nil
	}

	// Use current epoch's version for decoding
	version := c.beaconConfig.GetCurrentStateVersion(curEpoch)
	req := &cltypes.ExecutionPayloadEnvelopesByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, version); err != nil {
		return err
	}

	maxPayloads := c.beaconConfig.MaxRequestPayloadsLimit()
	if maxPayloads == 0 {
		return errors.New("MAX_REQUEST_PAYLOADS is zero")
	}
	// Validate count
	if req.Count > maxPayloads {
		return errors.New("request count exceeds MAX_REQUEST_PAYLOADS")
	}
	if req.Count == 0 {
		return nil
	}
	endSlot := req.StartSlot + req.Count
	if endSlot < req.StartSlot {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}

	if cost := min(int(req.Count), int(maxPayloads)) - 1; !c.consumeRateLimit(s, cost) {
		return nil
	}

	// Compute minimum serve slot: max(GLOAS_FORK_EPOCH, current_epoch - MIN_EPOCHS_FOR_BLOCK_REQUESTS) * SLOTS_PER_EPOCH
	minServeEpoch := c.beaconConfig.GloasForkEpoch
	if curEpoch > c.beaconConfig.MinEpochsForBlockRequests() {
		if lowerBound := curEpoch - c.beaconConfig.MinEpochsForBlockRequests(); lowerBound > minServeEpoch {
			minServeEpoch = lowerBound
		}
	}

	startSlot := max(req.StartSlot, minServeEpoch*c.beaconConfig.SlotsPerEpoch)
	if startSlot >= endSlot {
		return nil
	}
	if startSlot < c.forkChoiceReader.LowestAvailableSlot() {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}

	curSlot := c.ethClock.GetCurrentSlot()

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	head, headSlot, err := c.forkChoiceReader.GetHeadNode()
	if err != nil {
		return err
	}

	lastSlot := endSlot - 1
	lastSlot = min(lastSlot, curSlot, headSlot)
	if lastSlot < startSlot {
		return nil
	}

	type responseCandidate struct {
		root  common.Hash
		epoch uint64
	}
	responseCandidates := make([]responseCandidate, 0, req.Count)
	ancestorRoot := head.Root
	for slot := lastSlot; ; slot-- {

		// Only serve envelopes from GLOAS fork onwards
		epoch := slot / c.beaconConfig.SlotsPerEpoch
		if c.beaconConfig.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
			if slot == startSlot {
				break
			}
			continue
		}

		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return err
		}
		if blockRoot != (common.Hash{}) {
			payloadStatus := head.PayloadStatus
			if blockRoot != head.Root || slot != headSlot {
				ancestor := c.forkChoiceReader.Ancestor(ancestorRoot, slot)
				ancestorRoot = ancestor.Root
				if ancestor.Root != blockRoot {
					if slot == startSlot {
						break
					}
					continue
				}
				payloadStatus = ancestor.PayloadStatus
			}
			if payloadStatus == cltypes.PayloadStatusFull {
				responseCandidates = append(responseCandidates, responseCandidate{root: blockRoot, epoch: epoch})
			}
		}
		if slot == startSlot {
			break
		}
	}

	for _, candidate := range slices.Backward(responseCandidates) {
		if !c.forkChoiceReader.HasEnvelope(candidate.root) {
			continue
		}

		envelope, err := c.forkChoiceReader.ReadEnvelopeFromDisk(candidate.root)
		if err != nil {
			log.Debug("failed to read envelope from disk", "blockRoot", candidate.root, "error", err)
			continue
		}
		if envelope == nil {
			continue
		}

		forkDigest, err := c.ethClock.ComputeForkDigest(candidate.epoch)
		if err != nil {
			log.Debug("failed to compute fork digest", "error", err)
			return err
		}

		if _, err := s.Write([]byte{SuccessfulResponsePrefix}); err != nil {
			return err
		}
		if _, err := s.Write(forkDigest[:]); err != nil {
			return err
		}
		if err := ssz_snappy.EncodeAndWrite(s, envelope); err != nil {
			return err
		}

	}

	return nil
}

// executionPayloadEnvelopesByRootHandler handles the ExecutionPayloadEnvelopesByRoot v1 req/resp protocol.
// [New in Gloas:EIP7732]
func (c *ConsensusHandlers) executionPayloadEnvelopesByRootHandler(s network.Stream) error {
	curEpoch := c.ethClock.GetCurrentEpoch()
	if curEpoch < c.beaconConfig.GloasForkEpoch {
		return nil
	}

	maxPayloads := c.beaconConfig.MaxRequestPayloadsLimit()
	if maxPayloads == 0 {
		return errors.New("MAX_REQUEST_PAYLOADS is zero")
	}
	// Decode request: List[Root, MAX_REQUEST_PAYLOADS]
	var req solid.HashListSSZ = solid.NewHashList(int(maxPayloads))
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	if req.Length() == 0 {
		return nil
	}
	if req.Length() > int(maxPayloads) {
		return errors.New("request count exceeds MAX_REQUEST_PAYLOADS")
	}

	if cost := min(req.Length(), int(maxPayloads)) - 1; !c.consumeRateLimit(s, cost) {
		return nil
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Compute serve range: [max(GLOAS_FORK_EPOCH, current_epoch - MIN_EPOCHS_FOR_BLOCK_REQUESTS), current_epoch]
	// Spec: consensus-specs PR #4950
	minServeEpoch := c.beaconConfig.GloasForkEpoch
	if curEpoch > c.beaconConfig.MinEpochsForBlockRequests() {
		if lowerBound := curEpoch - c.beaconConfig.MinEpochsForBlockRequests(); lowerBound > minServeEpoch {
			minServeEpoch = lowerBound
		}
	}

	count := 0
	req.Range(func(_ int, blockRoot common.Hash, _ int) bool {
		if count >= int(maxPayloads) {
			return false
		}

		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		if err != nil {
			log.Debug("failed to read block slot by root", "blockRoot", blockRoot, "error", err)
			return true
		}
		if slot == nil {
			return true
		}

		// Only serve envelopes within the serve range
		epoch := *slot / c.beaconConfig.SlotsPerEpoch
		if epoch < minServeEpoch || epoch > curEpoch {
			return true
		}
		if c.beaconConfig.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
			return true
		}

		if !c.forkChoiceReader.HasEnvelope(blockRoot) {
			return true
		}

		envelope, err := c.forkChoiceReader.ReadEnvelopeFromDisk(blockRoot)
		if err != nil {
			log.Debug("failed to read envelope from disk", "blockRoot", blockRoot, "error", err)
			return true
		}
		if envelope == nil {
			return true
		}

		forkDigest, err := c.ethClock.ComputeForkDigest(epoch)
		if err != nil {
			log.Debug("failed to compute fork digest", "error", err)
			return false
		}

		if _, err := s.Write([]byte{SuccessfulResponsePrefix}); err != nil {
			return false
		}
		if _, err := s.Write(forkDigest[:]); err != nil {
			return false
		}
		if err := ssz_snappy.EncodeAndWrite(s, envelope); err != nil {
			return false
		}

		count++
		return true
	})

	return nil
}
