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

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
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

	head, headSlot, err := c.forkChoiceReader.GetHeadNode()
	if err != nil {
		return err
	}

	lastSlot := endSlot - 1
	lastSlot = min(lastSlot, c.ethClock.GetCurrentSlot(), headSlot)
	if lastSlot < startSlot {
		return nil
	}
	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	canonicalHeadSlot, canonicalHeadRoot, err := beacon_indicies.ReadCanonicalHead(tx)
	if err != nil {
		return err
	}
	if canonicalHeadSlot != headSlot || canonicalHeadRoot != head.Root {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}
	canonicalRoots, canonicalSlots, err := beacon_indicies.ReadBeaconBlockRootsInSlotRange(c.ctx, tx, startSlot, req.Count+1)
	if err != nil {
		return err
	}

	type responseCandidate struct {
		root  common.Hash
		epoch uint64
	}
	responseCandidates := make([]responseCandidate, 0, req.Count)
	canonicalBlocks := make([]*cltypes.SignedBeaconBlock, 0, len(canonicalRoots))
	for i, root := range canonicalRoots {
		if canonicalSlots[i] > lastSlot && len(canonicalBlocks) == 0 {
			break
		}
		block, ok := c.forkChoiceReader.GetBlock(root)
		if !ok || block == nil || block.Block == nil {
			return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
		}
		canonicalBlocks = append(canonicalBlocks, block)
		if canonicalSlots[i] > lastSlot {
			break
		}
	}
	for i, root := range canonicalRoots {
		slot := canonicalSlots[i]
		if slot > lastSlot {
			break
		}
		epoch := slot / c.beaconConfig.SlotsPerEpoch
		if c.beaconConfig.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
			continue
		}
		payloadStatus := head.PayloadStatus
		if slot != headSlot || root != head.Root {
			if i+1 >= len(canonicalBlocks) || canonicalBlocks[i+1].Block.ParentRoot != root {
				return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
			}
			payloadStatus = forkchoice.ParentPayloadStatusFromBids(canonicalBlocks[i], canonicalBlocks[i+1].Block)
		}
		if payloadStatus == cltypes.PayloadStatusFull {
			responseCandidates = append(responseCandidates, responseCandidate{root: root, epoch: epoch})
		}
	}

	wroteResponse := false
	for _, candidate := range responseCandidates {
		if !c.forkChoiceReader.HasEnvelope(candidate.root) {
			if wroteResponse {
				break
			}
			return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
		}

		envelope, err := c.forkChoiceReader.ReadEnvelopeFromDisk(candidate.root)
		if err != nil {
			log.Debug("failed to read envelope from disk", "blockRoot", candidate.root, "error", err)
			if wroteResponse {
				break
			}
			return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
		}
		if envelope == nil {
			if wroteResponse {
				break
			}
			return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
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
		wroteResponse = true
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
