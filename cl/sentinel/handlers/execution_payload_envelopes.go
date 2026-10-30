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
	"context"
	"errors"
	"math"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/envelope_history"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// executionPayloadEnvelopesByRangeHandler handles the ExecutionPayloadEnvelopesByRange v1 req/resp protocol.
// [New in Gloas:EIP7732]
func (c *ConsensusHandlers) executionPayloadEnvelopesByRangeHandler(s network.Stream) error {
	requestCtx, cancel := context.WithTimeout(c.ctx, StreamDeadline)
	defer cancel()

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
	if maxPayloads > math.MaxInt {
		return errors.New("MAX_REQUEST_PAYLOADS exceeds platform capacity")
	}
	if req.Count == 0 {
		return nil
	}
	endSlot := req.StartSlot + req.Count
	if endSlot < req.StartSlot {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}

	if cost := int(min(req.Count, maxPayloads)) - 1; !c.consumeRateLimit(s, cost) {
		return nil
	}

	minServeEpoch := minExecutionPayloadServeEpoch(c.beaconConfig, curEpoch, c.forkChoiceReader.FinalizedCheckpoint().Epoch)

	startSlot := max(req.StartSlot, minServeEpoch*c.beaconConfig.SlotsPerEpoch)
	if startSlot >= endSlot {
		return nil
	}
	lowestAvailableSlot := c.forkChoiceReader.LowestAvailableSlot()
	if c.envelopeRebuilder == nil && startSlot < lowestAvailableSlot {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}
	needsCanonicalParent := c.envelopeRebuilder != nil && startSlot < lowestAvailableSlot

	head, headSlot, err := c.forkChoiceReader.GetHeadNode()
	if err != nil {
		return err
	}

	lastSlot := endSlot - 1
	lastSlot = min(lastSlot, c.ethClock.GetCurrentSlot(), headSlot)
	if lastSlot < startSlot {
		return nil
	}
	tx, err := c.indiciesDB.BeginRo(requestCtx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	canonicalHeadSlot, canonicalHeadRoot, err := beacon_indicies.ReadCanonicalHead(tx)
	if err != nil {
		return err
	}
	headIndexMatches := canonicalHeadSlot == headSlot && canonicalHeadRoot == head.Root
	type responseCandidate struct {
		root        common.Hash
		slot        uint64
		epoch       uint64
		rebuildable bool
	}
	responseCandidates := make([]responseCandidate, 0, maxPayloads)
	type canonicalBlock struct {
		root  common.Hash
		slot  uint64
		block *cltypes.SignedBeaconBlock
	}
	var pending *canonicalBlock
	var lastCanonical *canonicalBlock
	canonicalUnavailable := false
	canonicalReadLimit := maxPayloads
	if canonicalReadLimit != math.MaxUint64 {
		canonicalReadLimit++
	}
	canonicalReadCount := uint64(0)
	scanLimitReached := false
	err = beacon_indicies.RangeBlockRoots(tx, startSlot, headSlot, func(slot uint64, root common.Hash) bool {
		if !needsCanonicalParent && pending == nil && slot > lastSlot {
			return false
		}
		block, ok := c.forkChoiceReader.GetBlock(root)
		if (!ok || block == nil) && c.envelopeRebuilder != nil {
			block, err = c.beaconDB.ReadBlockByRoot(requestCtx, tx, root)
			ok = err == nil && block != nil
		}
		if err != nil {
			canonicalUnavailable = true
			return false
		}
		if !ok || block == nil || block.Block == nil || block.Block.Body == nil {
			canonicalUnavailable = true
			return false
		}
		if needsCanonicalParent {
			needsCanonicalParent = false
			if slot > startSlot {
				parentRoot := block.Block.ParentRoot
				parentSlot, readErr := beacon_indicies.ReadBlockSlotByBlockRoot(tx, parentRoot)
				if readErr != nil || parentSlot == nil || *parentSlot >= startSlot {
					canonicalUnavailable = true
					return false
				}
				canonicalParent, readErr := beacon_indicies.ReadCanonicalBlockRoot(tx, *parentSlot)
				if readErr != nil || canonicalParent != parentRoot {
					canonicalUnavailable = true
					return false
				}
			}
		}
		current := &canonicalBlock{root: root, slot: slot, block: block}
		lastCanonical = current
		if pending != nil {
			if current.block.Block.ParentRoot != pending.root {
				canonicalUnavailable = true
				return false
			}
			epoch := pending.slot / c.beaconConfig.SlotsPerEpoch
			if c.beaconConfig.GetCurrentStateVersion(epoch) >= clparams.GloasVersion &&
				forkchoice.ParentPayloadStatusFromBids(pending.block, current.block.Block) == cltypes.PayloadStatusFull {
				responseCandidates = append(responseCandidates, responseCandidate{root: pending.root, slot: pending.slot, epoch: epoch, rebuildable: true})
				pending = nil
				if uint64(len(responseCandidates)) == maxPayloads {
					return false
				}
			}
			pending = nil
		}
		if slot > lastSlot {
			return false
		}
		pending = current
		canonicalReadCount++
		if canonicalReadCount == canonicalReadLimit {
			scanLimitReached = true
			return false
		}
		return true
	})
	if err != nil {
		return err
	}
	if needsCanonicalParent {
		canonicalUnavailable = true
	}
	if canonicalUnavailable {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}
	if !headIndexMatches {
		if lastCanonical == nil || c.forkChoiceReader.Ancestor(head.Root, lastCanonical.slot).Root != lastCanonical.root {
			return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
		}
	}
	if pending != nil {
		if pending.slot != headSlot || pending.root != head.Root {
			if scanLimitReached {
				if len(responseCandidates) == 0 {
					return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
				}
				pending = nil
			} else {
				return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
			}
		} else if uint64(len(responseCandidates)) == maxPayloads {
			pending = nil
		}
	}
	if pending != nil {
		epoch := pending.slot / c.beaconConfig.SlotsPerEpoch
		if c.beaconConfig.GetCurrentStateVersion(epoch) >= clparams.GloasVersion && head.PayloadStatus == cltypes.PayloadStatusFull {
			responseCandidates = append(responseCandidates, responseCandidate{root: pending.root, slot: pending.slot, epoch: epoch})
		}
	}

	wroteResponse := false
	rebuilds := 0
	for _, candidate := range responseCandidates {
		var envelope *cltypes.SignedExecutionPayloadEnvelope
		if c.forkChoiceReader.HasEnvelope(candidate.root) {
			envelope, err = c.forkChoiceReader.ReadEnvelopeFromDisk(candidate.root)
			if err != nil {
				log.Debug("failed to read envelope from disk", "blockRoot", candidate.root, "error", err)
			}
		}
		if envelope == nil && err == nil && candidate.rebuildable && c.envelopeRebuilder != nil && rebuilds < envelope_history.MaxRebuildsPerRequest {
			rebuilds++
			prepared, prepareErr := c.envelopeRebuilder.Prepare(requestCtx, tx, candidate.slot, candidate.root)
			err = prepareErr
			if err == nil && prepared != nil {
				envelope, err = c.envelopeRebuilder.TryRebuild(requestCtx, prepared)
			}
		}
		if err != nil || envelope == nil {
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
	requestCtx, cancel := context.WithTimeout(c.ctx, StreamDeadline)
	defer cancel()

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

	tx, err := c.indiciesDB.BeginRo(requestCtx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	minServeEpoch := minExecutionPayloadServeEpoch(c.beaconConfig, curEpoch, c.forkChoiceReader.FinalizedCheckpoint().Epoch)

	type rebuildCandidate struct {
		blockRoot common.Hash
		slot      uint64
		epoch     uint64
	}

	count := 0
	writeEnvelope := func(envelope *cltypes.SignedExecutionPayloadEnvelope, epoch uint64) bool {
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
	}

	rebuildCandidates := make([]rebuildCandidate, 0, req.Length())
	stopped := false
	req.Range(func(_ int, blockRoot common.Hash, _ int) bool {
		if count >= int(maxPayloads) {
			return false
		}

		slot, readErr := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		if readErr != nil {
			log.Debug("failed to read block slot by root", "blockRoot", blockRoot, "error", readErr)
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

		var envelope *cltypes.SignedExecutionPayloadEnvelope
		if c.forkChoiceReader.HasEnvelope(blockRoot) {
			envelope, readErr = c.forkChoiceReader.ReadEnvelopeFromDisk(blockRoot)
			if readErr != nil {
				log.Debug("failed to read envelope from disk", "blockRoot", blockRoot, "error", readErr)
				return true
			}
		}
		if envelope == nil {
			if c.envelopeRebuilder != nil {
				rebuildCandidates = append(rebuildCandidates, rebuildCandidate{blockRoot: blockRoot, slot: *slot, epoch: epoch})
			}
			return true
		}

		if !writeEnvelope(envelope, epoch) {
			stopped = true
			return false
		}
		return true
	})

	if stopped {
		return nil
	}

	rebuilds := 0
	for _, candidate := range rebuildCandidates {
		if count >= int(maxPayloads) || rebuilds >= envelope_history.MaxRebuildsPerRequest {
			break
		}

		prepared, err := c.envelopeRebuilder.Prepare(requestCtx, tx, candidate.slot, candidate.blockRoot)
		if err != nil {
			log.Debug("failed to rebuild execution payload envelope", "blockRoot", candidate.blockRoot, "error", err)
			break
		}
		if prepared == nil {
			continue
		}

		rebuilds++
		envelope, err := c.envelopeRebuilder.TryRebuild(requestCtx, prepared)
		if err != nil {
			log.Debug("failed to rebuild execution payload envelope", "blockRoot", candidate.blockRoot, "error", err)
			break
		}
		if envelope != nil && !writeEnvelope(envelope, candidate.epoch) {
			break
		}
	}

	return nil
}

func minExecutionPayloadServeEpoch(beaconConfig *clparams.BeaconChainConfig, currentEpoch, finalizedEpoch uint64) uint64 {
	minServeEpoch := beaconConfig.GloasForkEpoch
	if currentEpoch > beaconConfig.MinEpochsForBlockRequests() {
		if lowerBound := currentEpoch - beaconConfig.MinEpochsForBlockRequests(); lowerBound > minServeEpoch {
			minServeEpoch = lowerBound
		}
	}
	if finalizedEpoch < minServeEpoch {
		minServeEpoch = max(beaconConfig.GloasForkEpoch, finalizedEpoch)
	}
	return minServeEpoch
}
