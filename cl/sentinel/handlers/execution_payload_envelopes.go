package handlers

import (
	"errors"

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

	// Validate count
	if req.Count > c.beaconConfig.MaxRequestBlocksDeneb {
		return errors.New("request count exceeds MAX_REQUEST_BLOCKS_DENEB")
	}

	var (
		endSlot   = req.StartSlot + req.Count
		startSlot = max(req.StartSlot, c.beaconConfig.GloasForkEpoch*c.beaconConfig.SlotsPerEpoch)
	)

	curSlot := c.ethClock.GetCurrentSlot()

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	count := uint64(0)
	for slot := startSlot; slot < endSlot; slot++ {
		if slot > curSlot {
			break
		}

		// Only serve envelopes from GLOAS fork onwards
		epoch := slot / c.beaconConfig.SlotsPerEpoch
		if c.beaconConfig.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
			continue
		}

		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return err
		}
		if blockRoot == (common.Hash{}) {
			continue
		}

		if !c.forkChoiceReader.HasEnvelope(blockRoot) {
			continue
		}

		envelope, err := c.forkChoiceReader.ReadEnvelopeFromDisk(blockRoot)
		if err != nil {
			log.Debug("failed to read envelope from disk", "blockRoot", blockRoot, "error", err)
			continue
		}
		if envelope == nil {
			continue
		}

		forkDigest, err := c.ethClock.ComputeForkDigest(epoch)
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

		count++
		if count >= c.beaconConfig.MaxRequestBlocksDeneb {
			break
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

	// Decode request: List[Root, MAX_REQUEST_PAYLOADS]
	var req solid.HashListSSZ = solid.NewHashList(int(c.beaconConfig.MaxRequestBlocksDeneb))
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	if req.Length() == 0 {
		return nil
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	count := 0
	req.Range(func(_ int, blockRoot common.Hash, _ int) bool {
		if count >= int(c.beaconConfig.MaxRequestBlocksDeneb) {
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

		// Only serve envelopes from GLOAS fork onwards
		epoch := *slot / c.beaconConfig.SlotsPerEpoch
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
