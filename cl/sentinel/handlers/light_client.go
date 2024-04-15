package handlers

import (
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/libp2p/go-libp2p/core/network"
)

const maxLightClientsPerRequest = 100

func (c *ConsensusHandlers) optimisticLightClientUpdateHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "light_client", rateLimits.lightClientLimit, 1); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}
	lc := c.forkChoiceReader.NewestLightClientUpdate()
	if lc == nil {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavaiablePrefix)
	}
	version := lc.AttestedHeader.Version()
	// Read the fork digest
	forkDigest, err := c.ethClock.ComputeForkDigestForVersion(utils.Uint32ToBytes4(c.beaconConfig.GetForkVersionByVersion(version)))
	if err != nil {
		return err
	}

	prefix := append([]byte{SuccessfulResponsePrefix}, forkDigest[:]...)
	return ssz_snappy.EncodeAndWrite(s, &cltypes.LightClientOptimisticUpdate{
		AttestedHeader: lc.AttestedHeader,
		SyncAggregate:  lc.SyncAggregate,
		SignatureSlot:  lc.SignatureSlot,
	}, prefix...)
}

func (c *ConsensusHandlers) finalityLightClientUpdateHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "light_client", rateLimits.lightClientLimit, 1); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}
	lc := c.forkChoiceReader.NewestLightClientUpdate()
	if lc == nil {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavaiablePrefix)
	}

	forkDigest, err := c.ethClock.ComputeForkDigestForVersion(utils.Uint32ToBytes4(c.beaconConfig.GetForkVersionByVersion(lc.AttestedHeader.Version())))
	if err != nil {
		return err
	}
	prefix := append([]byte{SuccessfulResponsePrefix}, forkDigest[:]...)
	return ssz_snappy.EncodeAndWrite(s, &cltypes.LightClientFinalityUpdate{
		AttestedHeader:  lc.AttestedHeader,
		SyncAggregate:   lc.SyncAggregate,
		FinalizedHeader: lc.FinalizedHeader,
		FinalityBranch:  lc.FinalityBranch,
		SignatureSlot:   lc.SignatureSlot,
	}, prefix...)
}

func (c *ConsensusHandlers) lightClientBootstrapHandler(s network.Stream) error {
	root := &cltypes.Root{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, root, clparams.Phase0Version); err != nil {
		return err
	}

	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "light_client", rateLimits.lightClientLimit, 1); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}

	lc, has := c.forkChoiceReader.GetLightClientBootstrap(root.Root)
	if !has {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavaiablePrefix)
	}

	forkDigest, err := c.ethClock.ComputeForkDigestForVersion(utils.Uint32ToBytes4(c.beaconConfig.GetForkVersionByVersion(lc.Header.Version())))
	if err != nil {
		return err
	}

	prefix := append([]byte{SuccessfulResponsePrefix}, forkDigest[:]...)
	return ssz_snappy.EncodeAndWrite(s, lc, prefix...)
}

func (c *ConsensusHandlers) lightClientUpdatesByRangeHandler(s network.Stream) error {
	req := &cltypes.LightClientUpdatesByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "light_client", rateLimits.lightClientLimit, int(req.Count)); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}

	lightClientUpdates := make([]*cltypes.LightClientUpdate, 0, maxLightClientsPerRequest)

	endPeriod := req.StartPeriod + req.Count
	currentSlot := c.ethClock.GetCurrentSlot()
	if endPeriod > c.beaconConfig.SyncCommitteePeriod(currentSlot) {
		endPeriod = c.beaconConfig.SyncCommitteePeriod(currentSlot) + 1
	}

	notFoundPrev := false
	// Fetch from [start_period, start_period + count]
	for i := req.StartPeriod; i < endPeriod; i++ {
		update, has := c.forkChoiceReader.GetLightClientUpdate(i)
		if !has {
			notFoundPrev = true
			continue
		}
		if notFoundPrev {
			lightClientUpdates = lightClientUpdates[0:]
			notFoundPrev = false
		}

		lightClientUpdates = append(lightClientUpdates, update)
		if uint64(len(lightClientUpdates)) >= maxLightClientsPerRequest {
			break
		}
	}

	// Write the updates
	for _, update := range lightClientUpdates {
		if _, err := s.Write([]byte{0}); err != nil {
			return err
		}

		version := update.AttestedHeader.Version()
		// Read the fork digest
		forkDigest, err := c.ethClock.ComputeForkDigestForVersion(utils.Uint32ToBytes4(c.beaconConfig.GetForkVersionByVersion(version)))
		if err != nil {
			return err
		}

		if _, err := s.Write(forkDigest[:]); err != nil {
			return err
		}

		if err := ssz_snappy.EncodeAndWrite(s, update); err != nil {
			return err
		}
	}

	return nil
}
