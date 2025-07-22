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

package handlers

import (
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/libp2p/go-libp2p/core/network"
)

const maxLightClientsPerRequest = 100

func (c *ConsensusHandlers) optimisticLightClientUpdateHandler(s network.Stream) error {
	lc := c.forkChoiceReader.NewestLightClientUpdate()
	if lc == nil {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}
	// Read the fork digest
	forkDigest, err := c.ethClock.ComputeForkDigest(lc.AttestedHeader.Beacon.Slot / c.beaconConfig.SlotsPerEpoch)
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
	lc := c.forkChoiceReader.NewestLightClientUpdate()
	if lc == nil {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}

	forkDigest, err := c.ethClock.ComputeForkDigest(lc.AttestedHeader.Beacon.Slot / c.beaconConfig.SlotsPerEpoch)
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

	lc, has := c.forkChoiceReader.GetLightClientBootstrap(root.Root)
	if !has {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}

	forkDigest, err := c.ethClock.ComputeForkDigest(lc.Header.Beacon.Slot / c.beaconConfig.SlotsPerEpoch)
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

		// Read the fork digest
		forkDigest, err := c.ethClock.ComputeForkDigest(update.AttestedHeader.Beacon.Slot / c.beaconConfig.SlotsPerEpoch)
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
