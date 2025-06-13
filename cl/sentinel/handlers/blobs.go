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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/libp2p/go-libp2p/core/network"
)

const maxBlobsThroughoutputPerRequest = 72

func (c *ConsensusHandlers) blobsSidecarsByRangeHandlerElectra(s network.Stream) error {
	return c.blobsSidecarsByRangeHandler(s, clparams.ElectraVersion)
}

func (c *ConsensusHandlers) blobsSidecarsByRangeHandlerDeneb(s network.Stream) error {
	return c.blobsSidecarsByRangeHandler(s, clparams.DenebVersion)
}

func (c *ConsensusHandlers) blobsSidecarsByRangeHandler(s network.Stream, version clparams.StateVersion) error {

	req := &cltypes.BlobsByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, version); err != nil {
		return err
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	written := 0
	maxIter := 32
	currIter := 0
	for slot := req.StartSlot; slot < req.StartSlot+req.Count; slot++ {
		if currIter >= maxIter {
			break
		}
		currIter++
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return err
		}
		if blockRoot == (common.Hash{}) {
			continue
		}

		blobCount, err := c.blobsStorage.KzgCommitmentsCount(c.ctx, blockRoot)
		if err != nil {
			return err
		}

		for i := 0; i < int(blobCount) && written < maxBlobsThroughoutputPerRequest; i++ {
			version := c.beaconConfig.GetCurrentStateVersion(slot / c.beaconConfig.SlotsPerEpoch)
			// Read the fork digest
			forkDigest, err := c.ethClock.ComputeForkDigestForVersion(utils.Uint32ToBytes4(c.beaconConfig.GetForkVersionByVersion(version)))
			if err != nil {
				return err
			}
			if _, err := s.Write([]byte{0}); err != nil {
				return err
			}
			if _, err := s.Write(forkDigest[:]); err != nil {
				return err
			}
			if err := c.blobsStorage.WriteStream(s, slot, blockRoot, uint64(i)); err != nil {
				return err
			}
			written++
		}
	}
	return nil
}

func (c *ConsensusHandlers) blobsSidecarsByIdsHandlerElectra(s network.Stream) error {
	return c.blobsSidecarsByIdsHandler(s, clparams.ElectraVersion)
}

func (c *ConsensusHandlers) blobsSidecarsByIdsHandlerDeneb(s network.Stream) error {
	return c.blobsSidecarsByIdsHandler(s, clparams.DenebVersion)
}

func (c *ConsensusHandlers) blobsSidecarsByIdsHandler(s network.Stream, version clparams.StateVersion) error {

	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](40269, 40)
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, version); err != nil {
		return err
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	written := 0
	for i := 0; i < req.Len() && written < maxBlobsThroughoutputPerRequest; i++ {

		id := req.Get(i)
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, id.BlockRoot)
		if err != nil {
			return err
		}
		if slot == nil {
			break
		}
		version := c.beaconConfig.GetCurrentStateVersion(*slot / c.beaconConfig.SlotsPerEpoch)
		// Read the fork digest
		forkDigest, err := c.ethClock.ComputeForkDigestForVersion(utils.Uint32ToBytes4(c.beaconConfig.GetForkVersionByVersion(version)))
		if err != nil {
			return err
		}
		if _, err := s.Write([]byte{0}); err != nil {
			return err
		}
		if _, err := s.Write(forkDigest[:]); err != nil {
			return err
		}
		if err := c.blobsStorage.WriteStream(s, *slot, id.BlockRoot, id.Index); err != nil {
			return err
		}
		written++
	}
	return nil
}
