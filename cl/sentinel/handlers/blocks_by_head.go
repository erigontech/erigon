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
	"github.com/libp2p/go-libp2p/core/network"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common/log/v3"
)

func (c *ConsensusHandlers) beaconBlocksByHeadHandler(s network.Stream) error {
	req := &cltypes.BeaconBlocksByHeadRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	count := req.Count
	if count > c.beaconConfig.MaxRequestBlocksDeneb {
		count = c.beaconConfig.MaxRequestBlocksDeneb
	}
	if count == 0 {
		return nil
	}

	if cost := int(count) - 1; !c.consumeRateLimit(s, cost) {
		return nil
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	currentRoot := req.BeaconRoot
	for i := uint64(0); i < count; i++ {
		block, err := c.beaconDB.ReadBlockByRoot(c.ctx, tx, currentRoot)
		if err != nil {
			return err
		}
		if block == nil && c.forkChoiceReader != nil {
			block, _ = c.forkChoiceReader.GetBlock(currentRoot)
		}
		if block == nil {
			log.Debug("[Sentinel] beaconBlocksByHead: block not found", "root", currentRoot)
			break
		}

		forkDigest, err := c.ethClock.ComputeForkDigest(block.Block.Slot / c.beaconConfig.SlotsPerEpoch)
		if err != nil {
			return err
		}

		if _, err := s.Write([]byte{SuccessfulResponsePrefix}); err != nil {
			return err
		}
		if _, err := s.Write(forkDigest[:]); err != nil {
			return err
		}
		if err := ssz_snappy.EncodeAndWrite(s, block); err != nil {
			return err
		}

		currentRoot = block.Block.ParentRoot
	}

	return nil
}
