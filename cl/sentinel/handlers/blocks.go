// Copyright 2022 The Erigon Authors
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
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/libp2p/go-libp2p/core/network"
)

const (
	MaxRequestsBlocks = 96
)

func (c *ConsensusHandlers) beaconBlocksByRangeHandler(s network.Stream) error {

	req := &cltypes.BeaconBlocksByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	written := uint64(0)
	for slot := req.StartSlot; slot < req.StartSlot+MaxRequestsBlocks; slot++ {
		block, err := c.beaconDB.ReadBlockBySlot(c.ctx, tx, slot)
		if err != nil {
			return err
		}
		if block == nil {
			continue
		}

		// Read the fork digest
		forkDigest, err := c.ethClock.ComputeForkDigest(slot / c.beaconConfig.SlotsPerEpoch)
		if err != nil {
			return err
		}

		if _, err := s.Write([]byte{0}); err != nil {
			return err
		}
		if _, err := s.Write(forkDigest[:]); err != nil {
			return err
		}
		// obtain block and write
		if err := ssz_snappy.EncodeAndWrite(s, block); err != nil {
			return err
		}
		written++
		if written >= req.Count {
			break
		}
	}

	return nil
}

func (c *ConsensusHandlers) beaconBlocksByRootHandler(s network.Stream) error {

	var req solid.HashListSSZ = solid.NewHashList(100)
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	blockRoots := []common.Hash{}
	for i := 0; i < req.Length(); i++ {
		blockRoot := req.Get(i)
		blockRoots = append(blockRoots, blockRoot)
		// Limit the number of blocks to the count specified in the request.
		if len(blockRoots) >= MaxRequestsBlocks {
			break
		}
	}

	if len(blockRoots) == 0 {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}
	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var block *cltypes.SignedBeaconBlock
	req.Range(func(index int, blockRoot common.Hash, length int) bool {
		block, err = c.beaconDB.ReadBlockByRoot(c.ctx, tx, blockRoot)
		if err != nil {
			return false
		}
		if block == nil {
			return true
		}

		// Read the fork digest
		forkDigest, err := c.ethClock.ComputeForkDigest(block.Block.Slot / c.beaconConfig.SlotsPerEpoch)
		if err != nil {
			return false
		}

		if _, err = s.Write([]byte{0}); err != nil {
			return false
		}

		if _, err = s.Write(forkDigest[:]); err != nil {
			return false
		}
		// obtain block and write
		if err = ssz_snappy.EncodeAndWrite(s, block); err != nil {
			return false
		}
		return true
	})

	return err
}

type emptyString struct{}

func (e *emptyString) EncodeSSZ(xs []byte) ([]byte, error) {
	return append(xs, 0), nil
}

func (e *emptyString) EncodingSizeSSZ() int {
	return 1
}
