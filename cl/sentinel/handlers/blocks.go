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
	"errors"

	"github.com/libp2p/go-libp2p/core/network"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/format/snapshot_format"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	MaxRequestsBlocks = 96
)

func (c *ConsensusHandlers) beaconBlocksByRangeHandler(s network.Stream) error {
	req := &cltypes.BeaconBlocksByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	if req.StartSlot+req.Count < req.StartSlot {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}

	if cost := requestCost(req.Count, 1, MaxRequestsBlocks); !c.consumeRateLimit(s, cost) {
		return nil
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Serve only blocks in [start_slot, start_slot+count): empty slots are left out, not replaced by
	// later blocks. Search up to MAX_REQUEST_BLOCKS_DENEB slots and answer with at most
	// MaxRequestsBlocks blocks; the spec allows a response with fewer blocks than requested.
	endSlot := req.StartSlot + min(req.Count, c.beaconConfig.MaxRequestBlocksDeneb)
	written := 0
	for slot := req.StartSlot; slot < endSlot && written < MaxRequestsBlocks; slot++ {
		block, err := c.beaconDB.ReadBlockBySlot(c.ctx, tx, slot)
		if err != nil {
			if errors.Is(err, snapshot_format.ErrExecutionBodyNotFound) {
				break
			}
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
	}

	return nil
}

func (c *ConsensusHandlers) beaconBlocksByRootHandler(s network.Stream) error {
	// BeaconBlocksByRoot v2 keeps the original request limit until Deneb activates.
	maxRoots := c.beaconConfig.MaxRequestBlocks
	if c.ethClock.GetCurrentEpoch() >= c.beaconConfig.DenebForkEpoch {
		maxRoots = c.beaconConfig.MaxRequestBlocksDeneb
	}
	var req solid.HashListSSZ = solid.NewHashList(int(maxRoots))
	if err := ssz_snappy.DecodeAndReadNoForkDigestMax(s, req, clparams.Phase0Version, maxRoots*length.Hash); err != nil {
		if errors.Is(err, ssz_snappy.ErrPayloadExceedsLimit) {
			return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
		}
		return err
	}

	if req.Length() == 0 {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}

	// Cap the charge at the response limit so peers retain credit for the remaining roots.
	if cost := requestCost(uint64(req.Length()), 1, MaxRequestsBlocks); !c.consumeRateLimit(s, cost) {
		return nil
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	seen := make(map[common.Hash]struct{}, req.Length())
	// Only successfully written blocks count toward the response limit; missing or
	// duplicate roots must not prevent later known blocks from being served.
	written := 0
	for i := 0; i < req.Length() && written < MaxRequestsBlocks; i++ {
		blockRoot := req.Get(i)
		if _, ok := seen[blockRoot]; ok {
			continue
		}
		seen[blockRoot] = struct{}{}
		block, err := c.beaconDB.ReadBlockByRoot(c.ctx, tx, blockRoot)
		if errors.Is(err, snapshot_format.ErrExecutionBodyNotFound) {
			block, err = nil, nil
		}
		if err != nil {
			return err
		}
		// If the block is not in the database, check the fork choice store.
		// Recently received blocks (e.g. via gossip) may not have been persisted yet.
		if block == nil && c.forkChoiceReader != nil {
			block, _ = c.forkChoiceReader.GetBlock(blockRoot)
		}
		if block == nil {
			log.Debug("[Sentinel] beaconBlocksByRoot: block not found", "root", blockRoot)
			continue
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
		written++
	}

	return nil
}

type emptyString struct{}

func (e *emptyString) EncodeSSZ(xs []byte) ([]byte, error) {
	return append(xs, 0), nil
}

func (e *emptyString) EncodingSizeSSZ() int {
	return 1
}
