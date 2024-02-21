/*
   Copyright 2022 Erigon-Lightclient contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package handlers

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/libp2p/go-libp2p/core/network"
)

const (
	MaxRequestsBlocks = 96
)

func (c *ConsensusHandlers) beaconBlocksByRangeHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()

	req := &cltypes.BeaconBlocksByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}
	if err := c.checkRateLimit(peerId, "beaconBlocksByRange", rateLimits.beaconBlocksByRangeLimit, int(req.Count)); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
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

		version := c.beaconConfig.GetCurrentStateVersion(slot / c.beaconConfig.SlotsPerEpoch)
		// Read the fork digest
		forkDigest, err := fork.ComputeForkDigestForVersion(
			utils.Uint32ToBytes4(c.beaconConfig.GetForkVersionByVersion(version)),
			c.genesisConfig.GenesisValidatorRoot,
		)
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
	peerId := s.Conn().RemotePeer().String()

	var req solid.HashListSSZ = solid.NewHashList(100)
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}
	if err := c.checkRateLimit(peerId, "beaconBlocksByRoot", rateLimits.beaconBlocksByRootLimit, req.Length()); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}

	blockRoots := []libcommon.Hash{}
	for i := 0; i < req.Length(); i++ {
		blockRoot := req.Get(i)
		blockRoots = append(blockRoots, blockRoot)
		// Limit the number of blocks to the count specified in the request.
		if len(blockRoots) >= MaxRequestsBlocks {
			break
		}
	}

	if len(blockRoots) == 0 {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavaiablePrefix)
	}
	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var block *cltypes.SignedBeaconBlock
	req.Range(func(index int, blockRoot libcommon.Hash, length int) bool {
		block, err = c.beaconDB.ReadBlockByRoot(c.ctx, tx, blockRoot)
		if err != nil {
			return false
		}
		if block == nil {
			return true
		}

		// Read the fork digest
		var forkDigest [4]byte
		forkDigest, err = fork.ComputeForkDigestForVersion(
			utils.Uint32ToBytes4(c.beaconConfig.GetForkVersionByVersion(block.Version())),
			c.genesisConfig.GenesisValidatorRoot,
		)
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
