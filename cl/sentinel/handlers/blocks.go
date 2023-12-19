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
	"errors"
	"io"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/fork"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/libp2p/go-libp2p/core/network"
	"go.uber.org/zap/buffer"
)

const MAX_REQUEST_BLOCKS = 96

func (c *ConsensusHandlers) beaconBlocksByRangeHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "beaconBlocksByRange", rateLimits.beaconBlocksByRangeLimit); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}

	req := &cltypes.BeaconBlocksByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	if req.Step != 1 {
		return errors.New("step must be 1")
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Limit the number of blocks to the count specified in the request.
	if int(req.Count) > MAX_REQUEST_BLOCKS {
		req.Count = MAX_REQUEST_BLOCKS
	}

	beaconBlockRooots, slots, err := beacon_indicies.ReadBeaconBlockRootsInSlotRange(c.ctx, tx, req.StartSlot, req.Count)
	if err != nil {
		return err
	}

	// Read the fork digest
	forkDigest, err := fork.ComputeForkDigestForVersion(
		utils.Uint32ToBytes4(c.beaconConfig.GenesisForkVersion),
		c.genesisConfig.GenesisValidatorRoot,
	)
	if err != nil {
		return err
	}

	for i, slot := range slots {
		r, err := c.beaconDB.BlockReader(c.ctx, slot, beaconBlockRooots[i])
		if err != nil {
			return err
		}
		defer r.Close()

		if _, err := s.Write(forkDigest[:]); err != nil {
			return err
		}
		_, err = io.Copy(s, r)
		if err != nil {
			return err
		}
	}
	return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavaiablePrefix)
}

func (c *ConsensusHandlers) beaconBlocksByRootHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()
	if err := c.checkRateLimit(peerId, "beaconBlocksByRoot", rateLimits.beaconBlocksByRootLimit); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}

	var req solid.HashListSSZ = solid.NewHashList(100)
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.Phase0Version); err != nil {
		return err
	}

	blockRoots := []libcommon.Hash{}
	for i := 0; i < req.Length(); i++ {
		blockRoot := req.Get(i)
		blockRoots = append(blockRoots, blockRoot)
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Limit the number of blocks to the count specified in the request.
	if len(blockRoots) > MAX_REQUEST_BLOCKS {
		blockRoots = blockRoots[:MAX_REQUEST_BLOCKS]
	}

	// Read the fork digest
	forkDigest, err := fork.ComputeForkDigestForVersion(
		utils.Uint32ToBytes4(c.beaconConfig.GenesisForkVersion),
		c.genesisConfig.GenesisValidatorRoot,
	)
	if err != nil {
		return err
	}

	for i, blockRoot := range blockRoots {
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		if err != nil {
			return err
		}

		r, err := c.beaconDB.BlockReader(c.ctx, *slot, blockRoots[i])
		if err != nil {
			return err
		}
		defer r.Close()

		// Read block
		var resBuf buffer.Buffer
		block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig)
		if err := ssz_snappy.DecodeAndReadNoForkDigest(r, block, clparams.Phase0Version); err != nil {
			return err
		}
		ssz_snappy.EncodeAndWrite(&resBuf, block)
		resData := libcommon.CopyBytes(resBuf.Bytes())

		if _, err := s.Write(forkDigest[:]); err != nil {
			return err
		}
		s.Write(resData)
	}
	return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavaiablePrefix)
}

type emptyString struct{}

func (e *emptyString) EncodeSSZ(xs []byte) ([]byte, error) {
	return append(xs, 0), nil
}

func (e *emptyString) EncodingSizeSSZ() int {
	return 1
}
