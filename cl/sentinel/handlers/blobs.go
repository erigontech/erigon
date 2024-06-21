package handlers

import (
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/clparams"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/cltypes/solid"
	"github.com/ledgerwatch/erigon/cl/persistence/beacon_indicies"
	"github.com/ledgerwatch/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/ledgerwatch/erigon/cl/utils"
	"github.com/libp2p/go-libp2p/core/network"
)

const maxBlobsThroughoutputPerRequest = 72

func (c *ConsensusHandlers) blobsSidecarsByRangeHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()

	req := &cltypes.BlobsByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.DenebVersion); err != nil {
		return err
	}
	if err := c.checkRateLimit(peerId, "blobSidecar", rateLimits.blobSidecarsLimit, int(req.Count)); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
		return err
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	written := 0
	for slot := req.StartSlot; slot < req.StartSlot+req.Count; slot++ {
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return err
		}
		if blockRoot == (libcommon.Hash{}) {
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

func (c *ConsensusHandlers) blobsSidecarsByIdsHandler(s network.Stream) error {
	peerId := s.Conn().RemotePeer().String()

	req := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](40269, 40)
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.DenebVersion); err != nil {
		return err
	}

	if err := c.checkRateLimit(peerId, "blobSidecar", rateLimits.blobSidecarsLimit, req.Len()); err != nil {
		ssz_snappy.EncodeAndWrite(s, &emptyString{}, RateLimitedPrefix)
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
