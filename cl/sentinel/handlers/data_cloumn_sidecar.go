package handlers

import (
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/libp2p/go-libp2p/core/network"
)

func (c *ConsensusHandlers) dataColumnSidecarsByRangeHandler(s network.Stream) error {
	if c.ethClock.GetCurrentEpoch() < c.beaconConfig.FuluForkEpoch {
		return nil
	}

	req := &cltypes.ColumnSidecarsByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.FuluVersion); err != nil {
		return err
	}

	curSlot := c.ethClock.GetCurrentSlot()
	curEpoch := curSlot / c.beaconConfig.SlotsPerEpoch

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	count := 0

	for slot := req.StartSlot; slot < req.StartSlot+req.Count; slot++ {
		if slot > curSlot {
			// slot is in the future
			break
		}

		// check if epoch is after fulu fork
		epoch := slot / c.beaconConfig.SlotsPerEpoch
		if c.beaconConfig.GetCurrentStateVersion(epoch) < clparams.FuluVersion {
			continue
		}

		// check if epoch is too far
		if curEpoch-epoch > c.beaconConfig.MinEpochsForDataColumnSidecarsRequests {
			continue
		}

		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return err
		}
		if blockRoot == (common.Hash{}) {
			continue
		}

		req.Columns.Range(func(index int, columnIndex uint64, length int) bool {
			if count >= int(c.beaconConfig.MaxRequestDataColumnSidecars) {
				// max number of sidecars reached
				return false
			}
			if columnIndex >= c.beaconConfig.NumberOfColumns {
				// skip invalid column index
				return true
			}

			exists, err := c.dataColumnStorage.ColumnSidecarExists(c.ctx, slot, blockRoot, int64(columnIndex))
			if err != nil {
				log.Debug("failed to check if data column sidecar exists", "error", err)
				return false
			}
			if !exists {
				// skip
				return true
			}

			forkDigest, err := c.ethClock.ComputeForkDigest(slot / c.beaconConfig.SlotsPerEpoch)
			if err != nil {
				log.Debug("failed to compute fork digest", "error", err)
				return false
			}
			if _, err := s.Write([]byte{SuccessfulResponsePrefix}); err != nil {
				log.Debug("failed to write success byte", "error", err)
				return false
			}

			if _, err := s.Write(forkDigest[:]); err != nil {
				log.Debug("failed to write fork digest", "error", err)
				return false
			}

			if err := c.dataColumnStorage.WriteStream(s, slot, blockRoot, columnIndex); err != nil {
				log.Debug("failed to write stream data column sidecar", "error", err)
				return false
			}
			count++
			return true
		})
	}

	return nil
}

func (c *ConsensusHandlers) dataColumnSidecarsByRootHandler(s network.Stream) error {
	if c.ethClock.GetCurrentEpoch() < c.beaconConfig.FuluForkEpoch {
		return nil
	}

	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(c.beaconConfig.MaxRequestBlocksDeneb))
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, clparams.FuluVersion); err != nil {
		return err
	}

	curSlot := c.ethClock.GetCurrentSlot()
	curEpoch := curSlot / c.beaconConfig.SlotsPerEpoch

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	count := 0
	for i := 0; i < req.Len(); i++ {
		id := req.Get(i)
		blockRoot := id.BlockRoot
		columns := id.Columns

		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, blockRoot)
		if err != nil {
			return err
		}
		if slot == nil {
			log.Trace("block root not found", "block_root", blockRoot)
			continue
		}

		// check if epoch is after fulu fork
		epoch := *slot / c.beaconConfig.SlotsPerEpoch
		if c.beaconConfig.GetCurrentStateVersion(epoch) < clparams.FuluVersion {
			log.Trace("epoch is before fulu fork", "epoch", epoch, "block_root", blockRoot)
			continue
		}

		// check if epoch is too far
		if curEpoch-epoch > c.beaconConfig.MinEpochsForDataColumnSidecarsRequests {
			continue
		}

		columns.Range(func(index int, columnIndex uint64, length int) bool {
			if count >= int(c.beaconConfig.MaxRequestDataColumnSidecars) {
				// max number of sidecars reached
				return false
			}
			if columnIndex >= c.beaconConfig.NumberOfColumns {
				// skip invalid column index
				return true
			}

			exists, err := c.dataColumnStorage.ColumnSidecarExists(c.ctx, *slot, blockRoot, int64(columnIndex))
			if err != nil {
				log.Debug("failed to check if data column sidecar exists", "error", err)
				return false
			}
			if !exists {
				// skip
				return true
			}

			forkDigest, err := c.ethClock.ComputeForkDigest(*slot / c.beaconConfig.SlotsPerEpoch)
			if err != nil {
				log.Debug("failed to compute fork digest", "error", err)
				return false
			}
			if _, err := s.Write([]byte{SuccessfulResponsePrefix}); err != nil {
				log.Debug("failed to write success byte", "error", err)
				return false
			}

			if _, err := s.Write(forkDigest[:]); err != nil {
				log.Debug("failed to write fork digest", "error", err)
				return false
			}

			if err := c.dataColumnStorage.WriteStream(s, *slot, blockRoot, columnIndex); err != nil {
				log.Debug("failed to write stream data column sidecar", "error", err)
				return false
			}
			count++
			return true
		})
	}
	return nil
}
