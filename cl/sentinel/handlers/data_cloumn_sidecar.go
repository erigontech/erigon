package handlers

import (
	"errors"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/libp2p/go-libp2p/core/network"
)

var errInvalidDataColumnIndex = errors.New("invalid column index")

func writeDataColumnSidecarsEmptySuccess(s network.Stream) error {
	return nil
}

func (c *ConsensusHandlers) dataColumnSidecarsByRangeHandler(s network.Stream) error {
	curEpoch := c.ethClock.GetCurrentEpoch()

	// Use current epoch's version for decoding (supports Fulu and GLOAS)
	version := c.beaconConfig.GetCurrentStateVersion(curEpoch)
	req := &cltypes.ColumnSidecarsByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, version); err != nil {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}
	if curEpoch < c.beaconConfig.FuluForkEpoch {
		return writeDataColumnSidecarsEmptySuccess(s)
	}

	// check params.
	var (
		fuluStartSlot = c.beaconConfig.FuluForkEpoch * c.beaconConfig.SlotsPerEpoch
		endSlot       = req.StartSlot + req.Count
	)
	if endSlot < req.StartSlot {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}
	if req.Columns.Length() > int(c.beaconConfig.NumberOfColumns) {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}
	if err := solid.RangeErr(req.Columns, func(index int, columnIndex uint64, length int) error {
		if columnIndex >= c.beaconConfig.NumberOfColumns {
			return errInvalidDataColumnIndex
		}
		return nil
	}); err != nil {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}
	if req.Count == 0 || req.Columns.Length() == 0 || endSlot <= fuluStartSlot {
		return writeDataColumnSidecarsEmptySuccess(s)
	}
	startSlot := max(req.StartSlot, fuluStartSlot)
	if endSlot-startSlot > c.beaconConfig.MinEpochsForDataColumnSidecarsRequests*c.beaconConfig.SlotsPerEpoch {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}

	// Consume additional rate-limit tokens: slots × columns per slot, capped at config max.
	if cost := dataColumnSidecarsRequestCost(endSlot-startSlot, uint64(req.Columns.Length()), c.beaconConfig.MaxRequestDataColumnSidecars); !c.consumeRateLimit(s, cost) {
		return nil
	}

	curSlot := c.ethClock.GetCurrentSlot()
	if startSlot > curSlot {
		return writeDataColumnSidecarsEmptySuccess(s)
	}

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	count := 0
	var responseErr error
	for slot := startSlot; slot < endSlot; slot++ {
		if slot > curSlot {
			// slot is in the future
			break
		}

		// check if epoch is after fulu fork
		epoch := slot / c.beaconConfig.SlotsPerEpoch
		if c.beaconConfig.GetCurrentStateVersion(epoch) < clparams.FuluVersion {
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

			exists, err := c.dataColumnStorage.ColumnSidecarExists(c.ctx, slot, blockRoot, int64(columnIndex))
			if err != nil {
				log.Debug("failed to check if data column sidecar exists", "error", err)
				responseErr = err
				return false
			}
			if !exists {
				// skip
				return true
			}

			forkDigest, err := c.ethClock.ComputeForkDigest(slot / c.beaconConfig.SlotsPerEpoch)
			if err != nil {
				log.Debug("failed to compute fork digest", "error", err)
				responseErr = err
				return false
			}
			if _, err := s.Write([]byte{SuccessfulResponsePrefix}); err != nil {
				log.Debug("failed to write success byte", "error", err)
				responseErr = err
				return false
			}

			if _, err := s.Write(forkDigest[:]); err != nil {
				log.Debug("failed to write fork digest", "error", err)
				responseErr = err
				return false
			}

			if err := c.dataColumnStorage.WriteStream(s, slot, blockRoot, columnIndex); err != nil {
				log.Debug("failed to write stream data column sidecar", "error", err)
				responseErr = err
				return false
			}
			count++
			return true
		})
		if responseErr != nil {
			break
		}
		if count >= int(c.beaconConfig.MaxRequestDataColumnSidecars) {
			// max number of sidecars reached
			break
		}
	}
	if responseErr != nil {
		return responseErr
	}
	if count == 0 {
		return writeDataColumnSidecarsEmptySuccess(s)
	}
	return nil
}

func (c *ConsensusHandlers) dataColumnSidecarsByRootHandler(s network.Stream) error {
	curEpoch := c.ethClock.GetCurrentEpoch()

	// Use current epoch's version for decoding (supports Fulu and GLOAS)
	version := c.beaconConfig.GetCurrentStateVersion(curEpoch)
	req := solid.NewDynamicListSSZ[*cltypes.DataColumnsByRootIdentifier](int(c.beaconConfig.MaxRequestBlocksDeneb))
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, version); err != nil {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}
	if req.Len() > int(c.beaconConfig.MaxRequestBlocksDeneb) {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}

	// Consume additional rate-limit tokens: sum of column counts across all roots.
	totalColumns := 0
	for i := 0; i < req.Len(); i++ {
		columns := req.Get(i).Columns
		if columns.Length() > int(c.beaconConfig.NumberOfColumns) {
			return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
		}
		if err := solid.RangeErr(columns, func(index int, columnIndex uint64, length int) error {
			if columnIndex >= c.beaconConfig.NumberOfColumns {
				return errInvalidDataColumnIndex
			}
			return nil
		}); err != nil {
			return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
		}
		totalColumns += columns.Length()
	}
	if curEpoch < c.beaconConfig.FuluForkEpoch {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}
	if totalColumns == 0 {
		return writeDataColumnSidecarsEmptySuccess(s)
	}
	if cost := dataColumnSidecarsRequestCost(1, uint64(totalColumns), c.beaconConfig.MaxRequestDataColumnSidecars); !c.consumeRateLimit(s, cost) {
		return nil
	}

	curSlot := c.ethClock.GetCurrentSlot()
	curEpoch = curSlot / c.beaconConfig.SlotsPerEpoch

	tx, err := c.indiciesDB.BeginRo(c.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	count := 0
	var responseErr error
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

			exists, err := c.dataColumnStorage.ColumnSidecarExists(c.ctx, *slot, blockRoot, int64(columnIndex))
			if err != nil {
				log.Debug("failed to check if data column sidecar exists", "error", err)
				responseErr = err
				return false
			}
			if !exists {
				// skip
				return true
			}

			forkDigest, err := c.ethClock.ComputeForkDigest(*slot / c.beaconConfig.SlotsPerEpoch)
			if err != nil {
				log.Debug("failed to compute fork digest", "error", err)
				responseErr = err
				return false
			}
			if _, err := s.Write([]byte{SuccessfulResponsePrefix}); err != nil {
				log.Debug("failed to write success byte", "error", err)
				responseErr = err
				return false
			}

			if _, err := s.Write(forkDigest[:]); err != nil {
				log.Debug("failed to write fork digest", "error", err)
				responseErr = err
				return false
			}

			if err := c.dataColumnStorage.WriteStream(s, *slot, blockRoot, columnIndex); err != nil {
				log.Debug("failed to write stream data column sidecar", "error", err)
				responseErr = err
				return false
			}
			count++
			return true
		})
		if responseErr != nil {
			break
		}
	}
	if responseErr != nil {
		return responseErr
	}
	if count == 0 {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, ResourceUnavailablePrefix)
	}
	return nil
}

func dataColumnSidecarsRequestCost(slots, columns, maxSidecars uint64) int {
	if slots == 0 || columns == 0 || maxSidecars == 0 {
		return 0
	}
	if slots > maxSidecars/columns {
		return int(maxSidecars) - 1
	}
	sidecars := slots * columns
	if sidecars > maxSidecars {
		sidecars = maxSidecars
	}
	return int(sidecars) - 1
}
