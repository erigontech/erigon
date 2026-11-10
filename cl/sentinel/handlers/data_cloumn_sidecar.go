package handlers

import (
	"context"
	"errors"
	"slices"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/sentinel/communication/ssz_snappy"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/libp2p/go-libp2p/core/network"
)

var errInvalidDataColumnIndex = errors.New("invalid column index")

func (c *ConsensusHandlers) dataColumnSidecarsByRangeHandler(s network.Stream) error {
	requestCtx, cancel := context.WithTimeout(c.ctx, StreamDeadline)
	defer cancel()
	curEpoch := c.ethClock.GetCurrentEpoch()

	// Use current epoch's version for decoding (supports Fulu and GLOAS)
	version := c.beaconConfig.GetCurrentStateVersion(curEpoch)
	req := &cltypes.ColumnSidecarsByRangeRequest{}
	if err := ssz_snappy.DecodeAndReadNoForkDigest(s, req, version); err != nil {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}
	if curEpoch < c.beaconConfig.FuluForkEpoch {
		return nil
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
		return nil
	}
	startSlot := max(req.StartSlot, fuluStartSlot)
	if endSlot-startSlot > c.beaconConfig.MinEpochsForDataColumnSidecarsRequests*c.beaconConfig.SlotsPerEpoch {
		return ssz_snappy.EncodeAndWrite(s, &emptyString{}, InvalidRequestPrefix)
	}

	// Consume additional rate-limit tokens: slots × columns per slot, capped at config max.
	if cost := requestCost(endSlot-startSlot, uint64(req.Columns.Length()), c.beaconConfig.MaxRequestDataColumnSidecars); !c.consumeRateLimit(s, cost) {
		return nil
	}

	curSlot := c.ethClock.GetCurrentSlot()
	if startSlot > curSlot {
		return nil
	}

	tx, err := c.indiciesDB.BeginRo(requestCtx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return c.writeColumnsByRange(requestCtx, s, tx, req, startSlot, endSlot, curSlot)
}

// writeColumnsByRange serves whole blocks. Columns a block does not store take none of the response limit.
func (c *ConsensusHandlers) writeColumnsByRange(ctx context.Context, s network.Stream, tx kv.Tx, req *cltypes.ColumnSidecarsByRangeRequest, startSlot, endSlot, curSlot uint64) error {
	requested := make([]uint64, 0, req.Columns.Length())
	req.Columns.Range(func(_ int, columnIndex uint64, _ int) bool {
		requested = append(requested, columnIndex)
		return true
	})
	slices.Sort(requested)
	requested = slices.Compact(requested)
	served := uint64(0)
	for slot := startSlot; slot < endSlot && served < c.beaconConfig.MaxRequestDataColumnSidecars && ctx.Err() == nil; slot++ {
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

		columns := make([]uint64, 0, len(requested))
		for _, columnIndex := range requested {
			if ctx.Err() != nil {
				break
			}
			var exists bool
			if exists, err = c.dataColumnStorage.ColumnSidecarExists(ctx, slot, blockRoot, int64(columnIndex)); err != nil {
				break
			}
			if exists {
				columns = append(columns, columnIndex)
			}
		}
		if ctx.Err() != nil {
			break
		}
		if err != nil {
			log.Debug("failed to check if data column sidecar exists", "error", err)
			return err
		}
		if len(columns) == 0 {
			continue
		}
		if served > 0 && served+uint64(len(columns)) > c.beaconConfig.MaxRequestDataColumnSidecars {
			break
		}

		forkDigest, err := c.ethClock.ComputeForkDigest(epoch)
		if err != nil {
			log.Debug("failed to compute fork digest", "error", err)
			return err
		}
		for _, columnIndex := range columns {
			if _, err := s.Write([]byte{SuccessfulResponsePrefix}); err != nil {
				log.Debug("failed to write success byte", "error", err)
				return err
			}
			if _, err := s.Write(forkDigest[:]); err != nil {
				log.Debug("failed to write fork digest", "error", err)
				return err
			}
			if err := c.dataColumnStorage.WriteStream(s, slot, blockRoot, columnIndex); err != nil {
				log.Debug("failed to write stream data column sidecar", "error", err)
				return err
			}
		}
		served += uint64(len(columns))
	}
	if served == 0 {
		return ctx.Err() // an aborted scan must not look like an empty range
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
		return nil
	}
	if cost := requestCost(1, uint64(totalColumns), c.beaconConfig.MaxRequestDataColumnSidecars); !c.consumeRateLimit(s, cost) {
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
	return responseErr
}

// requestCost returns the estimated additional tokens to charge after admission.
// Check the cap by division so large counts cannot overflow the multiplication.
func requestCost(count, multiplier, limit uint64) int {
	if count == 0 || multiplier == 0 || limit == 0 {
		return 0
	}
	if count > limit/multiplier {
		return int(limit) - 1
	}
	return int(count*multiplier) - 1
}
