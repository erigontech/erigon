// Copyright 2026 The Erigon Authors
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

package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/common"
)

func acquireBlockDataAvailability(ctx context.Context, peerDas das.PeerDas, block *cltypes.SignedBeaconBlock) error {
	return acquireBlocksDataAvailability(ctx, peerDas, []cltypes.ColumnSyncableSignedBlock{block})[0]
}

func acquireBlocksDataAvailability(ctx context.Context, peerDas das.PeerDas, blocks []cltypes.ColumnSyncableSignedBlock) []error {
	errs := make([]error, len(blocks))
	if len(blocks) == 0 {
		return errs
	}
	if peerDas == nil {
		for i := range errs {
			errs[i] = forkchoice.ErrEIP7594ColumnDataNotAvailable
		}
		return errs
	}

	roots := make([]common.Hash, len(blocks))
	missing := make([]cltypes.ColumnSyncableSignedBlock, 0, len(blocks))
	missingIndexes := make([]int, 0, len(blocks))
	for i, block := range blocks {
		blockRoot, err := block.BlockHashSSZ()
		if err != nil {
			errs[i] = err
			continue
		}
		roots[i] = common.Hash(blockRoot)
		available, err := peerDas.IsDataAvailable(block.GetSlot(), roots[i])
		if err != nil {
			errs[i] = fmt.Errorf("check data column availability: %w", err)
			continue
		}
		if !available {
			missing = append(missing, block)
			missingIndexes = append(missingIndexes, i)
		}
	}
	if len(missing) == 0 {
		return errs
	}

	var downloadErr error
	if peerDas.IsArchivedMode() {
		downloadErr = peerDas.DownloadColumnsAndRecoverBlobs(ctx, missing)
	} else {
		downloadErr = peerDas.DownloadOnlyCustodyColumns(ctx, missing)
	}
	if downloadErr != nil {
		for _, i := range missingIndexes {
			errs[i] = fmt.Errorf("download data columns: %w", downloadErr)
		}
		return errs
	}

	for _, i := range missingIndexes {
		available, err := peerDas.IsDataAvailable(blocks[i].GetSlot(), roots[i])
		switch {
		case err != nil:
			errs[i] = fmt.Errorf("recheck data column availability: %w", err)
		case !available:
			errs[i] = forkchoice.ErrEIP7594ColumnDataNotAvailable
		}
	}
	return errs
}

func acquireRecentBlocksDataAvailability(ctx context.Context, cfg *Cfg, blocks []*cltypes.SignedBeaconBlock) []error {
	errs := make([]error, len(blocks))
	required := make([]cltypes.ColumnSyncableSignedBlock, 0, len(blocks))
	requiredIndexes := make([]int, 0, len(blocks))
	for i, block := range blocks {
		commitments := block.GetBlobKzgCommitments()
		if commitments == nil || commitments.Len() == 0 || !requiresRecentBlockDataAvailability(cfg, block) {
			continue
		}
		required = append(required, block)
		requiredIndexes = append(requiredIndexes, i)
	}
	if len(required) == 0 {
		return errs
	}

	timeout := time.Duration(cfg.beaconCfg.SecondsPerSlot) * time.Second
	if timeout <= 0 {
		timeout = time.Second
	}
	downloadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	acquisitionErrs := acquireBlocksDataAvailability(downloadCtx, cfg.peerDas, required)
	for i, requiredIndex := range requiredIndexes {
		errs[requiredIndex] = acquisitionErrs[i]
	}
	return errs
}

func requiresRecentBlockDataAvailability(cfg *Cfg, block *cltypes.SignedBeaconBlock) bool {
	blockVersion := block.Version()
	if cfg.beaconCfg.SlotsPerEpoch != 0 {
		blockVersion = cfg.beaconCfg.GetCurrentStateVersion(block.Block.Slot / cfg.beaconCfg.SlotsPerEpoch)
	}
	return das.IsDataAvailabilityRequired(cfg.beaconCfg, cfg.ethClock.GetCurrentSlot(), block.Block.Slot, blockVersion)
}
