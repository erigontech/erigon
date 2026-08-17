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
	if peerDas == nil {
		return forkchoice.ErrEIP7594ColumnDataNotAvailable
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	available, err := peerDas.IsDataAvailable(block.Block.Slot, common.Hash(blockRoot))
	if err != nil {
		return fmt.Errorf("check data column availability: %w", err)
	}
	if available {
		return nil
	}
	blocks := []cltypes.ColumnSyncableSignedBlock{block}
	if peerDas.IsArchivedMode() {
		err = peerDas.DownloadColumnsAndRecoverBlobs(ctx, blocks)
	} else {
		err = peerDas.DownloadOnlyCustodyColumns(ctx, blocks)
	}
	if err != nil {
		return fmt.Errorf("download data columns: %w", err)
	}
	available, err = peerDas.IsDataAvailable(block.Block.Slot, common.Hash(blockRoot))
	if err != nil {
		return fmt.Errorf("recheck data column availability: %w", err)
	}
	if !available {
		return forkchoice.ErrEIP7594ColumnDataNotAvailable
	}
	return nil
}

func acquireRecentBlockDataAvailability(ctx context.Context, cfg *Cfg, block *cltypes.SignedBeaconBlock) error {
	commitments := block.GetBlobKzgCommitments()
	if commitments == nil || commitments.Len() == 0 || !requiresRecentBlockDataAvailability(cfg, block) {
		return nil
	}
	timeout := time.Duration(cfg.beaconCfg.SecondsPerSlot) * time.Second
	if timeout <= 0 {
		timeout = time.Second
	}
	downloadCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return acquireBlockDataAvailability(downloadCtx, cfg.peerDas, block)
}

func requiresRecentBlockDataAvailability(cfg *Cfg, block *cltypes.SignedBeaconBlock) bool {
	return das.IsDataAvailabilityRequired(cfg.beaconCfg, cfg.ethClock.GetCurrentSlot(), block.Block.Slot, block.Version())
}
