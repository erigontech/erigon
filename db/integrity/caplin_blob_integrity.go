// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

package integrity

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
)

// CheckCaplinBlobSidecars verifies frozen blob snapshot coverage, canonical identity, and cryptographic proofs.
func CheckCaplinBlobSidecars(ctx context.Context, db kv.RoDB, snapshots *freezeblocks.CaplinSnapshots, beaconCfg *clparams.BeaconChainConfig, failFast bool, logger log.Logger) error {
	if snapshots == nil {
		return fmt.Errorf("blob snapshot integrity: caplin snapshots are unavailable")
	}
	if beaconCfg == nil {
		return fmt.Errorf("blob snapshot integrity: beacon chain config is missing")
	}
	reader := freezeblocks.NewBeaconSnapshotReader(snapshots, nil, beaconCfg)
	view := snapshots.View()
	defer view.Close()

	var firstErr error
	report := func(err error) error {
		logger.Error("[integrity] CaplinBlobSidecars", "err", err)
		if failFast {
			return err
		}
		if firstErr == nil {
			firstErr = err
		}
		return nil
	}
	if coverageErr := checkCaplinBlobSnapshotCoverage(view, beaconCfg); coverageErr != nil {
		if reportErr := report(coverageErr); reportErr != nil {
			return reportErr
		}
	}
	return db.View(ctx, func(tx kv.Tx) error {
		for _, segment := range view.BlobSidecars() {
			from, to := segment.Src().GetRange()
			for slot := from; slot < to; slot++ {
				if slotErr := checkCaplinBlobSnapshotSlot(ctx, tx, reader, snapshots, slot); slotErr != nil {
					if errors.Is(slotErr, context.Canceled) || errors.Is(slotErr, context.DeadlineExceeded) {
						return slotErr
					}
					if reportErr := report(slotErr); reportErr != nil {
						return reportErr
					}
				}
			}
		}
		return firstErr
	})
}

func checkCaplinBlobSnapshotSlot(ctx context.Context, tx kv.Tx, reader freezeblocks.BeaconSnapshotReader, snapshots *freezeblocks.CaplinSnapshots, slot uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil {
		return fmt.Errorf("blob snapshot slot %d: read canonical root: %w", slot, err)
	}
	block, err := reader.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
	if err != nil {
		return fmt.Errorf("blob snapshot slot %d: read beacon block: %w", slot, err)
	}
	sidecars, err := snapshots.ReadBlobSidecars(slot)
	if err != nil {
		return fmt.Errorf("blob snapshot slot %d: read sidecars: %w", slot, err)
	}
	if block == nil {
		if canonicalRoot != (common.Hash{}) {
			return fmt.Errorf("blob snapshot slot %d: canonical block is missing from beacon snapshots", slot)
		}
		if len(sidecars) != 0 {
			return fmt.Errorf("blob snapshot slot %d: expected 0 sidecars, got %d", slot, len(sidecars))
		}
		return nil
	}
	if block.Block == nil || block.Block.Body == nil {
		return fmt.Errorf("blob snapshot slot %d: beacon block is incomplete", slot)
	}
	if canonicalRoot == (common.Hash{}) {
		return fmt.Errorf("blob snapshot slot %d: canonical block root is missing", slot)
	}

	beaconHeader, _, _, err := snapshots.ReadHeader(slot, tx)
	if err != nil {
		return fmt.Errorf("blob snapshot slot %d: read beacon header: %w", slot, err)
	}
	if beaconHeader == nil || beaconHeader.Header == nil {
		return fmt.Errorf("blob snapshot slot %d: beacon snapshot header is missing", slot)
	}
	beaconRoot, err := beaconHeader.Header.HashSSZ()
	if err != nil {
		return fmt.Errorf("blob snapshot slot %d: hash beacon header: %w", slot, err)
	}
	if beaconRoot != canonicalRoot {
		return fmt.Errorf("blob snapshot slot %d: beacon snapshot root does not match canonical root", slot)
	}

	expected := 0
	if commitments := block.GetBlobKzgCommitments(); commitments != nil {
		expected = commitments.Len()
	}
	if len(sidecars) != expected {
		return fmt.Errorf("blob snapshot slot %d: expected %d sidecars, got %d", slot, expected, len(sidecars))
	}
	for i, sidecar := range sidecars {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil {
			return fmt.Errorf("blob snapshot slot %d index %d: incomplete sidecar", slot, i)
		}
		if sidecar.Index != uint64(i) {
			return fmt.Errorf("blob snapshot slot %d: expected index %d, got %d", slot, i, sidecar.Index)
		}
		if sidecar.SignedBlockHeader.Header.Slot != slot {
			return fmt.Errorf("blob snapshot slot %d index %d: header has slot %d", slot, i, sidecar.SignedBlockHeader.Header.Slot)
		}
		if sidecar.SignedBlockHeader.Signature != beaconHeader.Signature {
			return fmt.Errorf("blob snapshot slot %d index %d: sidecar header signature does not match beacon snapshot", slot, i)
		}
		sidecarRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil {
			return fmt.Errorf("blob snapshot slot %d index %d: hash sidecar header: %w", slot, i, err)
		}
		if sidecarRoot != canonicalRoot {
			return fmt.Errorf("blob snapshot slot %d index %d: sidecar block root does not match canonical root", slot, i)
		}
	}
	if err := blob_storage.VerifyBlobSidecars(sidecars, block.Version(), nil); err != nil {
		return fmt.Errorf("blob snapshot slot %d: KZG/proof verification failed: %w", slot, err)
	}
	return nil
}

func checkCaplinBlobSnapshotCoverage(view *freezeblocks.CaplinView, beaconCfg *clparams.BeaconChainConfig) error {
	blobSegments := view.BlobSidecars()
	if beaconCfg.DenebForkEpoch == math.MaxUint64 {
		if len(blobSegments) != 0 {
			return fmt.Errorf("unexpected blob snapshot coverage before Deneb")
		}
		return nil
	}
	if beaconCfg.SlotsPerEpoch == 0 || beaconCfg.DenebForkEpoch > math.MaxUint64/beaconCfg.SlotsPerEpoch {
		return fmt.Errorf("blob snapshot coverage: invalid Deneb fork slot")
	}

	beaconSegments := view.BeaconBlocks()
	if len(beaconSegments) == 0 {
		if len(blobSegments) != 0 {
			return fmt.Errorf("blob snapshots exist without beacon snapshot coverage")
		}
		return nil
	}
	denebSlot := beaconCfg.DenebForkEpoch * beaconCfg.SlotsPerEpoch
	expectedFrom := denebSlot / snaptype.CaplinMergeLimit * snaptype.CaplinMergeLimit
	beaconTo := beaconSegments[len(beaconSegments)-1].To()
	requiredTo := (beaconTo - 1) / snaptype.CaplinMergeLimit * snaptype.CaplinMergeLimit

	next := expectedFrom
	for _, segment := range blobSegments {
		from, to := segment.Src().GetRange()
		if from < next {
			return fmt.Errorf("overlapping or pre-Deneb blob snapshot range %d-%d; expected next slot %d", from, to, next)
		}
		if from > next {
			return fmt.Errorf("missing blob snapshot coverage %d-%d", next, from)
		}
		if to <= from || to > beaconTo {
			return fmt.Errorf("invalid blob snapshot range %d-%d; beacon snapshots end at %d", from, to, beaconTo)
		}
		next = to
	}
	if next < requiredTo {
		return fmt.Errorf("missing blob snapshot coverage %d-%d", next, requiredTo)
	}
	return nil
}
