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

package main

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/cl/blobfetch"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network"
)

// storeBlobsForBlock fetches and stores the sidecars a block needs, and fails if the store does
// not hold all of them afterwards.
func (c *ChainEndpoint) storeBlobsForBlock(
	ctx context.Context,
	blobDB blob_storage.BlobStorage,
	beaconConfig *clparams.BeaconChainConfig,
	block, child *cltypes.SignedBeaconBlock,
) error {
	if !c.Blobs || expectedBlobsForChild(block, child) == 0 {
		return nil
	}
	commitments := block.Block.Body.GetBlobKzgCommitments()
	ids, err := network.BlobsIdentifiersFromBlocks([]*cltypes.SignedBeaconBlock{block}, beaconConfig)
	if err != nil {
		return fmt.Errorf("failed to get blob identifiers at slot %d: %w", block.Block.Slot, err)
	}
	// BlobsIdentifiersFromBlocks drops a whole block once its commitments exceed the per-request
	// cap, and the remote block is decoded but never consensus-validated, so the endpoint chooses
	// this count. Without parity the checks below would range over an empty list and pass.
	if ids.Len() != commitments.Len() {
		return fmt.Errorf("got %d blob identifiers for %d commitments at slot %d", ids.Len(), commitments.Len(), block.Block.Slot)
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return err
	}
	blobs, err := blobfetch.Retrieve(ctx, c.Endpoint, blockRoot, block)
	if err != nil {
		return fmt.Errorf("failed to retrieve blobs at slot %d, root %x: %w", block.Block.Slot, blockRoot, err)
	}
	if err := blobfetch.Store(ctx, blobDB, ids, blobs, block); err != nil {
		return fmt.Errorf("failed to verify and store blobs at slot %d: %w", block.Block.Slot, err)
	}
	return nil
}

func expectedBlobsForChild(block, child *cltypes.SignedBeaconBlock) int {
	n := blobfetch.BlobCount(block)
	if n == 0 || block.Version() < clparams.GloasVersion || child == nil || forkchoice.ParentPayloadStatusFromBids(block, child.Block) == cltypes.PayloadStatusFull {
		return n
	}
	return 0
}
