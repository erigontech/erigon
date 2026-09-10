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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
)

// storeBlobsForBlock fetches and stores the sidecars a block needs, and fails if the store does
// not hold all of them afterwards. Every block the chain download commits goes through here,
// including the bootstrap head.
func (c *ChainEndpoint) storeBlobsForBlock(
	ctx context.Context,
	blobDB blob_storage.BlobStorage,
	beaconConfig *clparams.BeaconChainConfig,
	baseUriBlob string,
	block *cltypes.SignedBeaconBlock,
) error {
	commitments := block.Block.Body.GetBlobKzgCommitments()
	if !c.Blobs || commitments == nil || commitments.Len() == 0 {
		return nil
	}
	ids, err := network.BlobsIdentifiersFromBlocks([]*cltypes.SignedBeaconBlock{block}, beaconConfig)
	if err != nil {
		return fmt.Errorf("failed to get blob identifiers: %w", err)
	}
	blobs, err := retrieveBlobsFromRemoteEndpoint(ctx, beaconConfig, baseUriBlob, block)
	if err != nil {
		return fmt.Errorf("failed to retrieve blobs: %w, uri: %s", err, baseUriBlob)
	}
	if err := storeRemoteBlobs(ctx, blobDB, ids, blobs, block.Signature, block.Block.Slot); err != nil {
		return fmt.Errorf("failed to verify and store blobs at slot %d: %w", block.Block.Slot, err)
	}
	return nil
}

// storeRemoteBlobs verifies a remote blob response against the requested identifiers, stores what
// matches, and requires every requested identity to be present in the store afterwards. The insert
// reports a nil error when it stops early on a mismatch, so its error alone cannot tell a full
// insert from an empty one.
//
// Completeness is judged by reading the store rather than by this invocation's insert count: blob
// storage commits independently of the caller's beacon transaction, so a re-run can legitimately
// insert nothing and still be complete.
func storeRemoteBlobs(
	ctx context.Context,
	blobDB blob_storage.BlobStorage,
	ids *solid.ListSSZ[*cltypes.BlobIdentifier],
	blobs []*cltypes.BlobSidecar,
	blockSignature common.Bytes96,
	slot uint64,
) error {
	_, inserted, err := blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, blobDB, ids, blobs,
		func(header *cltypes.SignedBeaconBlockHeader) error {
			if header.Signature == blockSignature {
				return nil
			}
			return errors.New("mismatched block header in blob sidecar")
		})
	if err != nil {
		return err
	}
	for i := 0; i < ids.Len(); i++ {
		id := ids.Get(i)
		stored, err := blobDB.BlobSidecarExists(ctx, slot, id.BlockRoot, id.Index)
		if err != nil {
			return err
		}
		if !stored {
			return fmt.Errorf("blob sidecar for root %x index %d not in the store after insert (inserted %d of %d, responses: %d)",
				id.BlockRoot, id.Index, inserted, ids.Len(), len(blobs))
		}
	}
	return nil
}
