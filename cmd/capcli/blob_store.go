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
	// BlobsIdentifiersFromBlocks drops a whole block once its commitments exceed the per-request
	// cap, and the remote block is decoded but never consensus-validated, so the endpoint chooses
	// this count. Without parity the checks below would range over an empty list and pass.
	if ids.Len() != commitments.Len() {
		return fmt.Errorf("got %d blob identifiers for %d commitments at slot %d", ids.Len(), commitments.Len(), block.Block.Slot)
	}
	blobs, err := retrieveBlobsFromRemoteEndpoint(ctx, beaconConfig, baseUriBlob, block)
	if err != nil {
		return fmt.Errorf("failed to retrieve blobs: %w, uri: %s", err, baseUriBlob)
	}
	if err := storeRemoteBlobs(ctx, blobDB, ids, blobs, block); err != nil {
		return fmt.Errorf("failed to verify and store blobs at slot %d: %w", block.Block.Slot, err)
	}
	return nil
}

// storeRemoteBlobs verifies a remote blob response against the requested identifiers, stores what
// matches, and requires the store to serve every requested identity back afterwards. The insert
// reports a nil error when it stops early on a mismatch, so its error alone cannot tell a full
// insert from an empty one.
//
// Completeness is judged by reading the store rather than by this invocation's insert count: blob
// storage commits independently of the caller's beacon transaction, so a re-run can legitimately
// insert nothing and still be complete. The read has to be ReadBlobSidecars, the same call
// consumers make, because sidecar files are published before the count row they depend on is
// committed — checking for the files alone would accept a store nothing can read.
func storeRemoteBlobs(
	ctx context.Context,
	blobDB blob_storage.BlobStorage,
	ids *solid.ListSSZ[*cltypes.BlobIdentifier],
	blobs []*cltypes.BlobSidecar,
	block *cltypes.SignedBeaconBlock,
) error {
	_, inserted, err := blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, blobDB, ids, blobs,
		block.Version(),
		func(header *cltypes.SignedBeaconBlockHeader) error {
			if header.Signature == block.Signature {
				return nil
			}
			return errors.New("mismatched block header in blob sidecar")
		})
	if err != nil {
		return err
	}
	wanted := map[common.Hash]uint64{}
	for i := 0; i < ids.Len(); i++ {
		wanted[ids.Get(i).BlockRoot]++
	}
	for root, count := range wanted {
		stored, found, err := blobDB.ReadBlobSidecars(ctx, block.Block.Slot, root)
		if err != nil {
			return err
		}
		if !found || uint64(len(stored)) < count {
			return fmt.Errorf("blob sidecars for root %x not readable after insert: got %d of %d (inserted %d, responses: %d)",
				root, len(stored), count, inserted, len(blobs))
		}
	}
	return nil
}
