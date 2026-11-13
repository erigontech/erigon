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

	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
)

// storeBlobsForBlock fetches and stores the sidecars a block needs, and fails if the store does
// not hold all of them afterwards.
func (c *ChainEndpoint) storeBlobsForBlock(
	ctx context.Context,
	blobDB blob_storage.BlobStorage,
	beaconConfig *clparams.BeaconChainConfig,
	block *cltypes.SignedBeaconBlock,
) error {
	commitments := block.Block.Body.GetBlobKzgCommitments()
	if !c.Blobs || commitments == nil || commitments.Len() == 0 {
		return nil
	}
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
	blobs, err := retrieveBlobsFromRemoteEndpoint(ctx, c.Endpoint, blockRoot, commitments.Len())
	if err != nil {
		return fmt.Errorf("failed to retrieve blobs at slot %d, root %x: %w", block.Block.Slot, blockRoot, err)
	}
	if err := storeRemoteBlobs(ctx, blobDB, ids, blobs, block); err != nil {
		return fmt.Errorf("failed to verify and store blobs at slot %d: %w", block.Block.Slot, err)
	}
	return nil
}

// storeRemoteBlobs verifies a remote blob response against the requested identifiers, stores what
// matches, and requires the store to serve every requested identity back afterwards. Given the
// insert's early-stop contract, its error alone cannot tell a full insert from an empty one.
//
// Each response sidecar is bound to the block by commitment first. The insert skips the commitment
// inclusion proof from Gloas on, so without that comparison nothing ties a sidecar to the block it
// claims to belong to and a self-consistent blob, commitment and proof from elsewhere would verify.
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
	commitments := block.Block.Body.GetBlobKzgCommitments()
	for _, sidecar := range blobs {
		if sidecar == nil {
			return errors.New("blob response contains a nil sidecar")
		}
		if commitments == nil || sidecar.Index >= uint64(commitments.Len()) {
			return fmt.Errorf("blob sidecar index %d is outside the block's commitments", sidecar.Index)
		}
		if common.Bytes48(*commitments.Get(int(sidecar.Index))) != sidecar.KzgCommitment {
			return fmt.Errorf("blob sidecar at index %d carries a commitment the block does not reference", sidecar.Index)
		}
		if sidecar.KzgProof == common.Bytes48(goethkzg.PointAtInfinity) {
			proof, err := kzg.Ctx().ComputeBlobKZGProof((*goethkzg.Blob)(&sidecar.Blob), goethkzg.KZGCommitment(sidecar.KzgCommitment), 0)
			if err != nil {
				return fmt.Errorf("blob sidecar at index %d: compute its KZG proof: %w", sidecar.Index, err)
			}
			sidecar.KzgProof = common.Bytes48(proof)
		}
	}
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

// localBlobsStored reports whether the store holds every sidecar of a block. The count row alone is
// not enough: pruning deletes the sidecar files and keeps the row.
func localBlobsStored(ctx context.Context, blobDB blob_storage.BlobStorage, slot uint64, root common.Hash, commitments int) (bool, error) {
	count, err := blobDB.KzgCommitmentsCount(ctx, root)
	if err != nil || count != uint32(commitments) {
		return false, err
	}
	for idx := range commitments {
		present, err := blobDB.BlobSidecarExists(ctx, slot, root, uint64(idx))
		if err != nil || !present {
			return false, err
		}
	}
	return true, nil
}
