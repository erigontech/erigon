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

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
)

// storeRemoteBlobs verifies a remote blob response against the requested identifiers and stores
// what matches, treating a shortfall as a failure. The insert reports a nil error when it stops
// early on a mismatch, so the count is the only way to tell a full insert from an empty one.
func storeRemoteBlobs(
	ctx context.Context,
	blobDB blob_storage.BlobStorage,
	ids *solid.ListSSZ[*cltypes.BlobIdentifier],
	blobs []*cltypes.BlobSidecar,
	blockSignature common.Bytes96,
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
	if inserted < uint64(ids.Len()) {
		return fmt.Errorf("stored %d of %d blob sidecars, responses: %d", inserted, ids.Len(), len(blobs))
	}
	return nil
}
