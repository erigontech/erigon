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
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
)

func blobIdentifiers(t *testing.T, ids ...*cltypes.BlobIdentifier) *solid.ListSSZ[*cltypes.BlobIdentifier] {
	t.Helper()
	list := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](8, 40)
	for _, id := range ids {
		list.Append(id)
	}
	return list
}

// VerifyAgainstIdentifiersAndInsertIntoTheBlobStore stops at the first identifier a response
// does not match and returns a nil error, so a caller that checks only the error cannot tell a
// full insert from an empty one. Downloading a chain and reporting success while storing no
// blobs for a block leaves a gap that nothing later notices.
func TestStoreRemoteBlobsRejectsAnIncompleteInsert(t *testing.T) {
	ctrl := gomock.NewController(t)
	ids := blobIdentifiers(t, &cltypes.BlobIdentifier{BlockRoot: common.HexToHash("0xaa"), Index: 0})

	// A sidecar whose header hashes to some other root: the insert breaks on the first
	// identifier mismatch, stores nothing, and reports no error.
	sidecar := &cltypes.BlobSidecar{
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{Slot: 1},
		},
	}

	err := storeRemoteBlobs(t.Context(), blob_mock_services.NewMockBlobStorage(ctrl), ids,
		[]*cltypes.BlobSidecar{sidecar}, sidecar.SignedBlockHeader.Signature)

	require.Error(t, err, "a zero insert must not be reported as success")
	require.ErrorContains(t, err, "stored 0 of 1")
}

// An empty response is the same failure as a mismatched one: nothing was stored, and the
// insert reports a nil error for it.
func TestStoreRemoteBlobsRejectsAnEmptyResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	ids := blobIdentifiers(t, &cltypes.BlobIdentifier{BlockRoot: common.HexToHash("0xaa"), Index: 0})

	err := storeRemoteBlobs(t.Context(), blob_mock_services.NewMockBlobStorage(ctrl), ids, nil,
		common.Bytes96{})

	require.Error(t, err, "an empty response must not be reported as success")
	require.ErrorContains(t, err, "stored 0 of 1")
}
