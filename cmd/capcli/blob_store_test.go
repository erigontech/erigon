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

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

const testBlobSlot = 42

func blobSidecarAt(index uint64) *cltypes.BlobSidecar {
	return &cltypes.BlobSidecar{
		Index: index,
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{Slot: testBlobSlot},
		},
	}
}

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

	store := blob_mock_services.NewMockBlobStorage(ctrl)
	store.EXPECT().BlobSidecarExists(gomock.Any(), uint64(testBlobSlot), common.HexToHash("0xaa"), uint64(0)).
		Return(false, nil).AnyTimes()

	err := storeRemoteBlobs(t.Context(), store, ids,
		[]*cltypes.BlobSidecar{sidecar}, sidecar.SignedBlockHeader.Signature, testBlobSlot)

	require.Error(t, err, "a zero insert must not be reported as success")
	require.ErrorContains(t, err, "not in the store after insert")
}

// An empty response is the same failure as a mismatched one: nothing was stored, and the
// insert reports a nil error for it.
func TestStoreRemoteBlobsRejectsAnEmptyResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	ids := blobIdentifiers(t, &cltypes.BlobIdentifier{BlockRoot: common.HexToHash("0xaa"), Index: 0})

	store := blob_mock_services.NewMockBlobStorage(ctrl)
	store.EXPECT().BlobSidecarExists(gomock.Any(), uint64(testBlobSlot), common.HexToHash("0xaa"), uint64(0)).
		Return(false, nil).AnyTimes()

	err := storeRemoteBlobs(t.Context(), store, ids, nil, common.Bytes96{}, testBlobSlot)

	require.Error(t, err, "an empty response must not be reported as success")
	require.ErrorContains(t, err, "not in the store after insert")
}

// A restart can find the blob store already complete: blob storage commits independently of
// the beacon transaction, so an interrupted run leaves durable sidecars the endpoint may no
// longer serve. Judging completeness by this invocation's insert count aborts on data that is
// already there; the requested identities must be read back from the store instead.
func TestStoreRemoteBlobsAcceptsAStoreThatIsAlreadyComplete(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fs := afero.NewMemMapFs()
	root := common.HexToHash("0xaa")

	require.NoError(t, blob_storage.NewBlobStore(db, fs).
		WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{blobSidecarAt(0), blobSidecarAt(1)}))

	// Reopened store, and an endpoint that no longer answers for this root.
	reopened := blob_storage.NewBlobStore(db, fs)
	ids := blobIdentifiers(t,
		&cltypes.BlobIdentifier{BlockRoot: root, Index: 0},
		&cltypes.BlobIdentifier{BlockRoot: root, Index: 1})

	require.NoError(t, storeRemoteBlobs(t.Context(), reopened, ids, nil, common.Bytes96{}, testBlobSlot),
		"sidecars already durable must not be reported as a shortfall")
}

// The durable read must be keyed by the requested identities, not by the root alone: a store
// holding some of the requested indices is still incomplete.
func TestStoreRemoteBlobsRejectsAPartiallyPopulatedStore(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fs := afero.NewMemMapFs()
	root := common.HexToHash("0xaa")

	require.NoError(t, blob_storage.NewBlobStore(db, fs).
		WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{blobSidecarAt(0)}))

	ids := blobIdentifiers(t,
		&cltypes.BlobIdentifier{BlockRoot: root, Index: 0},
		&cltypes.BlobIdentifier{BlockRoot: root, Index: 1})

	err := storeRemoteBlobs(t.Context(), blob_storage.NewBlobStore(db, fs), ids, nil,
		common.Bytes96{}, testBlobSlot)

	require.Error(t, err, "a store missing index 1 must not be reported as complete")
	require.ErrorContains(t, err, "index 1")
}
