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
	"testing"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

const testBlobSlot = 42

// testBlock carries only what the blob path reads off a block: its version, slot and signature.
func testBlock(version clparams.StateVersion, slot uint64) *cltypes.SignedBeaconBlock {
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, version)
	block.Block.Slot = slot
	return block
}

func denebTestBlock() *cltypes.SignedBeaconBlock {
	return testBlock(clparams.DenebVersion, testBlobSlot)
}

// blobSidecarAt must be fully encodable: a sidecar with no inclusion-proof vector writes a short
// file that the store later discards as undecodable, which would make a readback test pass or fail
// for the wrong reason.
func blobSidecarAt(index uint64) *cltypes.BlobSidecar {
	return &cltypes.BlobSidecar{
		Index: index,
		SignedBlockHeader: &cltypes.SignedBeaconBlockHeader{
			Header: &cltypes.BeaconBlockHeader{Slot: testBlobSlot},
		},
		CommitmentInclusionProof: solid.NewHashVector(cltypes.CommitmentBranchSize),
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
	store.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(testBlobSlot), common.HexToHash("0xaa")).
		Return(nil, false, nil).AnyTimes()

	block := denebTestBlock()
	block.Signature = sidecar.SignedBlockHeader.Signature
	// The block references this sidecar's commitment, so the identifier mismatch below is what
	// stops the insert rather than the block/sidecar binding.
	commitTo(t, block, sidecar.KzgCommitment)
	err := storeRemoteBlobs(t.Context(), store, ids, []*cltypes.BlobSidecar{sidecar}, block)

	require.Error(t, err, "a zero insert must not be reported as success")
	require.ErrorContains(t, err, "not readable after insert")
}

// An empty response is the same failure as a mismatched one: nothing was stored, and the
// insert reports a nil error for it.
func TestStoreRemoteBlobsRejectsAnEmptyResponse(t *testing.T) {
	ctrl := gomock.NewController(t)
	ids := blobIdentifiers(t, &cltypes.BlobIdentifier{BlockRoot: common.HexToHash("0xaa"), Index: 0})

	store := blob_mock_services.NewMockBlobStorage(ctrl)
	store.EXPECT().ReadBlobSidecars(gomock.Any(), uint64(testBlobSlot), common.HexToHash("0xaa")).
		Return(nil, false, nil).AnyTimes()

	err := storeRemoteBlobs(t.Context(), store, ids, nil, denebTestBlock())

	require.Error(t, err, "an empty response must not be reported as success")
	require.ErrorContains(t, err, "not readable after insert")
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

	require.NoError(t, storeRemoteBlobs(t.Context(), reopened, ids, nil, denebTestBlock()),
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

	err := storeRemoteBlobs(t.Context(), blob_storage.NewBlobStore(db, fs), ids, nil, denebTestBlock())

	require.Error(t, err, "a store missing index 1 must not be reported as complete")
	require.ErrorContains(t, err, "got 1 of 2")
}

// WriteBlobSidecars publishes every sidecar file and only then commits the count row in a separate
// transaction. A crash in that window leaves the files on disk with no row, which a filesystem
// existence check cannot distinguish from a complete store — while ReadBlobSidecars, which every
// consumer uses, reads the row first and reports nothing found.
func TestStoreRemoteBlobsRejectsFilesWithoutTheirMetadataRow(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fs := afero.NewMemMapFs()
	root := common.HexToHash("0xaa")

	require.NoError(t, blob_storage.NewBlobStore(db, fs).
		WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{blobSidecarAt(0)}))
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		return tx.Delete(kv.BlockRootToKzgCommitments, root[:])
	}))

	ids := blobIdentifiers(t, &cltypes.BlobIdentifier{BlockRoot: root, Index: 0})
	err := storeRemoteBlobs(t.Context(), blob_storage.NewBlobStore(db, fs), ids, nil, denebTestBlock())

	require.Error(t, err, "sidecar files with no count row must not be reported as complete")
	require.ErrorContains(t, err, "not readable")
}

// BlobsIdentifiersFromBlocks breaks before appending anything once a block's commitments exceed the
// per-request cap, so a block with too many commitments yields no identifiers at all. Keying
// completeness off the identifier list then checks nothing and the block commits with no sidecars.
// The remote block is decoded but not consensus-validated, so the count is the endpoint's to choose.
func TestStoreBlobsForBlockRejectsFewerIdentifiersThanCommitments(t *testing.T) {
	beaconConfig := &clparams.MainnetBeaconConfig
	block := testBlock(clparams.DenebVersion, testBlobSlot)
	for range beaconConfig.MaxRequestBlobSidecarsByVersion(clparams.DenebVersion) + 1 {
		block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}

	endpoint := &ChainEndpoint{Blobs: true}
	err := endpoint.storeBlobsForBlock(t.Context(), nil, beaconConfig, "", block)

	require.Error(t, err, "a block whose commitments exceed the request cap must not pass silently")
	require.ErrorContains(t, err, "identifier")
}

// Gloas sidecars carry no commitment inclusion proof, a contract VerifyBlobSidecars states with a
// version gate and the insert path has to honour.
func TestStoreRemoteBlobsAcceptsAProoflessGloasSidecar(t *testing.T) {
	blob := goethkzg.Blob{}
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)

	header := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: testBlobSlot}}
	root, err := header.Header.HashSSZ()
	require.NoError(t, err)

	sidecar := cltypes.NewBlobSidecar(0, (*cltypes.Blob)(&blob), common.Bytes48(commitment),
		common.Bytes48(proof), header, solid.NewHashVector(cltypes.CommitmentBranchSize))

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fs := afero.NewMemMapFs()
	store := blob_storage.NewBlobStore(db, fs)
	ids := blobIdentifiers(t, &cltypes.BlobIdentifier{BlockRoot: root, Index: 0})

	block := testBlock(clparams.GloasVersion, testBlobSlot)
	commitTo(t, block, sidecar.KzgCommitment)
	require.NoError(t, storeRemoteBlobs(t.Context(), store, ids, []*cltypes.BlobSidecar{sidecar}, block))

	stored, found, err := blob_storage.NewBlobStore(db, fs).ReadBlobSidecars(t.Context(), testBlobSlot, root)
	require.NoError(t, err)
	require.True(t, found, "the Gloas sidecar was not durably stored")
	require.Len(t, stored, 1)
	require.Equal(t, common.Bytes48(commitment), stored[0].KzgCommitment)
}

// gloasSidecarFor builds a sidecar with a real KZG commitment and proof but no inclusion proof,
// bound to header. blobByte selects the blob it commits to, so a caller can build a sidecar that is
// internally consistent yet commits to something other than the block's commitment.
func gloasSidecarFor(t *testing.T, header *cltypes.SignedBeaconBlockHeader, blobByte byte) *cltypes.BlobSidecar {
	t.Helper()
	blob := goethkzg.Blob{}
	// Last byte of the first field element: a leading byte can exceed the BLS modulus and the
	// commitment call then rejects the scalar as non-canonical.
	blob[31] = blobByte
	commitment, err := kzg.Ctx().BlobToKZGCommitment(&blob, 0)
	require.NoError(t, err)
	proof, err := kzg.Ctx().ComputeBlobKZGProof(&blob, commitment, 0)
	require.NoError(t, err)
	return cltypes.NewBlobSidecar(0, (*cltypes.Blob)(&blob), common.Bytes48(commitment),
		common.Bytes48(proof), header, solid.NewHashVector(cltypes.CommitmentBranchSize))
}

// commitTo makes block commit to commitment at index 0. For Gloas the commitments live in the
// execution payload bid rather than the body, and NewSignedBeaconBlock leaves the bid unset.
func commitTo(t *testing.T, block *cltypes.SignedBeaconBlock, commitment common.Bytes48) {
	t.Helper()
	list := block.Block.Body.GetBlobKzgCommitments()
	require.NotNil(t, list, "the block has nowhere to hold commitments")
	kzgCommitment := cltypes.KZGCommitment(commitment)
	list.Append(&kzgCommitment)
}

// Gloas sidecars carry no inclusion proof, so without comparing each sidecar's commitment against
// the block's at that index nothing ties a sidecar to the block it claims to belong to: an
// internally consistent blob, commitment and proof taken from another block verifies and is stored.
func TestStoreRemoteBlobsRejectsASidecarCommittingToAnotherBlob(t *testing.T) {
	header := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: testBlobSlot}}
	root, err := header.Header.HashSSZ()
	require.NoError(t, err)
	sidecar := gloasSidecarFor(t, header, 1)

	block := testBlock(clparams.GloasVersion, testBlobSlot)
	commitTo(t, block, gloasSidecarFor(t, header, 2).KzgCommitment)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ids := blobIdentifiers(t, &cltypes.BlobIdentifier{BlockRoot: root, Index: 0})

	err = storeRemoteBlobs(t.Context(), blob_storage.NewBlobStore(db, afero.NewMemMapFs()), ids,
		[]*cltypes.BlobSidecar{sidecar}, block)

	require.Error(t, err, "a sidecar committing to a blob the block does not reference must be rejected")
	require.ErrorContains(t, err, "commitment")
}

// The version gate must still require the inclusion proof before Gloas. Without this, removing the
// proof check outright leaves the suite green: the other pre-Gloas cases here return early on an
// empty response or break on a mismatched root before the proof is reached.
func TestStoreRemoteBlobsRejectsAPreGloasSidecarWithABadInclusionProof(t *testing.T) {
	header := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: testBlobSlot}}
	root, err := header.Header.HashSSZ()
	require.NoError(t, err)
	sidecar := gloasSidecarFor(t, header, 1)

	block := testBlock(clparams.DenebVersion, testBlobSlot)
	commitTo(t, block, sidecar.KzgCommitment)

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	ids := blobIdentifiers(t, &cltypes.BlobIdentifier{BlockRoot: root, Index: 0})

	err = storeRemoteBlobs(t.Context(), blob_storage.NewBlobStore(db, afero.NewMemMapFs()), ids,
		[]*cltypes.BlobSidecar{sidecar}, block)

	require.Error(t, err, "a pre-Gloas sidecar with a zero inclusion proof must be rejected")
	require.ErrorContains(t, err, "inclusion proof")
}
