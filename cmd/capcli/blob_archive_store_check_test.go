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

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
	"go.uber.org/mock/gomock"

	"errors"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/spf13/afero"
)

type storeCheckReader struct {
	freezeblocks.BeaconSnapshotReader
	blocks map[uint64]*cltypes.SignedBeaconBlock
}

func (r *storeCheckReader) ReadBeaconBlockBodyBySlot(_ context.Context, _ kv.Tx, slot uint64) (*cltypes.SignedBeaconBlock, error) {
	return r.blocks[slot], nil
}

// storeCheckFixture builds a slot whose sidecars are stored under its canonical root, with the
// indexed root deliberately different from the block's own hash — which is what a payload-stripped
// read produces.
func storeCheckFixture(t *testing.T, slot uint64, commitments int) (kv.RwDB, *storeCheckReader, common.Hash) {
	t.Helper()
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	for range commitments {
		block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	}
	strippedRoot, err := block.Block.HashSSZ()
	require.NoError(t, err)

	canonical := common.HexToHash("0xc0ffee")
	require.NotEqual(t, common.Hash(strippedRoot), canonical, "fixture must keep the two roots distinct")
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(context.Background(), tx, slot, canonical)
	}))

	return db, &storeCheckReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block}}, canonical
}

// The audit resolved each block's root by hashing a payload-stripped block, so the store lookup
// always missed and every blob-bearing slot was reported as holding nothing.
func TestBlobArchiveStoreCheckReadsTheCountUnderTheCanonicalRoot(t *testing.T) {
	const slot = uint64(1_000)
	ctrl := gomock.NewController(t)
	db, reader, canonical := storeCheckFixture(t, slot, 2)

	storage := blob_mock_services.NewMockBlobStorage(ctrl)
	storage.EXPECT().KzgCommitmentsCount(gomock.Any(), canonical).Return(uint32(2), nil).AnyTimes()
	storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Not(canonical)).Return(uint32(0), nil).AnyTimes()
	// A count-equal slot is only complete if its files are still there.
	storage.EXPECT().BlobSidecarExists(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	mismatched, _, err := checkBlobStore(t.Context(), tx, reader, storage, slot, slot, false)
	require.NoError(t, err)
	require.Zero(t, mismatched, "a slot complete under its canonical root must not be reported as a gap")
}

// A check that deletes is unusable on a datadir about to be published from, so discarding a
// mismatched slot has to be opt-in.
func TestBlobArchiveStoreCheckDoesNotDeleteUnlessAsked(t *testing.T) {
	const slot = uint64(1_000)
	ctrl := gomock.NewController(t)
	db, reader, _ := storeCheckFixture(t, slot, 2)

	storage := blob_mock_services.NewMockBlobStorage(ctrl)
	storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).AnyTimes()
	storage.EXPECT().RemoveBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	mismatched, _, err := checkBlobStore(t.Context(), tx, reader, storage, slot, slot, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), mismatched, "the short slot must still be reported")
}

// An audit that cannot resolve a slot's root has not checked it, so finishing with
// mismatchedSlots=0 would be false confidence before publication. The snapshot reader serves block
// bodies independently of the canonical index, so a missing or partial index is exactly the state
// this has to surface rather than skip.
func TestBlobArchiveStoreCheckFailsWhenASlotHasNoCanonicalRoot(t *testing.T) {
	const slot = uint64(1_000)
	ctrl := gomock.NewController(t)

	// A readable blob-bearing block with no canonical row for its slot.
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	reader := &storeCheckReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block}}

	// Probing the store under a zero root would be meaningless.
	storage := blob_mock_services.NewMockBlobStorage(ctrl)
	storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Times(0)

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	mismatched, unresolved, err := checkBlobStore(t.Context(), tx, reader, storage, slot, slot, false)
	require.Error(t, err, "an unresolvable slot must not let the audit report success")
	require.Equal(t, uint64(1), unresolved, "the slot must be counted as unchecked")
	require.Zero(t, mismatched, "an unchecked slot is not a mismatch")
}

// The opt-in delete flag has to exist on the command the CLI actually builds, and be assigned when
// parsed: a struct tag alone leaves --remove-mismatched undefined at the command line.
func TestBlobArchiveStoreCheckExposesTheRemoveFlag(t *testing.T) {
	b := &BlobArchiveStoreCheck{}
	cmd := b.command()

	names := map[string]bool{}
	for _, f := range cmd.Flags {
		for _, n := range f.Names() {
			names[n] = true
		}
	}
	require.True(t, names["remove-mismatched"], "the command must define --remove-mismatched")

	parsed := &cli.Command{
		Flags: cmd.Flags,
		Action: func(_ context.Context, c *cli.Command) error {
			return b.fromCmd(c)
		},
	}
	require.NoError(t, parsed.Run(t.Context(), []string{"capcli", "--datadir", t.TempDir(), "--remove-mismatched"}))
	require.True(t, b.RemoveMismatched, "parsing --remove-mismatched must set the field")
}

// PruneBelow removes sidecar files but leaves their BlockRootToKzgCommitments rows, so a count can
// match for a slot whose files are gone. Accepting metadata equality as completeness makes the audit
// report a clean datadir right before it is published from. The old wrong-root lookup missed every
// count and flagged these by accident; reading the canonical root removes that accident.
func TestBlobArchiveStoreCheckFlagsCountEqualSlotsWithNoFiles(t *testing.T) {
	const slot = uint64(1_000)
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	fs := afero.NewMemMapFs()

	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	reader := &storeCheckReader{blocks: map[uint64]*cltypes.SignedBeaconBlock{slot: block}}

	canonical := common.HexToHash("0xc0ffee")
	require.NoError(t, db.Update(context.Background(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(context.Background(), tx, slot, canonical)
	}))

	sidecar := &cltypes.BlobSidecar{
		Index:                    0,
		SignedBlockHeader:        &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: slot}},
		CommitmentInclusionProof: solid.NewHashVector(cltypes.CommitmentBranchSize),
	}
	storage := blob_storage.NewBlobStore(db, fs)
	require.NoError(t, storage.WriteBlobSidecars(t.Context(), canonical, []*cltypes.BlobSidecar{sidecar}))
	require.NoError(t, storage.PruneBelow(10_000))

	// The state: the count row survives, the file does not.
	count, err := storage.KzgCommitmentsCount(t.Context(), canonical)
	require.NoError(t, err)
	require.Equal(t, uint32(1), count)

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	mismatched, unresolved, err := checkBlobStore(t.Context(), tx, reader,
		blob_storage.NewBlobStore(db, fs), slot, slot, false)
	require.NoError(t, err)
	require.Zero(t, unresolved)
	require.Equal(t, uint64(1), mismatched, "a count-equal slot with no files must not pass the audit")
}

// --remove-mismatched has to actually delete, not just be parseable.
func TestBlobArchiveStoreCheckDeletesWhenAsked(t *testing.T) {
	const slot = uint64(1_000)
	ctrl := gomock.NewController(t)
	db, reader, canonical := storeCheckFixture(t, slot, 2)

	storage := blob_mock_services.NewMockBlobStorage(ctrl)
	storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).AnyTimes()
	storage.EXPECT().RemoveBlobSidecars(gomock.Any(), slot, canonical).Return(nil).Times(1)

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	mismatched, _, err := checkBlobStore(t.Context(), tx, reader, storage, slot, slot, true)
	require.NoError(t, err)
	require.Equal(t, uint64(1), mismatched)
}

// A failed delete must stop the run rather than be counted as a successful audit.
func TestBlobArchiveStoreCheckPropagatesDeleteFailure(t *testing.T) {
	const slot = uint64(1_000)
	ctrl := gomock.NewController(t)
	db, reader, _ := storeCheckFixture(t, slot, 2)
	wantErr := errors.New("remove failed")

	storage := blob_mock_services.NewMockBlobStorage(ctrl)
	storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).AnyTimes()
	storage.EXPECT().RemoveBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Return(wantErr).AnyTimes()

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	_, _, err = checkBlobStore(t.Context(), tx, reader, storage, slot, slot, true)
	require.ErrorIs(t, err, wantErr, "a failed delete must not be swallowed")
}
