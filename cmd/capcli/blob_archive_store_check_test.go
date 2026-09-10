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
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	blob_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
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

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	mismatched, err := checkBlobStore(t.Context(), tx, reader, storage, slot, slot, false)
	require.NoError(t, err)
	require.Zero(t, mismatched, "a slot complete under its canonical root must not be reported as a gap")
}

// A check that deletes is unusable on a datadir about to be published from, so discarding a
// mismatched slot has to be opt-in.
func TestBlobArchiveStoreCheckDoesNotDeleteUnlessAsked(t *testing.T) {
	const slot = uint64(1_000)
	ctrl := gomock.NewController(t)
	db, reader, canonical := storeCheckFixture(t, slot, 2)

	storage := blob_mock_services.NewMockBlobStorage(ctrl)
	storage.EXPECT().KzgCommitmentsCount(gomock.Any(), gomock.Any()).Return(uint32(1), nil).AnyTimes()
	storage.EXPECT().RemoveBlobSidecars(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	_ = canonical

	tx, err := db.BeginRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	mismatched, err := checkBlobStore(t.Context(), tx, reader, storage, slot, slot, false)
	require.NoError(t, err)
	require.Equal(t, uint64(1), mismatched, "the short slot must still be reported")
}
