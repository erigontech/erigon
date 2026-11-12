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

package caplin1

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/utils/eth_clock"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
)

// frozenSidecars serves the sidecars of the slots below below.
type frozenSidecars struct {
	below uint64
	slots map[uint64][]*cltypes.BlobSidecar
}

func (f *frozenSidecars) FrozenBlobs() uint64 { return f.below }

func (f *frozenSidecars) ReadBlobSidecars(slot uint64) ([]*cltypes.BlobSidecar, error) {
	return f.slots[slot], nil
}

func sidecars(slot uint64, blobs ...byte) []*cltypes.BlobSidecar {
	out := make([]*cltypes.BlobSidecar, len(blobs))
	for i, b := range blobs {
		header := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: slot}}
		out[i] = cltypes.NewBlobSidecar(uint64(i), &cltypes.Blob{b}, common.Bytes48{b}, common.Bytes48{}, header, solid.NewHashVector(cltypes.CommitmentBranchSize))
	}
	return out
}

func TestBlobReader(t *testing.T) {
	const genesis = 1_000
	cfg := &clparams.MainnetBeaconConfig
	clock := eth_clock.NewEthereumClock(genesis, common.Hash{}, cfg)
	timeOf := func(slot uint64) uint64 { return genesis + slot*cfg.SecondsPerSlot }
	indexDB := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	blobs := blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), afero.NewMemMapFs())
	frozen := &frozenSidecars{below: 4, slots: map[uint64][]*cltypes.BlobSidecar{3: sidecars(3, 0x31, 0x32)}}

	// Slot 3 is frozen, 4 the first slot above the frozen ones, 5 holds two
	// blobs, 7 none, 6 has no block.
	payloads := map[uint64]common.Hash{3: {0x03}, 4: {0x04}, 5: {0x05}, 7: {0x07}}
	require.NoError(t, indexDB.Update(t.Context(), func(tx kv.RwTx) error {
		for slot, payload := range payloads {
			root := common.Hash{0xbb, byte(slot)}
			if err := beacon_indicies.MarkRootCanonical(tx, slot, root); err != nil {
				return err
			}
			if err := beacon_indicies.WriteExecutionBlockHash(tx, root, payload); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, blobs.WriteBlobSidecars(t.Context(), common.Hash{0xbb, 4}, sidecars(4, 0x41)))
	require.NoError(t, blobs.WriteBlobSidecars(t.Context(), common.Hash{0xbb, 5}, sidecars(5, 0x51, 0x52)))
	// Sidecars of a block that is not canonical at its slot.
	require.NoError(t, blobs.WriteBlobSidecars(t.Context(), common.Hash{0xcc, 5}, sidecars(5, 0x99)))

	r := NewBlobReader()
	got, found, err := r.ExecutionBlobs(t.Context(), payloads[5], timeOf(5))
	require.NoError(t, err)
	require.False(t, found, "found before the stores open")
	require.Nil(t, got)
	r.open(clock, indexDB, blobs, frozen)

	for _, tc := range []struct {
		name    string
		payload common.Hash
		time    uint64
		want    []byte
	}{
		{"stored", payloads[5], timeOf(5), []byte{0x51, 0x52}},
		{"stored, timestamp within the slot", payloads[5], timeOf(5) + cfg.SecondsPerSlot - 1, []byte{0x51, 0x52}},
		{"frozen", payloads[3], timeOf(3), []byte{0x31, 0x32}},
		{"first slot above the frozen ones", payloads[4], timeOf(4), []byte{0x41}},
		{"another execution block at the slot", common.Hash{0x55}, timeOf(5), nil},
		{"the block one slot later", payloads[5], timeOf(6), nil},
		{"no blobs stored", payloads[7], timeOf(7), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, found, err := r.ExecutionBlobs(t.Context(), tc.payload, tc.time)
			require.NoError(t, err)
			require.Equal(t, tc.want != nil, found)
			require.Len(t, got, len(tc.want))
			for i, b := range tc.want {
				require.Len(t, got[i], cltypes.BYTES_PER_BLOB)
				require.Equal(t, b, got[i][0], "blob %d", i)
			}
		})
	}
}
