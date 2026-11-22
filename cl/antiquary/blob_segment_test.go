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

package antiquary

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/antiquary/tests"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

// frozenSegments models the snapshot frontiers by where the last blob and block segments end.
type frozenSegments struct{ blobsTo, blocksTo uint64 }

func (f frozenSegments) FrozenBlobs() uint64 { return f.blobsTo }

func (f frozenSegments) VisibleSegmentsMaxTo(t snaptype.Enum) uint64 {
	if t == snaptype.BeaconBlocks.Enum() {
		return f.blocksTo
	}
	return f.blobsTo
}

func TestNextBlobSegment(t *testing.T) {
	// Deneb at slot 132,608*32 = 4,243,456, so the lowest blob segment starts at 4,240,000.
	cfg := &clparams.BeaconChainConfig{DenebForkEpoch: 132_608, SlotsPerEpoch: 32}
	for _, tc := range []struct {
		name                string
		frozen              frozenSegments
		from, to, backlogTo uint64
		ok                  bool
	}{
		{name: "blobs caught up with blocks", frozen: frozenSegments{blobsTo: 11_250_000, blocksTo: 11_250_000}},
		{name: "blocks short of a whole segment past the blobs", frozen: frozenSegments{blobsTo: 11_240_000, blocksTo: 11_249_999}},
		{
			name: "a frozen block segment makes its blob segment retirable", frozen: frozenSegments{blobsTo: 11_240_000, blocksTo: 11_250_000},
			from: 11_240_000, to: 11_250_000, backlogTo: 11_250_000, ok: true,
		},
		{
			name: "a backlog retires only its first segment", frozen: frozenSegments{blobsTo: 10_470_000, blocksTo: 11_190_000},
			from: 10_470_000, to: 10_480_000, backlogTo: 11_190_000, ok: true,
		},
		{
			name: "no blob segments yet starts at the Deneb segment", frozen: frozenSegments{blocksTo: 4_280_000},
			from: 4_240_000, to: 4_250_000, backlogTo: 4_280_000, ok: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from, to, backlogTo, ok := nextBlobSegment(tc.frozen, cfg)
			require.Equal(t, tc.ok, ok)
			require.Equal(t, tc.from, from)
			require.Equal(t, tc.to, to)
			require.Equal(t, tc.backlogTo, backlogTo)
		})
	}
}

// Compression parallelism must follow the backlog, not the one segment dumped from it: a
// single segment on its own reads as tip retirement and would compress a catch-up on one worker.
func TestNextBlobSegmentKeepsTheBacklogForCompressionParallelism(t *testing.T) {
	cfg := &clparams.BeaconChainConfig{DenebForkEpoch: 132_608, SlotsPerEpoch: 32}

	from, to, backlogTo, ok := nextBlobSegment(frozenSegments{blobsTo: 11_240_000, blocksTo: 11_260_000}, cfg)

	require.True(t, ok)
	require.False(t, isBlobBacklog(from, to))
	require.True(t, isBlobBacklog(from, backlogTo), "two pending segments are a catch-up")
}

// A sidecar file lost under a surviving count row is a hole: the segment waits for the file instead of failing the dump.
func TestAntiquateBlobsWaitsForALostSidecarFile(t *testing.T) {
	const limit = snaptype.CaplinMergeLimit
	const slot = uint64(7)
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeStateFixture(t, dirs.Snap, "beaconblocks", 0, limit, logger)
	cfg := clparams.MainnetBeaconConfig
	cfg.DenebForkEpoch = 0
	sn := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &cfg, dirs, logger)
	t.Cleanup(sn.Close)
	require.NoError(t, sn.OpenFolder())

	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	root := common.Hash{byte(slot)}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(tx, slot, root)
	}))
	block := cltypes.NewSignedBeaconBlock(&cfg, clparams.DenebVersion)
	block.Block.Slot = slot
	block.Block.Body.BlobKzgCommitments.Append(&cltypes.KZGCommitment{})
	reader := tests.NewMockBlockReader()
	reader.U[slot] = block

	fs := afero.NewMemMapFs()
	storage := blob_storage.NewBlobStore(db, fs)
	sidecar := &cltypes.BlobSidecar{
		SignedBlockHeader:        &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: slot}},
		CommitmentInclusionProof: solid.NewHashVector(cltypes.CommitmentBranchSize),
	}
	require.NoError(t, storage.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{sidecar}))
	require.NoError(t, fs.RemoveAll("0"))

	a := NewAntiquary(t.Context(), storage, nil, nil, &cfg, dirs, nil, db, nil, sn, reader, nil, logger, false, false, true, true, nil)
	blobSegment := snaptype.BlobSidecars.FileInfo(dirs.Snap, 0, limit).Path

	require.NoError(t, a.antiquateBlobs())
	require.NoFileExists(t, blobSegment)
	require.Equal(t, slot, mxBlobRetirementGapSlot.GetValueUint64())

	require.NoError(t, storage.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{sidecar}))
	require.NoError(t, a.antiquateBlobs())
	require.FileExists(t, blobSegment)
	require.Equal(t, uint64(limit), sn.FrozenBlobs())
	count, err := storage.KzgCommitmentsCount(t.Context(), root)
	require.NoError(t, err)
	require.Zero(t, count, "the retired sidecars are pruned from the store")
}
