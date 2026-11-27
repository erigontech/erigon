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
	"os"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/erigontech/erigon/cl/beacon/beaconevents"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	blob_storage_mock_services "github.com/erigontech/erigon/cl/persistence/blob_storage/mock_services"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/execution/chain/networkname"
	"github.com/erigontech/erigon/node/ethconfig"
)

func TestOldestStoredReadsTheLowestVisibleBlockSegment(t *testing.T) {
	logger := log.New()
	dirs := datadir.New(t.TempDir())
	writeStateFixture(t, dirs.Snap, snaptype.BeaconBlocks.Name(), 100, 200, logger)
	snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, logger)
	t.Cleanup(snapshots.Close)
	require.NoError(t, snapshots.OpenFolder())

	stored, err := OldestStored(t.Context(), nil, snapshots, nil, nil, nil)

	require.NoError(t, err)
	require.Equal(t, StoredSlot{Slot: 100, Holds: true}, stored.Blocks)
}

func TestOldestStoredSources(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		stored, err := OldestStored(t.Context(), nil, nil, nil, nil, nil)
		require.NoError(t, err)
		require.False(t, stored.Blocks.Holds)
		require.False(t, stored.Sidecars.Holds)
		require.False(t, stored.States.Holds)
		require.Len(t, stored.StateTables, len(snapshotsync.MakeCaplinStateSnapshotsTypes(nil, nil).KeyValueGetters))
		for _, table := range stored.StateTables {
			require.False(t, table.Holds)
		}
	})

	t.Run("segments", func(t *testing.T) {
		logger := log.New()
		dirs := datadir.New(t.TempDir())
		blobFrom := clparams.MainnetBeaconConfig.DenebForkEpoch * clparams.MainnetBeaconConfig.SlotsPerEpoch
		writeStateFixture(t, dirs.Snap, snaptype.BeaconBlocks.Name(), 100, 200, logger)
		blobSegment := writeStateFixture(t, dirs.Snap, snaptype.BlobSidecars.Name(), blobFrom, blobFrom+100, logger)
		blobIndex := strings.Replace(strings.TrimSuffix(blobSegment, ".seg")+".idx", "-blobsidecars.idx", "-blocksidecars.idx", 1)
		require.NoError(t, os.Rename(strings.TrimSuffix(blobSegment, ".seg")+".idx", blobIndex))
		for table := range snapshotsync.MakeCaplinStateSnapshotsTypes(nil, nil).KeyValueGetters {
			writeStateFixture(t, dirs.SnapCaplin, table, 300, 400, logger)
		}
		snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, logger)
		t.Cleanup(snapshots.Close)
		require.NoError(t, snapshots.OpenFolder())
		view := snapshots.View()
		require.Len(t, view.BlobSidecars(), 1)
		view.Close()
		stateSnapshots := snapshotsync.NewCaplinStateSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, dirs, snapshotsync.MakeCaplinStateSnapshotsTypes(nil, nil), logger)
		t.Cleanup(stateSnapshots.Close)
		require.NoError(t, stateSnapshots.OpenFolder())

		stored, err := OldestStored(t.Context(), nil, snapshots, stateSnapshots, nil, nil)

		require.NoError(t, err)
		require.Equal(t, StoredSlot{Slot: 100, Holds: true}, stored.Blocks)
		require.Equal(t, StoredSlot{Slot: blobFrom, Holds: true}, stored.Sidecars)
		require.Equal(t, StoredSlot{Slot: 300, Holds: true}, stored.States)
		for table, value := range stored.StateTables {
			require.Equal(t, StoredSlot{Slot: 300, Holds: true}, value, table)
		}
	})

	t.Run("mdbx and hot stores", func(t *testing.T) {
		db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
		require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
			if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(42, common.Hash{1}), []byte{1}); err != nil {
				return err
			}
			for table := range snapshotsync.MakeCaplinStateSnapshotsTypes(nil, nil).KeyValueGetters {
				if err := tx.Put(table, base_encoding.Encode64ToBytes4(90), []byte{1}); err != nil {
					return err
				}
			}
			return nil
		}))

		blobStorage := blob_storage_mock_services.NewMockBlobStorage(gomock.NewController(t))
		blobStorage.EXPECT().EarliestStoredSlot().Return(uint64(84), true, nil)
		columnStorage := blob_storage_mock_services.NewMockDataColumnStorage(gomock.NewController(t))
		columnStorage.EXPECT().EarliestStoredSlot().Return(uint64(120), true, nil)
		stored, err := OldestStored(t.Context(), db, nil, nil, blobStorage, columnStorage)

		require.NoError(t, err)
		require.Equal(t, StoredSlot{Slot: 42, Holds: true}, stored.Blocks)
		require.Equal(t, StoredSlot{Slot: 84, Holds: true}, stored.Sidecars)
		require.Equal(t, StoredSlot{Slot: 90, Holds: true}, stored.States)
	})

	for _, tc := range []struct {
		name        string
		segmentSlot uint64
		rowSlot     uint64
		want        uint64
	}{
		{name: "mdbx lower", segmentSlot: 100, rowSlot: 50, want: 50},
		{name: "segment lower", segmentSlot: 50, rowSlot: 100, want: 50},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger := log.New()
			dirs := datadir.New(t.TempDir())
			writeStateFixture(t, dirs.Snap, snaptype.BeaconBlocks.Name(), tc.segmentSlot, tc.segmentSlot+50, logger)
			snapshots := freezeblocks.NewCaplinSnapshots(ethconfig.BlocksFreezing{ChainName: networkname.Mainnet}, &clparams.MainnetBeaconConfig, dirs, logger)
			t.Cleanup(snapshots.Close)
			require.NoError(t, snapshots.OpenFolder())
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				return tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(tc.rowSlot, common.Hash{2}), []byte{1})
			}))

			stored, err := OldestStored(t.Context(), db, snapshots, nil, nil, nil)

			require.NoError(t, err)
			require.Equal(t, StoredSlot{Slot: tc.want, Holds: true}, stored.Blocks)
		})
	}
}

func TestOldestStoredUsesTheHighestStateTableSlot(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	lateTable := kv.StateEvents
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for table := range snapshotsync.MakeCaplinStateSnapshotsTypes(nil, nil).KeyValueGetters {
			slot := uint64(100)
			if table == lateTable {
				slot = 200
			}
			if err := tx.Put(table, base_encoding.Encode64ToBytes4(slot), []byte{1}); err != nil {
				return err
			}
		}
		return nil
	}))

	stored, err := OldestStored(t.Context(), db, nil, nil, nil, nil)

	require.NoError(t, err)
	require.Equal(t, StoredSlot{Slot: 200, Holds: true}, stored.States)
	require.Equal(t, StoredSlot{Slot: 200, Holds: true}, stored.StateTables[lateTable])
}

func TestOldestStoredBucketZero(t *testing.T) {
	cfg := &clparams.MainnetBeaconConfig
	if clparams.GetBeaconConfig() == nil {
		clparams.InitGlobalStaticConfig(cfg)
	}
	for _, store := range []string{"blob", "column"} {
		t.Run(store, func(t *testing.T) {
			root := common.Hash{1}
			header := &cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: 1}}
			var blobs blob_storage.BlobStorage
			var columns blob_storage.DataColumnStorage
			fs := afero.NewBasePathFs(afero.NewOsFs(), t.TempDir())
			if store == "blob" {
				blobs = blob_storage.NewBlobStore(mdbxtest.NewTestDB(t, dbcfg.ChainDB), fs)
				sidecar := cltypes.NewBlobSidecar(0, &cltypes.Blob{}, common.Bytes48{}, common.Bytes48{}, header, solid.NewHashVector(cltypes.CommitmentBranchSize))
				require.NoError(t, blobs.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{sidecar}))
				_, found, err := blobs.ReadBlobSidecars(t.Context(), 1, root)
				require.NoError(t, err)
				require.True(t, found)
			} else {
				columns = blob_storage.NewDataColumnStore(fs, cfg, beaconevents.NewEventEmitter())
				sidecar := cltypes.NewDataColumnSidecar()
				sidecar.SignedBlockHeader = header
				require.NoError(t, columns.WriteColumnSidecars(t.Context(), root, 0, sidecar))
				found, err := columns.ColumnSidecarExists(t.Context(), 1, root, 0)
				require.NoError(t, err)
				require.True(t, found)
			}
			got, err := OldestStored(t.Context(), nil, nil, nil, blobs, columns)
			require.NoError(t, err)
			if got.Sidecars != (StoredSlot{Slot: 0, Holds: true}) {
				t.Fatalf("readable %s in bucket 0: Sidecars=%+v", store, got.Sidecars)
			}
		})
	}
}

func TestPartialStateCountsAsExistingData(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error { return tx.Put(kv.StateEvents, base_encoding.Encode64ToBytes4(123), []byte{1}) }))
	oldest, err := OldestStored(t.Context(), db, nil, nil, nil, nil)
	require.NoError(t, err)
	require.True(t, oldest.StateTables[kv.StateEvents].Holds)
	require.False(t, oldest.States.Holds)
	cfg := &clparams.MainnetBeaconConfig
	requested, exact, err := clparams.LegacyRetention(clparams.CaplinConfig{ColumnKeepSlots: 1}, cfg)
	require.NoError(t, err)
	require.False(t, exact)
	_, action, err := clparams.ResolveRetention(clparams.CaplinRetention{}, false, requested, clparams.CaplinRetentionSourceLegacyInexact, oldest, 15660000, cfg, "erigon", "<dir>")
	if action != clparams.CaplinRetentionActionStore || err != nil {
		t.Fatalf("one state table holds data: action=%s err=%v", action, err)
	}
}
