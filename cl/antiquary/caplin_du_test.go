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
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"

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
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/snapcfg"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

func TestCollectCaplinDu(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writeCaplinDuFile(t, dirs.Snap, snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100), 11)
	writeCaplinDuFile(t, dirs.Snap, snaptype.BeaconBlocks.IdxFileNames(0, 100)[0], 13)
	writeCaplinDuFile(t, dirs.Snap, snaptype.BlobSidecars.FileName(version.V1_1, 100, 200), 17)
	writeCaplinDuFile(t, dirs.Snap, snaptype.BlobSidecars.IdxFileNames(100, 200)[0], 19)
	writeCaplinDuFile(t, dirs.SnapCaplin, snaptype.StateEvents.FileName(version.V1_1, 0, 300), 23)
	writeCaplinDuFile(t, dirs.SnapCaplin, snaptype.PtcWindowTable.FileName(version.V1_1, 300, 400), 29)

	writeCaplinDuFile(t, filepath.Join(dirs.CaplinBlobs, "1"), "blob", 31)
	writeCaplinDuFile(t, filepath.Join(dirs.CaplinColumnData, "2", "packs"), "column.pack", 37)

	db := openCaplinDuDB(t, dirs.CaplinIndexing)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(150, common.Hash{1}), []byte{1}); err != nil {
			return err
		}
		if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(250, common.Hash{2}), []byte{1}); err != nil {
			return err
		}
		return beacon_indicies.WriteCaplinRetention(tx, clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec})
	}))
	db.Close()

	indexDB := openCaplinDuReadOnly(t, dirs.CaplinIndexing)
	defer indexDB.Close()
	result, err := CollectCaplinDu(t.Context(), CaplinDuRequest{
		Chain:        "mainnet",
		Dirs:         dirs,
		BeaconConfig: &clparams.MainnetBeaconConfig,
		IndexDB:      indexDB,
	})
	require.NoError(t, err)
	require.Equal(t, "mainnet", result.Chain)
	require.Equal(t, "spec", result.Regime)
	require.True(t, result.RegimeStored)
	require.Equal(t, uint64(250), result.HeadSlot)
	hotBlockBytes := result.Rows[1].Bytes
	require.Greater(t, hotBlockBytes, uint64(0))
	require.Equal(t, []CaplinDuRow{
		{Family: "beacon blocks", Tier: "cold", FromSlot: 0, ToSlot: 100, Files: 2, Bytes: 24, HasData: true},
		{Family: "beacon blocks", Tier: "hot", FromSlot: 150, ToSlot: 250, Bytes: hotBlockBytes, InMDBX: true, HasData: true},
		{Family: "blob sidecars", Tier: "cold", FromSlot: 100, ToSlot: 200, Files: 2, Bytes: 36, HasData: true},
		{Family: "blob sidecars", Tier: "hot", FromSlot: 10_000, ToSlot: 20_000, Files: 1, Bytes: 31, BucketGranular: true, HasData: true},
		{Family: "data columns", Tier: "hot", FromSlot: 20_000, ToSlot: 30_000, Files: 1, Bytes: 37, BucketGranular: true, HasData: true},
		{Family: "beacon state", Tier: "cold", FromSlot: 0, ToSlot: 400, Files: 2, Bytes: 52, HasData: true},
	}, result.Rows)
	require.Equal(t, uint64(31+37), result.Rows[3].Bytes+result.Rows[4].Bytes)
	require.Greater(t, result.MDBX.Bytes, uint64(0))
	require.NotEmpty(t, result.MDBX.Tables)
	require.Equal(t, result.MDBX.Bytes+24+36+31+37+52, result.TotalBytes)
}

func TestCollectCaplinDuManifestMissing(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	local := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	writeCaplinDuFile(t, dirs.Snap, local, 11)
	writer := openCaplinDuDB(t, dirs.CaplinIndexing)
	writer.Close()
	db := openCaplinDuReadOnly(t, dirs.CaplinIndexing)
	defer db.Close()
	manifest := snapcfg.PreverifiedItems{
		{Name: local},
		{Name: snaptype.BeaconBlocks.FileName(version.V1_1, 100, 200)},
	}
	result, err := CollectCaplinDu(t.Context(), CaplinDuRequest{
		Chain:        "mainnet",
		Dirs:         dirs,
		BeaconConfig: &clparams.MainnetBeaconConfig,
		IndexDB:      db,
		Manifest:     manifest,
	})
	require.NoError(t, err)
	archive := estimateByName(t, result, "archive")
	require.Equal(t, uint64(11), archive.Bytes)
	require.Equal(t, 1, archive.NotLocal)
}

func TestCollectCaplinDuEmpty(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	writer := openCaplinDuDB(t, dirs.CaplinIndexing)
	writer.Close()
	db := openCaplinDuReadOnly(t, dirs.CaplinIndexing)
	defer db.Close()
	result, err := CollectCaplinDu(t.Context(), CaplinDuRequest{
		Chain:        "mainnet",
		Dirs:         dirs,
		BeaconConfig: &clparams.MainnetBeaconConfig,
		IndexDB:      db,
	})
	require.NoError(t, err)
	require.False(t, result.RegimeStored)
	require.Equal(t, "none stored", result.Regime)
	require.Nil(t, result.Floors)
	require.Zero(t, result.HeadSlot)
	for _, row := range result.Rows {
		require.Zero(t, row.Bytes, row.Family, row.Tier)
	}
}

func TestFormatCaplinDu(t *testing.T) {
	result := CaplinDuResult{
		Chain:        "mainnet",
		Regime:       "spec",
		RegimeStored: true,
		Floors:       &clparams.CaplinRetentionFloors{Blocks: 100, Sidecars: 200, States: 0},
		HeadSlot:     250,
		Rows: []CaplinDuRow{
			{Family: "beacon blocks", Tier: "cold", FromSlot: 0, ToSlot: 100, Files: 2, Bytes: 24, HasData: true},
			{Family: "beacon blocks", Tier: "hot", FromSlot: 150, ToSlot: 250, HasData: true},
		},
		MDBX:       CaplinDuMDBX{Bytes: 4096, Tables: []CaplinDuTable{{Name: "BeaconBlock", Bytes: 2048}}},
		TotalBytes: 4144,
		Estimates:  []CaplinDuEstimate{{Regime: "archive", Bytes: 24}, {Regime: "spec", Bytes: 24}},
	}
	var human bytes.Buffer
	FormatCaplinDu(&human, result)
	require.Equal(t, "chain mainnet · regime spec (stored) · floors blocks=100 sidecars=200 states=0 · head slot 250\n\n"+
		"family          tier  from slot   to slot files      size\n"+
		"beacon blocks   cold          0       100     2       24B\n"+
		"beacon blocks   hot         150       250     -        0B\n\n"+
		"caplin mdbx     -             -         -     -     4.0KB (BeaconBlock 2.0KB)\n"+
		"total                                                  4.0KB\n\n"+
		"estimated size per regime\n"+
		"archive           24B\n"+
		"spec              24B\n", human.String())

	var encoded bytes.Buffer
	require.NoError(t, FormatCaplinDuJSON(&encoded, result))
	var got map[string]any
	require.NoError(t, json.Unmarshal(encoded.Bytes(), &got))
	require.Equal(t, map[string]any{
		"chain":         "mainnet",
		"regime":        "spec",
		"regime_stored": true,
		"floors":        map[string]any{"blocks": float64(100), "sidecars": float64(200), "states": float64(0)},
		"head_slot":     float64(250),
		"rows": []any{
			map[string]any{"family": "beacon blocks", "tier": "cold", "from_slot": float64(0), "to_slot": float64(100), "files": float64(2), "bytes": float64(24)},
			map[string]any{"family": "beacon blocks", "tier": "hot", "from_slot": float64(150), "to_slot": float64(250), "files": float64(0), "bytes": float64(0)},
		},
		"mdbx": map[string]any{
			"bytes":  float64(4096),
			"tables": []any{map[string]any{"name": "BeaconBlock", "bytes": float64(2048)}},
		},
		"total_bytes": float64(4144),
		"estimates": []any{
			map[string]any{"regime": "archive", "bytes": float64(24), "not_local": float64(0)},
			map[string]any{"regime": "spec", "bytes": float64(24), "not_local": float64(0)},
		},
	}, got)
}

func writeCaplinDuFile(t *testing.T, dir, name string, size int) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, name), bytes.Repeat([]byte{'x'}, size), 0o644))
}

func openCaplinDuDB(t *testing.T, dir string) kv.RwDB {
	t.Helper()
	require.NoError(t, os.MkdirAll(dir, 0o700))
	db, err := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dir).WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).Open(t.Context())
	require.NoError(t, err)
	return db
}

func openCaplinDuReadOnly(t *testing.T, dir string) kv.RoDB {
	t.Helper()
	db, err := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dir).Accede(true).Readonly(true).WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).Open(t.Context())
	require.NoError(t, err)
	return db
}

func estimateByName(t *testing.T, result CaplinDuResult, name string) CaplinDuEstimate {
	t.Helper()
	for _, estimate := range result.Estimates {
		if estimate.Regime == name {
			return estimate
		}
	}
	t.Fatalf("estimate %q not found in %v", name, result.Estimates)
	return CaplinDuEstimate{}
}

func TestDuColdFiles(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	block := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	writeCaplinDuFile(t, dirs.Snap, block, 11)
	writeCaplinDuFile(t, dirs.Snap, snaptype.BeaconBlocks.IdxFileNames(0, 100)[0], 13)
	writeCaplinDuFile(t, dirs.Snap, block+".torrent", 17)
	writeCaplinDuFile(t, dirs.Snap, block+".tmp", 19)
	writeCaplinDuFile(t, dirs.Snap, snaptype.BlobSidecars.FileName(version.V1_1, 0, 10_000), 23)
	writeCaplinDuFile(t, dirs.Snap, snaptype.BlobSidecars.IdxFileNames(0, 10_000)[0], 29)
	for _, table := range snaptype.CaplinStateSnapshotTypes {
		writeCaplinDuFile(t, dirs.SnapCaplin, table.FileName(version.V1_1, 0, 100), 1)
	}
	writeCaplinDuFile(t, dirs.SnapCaplin, "v1.1-000000-000100-Unregistered.seg", 31)
	writeCaplinDuFile(t, dirs.Snap, "v1.1-000000-000100-headers.seg", 37)
	r, err := CollectCaplinDu(t.Context(), CaplinDuRequest{Chain: "mainnet", Dirs: dirs, BeaconConfig: &clparams.MainnetBeaconConfig})
	require.NoError(t, err)
	require.Equal(t, 2, r.Rows[2].Files)
	require.Equal(t, uint64(52), r.Rows[2].Bytes)
	require.Equal(t, len(snaptype.CaplinStateSnapshotTypes), r.Rows[5].Files)
	require.Equal(t, uint64(len(snaptype.CaplinStateSnapshotTypes)), r.Rows[5].Bytes)
	if row := r.Rows[0]; row.Files != 2 || row.Bytes != 24 {
		t.Fatalf("cold block row = %d files, %d bytes; want 2 files, 24 bytes", row.Files, row.Bytes)
	}
}

func TestDuTotalCountsMDBXOnce(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := openCaplinDuDB(t, dirs.CaplinIndexing)
	defer db.Close()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(42, common.Hash{1}), []byte{1})
	}))
	r, err := CollectCaplinDu(t.Context(), CaplinDuRequest{Chain: "mainnet", Dirs: dirs, IndexDB: db, BeaconConfig: &clparams.MainnetBeaconConfig})
	require.NoError(t, err)
	require.Equal(t, uint64(42), r.HeadSlot)
	require.Equal(t, uint64(42), r.Rows[1].FromSlot)
	require.Equal(t, uint64(42), r.Rows[1].ToSlot)
	stat, err := os.Stat(filepath.Join(dirs.CaplinIndexing, "mdbx.dat"))
	require.NoError(t, err)
	if n := int64(r.TotalBytes) - stat.Size(); n != 0 {
		t.Fatalf("total exceeds the sole data file by %d bytes (BeaconBlocks = %d)", n, r.Rows[1].Bytes)
	}
}

func TestDuColdHead(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	for _, span := range [][2]uint64{{0, 100}, {0, 50}, {50, 100}} {
		writeStateFixture(t, dirs.Snap, snaptype.BeaconBlocks.Name(), span[0], span[1], log.New())
	}
	db := openCaplinDuDB(t, dirs.CaplinIndexing)
	defer db.Close()
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		var parent common.Hash
		for slot := uint64(20_000); slot < 30_000; slot += 500 {
			root := common.Hash{byte(slot / 500)}
			if err := beacon_indicies.MarkRootCanonical(tx, slot, root); err != nil {
				return err
			}
			if err := beacon_indicies.WriteParentBlockRoot(tx, root, parent); err != nil {
				return err
			}
			if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(slot, root), root[:]); err != nil {
				return err
			}
			parent = root
		}
		return nil
	}))
	require.NoError(t, freezeblocks.DumpBeaconBlocks(t.Context(), db, 20_000, 30_000, 1, dirs, 1, log.LvlDebug, log.New()))
	r, err := CollectCaplinDu(t.Context(), CaplinDuRequest{Chain: "mainnet", Dirs: dirs, IndexDB: db, BeaconConfig: &clparams.MainnetBeaconConfig})
	require.NoError(t, err)
	require.Equal(t, 8, r.Rows[0].Files)
	require.Equal(t, uint64(0), r.Rows[0].FromSlot)
	require.Equal(t, uint64(30_000), r.Rows[0].ToSlot)
	if r.HeadSlot != 29_500 {
		t.Fatalf("head slot = %d; last block dumped at 29500", r.HeadSlot)
	}
}

func TestDuEmptyHotBuckets(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	for _, root := range []string{dirs.CaplinBlobs, dirs.CaplinColumnData} {
		require.NoError(t, os.Mkdir(filepath.Join(root, "1"), 0o755))
		writeCaplinDuFile(t, filepath.Join(root, "3"), common.Hash{1}.String()+"_0.tmp", 7)
		writeCaplinDuFile(t, filepath.Join(root, "old"), common.Hash{2}.String()+"_0", 11)
	}
	r, err := CollectCaplinDu(t.Context(), CaplinDuRequest{Chain: "mainnet", Dirs: dirs, BeaconConfig: &clparams.MainnetBeaconConfig})
	require.NoError(t, err)
	for _, row := range r.Rows[3:5] {
		if row.HasData || row.Files != 0 || row.Bytes != 0 {
			t.Errorf("%s = %d..%d, %d files, %d bytes; zero stored sidecars", row.Family, row.FromSlot, row.ToSlot, row.Files, row.Bytes)
		}
	}
}

func TestDuManifestNeedsRegularFile(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	name := snaptype.BlobSidecars.FileName(version.V1_1, 0, 10_000)
	require.NoError(t, os.Mkdir(filepath.Join(dirs.Snap, name), 0o755))
	r, err := CollectCaplinDu(t.Context(), CaplinDuRequest{
		Chain: "mainnet", Dirs: dirs, BeaconConfig: &clparams.MainnetBeaconConfig,
		Manifest: snapcfg.PreverifiedItems{{Name: name}},
	})
	require.NoError(t, err)
	e := estimateByName(t, r, "archive")
	if e.NotLocal != 1 || e.Bytes != 0 {
		t.Fatalf("archive = %d bytes, %d not local; only a directory exists", e.Bytes, e.NotLocal)
	}
}

func TestDuLargeColumns(t *testing.T) {
	r := CaplinDuResult{HeadSlot: 15_660_000, Rows: []CaplinDuRow{{
		Family: "beacon blocks", Tier: "cold", FromSlot: 14_600_000, ToSlot: 15_660_000, Files: 206, Bytes: 2 << 40, HasData: true,
	}}}
	var b bytes.Buffer
	FormatCaplinDu(&b, r)
	lines := strings.Split(b.String(), "\n")
	header, row := lines[2], lines[3]
	fields := strings.Fields(row)
	for i, name := range []string{"from slot", "to slot", "files", "size"} {
		value := fields[i+3]
		end := strings.Index(header, name) + len(name)
		if got := strings.Index(row, value) + len(value); got != end {
			t.Errorf("%s ends at column %d; heading ends at %d", name, got, end)
		}
	}
}

func TestDuOccupiedHotBucket(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := openCaplinDuDB(t, dirs.CaplinIndexing)
	defer db.Close()
	const slot = 12_345
	root := common.Hash{1}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteHeaderSlot(tx, root, slot)
	}))
	store := blob_storage.NewBlobStore(db, afero.NewBasePathFs(afero.NewOsFs(), dirs.CaplinBlobs))
	blob := cltypes.NewBlobSidecar(0, &cltypes.Blob{}, common.Bytes48{}, common.Bytes48{},
		&cltypes.SignedBeaconBlockHeader{Header: &cltypes.BeaconBlockHeader{Slot: slot}}, solid.NewHashVector(cltypes.CommitmentBranchSize))
	require.NoError(t, store.WriteBlobSidecars(t.Context(), root, []*cltypes.BlobSidecar{blob}))
	r, err := CollectCaplinDu(t.Context(), CaplinDuRequest{Chain: "mainnet", Dirs: dirs, IndexDB: db, BeaconConfig: &clparams.MainnetBeaconConfig})
	require.NoError(t, err)
	if row := r.Rows[3]; row.FromSlot != 10_000 || row.ToSlot != 20_000 || !row.BucketGranular {
		t.Fatalf("hot blobs = %d..%d, bucket granularity %t; want 10000..20000, true", row.FromSlot, row.ToSlot, row.BucketGranular)
	}
}
