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

package reset

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/anacrolix/torrent/bencode"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/preverified"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/version"
)

func TestCaplinResetPlanCoverageAndOnlyCopies(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	blockLocal := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	blockReplacement := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)
	blobLocal := snaptype.BlobSidecars.FileName(version.V1_1, 200, 300)
	stateLocal := snaptype.StateEvents.FileName(version.V1_1, 0, 100)
	stateReplacement := snaptype.StateEvents.FileName(version.V1_1, 0, 200)
	stateOnlyCopy := snaptype.StateRoot.FileName(version.V1_1, 0, 100)
	badTorrent := blockReplacement + ".torrent"
	unknown := "v1.1-000000-000100-BlockProposers.seg"
	for _, name := range []string{blockLocal, blobLocal, badTorrent, unknown} {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, name), []byte(name), 0o644))
	}
	for _, name := range []string{stateLocal, stateOnlyCopy} {
		require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapCaplin, name), []byte(name), 0o644))
	}

	items := preverified.SortedItems{{Name: blockReplacement, Hash: "block"}, {Name: stateReplacement, Hash: "state"}}
	items.Sort()
	plan, err := BuildCaplinResetPlan(context.Background(), dirs, items, nil, true, log.New())
	require.NoError(t, err)
	require.ElementsMatch(t, []string{blockLocal, blockReplacement, blobLocal, filepath.Join("caplin", stateLocal), filepath.Join("caplin", stateOnlyCopy)}, caplinResetNames(plan.Files))
	require.Contains(t, plan.Files, CaplinResetFile{Name: blockReplacement, Path: filepath.Join(dirs.Snap, badTorrent), Family: "blocks", From: 0, To: 200, Torrent: true})
	require.ElementsMatch(t, []string{"sidecars 200..300", "state/StateRoot 0..100"}, caplinResetOnlyCopyNames(plan.OnlyCopies))
	require.NotContains(t, caplinResetNames(plan.Files), unknown)

	err = plan.Apply(context.Background(), nil, os.Remove, false)
	require.ErrorContains(t, err, "only copy")
	for _, name := range []string{blockLocal, blobLocal} {
		require.FileExists(t, filepath.Join(dirs.Snap, name))
	}
	require.FileExists(t, filepath.Join(dirs.SnapCaplin, stateLocal))
	require.FileExists(t, filepath.Join(dirs.SnapCaplin, stateOnlyCopy))
	for _, name := range []string{blockLocal, blobLocal} {
		require.NoError(t, dir.RemoveFile(filepath.Join(dirs.Snap, name)))
	}
	require.NoError(t, dir.RemoveFile(filepath.Join(dirs.SnapCaplin, stateLocal)))
	require.NoError(t, dir.RemoveFile(filepath.Join(dirs.SnapCaplin, stateOnlyCopy)))
}

func TestCaplinResetDeletesDerivedRowsAndLowersProgress(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	local := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	replacement := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, local), []byte(local), 0o644))
	db, openErr := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dirs.CaplinIndexing).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).Open(t.Context())
	require.NoError(t, openErr)
	defer db.Close()
	root := common.HexToHash("0x01")
	stateRoot := common.HexToHash("0x02")
	parent := common.HexToHash("0x03")
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		if err := beacon_indicies.MarkRootCanonical(tx, 50, root); err != nil {
			return err
		}
		if err := beacon_indicies.WriteHeaderSlot(tx, root, 50); err != nil {
			return err
		}
		if err := beacon_indicies.WriteStateRoot(tx, root, stateRoot); err != nil {
			return err
		}
		if err := beacon_indicies.WriteParentBlockRoot(tx, root, parent); err != nil {
			return err
		}
		if err := beacon_indicies.WriteExecutionBlockNumber(tx, root, 7); err != nil {
			return err
		}
		if err := beacon_indicies.WriteExecutionBlockHash(tx, root, common.HexToHash("0x04")); err != nil {
			return err
		}
		if err := tx.Put(kv.BeaconBlocks, dbutils.BlockBodyKey(50, root), []byte("hot")); err != nil {
			return err
		}
		if err := beacon_indicies.WriteLastBeaconSnapshot(tx, 100); err != nil {
			return err
		}
		return beacon_indicies.WriteCaplinRetention(tx, clparams.CaplinRetention{Mode: clparams.CaplinPruneModeSpec})
	}))
	items := preverified.SortedItems{{Name: replacement, Hash: "replacement"}}
	plan, err := BuildCaplinResetPlan(t.Context(), dirs, items, db, true, log.New())
	require.NoError(t, err)
	require.True(t, plan.StoredRetentionSet)
	require.Equal(t, uint64(0), plan.ProgressAfter)
	require.NotEmpty(t, plan.DBRows)
	var report bytes.Buffer
	plan.Format(&report)
	require.Contains(t, report.String(), "stored regime spec (kept)")
	require.NoError(t, plan.Apply(t.Context(), db, os.Remove, false))
	require.NoFileExists(t, filepath.Join(dirs.Snap, local))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		value, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(50, root))
		if err != nil {
			return err
		}
		require.Equal(t, []byte("hot"), value)
		canonical, err := tx.GetOne(kv.CanonicalBlockRoots, []byte{0, 0, 0, 50})
		if err != nil {
			return err
		}
		require.Empty(t, canonical)
		progress, err := beacon_indicies.ReadLastBeaconSnapshot(tx)
		require.NoError(t, err)
		require.Zero(t, progress)
		stored, found, err := beacon_indicies.ReadCaplinRetention(tx)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, clparams.CaplinPruneModeSpec, stored.Mode)
		return nil
	}))
}

func caplinResetNames(files []CaplinResetFile) []string {
	names := make([]string, 0, len(files))
	for _, file := range files {
		names = append(names, file.Name)
	}
	return names
}

func caplinResetOnlyCopyNames(files []CaplinResetFile) []string {
	names := make([]string, 0, len(files))
	for _, file := range files {
		names = append(names, file.Family+" "+formatCaplinResetRange(file.From, file.To))
	}
	return names
}

func TestResetAdjacentReplacements(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	file := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, file), []byte(file), 0o600))
	items := preverified.SortedItems{
		{Name: snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)},
		{Name: snaptype.BeaconBlocks.FileName(version.V1_1, 100, 200)},
	}
	plan, err := BuildCaplinResetPlan(t.Context(), dirs, items, nil, true, log.New())
	require.NoError(t, err)
	if err := plan.Apply(t.Context(), nil, os.Remove, false); err != nil {
		t.Fatalf("slots [0,200) are covered: %v", err)
	}
}

func reviewResetDB(t *testing.T, dirs datadir.Dirs, progress uint64) kv.RwDB {
	t.Helper()
	db := mdbx.New(dbcfg.CaplinDB, log.New()).Path(dirs.CaplinIndexing).
		WithTableCfg(func(kv.TableCfg) kv.TableCfg { return kv.ChaindataTablesCfg }).MustOpen()
	t.Cleanup(db.Close)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteLastBeaconSnapshot(tx, progress)
	}))
	return db
}

func TestResetKeepsHotCanonicalLookup(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := reviewResetDB(t, dirs, 100)
	block := cltypes.NewSignedBeaconBlock(&clparams.MainnetBeaconConfig, clparams.Phase0Version)
	block.Block.Slot = 150
	root, err := block.Block.HashSSZ()
	require.NoError(t, err)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.WriteBeaconBlockAndIndicies(tx, block, true)
	}))
	file := snaptype.BeaconBlocks.FileName(version.V1_1, 100, 200)
	compressor, err := seg.NewCompressor(t.Context(), "unindexed segment", filepath.Join(dirs.Snap, file), dirs.Tmp, seg.DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer compressor.Close()
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		body, getErr := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(150, root))
		require.NoError(t, getErr)
		for slot := 100; slot < 200; slot++ {
			var word []byte
			if slot == 150 {
				word = body
			}
			require.NoError(t, compressor.AddWord(word))
		}
		return nil
	}))
	require.NoError(t, compressor.Compress())
	plan, err := BuildCaplinResetPlan(t.Context(), dirs, nil, db, true, log.New())
	require.NoError(t, err)
	require.NoError(t, plan.Apply(t.Context(), db, os.Remove, true))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		body, err := tx.GetOne(kv.BeaconBlocks, dbutils.BlockBodyKey(150, root))
		require.NoError(t, err)
		require.NotEmpty(t, body)
		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
		require.NoError(t, err)
		if slot == nil || *slot != 150 {
			t.Fatalf("surviving hot block: root lookup = %v, want slot 150", slot)
		}
		return nil
	}))
}

func TestResetKeepsRowsAboveDeletedRange(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := reviewResetDB(t, dirs, 400)
	file := snaptype.BeaconBlocks.FileName(version.V1_1, 100, 200)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, file), []byte(file), 0o600))
	roots := make(map[uint64]common.Hash)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for _, slot := range []uint64{150, 250, 350} {
			root := common.Hash{byte(slot)}
			roots[slot] = root
			if err := beacon_indicies.MarkRootCanonical(tx, slot, root); err != nil {
				return err
			}
			if err := beacon_indicies.WriteHeaderSlot(tx, root, slot); err != nil {
				return err
			}
			if err := beacon_indicies.WriteParentBlockRoot(tx, root, common.Hash{byte(slot + 1)}); err != nil {
				return err
			}
		}
		return nil
	}))
	plan, err := BuildCaplinResetPlan(t.Context(), dirs, nil, db, true, log.New())
	require.NoError(t, err)
	require.NoError(t, plan.Apply(t.Context(), db, os.Remove, true))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		for _, slot := range []uint64{250, 350} {
			root := roots[slot]
			canonical, err := tx.GetOne(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(slot))
			require.NoError(t, err)
			require.Equal(t, root[:], canonical)
			storedSlot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, root)
			require.NoError(t, err)
			require.NotNil(t, storedSlot)
			require.Equal(t, slot, *storedSlot)
		}
		canonical, err := tx.GetOne(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(150))
		require.NoError(t, err)
		require.Empty(t, canonical)
		return nil
	}))
}

func TestResetPartialDownloadKeepsIndex(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := reviewResetDB(t, dirs, 200)
	local := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100) + ".part"
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, local), nil, 0o600))
	complete := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, complete), []byte(complete), 0o600))
	plan, err := BuildCaplinResetPlan(t.Context(), dirs, preverified.SortedItems{{Name: complete}}, db, true, log.New())
	require.NoError(t, err)
	require.NoError(t, plan.Apply(t.Context(), db, os.Remove, false))
	require.NoFileExists(t, filepath.Join(dirs.Snap, local))
	require.FileExists(t, filepath.Join(dirs.Snap, complete))
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		progress, err := beacon_indicies.ReadLastBeaconSnapshot(tx)
		require.NoError(t, err)
		if progress != 200 {
			t.Fatalf("only a .part was removed: index progress = %d, want 200", progress)
		}
		return nil
	}))
}

func TestResetRetryAfterFailure(t *testing.T) {
	for _, failure := range []string{"second file", "DB update"} {
		t.Run(failure, func(t *testing.T) {
			dirs := datadir.New(t.TempDir())
			db := reviewResetDB(t, dirs, 200)
			start := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
			end := snaptype.BeaconBlocks.FileName(version.V1_1, 100, 200)
			for _, name := range []string{start, end} {
				require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, name), []byte(name), 0o600))
			}
			items := preverified.SortedItems{{Name: snaptype.BeaconBlocks.FileName(version.V1_1, 0, 200)}}
			build := func() CaplinResetPlan {
				plan, err := BuildCaplinResetPlan(t.Context(), dirs, items, db, true, log.New())
				require.NoError(t, err)
				return plan
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			err := build().Apply(ctx, db, func(path string) error {
				if filepath.Base(path) == end {
					if failure == "second file" {
						return os.ErrPermission
					}
					cancel()
				}
				return dir.RemoveFile(path)
			}, false)
			if failure == "second file" {
				require.ErrorIs(t, err, os.ErrPermission)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.NoError(t, build().Apply(t.Context(), db, dir.RemoveFile, false))
			require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
				progress, err := beacon_indicies.ReadLastBeaconSnapshot(tx)
				require.NoError(t, err)
				if progress != 0 {
					t.Fatalf("both files removed: retry left progress = %d, want 0", progress)
				}
				return nil
			}))
		})
	}
}

func TestResetReportsExistingRows(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := reviewResetDB(t, dirs, 100)
	root := common.Hash{1}
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		return beacon_indicies.MarkRootCanonical(tx, 42, root)
	}))
	for _, end := range []uint64{50, 100} {
		file := snaptype.BeaconBlocks.FileName(version.V1_1, 0, end)
		require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, file), []byte(file), 0o600))
	}
	plan, err := BuildCaplinResetPlan(t.Context(), dirs, nil, db, true, log.New())
	require.NoError(t, err)
	for _, row := range plan.DBRows {
		if row.Table == kv.CanonicalBlockRoots && row.Rows != 1 {
			t.Errorf("one canonical root: plan reports %d rows, want 1", row.Rows)
		}
		if row.Table == kv.BlockRootToBlockNumber && row.Rows != 0 {
			t.Errorf("empty %s: plan reports %d row, want 0", row.Table, row.Rows)
		}
	}
}

func TestResetNamesBadTorrent(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	name := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	info, err := bencode.Marshal(metainfo.Info{Name: name, PieceLength: 16384, Length: 1, Pieces: make([]byte, 20)})
	require.NoError(t, err)
	var buf bytes.Buffer
	mi := metainfo.MetaInfo{InfoBytes: info}
	require.NoError(t, mi.Write(&buf))
	path := filepath.Join(dirs.Snap, name)
	require.NoError(t, os.WriteFile(path, []byte{0}, 0o600))
	require.NoError(t, os.WriteFile(path+".torrent", buf.Bytes(), 0o600))
	items := preverified.SortedItems{{Name: name, Hash: "0000000000000000000000000000000000000000"}}
	plan, err := BuildCaplinResetPlan(t.Context(), dirs, items, nil, true, log.New())
	require.NoError(t, err)
	var report bytes.Buffer
	plan.Format(&report)
	require.NoError(t, plan.Apply(t.Context(), nil, os.Remove, false))
	require.FileExists(t, path)
	require.NoFileExists(t, path+".torrent")
	if !strings.Contains(report.String(), name+".torrent") {
		t.Fatalf("removed .seg.torrent but plan printed %q", strings.Split(report.String(), "\n")[1])
	}
}

type resetCursorErrorDB struct {
	kv.RoDB
	t *testing.T
}

func (db resetCursorErrorDB) View(ctx context.Context, f func(kv.Tx) error) error {
	return db.RoDB.View(ctx, func(tx kv.Tx) error { return f(resetCursorErrorTx{Tx: tx, t: db.t}) })
}

type resetCursorErrorTx struct {
	kv.Tx
	t *testing.T
}

func (tx resetCursorErrorTx) Cursor(table string) (kv.Cursor, error) {
	if table == kv.CanonicalBlockRoots {
		cursor, err := tx.Tx.Cursor(table)
		if err != nil {
			return nil, err
		}
		tx.t.Cleanup(cursor.Close)
		return &resetCursorError{Cursor: cursor}, nil
	}
	return tx.Tx.Cursor(table)
}

type resetCursorError struct{ kv.Cursor }

func (c *resetCursorError) Next() ([]byte, []byte, error) { return nil, nil, io.ErrUnexpectedEOF }

func TestResetRejectsIncompleteRootScan(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	db := reviewResetDB(t, dirs, 100)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for _, slot := range []uint64{42, 43} {
			require.NoError(t, beacon_indicies.MarkRootCanonical(tx, slot, common.Hash{byte(slot)}))
		}
		return nil
	}))
	name := snaptype.BeaconBlocks.FileName(version.V1_1, 0, 100)
	require.NoError(t, os.WriteFile(filepath.Join(dirs.Snap, name), nil, 0o600))
	plan, err := BuildCaplinResetPlan(t.Context(), dirs, nil, resetCursorErrorDB{RoDB: db, t: t}, true, log.New())
	if err == nil {
		t.Fatalf("cursor failed after 1 of 2 roots: plan error = %v, planned roots = %d", err, len(plan.roots))
	}
	require.ErrorIs(t, err, io.ErrUnexpectedEOF)
}
