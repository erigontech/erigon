// Copyright 2024 The Erigon Authors
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

package migrations

import (
	"context"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

// SegHeaderV2 upgrades all V1 .seg snapshot files to V2 by patching the
// two-byte file header in-place.  V2 is identical to V1 except that the
// featureFlagBitmask byte now reliably encodes KeyCompressionEnabled /
// ValCompressionEnabled in addition to PageLevelCompressionEnabled.
//
// The migration detects the actual per-file compression by calling
// seg.DetectCompressType on a getter, then overwrites bytes [0:2] of the
// file before any other state is touched.  The rest of the file is
// unchanged, so no re-compression is needed.
var SegHeaderV2 = Migration{
	Name: "seg_header_v2",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) error {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		snapDirs := []string{
			dirs.Snap,
			dirs.SnapDomain,
			dirs.SnapHistory,
			dirs.SnapIdx,
			dirs.SnapAccessors,
			dirs.SnapCaplin,
		}

		for _, dir := range snapDirs {
			if err := upgradeSegFilesInDir(dir, logger); err != nil {
				return err
			}
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}

func upgradeSegFilesInDir(dir string, logger log.Logger) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".seg" {
			return err
		}
		return upgradeSegHeaderV1toV2(path, logger)
	})
}

// upgradeSegHeaderV1toV2 patches a single .seg file from V1 to V2 format.
// It is a no-op for V0 files and files that are already V2+.
func upgradeSegHeaderV1toV2(path string, logger log.Logger) error {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return err
	}
	if d.CompressionFormatVersion() != seg.FileCompressionFormatV1 {
		d.Close()
		return nil
	}

	// Probe actual key/value compression before closing the mmap.
	pageCnt := d.CompressedPageValuesCount()
	fc := seg.DetectCompressType(d.MakeGetter())
	d.Close() // must release mmap before writing

	var bitmask seg.FeatureFlagBitmask
	if pageCnt > 0 {
		bitmask.Set(seg.PageLevelCompressionEnabled)
	}
	if fc.Has(seg.CompressKeys) {
		bitmask.Set(seg.KeyCompressionEnabled)
	}
	if fc.Has(seg.CompressVals) {
		bitmask.Set(seg.ValCompressionEnabled)
	}

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.WriteAt([]byte{seg.FileCompressionFormatV2, byte(bitmask)}, 0); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}

	logger.Debug("[seg_header_v2] upgraded", "file", filepath.Base(path))
	return nil
}
