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
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
)

// SegHeaderV2Seg upgrades all V1 .seg snapshot files (block and caplin) to V2
// by patching the two-byte file header in-place.
//
// Compression is determined from the filename, not file content:
//   - caplin files (dirs.SnapCaplin): always CompressKeys|CompressVals
//   - block files with range >= MergeSteps[last] (10 000): merger-produced, CompressKeys|CompressVals
//   - block files with range < MergeSteps[last]: dumpRange-produced, CompressNone
var SegHeaderV2Seg = Migration{
	Name: "seg_header_v2_seg",
	Up: func(db kv.RwDB, dirs datadir.Dirs, progress []byte, BeforeCommit Callback, logger log.Logger) error {
		tx, err := db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()

		if err := upgradeAndSmokeTestDotSegFilesInDir(dirs.Snap, false, logger); err != nil {
			return err
		}
		if err := upgradeAndSmokeTestDotSegFilesInDir(dirs.SnapCaplin, true, logger); err != nil {
			return err
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}

func upgradeAndSmokeTestDotSegFilesInDir(d string, isCaplinDir bool, logger log.Logger) error {
	return filepath.WalkDir(d, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		if filepath.Ext(path) != ".seg" {
			return nil
		}
		if err := upgradeSegHeaderV1toV2Seg(path, isCaplinDir, logger); err != nil {
			return err
		}
		return smokeTestSegFile(path, logger)
	})
}

// upgradeSegHeaderV1toV2Seg patches a single .seg file from V1 to V2.
//
// Compression is determined from the filename, not content.  Content-based
// detection (DetectCompressType) is unreliable: SkipUncompressed on a
// compressed word may silently misread the length field without panicking,
// leading to a false "CompressNone" result and a wrong bitmask.
//
// Rules (derived from how files are created):
//   - caplin directory: BeaconBlocks / BlobSidecars are always fully compressed.
//   - block directory: merger produces files whose range is a multiple of
//     MergeSteps[last] (10 000) and always uses CompressKeys|CompressVals;
//     dumpRange produces 1 000-block files and uses CompressNone.
func upgradeSegHeaderV1toV2Seg(path string, isCaplinDir bool, logger log.Logger) error {
	base := filepath.Base(path)
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return fmt.Errorf("error creating decompressor: %v, %s", err, path)
	}
	version := d.CompressionFormatVersion()
	pageCnt := d.CompressedPageValuesCount()
	d.Close()
	if version < seg.FileCompressionFormatV1 {
		return nil // V0: no header to patch
	}

	var fc seg.FileCompression
	if isCaplinDir {
		// Caplin files are always fully compressed regardless of range.
		fc = seg.CompressKeys | seg.CompressVals
	} else {
		// Block files: merger-produced files (range >= MergeSteps[last]) are compressed;
		// initial dumpRange files (range < MergeSteps[last]) are not.
		info, _, ok := snaptype.ParseFileName(filepath.Dir(path), base)
		if !ok {
			logger.Warn("[seg_header_v2_seg] skip (unparseable filename)", "file", base)
			return nil
		}
		mergeStep := snaptype.MergeSteps[len(snaptype.MergeSteps)-1]
		if info.To-info.From >= mergeStep {
			fc = seg.CompressKeys | seg.CompressVals
		}
	}

	var bitmask seg.FeatureFlagBitmask
	if pageCnt > 0 {
		bitmask.Set(seg.PageLevelCompressionEnabled)
	}
	if fc.Has(seg.CompressKeys) {
		bitmask.Set(seg.WordLevelKeyCompressionEnabled)
	}
	if fc.Has(seg.CompressVals) {
		bitmask.Set(seg.WordLevelValCompressionEnabled)
	}

	if err := setV2Header(path, bitmask); err != nil {
		return err
	}
	_ = dir.RemoveFile(path + ".torrent")

	logger.Debug("[seg_header_v2_seg] upgraded", "file", base, "compression", fc)
	return nil
}
