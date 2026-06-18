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
	"path/filepath"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/db/snaptype2"
)

// SegHeaderV2Seg patches V1 .seg snapshot headers to V2 in-place. Compression is
// inferred from the file's directory and name rather than its content, because
// content detection (SkipUncompressed on a compressed word) can silently misread.
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

func upgradeAndSmokeTestDotSegFilesInDir(d string, isCaplinStateDir bool, logger log.Logger) error {
	return walkSegDir(d, func(path string) error {
		if filepath.Ext(path) != ".seg" {
			return nil
		}
		if err := upgradeSegHeaderV1toV2Seg(path, isCaplinStateDir, logger); err != nil {
			return err
		}
		return smokeTestSegFile(path, logger)
	})
}

func upgradeSegHeaderV1toV2Seg(path string, isCaplinStateDir bool, logger log.Logger) error {
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

	fc, ok := dotSegCompression(path, isCaplinStateDir)
	if !ok {
		logger.Warn("[seg_header_v2_seg] skip (unparseable filename)", "file", base)
		return nil
	}

	if err := setV2Header(path, v2Bitmask(pageCnt, fc)); err != nil {
		return err
	}
	removeStaleTorrents(path)

	logger.Debug("[seg_header_v2_seg] upgraded", "file", base, "compression", fc)
	return nil
}

// dotSegCompression returns the compression a .seg file was written with: caplin
// state is uncompressed, header/body/transaction files are range-based (matching
// dumpRange), and every other type is always fully compressed.
func dotSegCompression(path string, isCaplinStateDir bool) (seg.FileCompression, bool) {
	if isCaplinStateDir {
		return seg.CompressNone, true
	}
	info, _, ok := snaptype.ParseFileName(filepath.Dir(path), filepath.Base(path))
	if !ok || info.Type == nil {
		return 0, false
	}
	switch info.Type.Enum() {
	case snaptype2.Enums.Headers, snaptype2.Enums.Bodies, snaptype2.Enums.Transactions:
		if info.To-info.From >= snaptype.Erigon2MergeLimit-1 {
			return seg.CompressKeys | seg.CompressVals, true
		}
		return seg.CompressNone, true
	default:
		return seg.CompressKeys | seg.CompressVals, true
	}
}
