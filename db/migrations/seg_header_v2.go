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
	"strings"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/seg"
)

// SegHeaderV2 upgrades all V1 .kv/.v/.ef snapshot files to V2 by patching
// the two-byte file header in-place.  V2 is identical to V1 except that the
// featureFlagBitmask byte now reliably encodes WordLevelKeyCompressionEnabled /
// WordLevelValCompressionEnabled in addition to PageLevelCompressionEnabled.
//
// The correct compression flags are looked up from statecfg.Schema so that
// no heuristic detection (DetectCompressType) is needed.
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

		// Smoke-test: open every upgraded file with a Reader and iterate all words
		// to catch any header corruption or compression-mismatch panics.
		for _, dir := range snapDirs {
			if err := smokeTestSegFiles(dir, logger); err != nil {
				return err
			}
		}

		if err := BeforeCommit(tx, nil, true); err != nil {
			return err
		}
		return tx.Commit()
	},
}

// segDataExts are the file extensions written by the seg Compressor that
// carry the version/featureFlag header.
var segDataExts = map[string]bool{".kv": true, ".v": true, ".ef": true}

func upgradeSegFilesInDir(dir string, logger log.Logger) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		ext := filepath.Ext(path)
		if !segDataExts[ext] {
			return nil
		}
		return upgradeSegHeaderV1toV2(path, ext, logger)
	})
}

// upgradeSegHeaderV1toV2 patches a single seg-format file from V1 to V2.
// It is a no-op for V0 files and files that are already V2+.
func upgradeSegHeaderV1toV2(path, ext string, logger log.Logger) error {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return err
	}
	if d.CompressionFormatVersion() != seg.FileCompressionFormatV1 {
		d.Close()
		return nil
	}

	pageCnt := d.CompressedPageValuesCount()
	d.Close() // release mmap before writing

	// Determine key/val compression from the schema lookup (tag = last dash-separated
	// component of the base name, e.g. "accounts" from "v1-000000-000100-accounts.seg").
	base := filepath.Base(path)
	tag := segTag(base, ext)
	fc := segCompressionAtV2[tag+ext] // zero value (CompressNone) if unknown

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

	logger.Debug("[seg_header_v2] upgraded", "file", base)
	return nil
}

// segTag extracts the file-type tag from a snapshot base name.
// Format: "{version}-{from}-{to}-{tag}.{ext}"  (first 3 fields are separated by "-").
func segTag(base, ext string) string {
	withoutExt := base[:len(base)-len(ext)]
	parts := strings.SplitN(withoutExt, "-", 4)
	if len(parts) == 4 {
		return parts[3]
	}
	return ""
}

// segCompressionAtV2 is a frozen snapshot of the compression settings that were in
// effect when the seg_header_v2 migration was written.  It intentionally does NOT
// read from statecfg.Schema so that future schema changes cannot alter what flags
// are patched into existing files.
//
// Layout: map["{filenameBase}{ext}"] = FileCompression
//
// Domains   (.kv = domain data, .v = history values, .ef = inverted-index keys)
// accounts  : kv=none,  v=none,      ef=none
// storage   : kv=keys,  v=none,      ef=none
// code      : kv=vals,  v=keys|vals, ef=none
// commitment: kv=keys,  v=none,      ef=none   (compression may be removed in the future)
// receipt   : kv=none,  v=none,      ef=none
// rcache    : kv=none,  v=none,      ef=none   (rcache disabled; included for completeness)
//
// Standalone inverted indexes
// logaddrs / logtopics / tracesfrom / tracesto : ef=none
var segCompressionAtV2 = map[string]seg.FileCompression{
	// --- accounts ---
	"accounts.kv": seg.CompressNone,
	"accounts.v":  seg.CompressNone,
	"accounts.ef": seg.CompressNone,
	// --- storage ---
	"storage.kv": seg.CompressKeys,
	"storage.v":  seg.CompressNone,
	"storage.ef": seg.CompressNone,
	// --- code ---
	"code.kv": seg.CompressVals,
	"code.v":  seg.CompressKeys | seg.CompressVals,
	"code.ef": seg.CompressNone,
	// --- commitment ---
	"commitment.kv": seg.CompressKeys,
	"commitment.v":  seg.CompressNone,
	"commitment.ef": seg.CompressNone,
	// --- receipt ---
	"receipt.kv": seg.CompressNone,
	"receipt.v":  seg.CompressNone,
	"receipt.ef": seg.CompressNone,
	// --- rcache ---
	"rcache.kv": seg.CompressNone,
	"rcache.v":  seg.CompressNone,
	"rcache.ef": seg.CompressNone,
	// --- standalone inverted indexes ---
	"logaddrs.ef":   seg.CompressNone,
	"logtopics.ef":  seg.CompressNone,
	"tracesfrom.ef": seg.CompressNone,
	"tracesto.ef":   seg.CompressNone,
}

// smokeTestSegFiles opens every upgraded file with a NewReader and iterates all
// words to confirm the header compression flags are consistent with the data.
// A panic from the decompressor means the flags are wrong.
func smokeTestSegFiles(dir string, logger log.Logger) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if !segDataExts[filepath.Ext(path)] {
			return nil
		}
		dec, err := seg.NewDecompressor(path)
		if err != nil {
			return err
		}
		defer dec.Close()

		g := dec.MakeGetter()
		r := seg.NewReader(g, seg.CompressNone) // NewReader will override from header for V2+
		r.Reset(0)
		var buf []byte
		for r.HasNext() {
			buf, _ = r.Next(buf[:0])
		}
		logger.Trace("[seg_header_v2] smoke-test ok", "file", filepath.Base(path))
		return nil
	})
}
