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
	"strings"

	"github.com/erigontech/erigon/common/dir"
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
// The correct compression flags are read from segCompressionAtV2, a frozen table
// captured at migration time, so future schema changes cannot alter what flags
// are written into existing files.
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
			if err := upgradeAndSmokeTestSegFilesInDir(dir, logger); err != nil {
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

// upgradeAndSmokeTestSegFilesInDir upgrades each eligible file in dir and
// immediately smoke-tests it in a single directory walk.
func upgradeAndSmokeTestSegFilesInDir(dir string, logger log.Logger) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		ext := filepath.Ext(path)
		if !segDataExts[ext] {
			return nil
		}
		if err := upgradeSegHeaderV1toV2(path, ext, logger); err != nil {
			return err
		}
		return smokeTestSegFile(path, logger)
	})
}

// upgradeSegHeaderV1toV2 patches a single seg-format file to V2.
// It is a no-op for V0 files (no version header). V1 and V2 files are both
// patched so that a previously interrupted migration with a wrong bitmask is
// corrected on re-run.
func upgradeSegHeaderV1toV2(path, ext string, logger log.Logger) error {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return err
	}
	if d.CompressionFormatVersion() < seg.FileCompressionFormatV1 {
		d.Close() // V0: no header to patch
		return nil
	}

	pageCnt := d.CompressedPageValuesCount()
	d.Close() // release mmap before writing

	base := filepath.Base(path)
	tag := segTag(base, ext)
	fc := segCompressionAtV2[tag+ext] // zero value if unknown; no key/value compression bits will be set

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

	// The header patch changes the file content, so any existing .torrent (which
	// is a hash of the file bytes) is now stale.  Remove it so the downloader
	// regenerates a correct one.
	_ = dir.RemoveFile(path + ".torrent")

	logger.Debug("[seg_header_v2] upgraded", "file", base)
	return nil
}

// setV2Header writes the V2 version byte and bitmask at offset 0.
// Snapshot files are read-only (0444), so it temporarily adds the owner-write
// bit and restores the original permissions afterwards.
func setV2Header(path string, bitmask seg.FeatureFlagBitmask) error {
	return withWritePerm(path, func(f *os.File) error {
		_, err := f.WriteAt([]byte{seg.FileCompressionFormatV2, byte(bitmask)}, 0)
		return err
	})
}

// withWritePerm temporarily grants owner-write permission on path, opens it
// O_RDWR, calls fn, syncs, closes, and restores the original file mode.
func withWritePerm(path string, fn func(*os.File) error) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	origMode := info.Mode()
	if err := os.Chmod(path, origMode|0200); err != nil { // 0200 = owner-write bit
		return err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		_ = os.Chmod(path, origMode)
		return err
	}
	fnErr := fn(f)
	syncErr := f.Sync()
	f.Close()
	_ = os.Chmod(path, origMode)
	if fnErr != nil {
		return fnErr
	}
	return syncErr
}

// segTag extracts the FilenameBase from an E3 snapshot file name.
// E3 format: "{version}-{name}.{from}-{to}.{ext}"
// e.g. "v2.0-accounts.0-128.kv" → "accounts"
//
//	"v3.0-logaddrs.0-128.ef"  → "logaddrs"
func segTag(base, ext string) string {
	withoutExt := base[:len(base)-len(ext)]
	// Split off the version prefix (everything before the first "-").
	dashIdx := strings.Index(withoutExt, "-")
	if dashIdx < 0 {
		return ""
	}
	nameAndRange := withoutExt[dashIdx+1:] // "accounts.0-128"
	// The name ends at the first ".".
	dotIdx := strings.Index(nameAndRange, ".")
	if dotIdx < 0 {
		return nameAndRange
	}
	return nameAndRange[:dotIdx] // "accounts"
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

// smokeTestSegFile reads a small prefix of a V2 file via NewReader, which
// routes each word to Next() (huffman) or NextUncompressed() based on the
// bitmask we just patched.  A wrong bitmask causes a word-boundary mismatch
// and a panic, catching header corruption early.
//
// Reading only the first smokeTestMaxWords words keeps the migration fast even
// on large snapshot sets, while still exercising the decompression path.
const smokeTestMaxWords = 2_000

func smokeTestSegFile(path string, logger log.Logger) (retErr error) {
	dec, err := seg.NewDecompressor(path)
	if err != nil {
		return fmt.Errorf("error creating decompressor: %v, %s", err, path)
	}
	defer dec.Close()

	if dec.CompressionFormatVersion() < seg.FileCompressionFormatV2 {
		return nil // not upgraded (V0), skip
	}

	fc, _ := dec.WordLevelCompression()
	defer func() {
		if rec := recover(); rec != nil {
			retErr = fmt.Errorf("smoke-test panic (wrong bitmask?): %v, file=%s, compression=%v", rec, path, fc)
		}
	}()

	logger.Debug("[seg_header_v2] smoke-test", "file", filepath.Base(path))
	g := dec.MakeGetter()
	r := seg.NewReader(g, seg.CompressNone) // NewReader reads WordLevelCompression from header
	r.Reset(0)
	var buf []byte
	for i := 0; i < smokeTestMaxWords && r.HasNext(); i++ {
		buf, _ = r.Next(buf[:0])
	}
	logger.Trace("[seg_header_v2] smoke-test ok", "file", filepath.Base(path))
	return nil
}
