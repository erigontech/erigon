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
	"errors"
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

// UpgradeSegHeadersV2 patches all V1 snapshot files (both E3 state files and
// block/caplin .seg files) to V2 in-place.  It is the manual equivalent of the
// SegHeaderV2 + SegHeaderV2Seg migrations.  Run it once after upgrading:
//
//	erigon snapshots upgrade-seg-headers --datadir=<path>
func UpgradeSegHeadersV2(dirs datadir.Dirs, logger log.Logger) error {
	for _, d := range []string{
		dirs.Snap,
		dirs.SnapDomain,
		dirs.SnapHistory,
		dirs.SnapIdx,
		dirs.SnapAccessors,
		dirs.SnapCaplin,
	} {
		if err := upgradeAndSmokeTestSegFilesInDir(d, logger); err != nil {
			return err
		}
	}
	if err := upgradeAndSmokeTestDotSegFilesInDir(dirs.Snap, false, logger); err != nil {
		return err
	}
	return upgradeAndSmokeTestDotSegFilesInDir(dirs.SnapCaplin, true, logger)
}

// SegHeaderV2 patches V1 .kv/.v/.ef snapshot headers to V2 in-place, recording
// word-level key/val compression flags taken from the frozen segCompressionAtV2 table.
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
	return walkSegDir(dir, func(path string) error {
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

// walkSegDir invokes fn for each file under dir, treating a missing dir and
// transient not-exist errors as a no-op.
func walkSegDir(dir string, fn func(path string) error) error {
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		return fn(path)
	})
}

// removeStaleTorrents removes the .torrent and any partial torrent artifacts
// (.torrent<suffix>) of a snapshot file whose bytes were just patched, so the
// downloader regenerates correct ones.
func removeStaleTorrents(path string) {
	_ = dir.RemoveFilesByMask(path + ".torrent*")
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

	if err := setV2Header(path, v2Bitmask(pageCnt, fc)); err != nil {
		return err
	}
	removeStaleTorrents(path)

	logger.Debug("[seg_header_v2] upgraded", "file", base)
	return nil
}

// v2Bitmask builds the V2 header bitmask from the page-value count and the
// file's word-level key/val compression.
func v2Bitmask(pageCnt int, fc seg.FileCompression) seg.FeatureFlagBitmask {
	var b seg.FeatureFlagBitmask
	if pageCnt > 0 {
		b.Set(seg.PageLevelCompressionEnabled)
	}
	if fc.Has(seg.CompressKeys) {
		b.Set(seg.WordLevelKeyCompressionEnabled)
	}
	if fc.Has(seg.CompressVals) {
		b.Set(seg.WordLevelValCompressionEnabled)
	}
	return b
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

// segTag extracts the filename base from an E3 snapshot name laid out as
// "{version}-{name}.{from}-{to}.{ext}", e.g. "v2.0-accounts.0-128.kv" → "accounts".
func segTag(base, ext string) string {
	withoutExt := base[:len(base)-len(ext)]
	dashIdx := strings.Index(withoutExt, "-")
	if dashIdx < 0 {
		return ""
	}
	nameAndRange := withoutExt[dashIdx+1:]
	dotIdx := strings.Index(nameAndRange, ".")
	if dotIdx < 0 {
		return nameAndRange
	}
	return nameAndRange[:dotIdx]
}

// segCompressionAtV2 freezes the per-file compression in effect when this
// migration was written, keyed by "{filenameBase}{ext}"; it deliberately does
// not read statecfg.Schema so later schema changes cannot alter patched files.
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

// smokeTestSegFile reads a bounded prefix of a patched V2 file via NewReader; a
// wrong bitmask makes decompression panic, which is recovered into an error.
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
	// Keep the scratch buffer separate from the return value: for mixed-compression
	// files Next returns an mmap-backed slice for uncompressed words, and feeding
	// that back as the append buffer for the next compressed word writes into the
	// read-only mmap (SIGBUS).
	buf := make([]byte, 0, 4096)
	for i := 0; i < smokeTestMaxWords && r.HasNext(); i++ {
		_, _ = r.Next(buf[:0])
	}
	logger.Trace("[seg_header_v2] smoke-test ok", "file", filepath.Base(path))
	return nil
}
