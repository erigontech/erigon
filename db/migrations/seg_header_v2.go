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
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
)

// segFormatExts are the extensions written by the seg Compressor that carry the
// version/feature-flag header.
var segFormatExts = map[string]bool{".kv": true, ".v": true, ".ef": true, ".seg": true}

// UpgradeSegHeadersV2 patches every V1 seg-format snapshot file (.kv/.v/.ef/.seg)
// to a V2 header in-place, recording the word-level key/val compression detected
// from the file's contents. Run once after upgrading:
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
		if err := walkSegDir(d, func(path string) error {
			if !segFormatExts[filepath.Ext(path)] {
				return nil
			}
			return upgradeSegFileToV2(path, logger)
		}); err != nil {
			return err
		}
	}
	return nil
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

// upgradeSegFileToV2 patches a single seg-format file to a V2 header. Compression
// is detected from the file's contents because filename/range is unreliable: the
// merger writes 10k-step files compressed while dumpRange writes sub-100k files
// uncompressed. A read with the detected compression is validated before writing,
// so a wrong guess aborts with the file untouched rather than half-patched.
func upgradeSegFileToV2(path string, logger log.Logger) error {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return err
	}
	if d.CompressionFormatVersion() < seg.FileCompressionFormatV1 {
		d.Close() // V0: no header to patch
		return nil
	}
	pageCnt := d.CompressedPageValuesCount()
	fc := seg.DetectCompressType(d.MakeGetter())
	if err := checkReadable(d, fc); err != nil {
		d.Close()
		return fmt.Errorf("[seg_header_v2] %s: %w", filepath.Base(path), err)
	}
	d.Close() // release mmap before writing

	if err := setV2Header(path, v2Bitmask(pageCnt, fc)); err != nil {
		return err
	}
	removeStaleTorrents(path)
	logger.Debug("[seg_header_v2] upgraded", "file", filepath.Base(path), "compression", fc)
	return nil
}

// checkReadableMaxWords bounds the validation read so the migration stays fast on
// large snapshot sets while still exercising the decompression path.
const checkReadableMaxWords = 2_000

// checkReadable reads a bounded prefix of d with the given compression, recovering
// the decompressor's panic into an error if the words don't decode.
func checkReadable(d *seg.Decompressor, fc seg.FileCompression) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("not readable as compression=%v: %v", fc, rec)
		}
	}()
	r := seg.NewReader(d.MakeGetter(), fc)
	r.Reset(0)
	buf := make([]byte, 0, 4096)
	for i := 0; i < checkReadableMaxWords && r.HasNext(); i++ {
		_, _ = r.Next(buf[:0])
	}
	return nil
}

// v2Bitmask builds the V2 header bitmask from the page-value count and the file's
// word-level key/val compression.
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

// removeStaleTorrents removes the .torrent and any partial torrent artifacts
// (.torrent<suffix>) of a snapshot file whose bytes were just patched, so the
// downloader regenerates correct ones.
func removeStaleTorrents(path string) {
	_ = dir.RemoveFilesByMask(path + ".torrent*")
}

// setV2Header writes the V2 version byte and bitmask at offset 0. Snapshot files
// are read-only (0444), so it temporarily adds the owner-write bit and restores
// the original permissions afterwards.
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
