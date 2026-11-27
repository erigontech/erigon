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
	"bytes"
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
	return walkSegDir(dirs.Snap, func(path string) error { // recurses into domain, history, idx, accessor and caplin
		if !segFormatExts[filepath.Ext(path)] {
			return nil
		}
		return upgradeSegFileToV2(path, logger)
	})
}

// walkSegDir invokes fn for each file under root, treating a missing directory
// and transient not-exist errors as a no-op.
func walkSegDir(root string, fn func(path string) error) error {
	return filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
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
// is detected from the file's contents (filename/range is unreliable) and validated
// by a read before writing, so a wrong guess aborts with the file untouched.
func upgradeSegFileToV2(path string, logger log.Logger) error {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		return err
	}
	if d.CompressionFormatVersion() < seg.FileCompressionFormatV1 {
		d.Close() // V0: no header to patch
		return nil
	}
	if d.CompressionFormatVersion() >= seg.FileCompressionFormatV2 {
		d.Close() // already upgraded; re-detecting would validate the header but write a fresh guess
		return nil
	}
	if !detectionSawData(d) {
		d.Close() // stays V1, which readers still open with the caller's compression
		logger.Warn("[seg_header_v2] keeping V1 header: compression sample has only empty words", "file", filepath.Base(path))
		return nil
	}
	fc := seg.DetectCompressType(d.MakeGetter())
	if err := checkReadable(d, fc); err != nil {
		d.Close()
		return fmt.Errorf("[seg_header_v2] %s: %w", filepath.Base(path), err)
	}
	d.Close() // release mmap before writing

	if err := setV2Header(path, fc); err != nil {
		return err
	}
	removeStaleTorrents(path)
	logger.Debug("[seg_header_v2] upgraded", "file", filepath.Base(path), "compression", fc)
	return nil
}

// detectionSawData reports whether DetectCompressType's sample has a non-empty word.
// Empty words decode the same under any compression, so such a sample proves nothing.
func detectionSawData(d *seg.Decompressor) (saw bool) {
	defer func() {
		if rec := recover(); rec != nil {
			saw = true // a compressed word that does not decode as uncompressed
		}
	}()
	g := d.MakeGetter()
	g.Reset(0)
	for i := 0; i < 2*seg.DetectCompressSamplePairs && g.HasNext(); i++ {
		if word, _ := g.NextUncompressed(); len(word) > 0 {
			return true
		}
	}
	return false
}

// checkReadable reads every word of d with the given compression and compares it with
// the full decode, which is right for every word: an uncompressed word is a word
// without patterns. A word read as uncompressed may not panic, it may only lose sync,
// so a panic alone does not prove the guess. A panic is returned as an error.
func checkReadable(d *seg.Decompressor, fc seg.FileCompression) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("not readable as compression=%v: %v", fc, rec)
		}
	}()
	r := seg.NewReader(d.MakeGetter(), fc)
	full := d.MakeGetter()
	r.Reset(0)
	full.Reset(0)
	// buf must stay a private heap scratch: Next may return an mmap-backed slice
	// for an uncompressed word, so feeding its return back as the next buffer
	// would make Next memmove into read-only mmap → SIGBUS on mixed-compression files.
	buf := make([]byte, 0, 4096)
	var want []byte
	for i := 0; r.HasNext(); i++ {
		want, _ = full.Next(want[:0])
		if got, _ := r.Next(buf[:0]); !bytes.Equal(got, want) {
			return fmt.Errorf("not readable as compression=%v: word %d differs", fc, i)
		}
	}
	return nil
}

// removeStaleTorrents removes the .torrent and any partial torrent artifacts
// (.torrent<suffix>) of a snapshot file whose bytes were just patched, so the
// downloader regenerates correct ones.
func removeStaleTorrents(path string) {
	_ = dir.RemoveFilesByMask(path + ".torrent*")
}

// setV2Header writes the V2 version byte at offset 0 and adds the word-level bits of fc
// to the existing bitmask. Snapshot files are read-only (0444), so it temporarily adds
// the owner-write bit and restores the original permissions afterwards.
func setV2Header(path string, fc seg.FileCompression) error {
	return withWritePerm(path, func(f *os.File) error {
		header := make([]byte, 2)
		if _, err := f.ReadAt(header, 0); err != nil {
			return err
		}
		bitmask := seg.FeatureFlagBitmask(header[1])
		if fc.Has(seg.CompressKeys) {
			bitmask.Set(seg.WordLevelKeyCompressionEnabled)
		}
		if fc.Has(seg.CompressVals) {
			bitmask.Set(seg.WordLevelValCompressionEnabled)
		}
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
	err = os.Chmod(path, origMode|0o200) // 0o200 = owner-write bit
	if err != nil {
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
