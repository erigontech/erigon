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
	"context"
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
func UpgradeSegHeadersV2(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
	// each dir separately: any of them can be a symlink to another disk
	for _, d := range []string{dirs.Snap, dirs.SnapDomain, dirs.SnapHistory, dirs.SnapIdx, dirs.SnapAccessors, dirs.SnapCaplin} {
		entries, err := os.ReadDir(d)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return err
		}
		for _, e := range entries {
			if e.IsDir() || !segFormatExts[filepath.Ext(e.Name())] {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := upgradeSegFileToV2(filepath.Join(d, e.Name()), logger); err != nil {
				return err
			}
		}
	}
	return nil
}

// upgradeSegFileToV2 patches a single seg-format file to a V2 header. A file that
// cannot be read keeps its V1 header, which readers open with the caller's compression.
func upgradeSegFileToV2(path string, logger log.Logger) error {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		logger.Warn("[seg_header_v2] keeping V1 header", "file", filepath.Base(path), "err", err)
		return nil
	}
	defer d.Close()
	if d.CompressionFormatVersion() != seg.FileCompressionFormatV1 {
		return nil // V0 has no header to patch, V2 is already upgraded
	}
	fc, err := detectCompression(d)
	if err != nil {
		logger.Warn("[seg_header_v2] keeping V1 header", "file", filepath.Base(path), "err", err)
		return nil
	}
	d.Close() // release mmap before writing

	// before the patch: a crash after it must not leave a .torrent for the old bytes
	if err := dir.RemoveFilesByMask(path + ".torrent*"); err != nil {
		return err
	}
	if err := dir.FsyncDir(filepath.Dir(path)); err != nil {
		return err
	}
	if err := setV2Header(path, fc); err != nil {
		return err
	}
	logger.Info("[seg_header_v2] upgraded", "file", filepath.Base(path), "compression", fc)
	return nil
}

// detectCompression reads every word twice: with the full decode, which is right for
// every word, and as uncompressed. A key (even word) or value (odd word) that does not
// read back the same as uncompressed sets the compression bit for its side.
func detectCompression(d *seg.Decompressor) (fc seg.FileCompression, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("corrupted: %v", rec)
		}
	}()
	full, raw := d.MakeGetter(), d.MakeGetter()
	full.Reset(0)
	var keys, vals bool
	var word []byte
	var start, end uint64
	words := 0
	for ; full.HasNext(); words++ {
		word, end = full.Next(word[:0])
		if !readsUncompressed(raw, start, word, end) {
			keys, vals = keys || words%2 == 0, vals || words%2 == 1
		}
		start = end
	}
	if words != d.Count() {
		return 0, fmt.Errorf("has %d words, header says %d", words, d.Count())
	}
	switch {
	case keys && vals:
		return seg.CompressKeys | seg.CompressVals, nil
	case keys:
		return seg.CompressKeys, nil
	case vals:
		return seg.CompressVals, nil
	}
	return seg.CompressNone, nil
}

func readsUncompressed(g *seg.Getter, offset uint64, want []byte, wantEnd uint64) (ok bool) {
	defer func() {
		if recover() != nil {
			ok = false // a compressed word read as uncompressed can run past the data
		}
	}()
	g.Reset(offset)
	got, end := g.NextUncompressed()
	return end == wantEnd && bytes.Equal(got, want)
}

// setV2Header adds the word-level bits of fc to the bitmask, then sets the V2 version
// byte, each synced: a crash in between leaves a V1 file, whose readers ignore the bits.
// Snapshot files are read-only (0444), so it adds the owner-write bit for the write.
func setV2Header(path string, fc seg.FileCompression) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if err := os.Chmod(path, info.Mode()|0o200); err != nil {
		return err
	}
	defer func() { _ = os.Chmod(path, info.Mode()) }()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	bitmask := make([]byte, 1)
	if _, err := f.ReadAt(bitmask, 1); err != nil {
		return err
	}
	if fc.Has(seg.CompressKeys) {
		bitmask[0] |= byte(seg.WordLevelKeyCompressionEnabled)
	}
	if fc.Has(seg.CompressVals) {
		bitmask[0] |= byte(seg.WordLevelValCompressionEnabled)
	}
	if _, err := f.WriteAt(bitmask, 1); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	if _, err := f.WriteAt([]byte{seg.FileCompressionFormatV2}, 0); err != nil {
		return err
	}
	return f.Sync()
}
