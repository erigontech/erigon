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

// UpgradeSegHeaders records in the header of every seg-format snapshot file
// (.kv/.v/.ef/.seg) the word-level key/val compression detected from its contents.
// Run once after upgrading:
//
//	erigon snapshots upgrade-seg-headers --datadir=<path>
func UpgradeSegHeaders(ctx context.Context, dirs datadir.Dirs, logger log.Logger) error {
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
			if e.Type()&os.ModeSymlink != 0 { // its .torrent lives next to the target, out of reach
				logger.Warn("[seg_header] skipping a symlinked file", "file", e.Name())
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := upgradeSegHeader(ctx, filepath.Join(d, e.Name()), logger); err != nil {
				return err
			}
		}
	}
	return nil
}

// upgradeSegHeader patches the header of a single seg-format file. A file that cannot
// be read keeps its header, and readers open it with the caller's compression.
func upgradeSegHeader(ctx context.Context, path string, logger log.Logger) error {
	d, err := seg.NewDecompressor(path)
	if err != nil {
		logger.Warn("[seg_header] keeping the header", "file", filepath.Base(path), "err", err)
		return nil
	}
	defer d.Close()
	if d.CompressionFormatVersion() != seg.FileCompressionFormatV1 {
		return nil // V0 has no header to patch
	}
	if _, ok := d.WordLevelCompression(); ok {
		return nil
	}
	fc, err := detectCompression(ctx, d)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err != nil {
		logger.Warn("[seg_header] keeping the header", "file", filepath.Base(path), "err", err)
		return nil
	}
	fc = seg.CompressionForFile(path, fc)
	d.Close() // release mmap before writing

	// before the patch: a crash after it must not leave a .torrent for the old bytes
	if err := dir.RemoveFilesByMask(path + ".torrent*"); err != nil {
		return err
	}
	if err := dir.FsyncDir(filepath.Dir(path)); err != nil {
		return err
	}
	if err := setWordLevelFlags(path, fc); err != nil {
		return err
	}
	logger.Info("[seg_header] upgraded", "file", filepath.Base(path), "compression", fc)
	return nil
}

// detectCompression reads every word twice: with the full decode, which is right for
// every word, and as uncompressed. A key (even word) or value (odd word) that does not
// read back the same as uncompressed sets the compression bit for its side.
func detectCompression(ctx context.Context, d *seg.Decompressor) (fc seg.FileCompression, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("corrupted: %v", rec)
		}
	}()
	full, raw := d.MakeGetter(), d.MakeGetter()
	full.Reset(0)
	var word []byte
	var start, end uint64
	words := 0
	for ; full.HasNext(); words++ {
		if words == d.Count() {
			return 0, fmt.Errorf("has more words than the header says (%d)", d.Count())
		}
		if words%(1<<20) == 0 && ctx.Err() != nil {
			return 0, ctx.Err()
		}
		word, end = full.Next(word[:0])
		if !readsUncompressed(raw, start, word, end) {
			if words%2 == 0 {
				fc |= seg.CompressKeys
			} else {
				fc |= seg.CompressVals
			}
		}
		start = end
	}
	if words != d.Count() {
		return 0, fmt.Errorf("has %d words, header says %d", words, d.Count())
	}
	if fc == 0 {
		return seg.CompressNone, nil
	}
	return fc, nil
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

// setWordLevelFlags adds WordLevelCompressionKnown and the word-level bits of fc to the
// bitmask byte. Snapshot files are read-only (0444), so it adds the owner-write bit for
// the write.
func setWordLevelFlags(path string, fc seg.FileCompression) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if sharesInode(info) {
		return patchCopy(path, info.Mode(), fc)
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
	return patchFlags(f, fc)
}

// patchCopy patches a private copy and renames it over path: snapshots are hardlinked as
// immutable files, so an in-place patch would change every link.
func patchCopy(path string, mode os.FileMode, fc seg.FileCompression) (err error) {
	src, err := os.Open(path)
	if err != nil {
		return err
	}
	defer src.Close()
	tmp, err := dir.CreateTemp(path)
	if err != nil {
		return err
	}
	defer func() {
		_ = tmp.Close()
		if err != nil {
			_ = dir.RemoveFile(tmp.Name())
		}
	}()
	if _, err = tmp.ReadFrom(src); err != nil {
		return err
	}
	if err = patchFlags(tmp, fc); err != nil {
		return err
	}
	if err = tmp.Chmod(mode); err != nil {
		return err
	}
	if err = os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	return dir.FsyncDir(filepath.Dir(path))
}

func patchFlags(f *os.File, fc seg.FileCompression) error {
	bitmask := make([]byte, 1)
	if _, err := f.ReadAt(bitmask, 1); err != nil {
		return err
	}
	bitmask[0] |= byte(seg.WordLevelCompressionKnown)
	if fc.Has(seg.CompressKeys) {
		bitmask[0] |= byte(seg.WordLevelKeyCompressionEnabled)
	}
	if fc.Has(seg.CompressVals) {
		bitmask[0] |= byte(seg.WordLevelValCompressionEnabled)
	}
	if _, err := f.WriteAt(bitmask, 1); err != nil {
		return err
	}
	return f.Sync()
}
