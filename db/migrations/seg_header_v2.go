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
	"github.com/erigontech/erigon/db/state/statecfg"
)

// SegHeaderV2 upgrades all V1 .seg/.v/.ef snapshot files to V2 by patching
// the two-byte file header in-place.  V2 is identical to V1 except that the
// featureFlagBitmask byte now reliably encodes KeyCompressionEnabled /
// ValCompressionEnabled in addition to PageLevelCompressionEnabled.
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

		lookup := buildSegCompressionLookup()

		snapDirs := []string{
			dirs.Snap,
			dirs.SnapDomain,
			dirs.SnapHistory,
			dirs.SnapIdx,
			dirs.SnapAccessors,
			dirs.SnapCaplin,
		}
		for _, dir := range snapDirs {
			if err := upgradeSegFilesInDir(dir, lookup, logger); err != nil {
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
var segDataExts = map[string]bool{".seg": true, ".v": true, ".ef": true}

func upgradeSegFilesInDir(dir string, lookup map[string]seg.FileCompression, logger log.Logger) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		ext := filepath.Ext(path)
		if !segDataExts[ext] {
			return nil
		}
		return upgradeSegHeaderV1toV2(path, ext, lookup, logger)
	})
}

// upgradeSegHeaderV1toV2 patches a single seg-format file from V1 to V2.
// It is a no-op for V0 files and files that are already V2+.
func upgradeSegHeaderV1toV2(path, ext string, lookup map[string]seg.FileCompression, logger log.Logger) error {
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
	fc := lookup[tag+ext] // zero value (no compression) if unknown

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

// buildSegCompressionLookup returns a map from "{tag}{ext}" to the FileCompression
// used when writing that file type, derived from the global statecfg.Schema.
func buildSegCompressionLookup() map[string]seg.FileCompression {
	s := statecfg.Schema
	m := make(map[string]seg.FileCompression)

	domains := []statecfg.DomainCfg{
		s.AccountsDomain,
		s.StorageDomain,
		s.CodeDomain,
		s.CommitmentDomain,
		s.ReceiptDomain,
		s.RCacheDomain,
	}
	for _, d := range domains {
		name := d.Name.String()
		m[name+".seg"] = d.Compression
		m[name+".v"] = d.Hist.Compression
		// Each domain's inverted-index uses the domain name as its FilenameBase.
		m[d.Hist.IiCfg.FilenameBase+".ef"] = d.Hist.IiCfg.Compression
	}

	// Standalone inverted indexes (log/trace).
	for _, ii := range []statecfg.InvIdxCfg{
		s.LogAddrIdx,
		s.LogTopicIdx,
		s.TracesFromIdx,
		s.TracesToIdx,
	} {
		m[ii.FilenameBase+".ef"] = ii.Compression
	}

	return m
}
