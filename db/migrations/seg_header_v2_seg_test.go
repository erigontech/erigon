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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

// writeV1SegFile writes a genuine V1 .seg file (raw Compressor, not seg.NewWriter,
// which would force V2) whose key/val words follow the given compression scheme.
func writeV1SegFile(t *testing.T, path string, compress seg.FileCompression, pairs [][2]string) {
	t.Helper()
	logger := log.New()
	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1
	c, err := seg.NewCompressor(context.Background(), t.Name(), path, t.TempDir(), cfg, log.LvlError, logger)
	require.NoError(t, err)
	defer c.Close()

	add := func(word []byte, compressed bool) {
		if compressed {
			require.NoError(t, c.AddWord(word))
		} else {
			require.NoError(t, c.AddUncompressedWord(word))
		}
	}
	for _, kv := range pairs {
		add([]byte(kv[0]), compress.Has(seg.CompressKeys))
		add([]byte(kv[1]), compress.Has(seg.CompressVals))
	}
	require.NoError(t, c.Compress())

	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	require.Equal(t, seg.FileCompressionFormatV1, d.CompressionFormatVersion(), "helper must produce a V1 file")
	d.Close()
}

// readBackPairs reads every key/val word from a file via NewReader, which routes
// purely from the (patched) header.
func readBackPairs(t *testing.T, path string) [][2]string {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	r := seg.NewReader(d.MakeGetter(), seg.CompressNone)
	r.Reset(0)
	var out [][2]string
	for r.HasNext() {
		k, _ := r.Next(nil)
		require.True(t, r.HasNext(), "dangling key without value")
		v, _ := r.Next(nil)
		out = append(out, [2]string{string(k), string(v)})
	}
	return out
}

// TestSegHeaderV2SegMigration patches a V1 .seg file per class, then reads it back
// through the header the migration wrote. The non-empty uncompressed caplin-state
// case guards the regression where dirs.SnapCaplin was assumed fully compressed;
// the mid-range headers case guards the old MergeSteps[last] (10k) threshold.
func TestSegHeaderV2SegMigration(t *testing.T) {
	pairs := [][2]string{{"key1", "val1"}, {"key2", "val2"}, {"key3", "val3"}}

	cases := []struct {
		name             string
		file             string
		isCaplinStateDir bool
		written          seg.FileCompression
		want             seg.FileCompression
	}{
		{"caplin_state_uncompressed", "v1.0-000000-000010-randaomixes.seg", true, seg.CompressNone, seg.CompressNone},
		{"beaconblocks_compressed", "v1.0-000000-000010-beaconblocks.seg", false, seg.CompressKeys | seg.CompressVals, seg.CompressKeys | seg.CompressVals},
		{"blobsidecars_compressed", "v1.0-000000-000010-blobsidecars.seg", false, seg.CompressKeys | seg.CompressVals, seg.CompressKeys | seg.CompressVals},
		{"headers_small_uncompressed", "v1.0-000000-000001-headers.seg", false, seg.CompressNone, seg.CompressNone},
		{"headers_mid_uncompressed", "v1.0-000000-000050-headers.seg", false, seg.CompressNone, seg.CompressNone},
		{"headers_large_compressed", "v1.0-000000-000100-headers.seg", false, seg.CompressKeys | seg.CompressVals, seg.CompressKeys | seg.CompressVals},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := t.TempDir()
			path := filepath.Join(d, tc.file)
			writeV1SegFile(t, path, tc.written, pairs)

			require.NoError(t, upgradeAndSmokeTestDotSegFilesInDir(d, tc.isCaplinStateDir, log.New()))

			dec, err := seg.NewDecompressor(path)
			require.NoError(t, err)
			require.Equal(t, seg.FileCompressionFormatV2, dec.CompressionFormatVersion())
			fc, ok := dec.WordLevelCompression()
			dec.Close()
			require.True(t, ok)
			require.Equal(t, tc.want, fc, "patched header compression mismatch")

			require.Equal(t, pairs, readBackPairs(t, path))
		})
	}
}

// TestDotSegCompression covers the filename/dir → compression inference directly.
func TestDotSegCompression(t *testing.T) {
	cases := []struct {
		file             string
		isCaplinStateDir bool
		want             seg.FileCompression
		wantOK           bool
	}{
		{"v1.0-000000-000010-randaomixes.seg", true, seg.CompressNone, true},
		{"v1.0-000000-000010-beaconblocks.seg", false, seg.CompressKeys | seg.CompressVals, true},
		{"v1.0-000000-000010-blobsidecars.seg", false, seg.CompressKeys | seg.CompressVals, true},
		{"v1.0-000000-000001-headers.seg", false, seg.CompressNone, true},
		{"v1.0-000000-000050-headers.seg", false, seg.CompressNone, true},
		{"v1.0-000000-000100-headers.seg", false, seg.CompressKeys | seg.CompressVals, true},
		{"garbage.seg", false, 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.file, func(t *testing.T) {
			fc, ok := dotSegCompression(tc.file, tc.isCaplinStateDir)
			require.Equal(t, tc.wantOK, ok)
			if tc.wantOK {
				require.Equal(t, tc.want, fc)
			}
		})
	}
}
