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

// manyPairs builds enough realistic, repetitive key/val pairs that the compressor
// produces a non-trivial dictionary and DetectCompressType can classify the file
// (matching real snapshots; tiny inputs don't exercise the detector).
func manyPairs() [][2]string {
	const prefix = "0xf90211a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pairs := make([][2]string, 0, 500)
	for i := 0; i < 500; i++ {
		c := string(rune('A' + i%26))
		pairs = append(pairs, [2]string{prefix + "key" + c, prefix + "val" + c})
	}
	return pairs
}

// writeV1SegFile writes a genuine V1 seg file (raw Compressor, not seg.NewWriter
// which forces V2) whose key/val words follow the given compression scheme.
func writeV1SegFile(t *testing.T, path string, compress seg.FileCompression, pairs [][2]string) {
	t.Helper()
	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1
	c, err := seg.NewCompressor(context.Background(), t.Name(), path, t.TempDir(), cfg, log.LvlError, log.New())
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

// TestUpgradeSegHeadersV2 patches a V1 file per class via content detection, then
// reads it back through the patched header. The 10k merged-compressed .seg case
// guards the regression where a filename range threshold under-labeled it as
// uncompressed; keys-only/vals-only cover mixed domain files.
func TestUpgradeSegHeadersV2(t *testing.T) {
	pairs := manyPairs()

	cases := []struct {
		name    string
		file    string
		written seg.FileCompression
	}{
		{"seg_merged_10k_compressed", "v1.0-000000-000010-headers.seg", seg.CompressKeys | seg.CompressVals},
		{"seg_dump_small_uncompressed", "v1.0-000000-000001-headers.seg", seg.CompressNone},
		{"seg_caplin_state_uncompressed", "v1.0-000000-000010-randaomixes.seg", seg.CompressNone},
		{"kv_none", "v1.0-accounts.0-1024.kv", seg.CompressNone},
		{"kv_keys", "v1.0-storage.0-1024.kv", seg.CompressKeys},
		{"kv_vals", "v1.0-code.0-1024.kv", seg.CompressVals},
		{"v_keysvals", "v1.0-code.0-1024.v", seg.CompressKeys | seg.CompressVals},
		{"ef_none", "v1.0-logaddrs.0-1024.ef", seg.CompressNone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := t.TempDir()
			path := filepath.Join(d, tc.file)
			writeV1SegFile(t, path, tc.written, pairs)

			require.NoError(t, upgradeSegFileToV2(path, log.New()))

			dec, err := seg.NewDecompressor(path)
			require.NoError(t, err)
			require.Equal(t, seg.FileCompressionFormatV2, dec.CompressionFormatVersion())
			fc, ok := dec.WordLevelCompression()
			dec.Close()
			require.True(t, ok)
			require.Equal(t, tc.written, fc, "patched header compression mismatch")

			require.Equal(t, pairs, readBackPairs(t, path))
		})
	}
}

// TestUpgradeSegHeadersV2_MissingDir ensures a non-existent snapshot dir is a no-op.
func TestUpgradeSegHeadersV2_MissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "does-not-exist")
	require.NoError(t, walkSegDir(missing, func(string) error {
		t.Fatal("fn must not be called for a missing dir")
		return nil
	}))
}
