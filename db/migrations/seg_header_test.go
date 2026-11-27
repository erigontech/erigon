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
	"math/rand/v2"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/seg"
)

// manyPairs builds enough realistic, repetitive key/val pairs that the compressor
// produces a non-trivial dictionary and DetectCompressType can classify the file
// (matching real snapshots; tiny inputs don't exercise the detector).
func manyPairs() [][2]string {
	const prefix = "0xf90211a0aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	pairs := make([][2]string, 0, 500)
	for i := range 500 {
		c := string(rune('A' + i%26))
		pairs = append(pairs, [2]string{prefix + "key" + c, prefix + "val" + c})
	}
	return pairs
}

// writeV1SegFile writes a seg file as older releases did (raw Compressor, not seg.NewWriter,
// which records the compression) whose key/val words follow the given compression scheme.
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
	_, known := d.WordLevelCompression()
	require.False(t, known, "helper must not record the compression")
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

// TestUpgradeSegHeaders patches a file per compression class via content
// detection, then reads it back through the patched header.
func TestUpgradeSegHeaders(t *testing.T) {
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
		{"v_keys_and_vals", "v1.0-code.0-1024.v", seg.CompressKeys | seg.CompressVals},
		{"ef_none", "v1.0-logaddrs.0-1024.ef", seg.CompressNone},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d := t.TempDir()
			path := filepath.Join(d, tc.file)
			writeV1SegFile(t, path, tc.written, pairs)

			require.NoError(t, upgradeSegHeader(t.Context(), path, log.New()))

			dec, err := seg.NewDecompressor(path)
			require.NoError(t, err)
			require.Equal(t, seg.FileCompressionFormatV1, dec.CompressionFormatVersion())
			fc, ok := dec.WordLevelCompression()
			dec.Close()
			require.True(t, ok)
			require.Equal(t, tc.written, fc, "patched header compression mismatch")

			require.Equal(t, pairs, readBackPairs(t, path))
		})
	}
}

func TestUpgradeSegHeaders_MissingDirs(t *testing.T) {
	require.NoError(t, UpgradeSegHeaders(t.Context(), datadir.New(t.TempDir()), log.New()))
}

func requireHeader(t *testing.T, path string, want seg.FileCompression) {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	fc, ok := d.WordLevelCompression()
	require.True(t, ok, "%s is not patched", filepath.Base(path))
	require.Equal(t, want, fc)
}

func requireNotPatched(t *testing.T, path string) {
	t.Helper()
	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	_, known := d.WordLevelCompression()
	require.False(t, known)
}

func TestUpgradeSegHeaders_EmptySample(t *testing.T) {
	pairs := append(make([][2]string, 100), manyPairs()...)
	path := filepath.Join(t.TempDir(), "v1.0-code.0-1024.v")
	writeV1SegFile(t, path, seg.CompressKeys|seg.CompressVals, pairs)

	require.NoError(t, upgradeSegHeader(t.Context(), path, log.New()))
	requireHeader(t, path, seg.CompressKeys|seg.CompressVals)
	require.Equal(t, pairs, readBackPairs(t, path))
}

func TestUpgradeSegHeaders_KeepsPageLevelFlag(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.0-code.0-1024.v")
	cfg := seg.DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1
	cfg.ValuesOnCompressedPage = 256 // the flag is set, the stored uint8 count is 0
	c, err := seg.NewCompressor(context.Background(), t.Name(), path, t.TempDir(), cfg, log.LvlError, log.New())
	require.NoError(t, err)
	pairs := manyPairs()
	for _, kv := range pairs {
		require.NoError(t, c.AddWord([]byte(kv[0])))
		require.NoError(t, c.AddWord([]byte(kv[1])))
	}
	require.NoError(t, c.Compress())
	c.Close()

	require.NoError(t, upgradeSegHeader(t.Context(), path, log.New()))
	require.Equal(t, pairs, readBackPairs(t, path))
}

func TestUpgradeSegHeaders_LatePatterns(t *testing.T) {
	rnd := rand.New(rand.NewPCG(1, 2))
	var pairs [][2]string
	for range 2_000 {
		k, v := make([]byte, 32), make([]byte, 32)
		for i := range k {
			k[i], v[i] = byte(rnd.Uint32()), byte(rnd.Uint32())
		}
		pairs = append(pairs, [2]string{string(k), string(v)})
	}
	pairs = append(pairs, manyPairs()...)
	path := filepath.Join(t.TempDir(), "v1.0-storage.0-1024.kv")
	writeV1SegFile(t, path, seg.CompressKeys, pairs)

	require.NoError(t, upgradeSegHeader(t.Context(), path, log.New()))
	requireHeader(t, path, seg.CompressKeys)
	require.Equal(t, pairs, readBackPairs(t, path))
}

func TestUpgradeSegHeaders_SymlinkedDirs(t *testing.T) {
	root := t.TempDir()
	external := filepath.Join(root, "external")
	require.NoError(t, os.MkdirAll(filepath.Join(external, "domain"), 0o755))
	dataDir := filepath.Join(root, "datadir")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.Symlink(external, filepath.Join(dataDir, "snapshots")))
	dirs := datadir.New(dataDir)

	other := filepath.Join(root, "other-disk")
	require.NoError(t, os.MkdirAll(other, 0o755))
	require.NoError(t, os.Remove(dirs.SnapCaplin))
	require.NoError(t, os.Symlink(other, dirs.SnapCaplin))

	kvPath := filepath.Join(dirs.SnapDomain, "v1.0-code.0-1024.kv")
	segPath := filepath.Join(dirs.SnapCaplin, "v1.0-000000-000010-randaomixes.seg")
	writeV1SegFile(t, kvPath, seg.CompressVals, manyPairs())
	writeV1SegFile(t, segPath, seg.CompressKeys|seg.CompressVals, manyPairs())

	require.NoError(t, UpgradeSegHeaders(t.Context(), dirs, log.New()))
	requireHeader(t, kvPath, seg.CompressVals)
	requireHeader(t, segPath, seg.CompressKeys|seg.CompressVals)
}

func TestUpgradeSegHeaders_BadFileKeepsGoing(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	good := filepath.Join(dirs.SnapDomain, "v1.0-code.0-1024.kv")
	writeV1SegFile(t, good, seg.CompressVals, manyPairs())

	truncated := filepath.Join(dirs.SnapDomain, "v1.0-code.1024-2048.kv")
	writeV1SegFile(t, truncated, seg.CompressVals, manyPairs())
	d, err := seg.NewDecompressor(truncated)
	require.NoError(t, err)
	g := d.MakeGetter()
	var lastStart, end uint64
	for g.HasNext() {
		lastStart = end
		_, end = g.Next(nil)
	}
	size := d.Size()
	d.Close()
	require.NoError(t, os.Chmod(truncated, 0o644))
	require.NoError(t, os.Truncate(truncated, size-int64(end-lastStart))) // drop the last word at its boundary

	require.NoError(t, os.WriteFile(filepath.Join(dirs.SnapDomain, "v1.0-code.2048-3072.kv"), nil, 0o644))

	require.NoError(t, UpgradeSegHeaders(t.Context(), dirs, log.New()))
	requireHeader(t, good, seg.CompressVals)
	requireNotPatched(t, truncated)
}

func TestUpgradeSegHeaders_TorrentRemovalFails(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	path := filepath.Join(dirs.SnapDomain, "v1.0-code.0-1024.kv")
	writeV1SegFile(t, path, seg.CompressVals, manyPairs())
	require.NoError(t, os.WriteFile(path+".torrent", []byte("stale"), 0o644))
	require.NoError(t, os.Chmod(dirs.SnapDomain, 0o555))
	defer os.Chmod(dirs.SnapDomain, 0o755) //nolint:errcheck

	require.Error(t, UpgradeSegHeaders(t.Context(), dirs, log.New()))
	requireNotPatched(t, path)
}

func TestUpgradeSegHeaders_HistoryRandomAccess(t *testing.T) {
	pairs := manyPairs()
	path := filepath.Join(t.TempDir(), "v1.0-code.0-1024.v")
	writeV1SegFile(t, path, seg.CompressVals, pairs)

	require.NoError(t, upgradeSegHeader(t.Context(), path, log.New()))

	d, err := seg.NewDecompressor(path)
	require.NoError(t, err)
	defer d.Close()
	g := d.MakeGetter()
	var valueOffsets []uint64
	for g.HasNext() {
		_, keyEnd := g.Next(nil)
		valueOffsets = append(valueOffsets, keyEnd)
		g.Skip()
	}
	r := seg.NewReader(d.MakeGetter(), seg.CompressNone)
	for i, off := range valueOffsets { // history reads a value at its own offset, as the first word after Reset
		r.Reset(off)
		v, _ := r.Next(nil)
		require.Equal(t, pairs[i][1], string(v))
	}
}

func TestUpgradeSegHeaders_HardlinkedMirrorKeepsBytes(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "v1.0-code.0-1024.kv")
	writeV1SegFile(t, path, seg.CompressVals, manyPairs())
	mirror := filepath.Join(root, "mirror.kv")
	require.NoError(t, os.Link(path, mirror))
	before, err := os.ReadFile(mirror)
	require.NoError(t, err)

	require.NoError(t, upgradeSegHeader(t.Context(), path, log.New()))

	requireHeader(t, path, seg.CompressVals)
	after, err := os.ReadFile(mirror)
	require.NoError(t, err)
	require.Equal(t, before, after, "a hardlinked mirror must not change")
}

func TestUpgradeSegHeaders_CanceledKeepsHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v1.0-code.0-1024.kv")
	writeV1SegFile(t, path, seg.CompressVals, manyPairs())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	require.ErrorIs(t, upgradeSegHeader(ctx, path, log.New()), context.Canceled)
	requireNotPatched(t, path)
}

func TestUpgradeSegHeaders_HardlinkKeepsExistingTmp(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "v1.0-code.0-1024.kv")
	writeV1SegFile(t, path, seg.CompressVals, manyPairs())
	require.NoError(t, os.Link(path, filepath.Join(root, "mirror.kv")))
	require.NoError(t, os.WriteFile(path+".tmp", []byte("keep"), 0o644))

	require.NoError(t, upgradeSegHeader(t.Context(), path, log.New()))

	requireHeader(t, path, seg.CompressVals)
	tmp, err := os.ReadFile(path + ".tmp")
	require.NoError(t, err)
	require.Equal(t, "keep", string(tmp))
}

func TestUpgradeSegHeaders_SkipsFileSymlinks(t *testing.T) {
	dirs := datadir.New(t.TempDir())
	target := filepath.Join(t.TempDir(), "v1.0-code.0-1024.kv")
	writeV1SegFile(t, target, seg.CompressVals, manyPairs())
	require.NoError(t, os.WriteFile(target+".torrent", []byte("t"), 0o644))
	require.NoError(t, os.Symlink(target, filepath.Join(dirs.SnapDomain, filepath.Base(target))))

	require.NoError(t, UpgradeSegHeaders(t.Context(), dirs, log.New()))

	requireNotPatched(t, target)
	require.FileExists(t, target+".torrent")
}
