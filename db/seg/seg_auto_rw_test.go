// Copyright 2026 The Erigon Authors
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

package seg

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// makeTestFileCompressed writes key-value pairs using NewWriter with the given
// compression flags and returns the path to the completed file.
func makeTestFileCompressed(t *testing.T, compress FileCompression) string {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "test.kv")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 1
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)

	w := NewWriter(c, compress)
	defer w.Close()
	pairs := [][2]string{
		{"key1", "val1"},
		{"key2", "val2"},
		{"key3", "val3"},
	}
	for _, kv := range pairs {
		_, err = w.Write([]byte(kv[0]))
		require.NoError(t, err)
		_, err = w.Write([]byte(kv[1]))
		require.NoError(t, err)
	}
	require.NoError(t, w.Compressor.Compress())
	return file
}

// TestNewWriterSetsV2Header verifies that NewWriter always produces a V2 file
// and that the header bitmask reflects the requested key/val compression flags.
func TestNewWriterSetsV2Header(t *testing.T) {
	cases := []struct {
		name        string
		compress    FileCompression
		wantKeyFlag bool
		wantValFlag bool
	}{
		{"CompressNone", CompressNone, false, false},
		{"CompressKeys", CompressKeys, true, false},
		{"CompressVals", CompressVals, false, true},
		{"CompressKeysVals", CompressKeys | CompressVals, true, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			file := makeTestFileCompressed(t, tc.compress)
			d, err := NewDecompressor(file)
			require.NoError(t, err)
			defer d.Close()

			require.Equal(t, FileCompressionFormatV2, d.CompressionFormatVersion(),
				"NewWriter must produce a V2 file")

			fc, ok := d.WordLevelCompression()
			require.True(t, ok, "V2 file must report WordLevelCompression ok=true")
			require.Equal(t, tc.wantKeyFlag, fc.Has(CompressKeys), "CompressKeys flag mismatch")
			require.Equal(t, tc.wantValFlag, fc.Has(CompressVals), "CompressVals flag mismatch")
		})
	}
}

// TestNewReaderUsesHeaderForV2 verifies that NewReader ignores the caller-supplied
// fallback and routes purely from the V2 header.
//
// A wrong fallback for CompressNone files would call Next() (huffman) on
// uncompressed data → crash.  The test passes iff the header overrides the fallback.
func TestNewReaderUsesHeaderForV2(t *testing.T) {
	pairs := [][2]string{
		{"key1", "val1"},
		{"key2", "val2"},
		{"key3", "val3"},
	}

	// Only test CompressNone: wrong fallback (CompressKeys|CompressVals) would
	// panic if used, so a clean read proves the header was read instead.
	// CompressKeys/CompressVals files with a wrong fallback would also panic,
	// but that requires a guaranteed-non-empty huffman dict to be reliable.
	t.Run("CompressNone_wrong_fallback", func(t *testing.T) {
		file := makeTestFileCompressed(t, CompressNone)
		d, err := NewDecompressor(file)
		require.NoError(t, err)
		defer d.Close()

		// V2 header says no word-level compression; passing wrong fallback
		// (CompressKeys|CompressVals) must be ignored.
		g := d.MakeGetter()
		r := NewReader(g, CompressKeys|CompressVals)
		r.Reset(0)

		for i, want := range pairs {
			require.True(t, r.HasNext(), "pair %d: expected key", i)
			key, _ := r.Next(nil) // pass nil to avoid reusing mmap-backed slices
			require.Equal(t, want[0], string(key), "pair %d: key mismatch", i)

			require.True(t, r.HasNext(), "pair %d: expected val", i)
			val, _ := r.Next(nil)
			require.Equal(t, want[1], string(val), "pair %d: val mismatch", i)
		}
		require.False(t, r.HasNext(), "no more words expected")
	})
}

// A Compressor carrying word-level flags from an earlier Writer must not leak
// them into the next Writer's header: the bitmask describes one file's layout,
// and a stale bit routes every read of that file through the wrong path.
func TestNewWriterReplacesWordLevelFlags(t *testing.T) {
	c, err := NewCompressor(context.Background(), "test", filepath.Join(t.TempDir(), "test.seg"),
		t.TempDir(), DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()

	NewWriter(c, CompressKeys|CompressVals)
	NewWriter(c, CompressNone)

	require.False(t, c.featureFlagBitmask.Has(WordLevelKeyCompressionEnabled))
	require.False(t, c.featureFlagBitmask.Has(WordLevelValCompressionEnabled))
}

// writeKVFile builds a key/value seg file under the given compression and
// returns its path.
func writeKVFile(t *testing.T, compression FileCompression, words [][]byte) string {
	t.Helper()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "src")
	c, err := NewCompressor(t.Context(), t.Name(), file, tmpDir, DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()

	w := NewWriter(c, compression)
	for _, word := range words {
		_, err = w.Write(word)
		require.NoError(t, err)
	}
	require.NoError(t, c.Compress())
	return file
}

// TestWriterReadFromMixedCompression copies a file whose keys are stored raw
// and whose values are compressed. An uncompressed read hands back a slice of
// the read-only mapping, so reusing it as the next read's buffer faults.
//
// The mapping slice's capacity runs to the end of the file, so the fault needs
// a file long enough that the bytes left after a key still exceed the next
// value: below that, slices.Grow reallocates and hides the aliasing.
func TestWriterReadFromMixedCompression(t *testing.T) {
	const pairs = 4096
	words := make([][]byte, 0, 2*pairs)
	for i := range pairs {
		// Alternate long and short values: a value no longer than the key it
		// follows still fits the key's own slice, so a capacity bound alone
		// does not stop the decode from landing in the mapping.
		value := bytes.Repeat([]byte(fmt.Sprintf("value-%06d-", i)), 16)
		if i%2 == 1 {
			value = []byte(fmt.Sprintf("v%03d", i%1000))
		}
		words = append(words, []byte(fmt.Sprintf("key-%06d", i)), value)
	}

	src := writeKVFile(t, CompressVals, words)
	srcDecomp, err := NewDecompressor(src)
	require.NoError(t, err)
	defer srcDecomp.Close()

	dstDir := t.TempDir()
	dst := filepath.Join(dstDir, "dst")
	c, err := NewCompressor(t.Context(), t.Name(), dst, dstDir, DefaultCfg, log.LvlDebug, log.New())
	require.NoError(t, err)
	defer c.Close()

	require.NoError(t, NewWriter(c, CompressVals).ReadFrom(NewReader(srcDecomp.MakeGetter(), CompressVals)))
	require.NoError(t, c.Compress())

	dstDecomp, err := NewDecompressor(dst)
	require.NoError(t, err)
	defer dstDecomp.Close()

	g := NewReader(dstDecomp.MakeGetter(), CompressVals)
	for i, want := range words {
		require.True(t, g.HasNext(), "word %d missing", i)
		got, _ := g.Next(nil)
		require.Equal(t, want, got, "word %d", i)
	}
	require.False(t, g.HasNext())
}

// TestNextUncompressedCapacityBound pins the capacity contract: a returned word
// must not let the caller append into the read-only mapping. Empty words go
// through a separate return path, so cover both.
func TestNextUncompressedCapacityBound(t *testing.T) {
	words := [][]byte{[]byte("key-0"), {}, []byte("key-1"), []byte("value-1")}
	d, err := NewDecompressor(writeKVFile(t, CompressNone, words))
	require.NoError(t, err)
	defer d.Close()

	g := d.MakeGetter()
	for i := range words {
		w, _ := g.NextUncompressed()
		require.Equal(t, len(w), cap(w), "word %d: cap must not run past the word", i)
	}
	require.False(t, g.HasNext())
}

func TestFileCompressionFromString(t *testing.T) {
	for s, want := range map[string]FileCompression{
		"": CompressNone, "none": CompressNone,
		"k": CompressKeys, "keys": CompressKeys,
		"v": CompressVals, "values": CompressVals,
		"kv": CompressKeys | CompressVals, "all": CompressKeys | CompressVals,
	} {
		var c FileCompression
		require.NoError(t, c.FromString(s), s)
		require.Equal(t, want, c, s)
	}
	var c FileCompression
	require.Error(t, c.FromString("nope"))
}
