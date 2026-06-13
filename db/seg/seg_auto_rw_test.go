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

package seg

import (
	"context"
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
	w.Close()
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
