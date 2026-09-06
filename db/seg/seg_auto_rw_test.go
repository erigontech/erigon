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
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
)

// writeKVFile builds a key/value seg file under the given compression and
// returns its path.
func writeKVFile(t *testing.T, name string, compression FileCompression, words [][]byte) string {
	t.Helper()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, name)
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
		words = append(words,
			[]byte(fmt.Sprintf("key-%06d", i)),
			bytes.Repeat([]byte(fmt.Sprintf("value-%06d-", i)), 16))
	}

	src := writeKVFile(t, "src", CompressVals, words)
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
