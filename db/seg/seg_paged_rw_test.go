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
	"fmt"
	"hash/crc32"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

func prepareLoremDictOnPagedWriter(t *testing.T, pageSize int, pageCompression bool) *Decompressor {
	t.Helper()
	var loremStrings = append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases
	logger, require := log.New(), require.New(t)
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "compressed1")
	cfg := DefaultCfg.WithValuesOnCompressedPage(pageSize)
	cfg.MinPatternScore = 1
	cfg.Workers = 1
	c, err := NewCompressor(context.Background(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(err)
	defer c.Close()

	p := NewPagedWriter(t.Context(), NewWriter(c, CompressNone), pageCompression)
	for k, w := range loremStrings {
		key := fmt.Sprintf("key %d", k)
		val := fmt.Sprintf("%s %d", w, k)
		require.NoError(p.Add([]byte(key), []byte(val)))
	}
	require.NoError(p.Flush())
	require.NoError(p.Compress())

	d, err := NewDecompressor(file)
	require.NoError(err)
	return d
}

func TestPagedReader(t *testing.T) {
	var loremStrings = append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases

	require := require.New(t)
	d := prepareLoremDictOnPagedWriter(t, 2, false)
	defer d.Close()
	g1 := NewPagedReader(d.MakeGetter(), 2, false)
	var buf []byte
	_, _, buf, o1 := g1.Next2(buf[:0])
	require.Zero(o1)
	_, _, buf, o1 = g1.Next2(buf[:0])
	require.Zero(o1)
	_, _, buf, o1 = g1.Next2(buf[:0])
	require.NotZero(o1)

	g := NewPagedReader(d.MakeGetter(), 2, false)
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		var key, word []byte
		key, word, buf, _ = g.Next2(buf[:0])
		expected := fmt.Sprintf("%s %d", w, i)
		expectedK := fmt.Sprintf("key %d", i)
		require.Equal(expected, string(word))
		require.Equal(expectedK, string(key))
		i++
	}

	g.Reset(0)
	_, offset := g.Next(buf[:0])
	require.Equal(0, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(42, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(42, int(offset))
	_, offset = g.Next(buf[:0])
	require.Equal(82, int(offset))
}

// multyBytesWriter is a writer for [][]byte, similar to bytes.Writer.
type multyBytesWriter struct {
	buffer   [][]byte
	pageSize int
}

func (w *multyBytesWriter) Write(p []byte) (n int, err error) {
	w.buffer = append(w.buffer, common.Copy(p))
	return len(p), nil
}
func (w *multyBytesWriter) Bytes() [][]byte                { return w.buffer }
func (w *multyBytesWriter) FileName() string               { return "" }
func (w *multyBytesWriter) Count() int                     { return 0 }
func (w *multyBytesWriter) Close()                         {}
func (w *multyBytesWriter) Compress() error                { return nil }
func (w *multyBytesWriter) Reset()                         { w.buffer = nil }
func (w *multyBytesWriter) SetMetadata([]byte)             {}
func (w *multyBytesWriter) GetValuesOnCompressedPage() int { return w.pageSize }

func TestPage(t *testing.T) {
	sampling := 2
	buf, require := &multyBytesWriter{pageSize: sampling}, require.New(t)
	w := NewPagedWriter(t.Context(), buf, false)
	for i := 0; i < sampling+1; i++ {
		k, v := fmt.Sprintf("k %d", i), fmt.Sprintf("v %d", i)
		require.NoError(w.Add([]byte(k), []byte(v)))
	}
	require.NoError(w.Flush())
	pages := buf.Bytes()
	pageNum := 0
	p1 := &Page{}
	p1.Reset(pages[0], false)

	iter := 0
	for i := 0; i < sampling+1; i++ {
		iter++
		expectK, expectV := fmt.Sprintf("k %d", i), fmt.Sprintf("v %d", i)
		v, _ := GetFromPage([]byte(expectK), pages[pageNum], nil, false)
		require.Equal(expectV, string(v), i)
		require.True(p1.HasNext())
		k, v := p1.Next()
		require.Equal(expectK, string(k), i)
		require.Equal(expectV, string(v), i)

		if iter%sampling == 0 {
			pageNum++

			require.False(p1.HasNext())
			p1.Reset(pages[pageNum], false)
		}
	}
}

func TestPagedReaderWithCompression(t *testing.T) {
	var loremStrings = append(strings.Split(rmNewLine(lorem), " "), "") // including emtpy string - to trigger corner cases

	require := require.New(t)
	d := prepareLoremDictOnPagedWriter(t, 2, true) // Enable page-level compression
	defer d.Close()

	g := NewPagedReader(d.MakeGetter(), 2, true) // Read with compression enabled
	var buf []byte
	i := 0
	for g.HasNext() {
		w := loremStrings[i]
		var key, word []byte
		key, word, buf, _ = g.Next2(buf[:0])
		expected := fmt.Sprintf("%s %d", w, i)
		expectedK := fmt.Sprintf("key %d", i)
		require.Equal(expected, string(word), "mismatch at index %d", i)
		require.Equal(expectedK, string(key), "key mismatch at index %d", i)
		i++
	}
	require.Equal(len(loremStrings), i, "should have read all entries")
}

func TestPagedWriterCRC32Sequential(t *testing.T) {
	// Test that we can compute CRC32 of written pages
	mock := &multyBytesWriter{pageSize: 4}
	pw := NewPagedWriter(t.Context(), mock, true)

	// Add test data
	testData := []struct{ k, v string }{
		{"k1", "v1"}, {"k2", "v2"}, {"k3", "v3"},
		{"k4", "v4"}, {"k5", "v5"}, {"k6", "v6"},
		{"k7", "v7"}, {"k8", "v8"}, {"k9", "v9"},
		{"k10", "v10"}, {"k11", "v11"}, {"k12", "v12"},
		{"k13", "longer_value_here"}, {"k14", "another_longer_value"},
		{"k15", ""}, // empty value
		{"key_with_spaces", "value with spaces"},
		{"unicode_key_αβγ", "unicode_value_δεζ"},
		{"binary_like", "\x00\x01\x02\x03\x04\x05\x06\x07"},
		{"repeated", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
	}

	for _, kv := range testData {
		if err := pw.Add([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}
	if err := pw.Compress(); err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Compute CRC32 of all pages written
	hash := crc32.NewIEEE()
	for _, page := range mock.Bytes() {
		hash.Write(page)
	}
	crc32Sequential := hash.Sum32()
	t.Logf("Sequential CRC32: 0x%08x", crc32Sequential)

	// Now test parallel compression produces same CRC32
	mock2 := &multyBytesWriter{pageSize: 4}
	pw2 := NewPagedWriterWithWorkers(t.Context(), mock2, true, 4)

	for _, kv := range testData {
		if err := pw2.Add([]byte(kv.k), []byte(kv.v)); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}
	if err := pw2.Compress(); err != nil {
		t.Fatalf("Compress failed: %v", err)
	}

	// Compute CRC32 of parallel compression
	hash2 := crc32.NewIEEE()
	for _, page := range mock2.Bytes() {
		hash2.Write(page)
	}
	crc32Parallel := hash2.Sum32()
	t.Logf("Parallel CRC32:   0x%08x", crc32Parallel)

	if crc32Sequential != crc32Parallel {
		t.Errorf("CRC32 mismatch: sequential=0x%08x, parallel=0x%08x", crc32Sequential, crc32Parallel)
	}
}

func TestBytesUncompressedToMatchesSync(t *testing.T) {
	// Regression test: bytesUncompressedTo (parallel path) must produce
	// the same page as bytesUncompressed (sync path).
	// Bug: bytesUncompressedTo pre-allocated keys+vals in neededSize,
	// then clear() zeroed them, then append() placed data past the zeroed region.
	require := require.New(t)

	pageSize := 16
	testKeys := []string{"account_key_001", "account_key_002", "account_key_003", "account_key_004"}
	testVals := []string{"val1", "val2", "val3", "val4"}

	// Build page via sync path
	mock1 := &multyBytesWriter{pageSize: pageSize}
	pw1 := NewPagedWriter(t.Context(), mock1, false)
	for i := range testKeys {
		require.NoError(pw1.Add([]byte(testKeys[i]), []byte(testVals[i])))
	}
	syncPage, syncOK := pw1.bytesUncompressed()
	require.True(syncOK)

	// Build page via async path (bytesUncompressedTo)
	mock2 := &multyBytesWriter{pageSize: pageSize}
	pw2 := NewPagedWriter(t.Context(), mock2, false)
	for i := range testKeys {
		require.NoError(pw2.Add([]byte(testKeys[i]), []byte(testVals[i])))
	}
	asyncPage, asyncOK := pw2.bytesUncompressedTo(nil)
	require.True(asyncOK)

	require.Equal(syncPage, asyncPage,
		"bytesUncompressedTo must produce identical page to bytesUncompressed")

	// Verify keys are readable from the async page
	for i := range testKeys {
		v, _ := GetFromPage([]byte(testKeys[i]), asyncPage, nil, false)
		require.Equal(testVals[i], string(v), "key %s not found in async page", testKeys[i])
	}
}

func BenchmarkName(b *testing.B) {
	buf := &multyBytesWriter{pageSize: 16}
	w := NewPagedWriter(b.Context(), buf, false)
	for i := 0; i < 16; i++ {
		w.Add([]byte{byte(i)}, []byte{10 + byte(i)})
	}
	bts := buf.Bytes()[0]

	k := []byte{15}

	b.Run("1", func(b *testing.B) {
		for b.Loop() {
			GetFromPage(k, bts, nil, false)
		}
	})

}
