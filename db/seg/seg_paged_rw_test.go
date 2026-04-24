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
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"path/filepath"
	"slices"
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
	c, err := NewCompressor(t.Context(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(err)
	defer c.Close()

	p := NewPagedWriter(t.Context(), NewWriter(c, CompressNone), pageCompression, 1)
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
	w := NewPagedWriter(t.Context(), buf, false, 1)
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
	pw := NewPagedWriter(t.Context(), mock, true, 1)

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
	pw2 := NewPagedWriter(t.Context(), mock2, true, 4)

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

func TestHeaderToMatchesSync(t *testing.T) {
	// Regression test: headerTo + keys/vals concatenation (parallel path) must produce
	// the same page as bytesUncompressed (sync path).
	require := require.New(t)

	pageSize := 16
	testKeys := []string{"account_key_001", "account_key_002", "account_key_003", "account_key_004"}
	testVals := []string{"val1", "val2", "val3", "val4"}

	// Build page via sync path
	mock1 := &multyBytesWriter{pageSize: pageSize}
	pw1 := NewPagedWriter(t.Context(), mock1, false, 1)
	for i := range testKeys {
		require.NoError(pw1.Add([]byte(testKeys[i]), []byte(testVals[i])))
	}
	syncPage, syncOK := pw1.bytesUncompressed()
	require.True(syncOK)

	// Build page via async path (headerTo + concatenation, same as worker does)
	mock2 := &multyBytesWriter{pageSize: pageSize}
	pw2 := NewPagedWriter(t.Context(), mock2, false, 1)
	for i := range testKeys {
		require.NoError(pw2.Add([]byte(testKeys[i]), []byte(testVals[i])))
	}
	asyncPage := pageHeaderTo(nil, pw2.kLengths, pw2.vLengths, 0)
	asyncPage = append(asyncPage, pw2.keys...)
	asyncPage = append(asyncPage, pw2.vals...)

	require.Equal(syncPage, asyncPage,
		"headerTo+concat must produce identical page to bytesUncompressed")

	// Verify keys are readable from the async page
	for i := range testKeys {
		v, _ := GetFromPage([]byte(testKeys[i]), asyncPage, nil, false)
		require.Equal(testVals[i], string(v), "key %s not found in async page", testKeys[i])
	}
}

// TestDecompressorEmptyWordsInvariant verifies (property 1):
// emptyWordsCount <= Count() for any decompressor produced by PagedWriter.
func TestDecompressorEmptyWordsInvariant(t *testing.T) {
	// uncompressed pages
	d := prepareLoremDictOnPagedWriter(t, 4, false)
	defer d.Close()
	require.LessOrEqual(t, d.EmptyWordsCount(), d.Count(),
		"emptyWordsCount=%d > Count()=%d", d.EmptyWordsCount(), d.Count())

	// compressed pages
	d2 := prepareLoremDictOnPagedWriter(t, 4, true)
	defer d2.Close()
	require.LessOrEqual(t, d2.EmptyWordsCount(), d2.Count(),
		"compressed: emptyWordsCount=%d > Count()=%d", d2.EmptyWordsCount(), d2.Count())
}

// TestPageLayoutConsistency verifies (property 2):
// every raw page satisfies: len(page) == 1 + cnt*8 + sum(kLens) + sum(vLens).
func TestPageLayoutConsistency(t *testing.T) {
	const pageSize = 4
	mock := &multyBytesWriter{pageSize: pageSize}
	pw := NewPagedWriter(t.Context(), mock, false, 1)

	testPairs := []struct{ k, v string }{
		{"alpha", "one"}, {"beta", "two"}, {"gamma", "three"},
		{"delta", "four"}, {"epsilon", "five"}, {"zeta", "six"},
		{"eta", "seven"}, {"theta", "eight"}, {"iota", "nine"},
	}
	for _, kv := range testPairs {
		require.NoError(t, pw.Add([]byte(kv.k), []byte(kv.v)))
	}
	require.NoError(t, pw.Flush())

	for pageIdx, page := range mock.Bytes() {
		require.NotEmpty(t, page, "page %d is empty", pageIdx)
		cnt := int(page[0])
		require.Positive(t, cnt, "page %d: cnt must be > 0", pageIdx)
		require.LessOrEqual(t, cnt, 255, "page %d: cnt=%d overflows byte", pageIdx, cnt)

		metaLen := cnt * 4 * 2 // kLens + vLens, each cnt*4 bytes
		require.GreaterOrEqual(t, len(page), 1+metaLen,
			"page %d: too short for metadata (len=%d, need %d)", pageIdx, len(page), 1+metaLen)

		kLens := page[1 : 1+cnt*4]
		vLens := page[1+cnt*4 : 1+metaLen]
		data := page[1+metaLen:]

		var totalKeys, totalVals uint32
		for i := 0; i < cnt*4; i += 4 {
			totalKeys += be.Uint32(kLens[i:])
			totalVals += be.Uint32(vLens[i:])
		}
		require.Equal(t, int(totalKeys+totalVals), len(data),
			"page %d: data len=%d but sum(kLens)+sum(vLens)=%d",
			pageIdx, len(data), totalKeys+totalVals)
	}
}

// TestPagedWriterRoundTripParallel verifies (property 3):
// parallel PagedWriter (workers > 1) round-trips key-value content identically to sequential.
func TestPagedWriterRoundTripParallel(t *testing.T) {
	require := require.New(t)

	testPairs := []struct{ k, v []byte }{
		{[]byte("aaa"), []byte("111")},
		{[]byte("bbb"), []byte("222")},
		{[]byte("ccc"), []byte("333")},
		{[]byte("ddd"), []byte("444")},
		{[]byte("eee"), []byte("555")},
		{[]byte("fff"), []byte("666")},
		{[]byte("ggg"), []byte("")}, // empty value
		{[]byte("hhh"), []byte("888")},
	}

	write := func(workers int) [][]byte {
		mock := &multyBytesWriter{pageSize: 3}
		var pw *PagedWriter
		if workers == 1 {
			pw = NewPagedWriter(t.Context(), mock, true, 1)
		} else {
			pw = NewPagedWriter(t.Context(), mock, true, workers)
		}
		for _, kv := range testPairs {
			require.NoError(pw.Add(kv.k, kv.v))
		}
		require.NoError(pw.Compress())
		return mock.Bytes()
	}

	seqPages := write(1)
	parPages := write(4)
	require.Equal(len(seqPages), len(parPages), "page count mismatch: seq=%d par=%d", len(seqPages), len(parPages))
	for i := range seqPages {
		require.Equal(seqPages[i], parPages[i], "page %d content differs between sequential and parallel", i)
	}
}

// TestPagedReaderSortedKeyOrder verifies (property 4):
// keys returned by PagedReader.Next2 are strictly increasing within the written sequence
// when keys are written in sorted order (as domain collation always does).
func TestPagedReaderSortedKeyOrder(t *testing.T) {
	require := require.New(t)

	// Keys written in strictly increasing lexicographic order.
	sortedPairs := []struct{ k, v string }{
		{"a", "v1"}, {"b", "v2"}, {"c", "v3"},
		{"d", "v4"}, {"e", "v5"}, {"f", "v6"},
		{"g", "v7"}, {"h", "v8"},
	}

	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "sorted_keys")
	logger := log.New()
	cfg := DefaultCfg.WithValuesOnCompressedPage(3)
	cfg.MinPatternScore = 1
	cfg.Workers = 1
	c, err := NewCompressor(t.Context(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(err)
	defer c.Close()

	pw := NewPagedWriter(t.Context(), NewWriter(c, CompressNone), false, 1)
	for _, kv := range sortedPairs {
		require.NoError(pw.Add([]byte(kv.k), []byte(kv.v)))
	}
	require.NoError(pw.Flush())
	require.NoError(pw.Compress())

	d, err := NewDecompressor(file)
	require.NoError(err)
	defer d.Close()

	g := NewPagedReader(d.MakeGetter(), 3, false)
	var buf []byte
	var prevKey []byte
	i := 0
	for g.HasNext() {
		var key []byte
		key, _, buf, _ = g.Next2(buf[:0])
		if prevKey != nil {
			require.Positive(strings.Compare(string(key), string(prevKey)),
				"pair %d: key %q not strictly greater than prevKey %q", i, key, prevKey)
		}
		prevKey = append(prevKey[:0], key...)
		i++
	}
	require.Equal(len(sortedPairs), i, "should have read all %d pairs", len(sortedPairs))
}

func BenchmarkPagedWriterAdd(b *testing.B) {
	const pageSize = 16
	key := make([]byte, 20)
	val := make([]byte, 100)
	for i := range key {
		key[i] = byte(i)
	}
	for i := range val {
		val[i] = byte(i)
	}

	cases := []struct {
		name       string
		compress   bool
		numWorkers int
	}{
		{"noCompression", false, 1},
		{"compression_sync", true, 1},
		{"compression_workers2", true, 2},
		{"compression_workers4", true, 4},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			buf := &multyBytesWriter{pageSize: pageSize}
			w := NewPagedWriter(b.Context(), buf, tc.compress, tc.numWorkers)
			b.ResetTimer()
			for b.Loop() {
				w.Add(key, val) //nolint:errcheck
			}
			w.Flush() //nolint:errcheck
		})
	}
}

func BenchmarkName(b *testing.B) {
	buf := &multyBytesWriter{pageSize: 16}
	w := NewPagedWriter(b.Context(), buf, false, 1)
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

// prepareKVUncompressedKeysCompressedVals creates a file with sorted KV pairs:
// uncompressed keys (like domain keys) and compressed values.
func prepareKVUncompressedKeysCompressedVals(t *testing.T, keyLen int, numPairs int) (*Decompressor, [][]byte, [][]byte) {
	t.Helper()
	logger := log.New()
	tmpDir := t.TempDir()
	file := filepath.Join(tmpDir, "kv_uncomp_keys")
	cfg := DefaultCfg
	cfg.MinPatternScore = 1
	cfg.Workers = 2
	c, err := NewCompressor(t.Context(), t.Name(), file, tmpDir, cfg, log.LvlDebug, logger)
	require.NoError(t, err)

	require.GreaterOrEqual(t, keyLen, 8, "keyLen must be >= 8 for binary.BigEndian.PutUint64")

	type kvPair struct {
		key []byte
		val []byte
	}
	pairs := make([]kvPair, numPairs)
	for i := range pairs {
		k := make([]byte, keyLen)
		binary.BigEndian.PutUint64(k[keyLen-8:], uint64(i*37+1))
		k[0] = byte(i >> 8)
		k[1] = byte(i)
		// values have common patterns to trigger compression
		v := make([]byte, 50+i%30)
		copy(v, "common_prefix_value_")
		binary.BigEndian.PutUint64(v[20:], uint64(i))
		pairs[i] = kvPair{key: k, val: v}
	}
	slices.SortFunc(pairs, func(a, b kvPair) int {
		return bytes.Compare(a.key, b.key)
	})

	keys := make([][]byte, numPairs)
	vals := make([][]byte, numPairs)
	for i, p := range pairs {
		keys[i] = p.key
		vals[i] = p.val
		require.NoError(t, c.AddUncompressedWord(p.key)) // key: uncompressed
		require.NoError(t, c.AddWord(p.val))             // value: compressed
	}
	require.NoError(t, c.Compress())
	c.Close()

	d, err := NewDecompressor(file)
	require.NoError(t, err)
	t.Cleanup(d.Close)
	return d, keys, vals
}

// TestReaderMatchCmpUncompressedKeys tests Reader.MatchCmp with CompressVals (uncompressed keys).
// This simulates domain file lookups (accounts, code, storage).
func TestReaderMatchCmpUncompressedKeys(t *testing.T) {
	d, keys, vals := prepareKVUncompressedKeysCompressedVals(t, 20, 200)

	g := NewReader(d.MakeGetter(), CompressVals)

	// Sanity check: read all pairs with Next first
	g.Reset(0)
	for i := 0; g.HasNext(); i++ {
		k, _ := g.Next(nil)
		require.Equal(t, keys[i], k, "sanity pair %d: key mismatch (Next)", i)
		v, _ := g.Next(nil)
		require.Equal(t, vals[i], v, "sanity pair %d: val mismatch (Next)", i)
	}

	g.Reset(0)
	// Test 1: sequential exact matches — MatchCmp should advance past key, Next reads value
	for i, k := range keys {
		require.True(t, g.HasNext(), "pair %d", i)
		cmp := g.MatchCmp(k)
		require.Equal(t, 0, cmp, "pair %d: expected match for key %x", i, k)
		v, _ := g.Next(nil)
		require.Equal(t, vals[i], v, "pair %d: wrong value", i)
	}

	// Test 2: random access — Reset + MatchCmp then read value
	g.Reset(0)
	for g.HasNext() {
		pos := g.Getter.dataP
		key, _ := g.Next(nil)
		val, _ := g.Next(nil)

		// re-read same pair with MatchCmp
		g.Reset(pos)
		cmp := g.MatchCmp(key)
		require.Equal(t, 0, cmp, "expected match for key %x at offset %d", key, pos)
		v2, _ := g.Next(nil)
		require.Equal(t, val, v2, "value mismatch after MatchCmp for key %x", key)
	}

	// Test 3: MatchCmp with wrong key resets position
	g.Reset(0)
	wrongKey := make([]byte, 20)
	wrongKey[0] = 0xFF
	cmp := g.MatchCmp(wrongKey)
	require.NotEqual(t, 0, cmp, "should not match wrong key")
	firstKey, _ := g.Next(nil)
	require.Equal(t, keys[0], firstKey, "position should be reset after MatchCmp mismatch")

	// Test 4: verify MatchCmp result matches bytes.Compare for every key against neighbors
	g.Reset(0)
	for i := range keys {
		pos := g.Getter.dataP
		cmp := g.MatchCmp(keys[i])
		require.Equal(t, 0, cmp, "pair %d should match", i)
		g.Skip() // skip value

		if i > 0 {
			g.Reset(pos)
			cmp = g.MatchCmp(keys[i-1])
			expectedCmp := bytes.Compare(keys[i-1], keys[i])
			require.Equal(t, expectedCmp, cmp, "pair %d: MatchCmp(keys[%d]) wrong", i, i-1)
			g.Reset(pos)
			g.Skip() // skip key
			g.Skip() // skip value
		}
	}
}

// TestReaderBinarySearch tests Reader.BinarySearch with CompressVals (uncompressed keys).
func TestReaderBinarySearch(t *testing.T) {
	numPairs := 500
	d, keys, _ := prepareKVUncompressedKeysCompressedVals(t, 20, numPairs)

	g := NewReader(d.MakeGetter(), CompressVals)

	// Build offset table
	offsets := make([]uint64, 0, numPairs)
	g.Reset(0)
	for g.HasNext() {
		offsets = append(offsets, g.Getter.dataP)
		g.Skip() // skip key
		g.Skip() // skip value
	}
	require.Equal(t, numPairs, len(offsets))

	getOffset := func(i uint64) uint64 { return offsets[i] }

	// Test 1: find every existing key
	for i, k := range keys {
		foundOffset, ok := g.BinarySearch(k, numPairs, getOffset)
		require.True(t, ok, "key %d not found: %x", i, k)
		require.Equal(t, offsets[i], foundOffset, "key %d: wrong offset", i)
	}

	// Test 2: key smaller than all — should find first
	smallKey := make([]byte, 20)
	foundOffset, ok := g.BinarySearch(smallKey, numPairs, getOffset)
	if ok {
		require.Equal(t, offsets[0], foundOffset, "small key should find first entry")
	}

	// Test 3: key larger than all — should not find
	bigKey := make([]byte, 20)
	for i := range bigKey {
		bigKey[i] = 0xFF
	}
	_, ok = g.BinarySearch(bigKey, numPairs, getOffset)
	require.False(t, ok, "big key should not be found")

	// Test 4: MatchPrefix/MatchCmp on last key with prefix beyond it
	lastKey := keys[len(keys)-1]
	lastOffset := offsets[len(offsets)-1]

	// exact prefix of last key should match
	g.Reset(lastOffset)
	require.True(t, g.MatchPrefix(lastKey[:10]), "prefix of last key should match")

	// prefix larger than last key (appended 0xFF) should not match
	beyondKey := append(lastKey[:len(lastKey):len(lastKey)], 0xFF)
	g.Reset(lastOffset)
	require.False(t, g.MatchPrefix(beyondKey), "prefix beyond last key should not match")

	// MatchCmp with key larger than last key
	g.Reset(lastOffset)
	cmpResult := g.MatchCmp(beyondKey)
	require.Equal(t, 1, cmpResult, "key beyond last should be > last key")

	// MatchCmp with exact last key should match
	g.Reset(lastOffset)
	cmpResult = g.MatchCmp(lastKey)
	require.Equal(t, 0, cmpResult, "exact last key should match")
}
