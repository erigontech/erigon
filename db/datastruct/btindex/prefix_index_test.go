package btindex

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
)

// buildTestPrefixIndex creates a standalone PrefixIndex from a .kv file (using the .bt file
// generated alongside it by generateKV). Returns the decompressor (caller must close),
// the PrefixIndex, the EliasFano, and the reader.
func buildTestPrefixIndex(t testing.TB, kvPath string, compressFlags seg.FileCompression) (*seg.Decompressor, *PrefixIndex, *eliasfano32.EliasFano, *seg.Reader) {
	t.Helper()

	decomp, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	g := seg.NewReader(decomp.MakeGetter(), compressFlags)

	// Build EliasFano from sequential scan.
	g.Reset(0)
	var offsets []uint64
	pos := uint64(0)
	for g.HasNext() {
		offsets = append(offsets, pos)
		_, _ = g.Next(nil) // key
		pos, _ = g.Skip()  // skip value
	}

	if len(offsets) == 0 {
		return decomp, nil, nil, g
	}

	ef := eliasfano32.NewEliasFano(uint64(len(offsets)), offsets[len(offsets)-1])
	for _, o := range offsets {
		ef.AddOffset(o)
	}
	ef.Build()
	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))

	ir := NewMockIndexReader(efi)

	pi := NewPrefixIndex(g, efi, ir.dataLookup, ir.keyCmp)
	pi.cursorGetter = ir.newCursor

	return decomp, pi, efi, g
}

// buildTestPrefixIndexWithNodes creates a PrefixIndex using pre-built nodes (the
// NewPrefixIndexWithNodes path), simulating the backward compat path.
func buildTestPrefixIndexWithNodes(t testing.TB, kvPath string, compressFlags seg.FileCompression) (*seg.Decompressor, *PrefixIndex, *eliasfano32.EliasFano, *seg.Reader) {
	t.Helper()

	decomp, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	g := seg.NewReader(decomp.MakeGetter(), compressFlags)

	// Build EliasFano from sequential scan.
	g.Reset(0)
	var offsets []uint64
	pos := uint64(0)
	for g.HasNext() {
		offsets = append(offsets, pos)
		_, _ = g.Next(nil)
		pos, _ = g.Skip()
	}

	if len(offsets) == 0 {
		return decomp, nil, nil, g
	}

	ef := eliasfano32.NewEliasFano(uint64(len(offsets)), offsets[len(offsets)-1])
	for _, o := range offsets {
		ef.AddOffset(o)
	}
	ef.Build()
	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))

	ir := NewMockIndexReader(efi)

	// Build nodes the same way BtIndexWriter does (every M-th key).
	M := DefaultBtreeM
	var nodes []Node
	g.Reset(0)
	var key []byte
	for di := uint64(0); g.HasNext(); di++ {
		key, _ = g.Next(key[:0])
		g.Skip()
		if di > 0 && di%M == 0 {
			nodes = append(nodes, Node{key: common.Copy(key), di: di})
		}
	}

	pi := NewPrefixIndexWithNodes(g, efi, ir.dataLookup, ir.keyCmp, nodes)
	pi.cursorGetter = ir.newCursor

	return decomp, pi, efi, g
}

func TestPrefixIndexBuild(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, efi, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	count := efi.Count()
	require.EqualValues(t, keyCount, count)

	// Verify non-empty buckets have firstDI < endDI.
	activeBuckets := 0
	totalNodes := 0
	for prefix := 0; prefix < 65536; prefix++ {
		b := &pi.buckets[prefix]
		if b.firstDI == math.MaxUint64 {
			require.Empty(t, b.nodes, "empty bucket %04x should have no nodes", prefix)
			continue
		}
		activeBuckets++
		require.Less(t, b.firstDI, b.endDI, "bucket %04x: firstDI must be < endDI", prefix)

		// Nodes are within bucket DI range.
		for i, n := range b.nodes {
			require.GreaterOrEqual(t, n.di, b.firstDI, "bucket %04x node %d DI below firstDI", prefix, i)
			require.Less(t, n.di, b.endDI, "bucket %04x node %d DI at or above endDI", prefix, i)
		}

		// Nodes are sorted by key within each bucket.
		for i := 1; i < len(b.nodes); i++ {
			require.True(t, bytes.Compare(b.nodes[i-1].key, b.nodes[i].key) < 0,
				"bucket %04x: nodes not sorted at %d", prefix, i)
		}

		// At most maxNodesPerBucket nodes.
		require.LessOrEqual(t, len(b.nodes), maxNodesPerBucket,
			"bucket %04x: too many nodes", prefix)
		totalNodes += len(b.nodes)
	}
	require.Greater(t, activeBuckets, 0, "should have at least one active bucket")
	require.Greater(t, totalNodes, 0, "should have at least one node")

	// Verify L1 aggregation.
	for b0 := 0; b0 < 256; b0++ {
		minFirst := uint64(math.MaxUint64)
		maxEnd := uint64(0)
		for b1 := 0; b1 < 256; b1++ {
			prefix := b0<<8 | b1
			b := &pi.buckets[prefix]
			if b.firstDI == math.MaxUint64 {
				continue
			}
			if b.firstDI < minFirst {
				minFirst = b.firstDI
			}
			if b.endDI > maxEnd {
				maxEnd = b.endDI
			}
		}
		require.Equal(t, minFirst, pi.l1First[b0], "L1 first mismatch for byte %02x", b0)
		if minFirst != math.MaxUint64 {
			require.Equal(t, maxEnd, pi.l1End[b0], "L1 end mismatch for byte %02x", b0)
		}
	}
}

func TestPrefixIndexNodeDistribution(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	// Generate keys with enough variety that different prefixes get nodes.
	keyCount := 5000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Verify that all non-empty buckets have at least 1 node.
	for prefix := 0; prefix < 65536; prefix++ {
		b := &pi.buckets[prefix]
		if b.firstDI == math.MaxUint64 {
			continue
		}
		require.NotEmpty(t, b.nodes, "non-empty bucket %04x should have nodes", prefix)
	}
}

func TestPrefixIndexSeek(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Collect all keys.
	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.Len(t, keys, keyCount)

	// Seek nil -> first key.
	c, err := pi.Seek(g, nil)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[0], c.Key())
	c.Close()

	// Seek each key -> should find that exact key.
	for i := 0; i < len(keys); i++ {
		c, err := pi.Seek(g, keys[i])
		require.NoErrorf(t, err, "i=%d key=%x", i, keys[i])
		require.NotNilf(t, c, "i=%d key=%x", i, keys[i])
		require.Equalf(t, keys[i], c.Key(), "i=%d", i)
		c.Close()
	}

	// Seek with key-1 (decrement last byte) should still find the original key.
	for i := 1; i < len(keys); i++ {
		alt := common.Copy(keys[i])
		for j := len(alt) - 1; j >= 0; j-- {
			if alt[j] > 0 {
				alt[j]--
				break
			}
		}
		c, err := pi.Seek(g, alt)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.True(t, bytes.Compare(c.Key(), alt) >= 0,
			"seek(%x) returned key %x which is less", alt, c.Key())
		c.Close()
	}
}

func TestPrefixIndexSeekBeyond(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 100
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Seek with key larger than all entries -> nil cursor.
	c, err := pi.Seek(g, common.FromHex("0xffffffffffffff"))
	require.NoError(t, err)
	require.Nil(t, c)
	c.Close()
}

func TestPrefixIndexGet(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Collect all keys and values from kv file.
	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.Len(t, keys, keyCount)

	// Collect all values for verification.
	g.Reset(0)
	values := make([][]byte, 0, keyCount)
	for g.HasNext() {
		_, _ = g.Next(nil) // skip key
		v, _ := g.Next(nil)
		values = append(values, common.Copy(v))
	}

	// Get each key -> should find exact value.
	for i := 0; i < len(keys); i++ {
		v, ok, _, err := pi.Get(g, keys[i])
		require.NoErrorf(t, err, "i=%d key=%x", i, keys[i])
		require.Truef(t, ok, "i=%d key=%x not found", i, keys[i])
		require.Equalf(t, values[i], v, "i=%d value mismatch", i)
	}
}

func TestPrefixIndexGetMissing(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 100
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Get non-existent key -> not found.
	v, ok, _, err := pi.Get(g, common.FromHex("0xdeadbeefdeadbeef01"))
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, v)
}

func TestPrefixIndexSmallFile(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	// Small file with very few keys - PrefixIndex should still work (single code path).
	keyCount := 10
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.Len(t, keys, keyCount)

	// Seek and Get each key.
	for i := 0; i < len(keys); i++ {
		c, err := pi.Seek(g, keys[i])
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, keys[i], c.Key())
		c.Close()

		v, ok, _, err := pi.Get(g, keys[i])
		require.NoError(t, err)
		require.True(t, ok)
		require.NotEmpty(t, v)
	}

	// Seek nil -> first key.
	c, err := pi.Seek(g, nil)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[0], c.Key())
	c.Close()
}

func TestPrefixIndexBackwardCompat(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	// Build PrefixIndex with pre-built nodes (backward compat path).
	decomp, piNodes, _, g := buildTestPrefixIndexWithNodes(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, piNodes)
	defer piNodes.Close()

	// Also build using OpenBtreeIndexAndDataFile for BpsTree comparison.
	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	kv2, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, compressFlags, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv2.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	getter := seg.NewReader(kv2.MakeGetter(), compressFlags)

	// Compare Get results between PrefixIndex (with nodes) and BpsTree.
	for i := 0; i < len(keys); i++ {
		v1, ok1, _, err1 := piNodes.Get(g, keys[i])
		_, v2, _, ok2, err2 := bt.Get(keys[i], getter)
		require.NoError(t, err1, "i=%d", i)
		require.NoError(t, err2, "i=%d", i)
		require.Equal(t, ok1, ok2, "i=%d found mismatch", i)
		require.Equal(t, v1, v2, "i=%d value mismatch", i)
	}

	// Compare Seek results.
	for i := 0; i < len(keys); i++ {
		c1, err1 := piNodes.Seek(g, keys[i])
		c2, err2 := bt.Seek(getter, keys[i])
		require.NoError(t, err1, "i=%d", i)
		require.NoError(t, err2, "i=%d", i)
		if c1 == nil && c2 == nil {
			continue
		}
		require.NotNil(t, c1, "i=%d", i)
		require.NotNil(t, c2, "i=%d", i)
		require.Equal(t, c1.Key(), c2.Key(), "i=%d seek key mismatch", i)
		c1.Close()
		c2.Close()
	}
}

func TestPrefixIndexSeekIteration(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 200
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.Len(t, keys, keyCount)

	// Seek nil and iterate through all keys.
	c, err := pi.Seek(g, nil)
	require.NoError(t, err)
	require.NotNil(t, c)
	for i := 0; i < len(keys); i++ {
		require.Equalf(t, keys[i], c.Key(), "i=%d", i)
		if i < len(keys)-1 {
			ok := c.Next()
			require.Truef(t, ok, "i=%d: expected Next() to succeed", i)
		}
	}
	c.Close()
}

func TestPrefixIndexLookup(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 500
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, efi, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	count := efi.Count()

	// lookup with empty key -> full range.
	l, r := pi.lookup(nil)
	require.Equal(t, uint64(0), l)
	require.Equal(t, count, r)

	// lookup with 1-byte key -> L1 range.
	l, r = pi.lookup([]byte{0x00})
	if pi.l1First[0] != math.MaxUint64 {
		require.Equal(t, pi.l1First[0], l)
		require.Equal(t, pi.l1End[0], r)
	} else {
		require.Equal(t, uint64(0), l)
		require.Equal(t, uint64(0), r)
	}
}

func TestPrefixIndexNarrowWithNodes(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 2000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	// For keys that have cached nodes, narrowWithNodes should tighten the range.
	for i := 0; i < len(keys); i++ {
		l, r := pi.lookup(keys[i])
		if l == 0 && r == 0 {
			continue
		}
		nl, nr, exactDI, found := pi.narrowWithNodes(keys[i], l, r)
		if found {
			// Verify the exact DI matches a cached node.
			prefix := uint16(keys[i][0])<<8 | uint16(keys[i][1])
			bucket := &pi.buckets[prefix]
			nodeFound := false
			for _, n := range bucket.nodes {
				if n.di == exactDI {
					nodeFound = true
					break
				}
			}
			require.True(t, nodeFound, "exact DI %d not in bucket nodes for key %x", exactDI, keys[i])
		} else {
			require.LessOrEqual(t, nl, nr, "narrowed range invalid for key %x", keys[i])
		}
	}

	// Verify Seek still works after narrowing.
	for i := 0; i < len(keys); i++ {
		c, err := pi.Seek(g, keys[i])
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, keys[i], c.Key())
		c.Close()
	}
}

func TestPrefixIndexDistances(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 100
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	distances, err := pi.Distances()
	require.NoError(t, err)
	require.NotEmpty(t, distances)
}

func TestPrefixIndexOffsets(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 100
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, efi, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	require.Equal(t, efi, pi.Offsets())
}

func TestPrefixIndexBucketNodesAreSorted(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 5000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	for prefix := 0; prefix < 65536; prefix++ {
		b := &pi.buckets[prefix]
		if len(b.nodes) < 2 {
			continue
		}
		require.True(t, sort.SliceIsSorted(b.nodes, func(i, j int) bool {
			return bytes.Compare(b.nodes[i].key, b.nodes[j].key) < 0
		}), "bucket %04x nodes not sorted by key", prefix)
		require.True(t, sort.SliceIsSorted(b.nodes, func(i, j int) bool {
			return b.nodes[i].di < b.nodes[j].di
		}), "bucket %04x nodes not sorted by DI", prefix)
	}
}

// ================================================================
// T1: INV-C5 — every cached node's key has the same 2-byte prefix as its bucket
// ================================================================
func TestPrefixIndexNodePrefixMatch(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 5000, logger, compressFlags)

	decomp, pi, _, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	for prefix := 0; prefix < 65536; prefix++ {
		b := &pi.buckets[prefix]
		if b.firstDI == math.MaxUint64 || len(b.nodes) == 0 {
			continue
		}
		p0 := byte(prefix >> 8)
		p1 := byte(prefix & 0xFF)
		for i := range b.nodes {
			require.GreaterOrEqual(t, len(b.nodes[i].key), 2,
				"bucket %04x node %d: key too short", prefix, i)
			require.Equal(t, p0, b.nodes[i].key[0],
				"bucket %04x node %d: first byte mismatch", prefix, i)
			require.Equal(t, p1, b.nodes[i].key[1],
				"bucket %04x node %d: second byte mismatch", prefix, i)
		}
	}
}

// ================================================================
// T2: INV-C9 — node keys are copies, not slices into the decompressor buffer
// ================================================================
func TestPrefixIndexNodeKeysAreIndependent(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 1000, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Collect all node key pointers and their original values.
	type nodeRecord struct {
		prefix int
		idx    int
		orig   []byte
		ptr    uintptr
	}
	var records []nodeRecord
	for prefix := 0; prefix < 65536; prefix++ {
		b := &pi.buckets[prefix]
		for i := range b.nodes {
			records = append(records, nodeRecord{
				prefix: prefix,
				idx:    i,
				orig:   common.Copy(b.nodes[i].key),
				ptr:    uintptr(unsafe.Pointer(&b.nodes[i].key[0])),
			})
		}
	}
	require.NotEmpty(t, records)

	// Verify no two nodes share the same backing array.
	ptrs := make(map[uintptr]bool, len(records))
	for _, r := range records {
		require.False(t, ptrs[r.ptr],
			"bucket %04x node %d shares backing array with another node", r.prefix, r.idx)
		ptrs[r.ptr] = true
	}

	// Read through the file (mutates reader state) and verify node keys are unchanged.
	g.Reset(0)
	for g.HasNext() {
		_, _ = g.Next(nil)
		_, _ = g.Skip()
	}

	idx := 0
	for prefix := 0; prefix < 65536; prefix++ {
		b := &pi.buckets[prefix]
		for i := range b.nodes {
			require.Equal(t, records[idx].orig, b.nodes[i].key,
				"bucket %04x node %d: key changed after reader scan", prefix, i)
			idx++
		}
	}
}

// ================================================================
// T3: F4 — modifying the original nodes slice must not corrupt the PrefixIndex
// ================================================================
func TestPrefixIndexWithNodesNodeKeysStable(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 1000, logger, compressFlags)

	decomp, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	defer decomp.Close()
	g := seg.NewReader(decomp.MakeGetter(), compressFlags)

	// Build EliasFano.
	g.Reset(0)
	var offsets []uint64
	pos := uint64(0)
	for g.HasNext() {
		offsets = append(offsets, pos)
		_, _ = g.Next(nil)
		pos, _ = g.Skip()
	}
	require.NotEmpty(t, offsets)

	ef := eliasfano32.NewEliasFano(uint64(len(offsets)), offsets[len(offsets)-1])
	for _, o := range offsets {
		ef.AddOffset(o)
	}
	ef.Build()
	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))
	ir := NewMockIndexReader(efi)

	// Build nodes the same way BtIndexWriter does (every M-th key).
	M := DefaultBtreeM
	var nodes []Node
	g.Reset(0)
	var key []byte
	for di := uint64(0); g.HasNext(); di++ {
		key, _ = g.Next(key[:0])
		g.Skip()
		if di > 0 && di%M == 0 {
			nodes = append(nodes, Node{key: common.Copy(key), di: di})
		}
	}
	require.NotEmpty(t, nodes)

	// Build PrefixIndex with pre-built nodes.
	pi := NewPrefixIndexWithNodes(g, efi, ir.dataLookup, ir.keyCmp, nodes)
	pi.cursorGetter = ir.newCursor
	defer pi.Close()

	// Corrupt the original nodes' key bytes.
	for i := range nodes {
		for j := range nodes[i].key {
			nodes[i].key[j] = 0xFF
		}
	}

	// Get should still work for every key in the file.
	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	for i, k := range keys {
		v, ok, _, err := pi.Get(g, k)
		require.NoErrorf(t, err, "i=%d key=%x", i, k)
		require.Truef(t, ok, "i=%d key=%x not found after node corruption", i, k)
		require.NotEmpty(t, v)
	}
}

// ================================================================
// T4: All keys share a single 2-byte prefix — bucket gets up to 8 nodes
// ================================================================
func TestPrefixIndexBuildWithSinglePrefix(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	prefix := []byte{0xAB, 0xCD}
	kvPath, keys, _ := generateControlledKV(t, tmp, [][]byte{prefix}, 20, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Verify exactly 1 active bucket (0xABCD).
	activeBuckets := 0
	for p := 0; p < 65536; p++ {
		if pi.buckets[p].firstDI != math.MaxUint64 {
			activeBuckets++
			require.Equal(t, 0xABCD, p, "unexpected active bucket %04x", p)
		}
	}
	require.Equal(t, 1, activeBuckets)

	b := &pi.buckets[0xABCD]
	require.LessOrEqual(t, len(b.nodes), maxNodesPerBucket)
	require.NotEmpty(t, b.nodes)

	// Seek and Get every key.
	for i, k := range keys {
		c, err := pi.Seek(g, k)
		require.NoErrorf(t, err, "i=%d", i)
		require.NotNilf(t, c, "i=%d", i)
		require.Equalf(t, k, c.Key(), "i=%d seek mismatch", i)
		c.Close()

		v, ok, _, err := pi.Get(g, k)
		require.NoErrorf(t, err, "i=%d", i)
		require.Truef(t, ok, "i=%d get failed", i)
		require.NotEmpty(t, v)
	}
}

// ================================================================
// T5: E1, E2 — Seek first and last key in the file
// ================================================================
func TestPrefixIndexSeekFirstAndLast(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 1000, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	// Seek first key.
	c, err := pi.Seek(g, keys[0])
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[0], c.Key())
	c.Close()

	// Seek last key.
	c, err = pi.Seek(g, keys[len(keys)-1])
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[len(keys)-1], c.Key())
	c.Close()
}

// ================================================================
// T6: E6, E8 — Seek with key whose prefix falls in a gap between two prefix buckets
// ================================================================
func TestPrefixIndexSeekBetweenPrefixes(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	// Prefixes 0x0100 and 0x0300 with a gap at 0x0200.
	kvPath, keys, _ := generateControlledKV(t, tmp,
		[][]byte{{0x01, 0x00}, {0x03, 0x00}}, 20, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Find first key with prefix 0x0300.
	var first0300 []byte
	for _, k := range keys {
		if k[0] == 0x03 && k[1] == 0x00 {
			first0300 = k
			break
		}
	}
	require.NotNil(t, first0300, "no key with prefix 0x0300")

	// Seek(0x0200...) -> should return first key with prefix 0x0300.
	seekKey := []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	c, err := pi.Seek(g, seekKey)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, first0300, c.Key())
	c.Close()

	// Seek(0x02FF...) -> should also return first key with prefix 0x0300.
	seekKey2 := []byte{0x02, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	c, err = pi.Seek(g, seekKey2)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, first0300, c.Key())
	c.Close()

	// Get(0x0200...) -> key doesn't exist.
	v, ok, _, err := pi.Get(g, seekKey)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, v)
}

// ================================================================
// T7: E3 — Seek with key before all keys returns first key
// ================================================================
func TestPrefixIndexSeekBeforeAll(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	// All keys start with 0x50 (well above 0x00).
	kvPath, keys, _ := generateControlledKV(t, tmp,
		[][]byte{{0x50, 0x00}}, 30, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Seek with key below all -> first key.
	c, err := pi.Seek(g, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[0], c.Key())
	c.Close()
}

// ================================================================
// T8: E4 — Seek with key after all keys returns nil cursor
// ================================================================
func TestPrefixIndexSeekAfterAll(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	// All keys start with 0x50.
	kvPath, _, _ := generateControlledKV(t, tmp,
		[][]byte{{0x50, 0x00}}, 30, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Seek beyond all keys.
	c, err := pi.Seek(g, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	require.NoError(t, err)
	require.Nil(t, c)
}

// ================================================================
// T9: INV-S2 — Seek every key returns exact match (large file, controlled prefixes)
// ================================================================
func TestPrefixIndexSeekEveryKeyExact(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	// Multiple prefixes for wider coverage.
	kvPath, keys, _ := generateControlledKV(t, tmp,
		[][]byte{{0x10, 0x20}, {0x30, 0x40}, {0x50, 0x60}, {0xA0, 0xB0}},
		50, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	for i, k := range keys {
		c, err := pi.Seek(g, k)
		require.NoErrorf(t, err, "i=%d key=%x", i, k)
		require.NotNilf(t, c, "i=%d key=%x", i, k)
		require.Equalf(t, k, c.Key(), "i=%d: Seek returned wrong key", i)
		c.Close()
	}
}

// ================================================================
// T10: INV-S1, S2 — Seek with gap keys (between every adjacent pair) returns next key
// ================================================================
func TestPrefixIndexSeekEveryGap(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	kvPath, keys, _ := generateControlledKV(t, tmp,
		[][]byte{{0x10, 0x20}, {0x30, 0x40}, {0x50, 0x60}},
		30, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	for i := 0; i < len(keys)-1; i++ {
		gap := gapKey(keys[i])
		// gap might equal keys[i+1] if keys are adjacent (unlikely with random suffixes)
		if bytes.Equal(gap, keys[i+1]) {
			continue
		}
		// gap should be > keys[i], so Seek(gap) should return keys[i+1] (the next key >= gap)
		if bytes.Compare(gap, keys[i+1]) >= 0 {
			continue // gap overshot; skip
		}
		c, err := pi.Seek(g, gap)
		require.NoErrorf(t, err, "i=%d gap=%x", i, gap)
		require.NotNilf(t, c, "i=%d gap=%x", i, gap)
		require.Equalf(t, keys[i+1], c.Key(),
			"i=%d gap=%x: expected next key %x, got %x", i, gap, keys[i+1], c.Key())
		c.Close()
	}
}

// ================================================================
// T11: E6, F6 — Seek with key whose prefix doesn't exist (and first byte has no keys)
// ================================================================
func TestPrefixIndexSeekNonExistentPrefix(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	// All keys start with 0x01.
	kvPath, keys, _ := generateControlledKV(t, tmp,
		[][]byte{{0x01, 0x00}}, 30, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Seek(0xFF...) -> nil (no keys >= 0xFF).
	c, err := pi.Seek(g, []byte{0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	require.Nil(t, c)

	// Seek(0x00...) -> first key (0x01... >= 0x00...).
	c, err = pi.Seek(g, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[0], c.Key())
	c.Close()
}

// ================================================================
// T12: INV-S4 — Get returns the exact value paired with each key
// ================================================================
func TestPrefixIndexGetValueCorrectness(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	kvPath, keys, values := generateControlledKV(t, tmp,
		[][]byte{{0x10, 0x20}, {0x30, 0x40}, {0x50, 0x60}},
		40, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	for i, k := range keys {
		v, ok, off, err := pi.Get(g, k)
		require.NoErrorf(t, err, "i=%d key=%x", i, k)
		require.Truef(t, ok, "i=%d key=%x not found", i, k)
		require.Equalf(t, values[i], v, "i=%d value mismatch", i)

		// Verify the offset actually points to this key in the file.
		g.Reset(off)
		fileKey, _ := g.Next(nil)
		require.Equalf(t, k, fileKey, "i=%d offset %d points to wrong key", i, off)
	}
}

// ================================================================
// T13: E7 — Get with existing prefix but non-existent full key
// ================================================================
func TestPrefixIndexGetMissingWithExistingPrefix(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	kvPath, keys, _ := generateControlledKV(t, tmp,
		[][]byte{{0xAB, 0xCD}}, 20, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Fabricate a key with the same prefix but different suffix.
	// Use a suffix that can't collide with generated suffixes.
	require.NotEmpty(t, keys)
	fabricated := []byte{0xAB, 0xCD, 0xFF, 0xFF, 0xFF, 0xFE, 0xFE, 0xFE}
	v, ok, _, err := pi.Get(g, fabricated)
	require.NoError(t, err)
	require.False(t, ok, "should not find fabricated key")
	require.Nil(t, v)
}

// ================================================================
// T14: F3, E18 — Get non-existent key in the last bucket (endDI == count)
// Tests the F3 fix: should return clean "not found", not an error.
// ================================================================
func TestPrefixIndexGetLastBucketBoundary(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 1000, logger, compressFlags)

	decomp, pi, efi, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	count := efi.Count()
	lastKey := keys[len(keys)-1]

	// Verify this key's bucket has endDI == count.
	require.GreaterOrEqual(t, len(lastKey), 2)
	prefix := uint16(lastKey[0])<<8 | uint16(lastKey[1])
	b := &pi.buckets[prefix]
	require.Equal(t, count, b.endDI, "last key's bucket endDI should equal count")

	// Fabricate a key in the same prefix bucket but greater than lastKey.
	fabricated := append(common.Copy(lastKey), 0xFF)
	v, ok, _, err := pi.Get(g, fabricated)
	require.NoError(t, err, "F3: Get should not return error for non-existent key at boundary")
	require.False(t, ok, "should not find fabricated key")
	require.Nil(t, v)
}

// ================================================================
// T15: E5 — Get with nil and empty key
// ================================================================
func TestPrefixIndexGetEmptyKey(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 100, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// For standard test data where the first key is non-empty, Get(nil) returns not-found.
	_, ok, _, err := pi.Get(g, nil)
	require.NoError(t, err)
	require.False(t, ok, "Get(nil) should return false when first key is non-empty")

	_, ok, _, err = pi.Get(g, []byte{})
	require.NoError(t, err)
	require.False(t, ok, "Get(empty) should return false when first key is non-empty")
}

// ================================================================
// T16: E10 — Empty file (0 keys)
// EliasFano doesn't support count=0, so buildTestPrefixIndex returns nil.
// ================================================================
func TestPrefixIndexEmptyFile(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	kvPath := generateMinimalKV(t, tmp, nil, nil, compressFlags)

	decomp, pi, _, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()

	// With 0 keys, PrefixIndex is nil (EliasFano cannot represent 0 entries).
	require.Nil(t, pi)
	// Close on nil is safe.
	pi.Close()
}

// ================================================================
// T17: E11 — File with exactly 1 key
// ================================================================
func TestPrefixIndexSingleKey(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone

	theKey := []byte{0x42, 0x7A, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06}
	theVal := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	kvPath := generateMinimalKV(t, tmp, [][]byte{theKey}, [][]byte{theVal}, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Seek(thatKey) -> cursor at that key.
	c, err := pi.Seek(g, theKey)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, theKey, c.Key())
	c.Close()

	// Seek(nil) -> cursor at that key.
	c, err = pi.Seek(g, nil)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, theKey, c.Key())
	c.Close()

	// Seek(keyBefore) -> cursor at that key.
	c, err = pi.Seek(g, []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, theKey, c.Key())
	c.Close()

	// Seek(keyAfter) -> nil.
	c, err = pi.Seek(g, []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	require.NoError(t, err)
	require.Nil(t, c)

	// Get(thatKey) -> (value, true).
	v, ok, _, err := pi.Get(g, theKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, theVal, v)

	// Get(otherKey) -> (nil, false).
	v, ok, _, err = pi.Get(g, []byte{0x42, 0x7A, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, v)
}

// ================================================================
// T18: E12 — File with exactly 8 keys (all same prefix, all become nodes)
// ================================================================
func TestPrefixIndexExactly8Keys(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	kvPath, keys, vals := generateControlledKV(t, tmp,
		[][]byte{{0xBE, 0xEF}}, 8, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// All 8 keys should become nodes (since count <= maxNodesPerBucket).
	b := &pi.buckets[0xBEEF]
	require.Equal(t, len(keys), len(b.nodes),
		"all %d keys should become nodes", len(keys))

	for i, k := range keys {
		c, err := pi.Seek(g, k)
		require.NoError(t, err)
		require.NotNil(t, c)
		require.Equal(t, k, c.Key())
		c.Close()

		v, ok, _, err := pi.Get(g, k)
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, vals[i], v)
	}
}

// ================================================================
// T19: E13 — File with exactly 9 keys (same prefix, only 8 become nodes)
// ================================================================
func TestPrefixIndexExactly9Keys(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	compressFlags := seg.CompressNone
	kvPath, keys, vals := generateControlledKV(t, tmp,
		[][]byte{{0xCA, 0xFE}}, 9, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// 9 > maxNodesPerBucket(8), so only 8 become nodes.
	b := &pi.buckets[0xCAFE]
	require.Equal(t, maxNodesPerBucket, len(b.nodes),
		"should have exactly %d nodes for 9 keys", maxNodesPerBucket)

	// All 9 keys (including the non-cached one) must still be findable.
	for i, k := range keys {
		c, err := pi.Seek(g, k)
		require.NoErrorf(t, err, "i=%d key=%x", i, k)
		require.NotNilf(t, c, "i=%d key=%x", i, k)
		require.Equalf(t, k, c.Key(), "i=%d", i)
		c.Close()

		v, ok, _, err := pi.Get(g, k)
		require.NoErrorf(t, err, "i=%d key=%x", i, k)
		require.Truef(t, ok, "i=%d key=%x get failed", i, k)
		require.Equalf(t, vals[i], v, "i=%d value mismatch", i)
	}
}

// ================================================================
// T20: INV-E1, E2 — Exhaustive equivalence between PrefixIndex and BpsTree
// ================================================================
func TestPrefixIndexEquivalenceExhaustive(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	keyCount := 5000
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	// Build PrefixIndex.
	decomp1, pi, _, g1 := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp1.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Build BtIndex (wraps BpsTree).
	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	kv2, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, compressFlags, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv2.Close()
	g2 := seg.NewReader(kv2.MakeGetter(), compressFlags)

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.Len(t, keys, keyCount)

	// Compare Seek and Get for every key.
	for i, k := range keys {
		// Compare Seek.
		c1, err1 := pi.Seek(g1, k)
		c2, err2 := bt.Seek(g2, k)
		require.NoErrorf(t, err1, "pi.Seek i=%d", i)
		require.NoErrorf(t, err2, "bt.Seek i=%d", i)
		if c1 == nil && c2 == nil {
			continue
		}
		require.NotNilf(t, c1, "pi nil at i=%d", i)
		require.NotNilf(t, c2, "bt nil at i=%d", i)
		require.Equalf(t, c2.Key(), c1.Key(), "seek key mismatch i=%d", i)
		c1.Close()
		c2.Close()

		// Compare Get.
		v1, ok1, _, err1 := pi.Get(g1, k)
		_, v2, _, ok2, err2 := bt.Get(k, g2)
		require.NoErrorf(t, err1, "pi.Get i=%d", i)
		require.NoErrorf(t, err2, "bt.Get i=%d", i)
		require.Equalf(t, ok2, ok1, "found mismatch i=%d", i)
		require.Equalf(t, v2, v1, "value mismatch i=%d", i)
	}

	// Also compare with gap keys between every 10th pair.
	for i := 0; i < len(keys)-1; i += 10 {
		gap := gapKey(keys[i])
		if bytes.Compare(gap, keys[i+1]) >= 0 {
			continue
		}

		c1, err1 := pi.Seek(g1, gap)
		c2, err2 := bt.Seek(g2, gap)
		require.NoErrorf(t, err1, "pi.Seek gap i=%d", i)
		require.NoErrorf(t, err2, "bt.Seek gap i=%d", i)
		if c1 == nil && c2 == nil {
			continue
		}
		require.NotNilf(t, c1, "pi nil gap i=%d", i)
		require.NotNilf(t, c2, "bt nil gap i=%d", i)
		require.Equalf(t, c2.Key(), c1.Key(), "seek gap key mismatch i=%d gap=%x", i, gap)
		c1.Close()
		c2.Close()
	}
}

// ================================================================
// T21: INV-E3 — NewPrefixIndex vs NewPrefixIndexWithNodes produce same results
// ================================================================
func TestPrefixIndexPathEquivalence(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 2000, logger, compressFlags)

	// Path 1: fresh 2-pass scan.
	decomp1, pi1, _, g1 := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp1.Close()
	require.NotNil(t, pi1)
	defer pi1.Close()

	// Path 2: pre-built nodes.
	decomp2, pi2, _, g2 := buildTestPrefixIndexWithNodes(t, kvPath, compressFlags)
	defer decomp2.Close()
	require.NotNil(t, pi2)
	defer pi2.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	for i, k := range keys {
		// Compare Seek.
		c1, err1 := pi1.Seek(g1, k)
		c2, err2 := pi2.Seek(g2, k)
		require.NoErrorf(t, err1, "pi1.Seek i=%d", i)
		require.NoErrorf(t, err2, "pi2.Seek i=%d", i)
		if c1 == nil && c2 == nil {
			continue
		}
		require.NotNilf(t, c1, "pi1 nil i=%d", i)
		require.NotNilf(t, c2, "pi2 nil i=%d", i)
		require.Equalf(t, c1.Key(), c2.Key(), "seek mismatch i=%d", i)
		c1.Close()
		c2.Close()

		// Compare Get.
		v1, ok1, _, err1 := pi1.Get(g1, k)
		v2, ok2, _, err2 := pi2.Get(g2, k)
		require.NoErrorf(t, err1, "pi1.Get i=%d", i)
		require.NoErrorf(t, err2, "pi2.Get i=%d", i)
		require.Equalf(t, ok1, ok2, "found mismatch i=%d", i)
		require.Equalf(t, v1, v2, "value mismatch i=%d", i)
	}
}

// ================================================================
// T22: F5 — Concurrent reads on the same PrefixIndex
// ================================================================
func TestPrefixIndexConcurrentReads(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 1000, logger, compressFlags)

	decomp, pi, _, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g := seg.NewReader(decomp.MakeGetter(), compressFlags)
			for _, k := range keys {
				c, err := pi.Seek(g, k)
				if err != nil {
					t.Errorf("Seek(%x): %v", k, err)
					return
				}
				if c == nil {
					t.Errorf("Seek(%x): nil cursor", k)
					return
				}
				if !bytes.Equal(c.Key(), k) {
					t.Errorf("Seek(%x): got %x", k, c.Key())
				}
				c.Close()

				_, ok, _, err := pi.Get(g, k)
				if err != nil {
					t.Errorf("Get(%x): %v", k, err)
					return
				}
				if !ok {
					t.Errorf("Get(%x): not found", k)
				}
			}
		}()
	}
	wg.Wait()
}

// ================================================================
// T23: INV-S7 — Seek to middle key, then iterate forward to end
// ================================================================
func TestPrefixIndexSeekThenIterateFromMiddle(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 500, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	mid := len(keys) / 2
	c, err := pi.Seek(g, keys[mid])
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[mid], c.Key())

	for i := mid + 1; i < len(keys); i++ {
		ok := c.Next()
		require.Truef(t, ok, "Next() failed at i=%d", i)
		require.Equalf(t, keys[i], c.Key(), "iteration mismatch at i=%d", i)
	}

	// Should not be able to advance past the end.
	require.False(t, c.Next(), "Next() should return false at end")
	c.Close()
}

// ================================================================
// T24: Seek a gap key, then iterate forward
// ================================================================
func TestPrefixIndexSeekThenIterateFromGap(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, 500, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	// Pick a gap between keys[mid] and keys[mid+1].
	mid := len(keys) / 2
	gap := gapKey(keys[mid])
	if bytes.Compare(gap, keys[mid+1]) >= 0 {
		// Gap overshot, skip this variant.
		t.Skip("gap key overshot next key")
	}

	c, err := pi.Seek(g, gap)
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[mid+1], c.Key(), "should land on next key after gap")

	for i := mid + 2; i < len(keys); i++ {
		ok := c.Next()
		require.Truef(t, ok, "Next() failed at i=%d", i)
		require.Equalf(t, keys[i], c.Key(), "iteration mismatch at i=%d", i)
	}
	c.Close()
}

// ================================================================
// Tests 1-11: PrefixIndex correctness suite
// ================================================================

func TestPrefixIndex_SeekFirstKey(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.Len(t, keys, keyCount)

	c, err := pi.Seek(g, keys[0])
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, keys[0], c.Key())
	c.Close()
}

func TestPrefixIndex_SeekLastKey(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.Len(t, keys, keyCount)

	lastKey := keys[len(keys)-1]
	c, err := pi.Seek(g, lastKey)
	require.NoError(t, err)
	require.NotNil(t, c, "seek for last key should not return nil")
	require.Equal(t, lastKey, c.Key())
	c.Close()
}

func TestPrefixIndex_SeekBeyondAll(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 100
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	beyondKey := bytes.Repeat([]byte{0xff}, 20)
	c, err := pi.Seek(g, beyondKey)
	require.NoError(t, err)
	require.Nil(t, c, "seek beyond all keys should return nil cursor")
}

func TestPrefixIndex_SeekBeforeAll(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 100
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	beforeKey := bytes.Repeat([]byte{0x00}, 10)
	c, err := pi.Seek(g, beforeKey)
	require.NoError(t, err)
	require.NotNil(t, c, "seek before all keys should return first key")
	require.True(t, bytes.Compare(c.Key(), beforeKey) >= 0)
	require.Equal(t, keys[0], c.Key())
	c.Close()
}

func TestPrefixIndex_GetExact(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.Len(t, keys, keyCount)

	// Collect values.
	g.Reset(0)
	values := make([][]byte, 0, keyCount)
	for g.HasNext() {
		_, _ = g.Next(nil)
		v, _ := g.Next(nil)
		values = append(values, common.Copy(v))
	}

	for i, k := range keys {
		v, ok, _, err := pi.Get(g, k)
		require.NoErrorf(t, err, "i=%d key=%x", i, k)
		require.Truef(t, ok, "i=%d key=%x not found", i, k)
		require.Equalf(t, values[i], v, "i=%d value mismatch", i)
	}
}

func TestPrefixIndex_GetNonExistent(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 100
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	v, ok, _, err := pi.Get(g, bytes.Repeat([]byte{0xde}, 10))
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, v)
}

func TestPrefixIndex_GetLastBucketBoundary(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 100
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, efi, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	count := efi.Count()

	// Find the last non-empty bucket where endDI == count.
	lastPrefix := -1
	for prefix := 65535; prefix >= 0; prefix-- {
		b := &pi.buckets[prefix]
		if b.firstDI != math.MaxUint64 && b.endDI == count {
			lastPrefix = prefix
			break
		}
	}
	require.NotEqual(t, -1, lastPrefix, "should find a last bucket with endDI==count")

	// Craft a non-existent key with this prefix (0xff padding to maximize).
	fakeKey := make([]byte, 10)
	fakeKey[0] = byte(lastPrefix >> 8)
	fakeKey[1] = byte(lastPrefix & 0xff)
	for i := 2; i < len(fakeKey); i++ {
		fakeKey[i] = 0xff
	}

	// F3 fix: this should NOT return an error.
	v, ok, _, err := pi.Get(g, fakeKey)
	require.NoError(t, err, "Get in last bucket should not return ErrBtIndexLookupBounds")
	require.False(t, ok, "non-existent key should return ok=false")
	require.Nil(t, v)
}

func TestPrefixIndex_NodeKeyStability(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	defer decomp.Close()
	g := seg.NewReader(decomp.MakeGetter(), compressFlags)

	// Build EliasFano.
	g.Reset(0)
	var offsets []uint64
	pos := uint64(0)
	for g.HasNext() {
		offsets = append(offsets, pos)
		_, _ = g.Next(nil)
		pos, _ = g.Skip()
	}
	require.NotEmpty(t, offsets)

	ef := eliasfano32.NewEliasFano(uint64(len(offsets)), offsets[len(offsets)-1])
	for _, o := range offsets {
		ef.AddOffset(o)
	}
	ef.Build()
	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))
	ir := NewMockIndexReader(efi)

	// Build nodes manually (retain references to key slices).
	M := DefaultBtreeM
	var nodes []Node
	g.Reset(0)
	var key []byte
	for di := uint64(0); g.HasNext(); di++ {
		key, _ = g.Next(key[:0])
		g.Skip()
		if di > 0 && di%M == 0 {
			nodeKey := make([]byte, len(key))
			copy(nodeKey, key)
			nodes = append(nodes, Node{key: nodeKey, di: di})
		}
	}
	require.NotEmpty(t, nodes)

	// Save original key values.
	origKeys := make([][]byte, len(nodes))
	for i, n := range nodes {
		origKeys[i] = common.Copy(n.key)
	}

	// Build PrefixIndex with these nodes.
	pi := NewPrefixIndexWithNodes(g, efi, ir.dataLookup, ir.keyCmp, nodes)
	pi.cursorGetter = ir.newCursor
	require.NotNil(t, pi)
	defer pi.Close()

	// Verify internal keys don't share backing arrays with source nodes.
	for _, n := range nodes {
		if len(n.key) < 2 {
			continue
		}
		srcPtr := uintptr(unsafe.Pointer(&n.key[0]))
		prefix := uint16(n.key[0])<<8 | uint16(n.key[1])
		for _, bn := range pi.buckets[prefix].nodes {
			if len(bn.key) > 0 {
				intPtr := uintptr(unsafe.Pointer(&bn.key[0]))
				require.NotEqual(t, srcPtr, intPtr,
					"internal key at prefix %04x shares backing array with source node", prefix)
			}
		}
	}

	// Mutate source nodes' keys.
	for i := range nodes {
		for j := range nodes[i].key {
			nodes[i].key[j] = 0xff
		}
	}

	// Verify internal keys are unaffected (not all 0xff).
	for prefix := 0; prefix < 65536; prefix++ {
		for _, n := range pi.buckets[prefix].nodes {
			allFF := true
			for _, b := range n.key {
				if b != 0xff {
					allFF = false
					break
				}
			}
			if allFF && len(n.key) > 0 {
				t.Fatalf("internal key at prefix %04x was mutated to all 0xff: %x", prefix, n.key)
			}
		}
	}

	// Verify Get still works with original keys.
	for _, k := range origKeys {
		v, ok, _, err := pi.Get(g, k)
		require.NoError(t, err, "key=%x", k)
		require.True(t, ok, "key=%x should be found", k)
		require.NotEmpty(t, v)
	}
}

func TestPrefixIndex_SeekBetweenBuckets(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, g := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	// Find an empty prefix bucket between two non-empty ones.
	emptyPrefix := -1
	for prefix := 1; prefix < 65535; prefix++ {
		if pi.buckets[prefix].firstDI != math.MaxUint64 {
			continue
		}
		hasBefore, hasAfter := false, false
		for p := prefix - 1; p >= 0; p-- {
			if pi.buckets[p].firstDI != math.MaxUint64 {
				hasBefore = true
				break
			}
		}
		for p := prefix + 1; p < 65536; p++ {
			if pi.buckets[p].firstDI != math.MaxUint64 {
				hasAfter = true
				break
			}
		}
		if hasBefore && hasAfter {
			emptyPrefix = prefix
			break
		}
	}
	require.NotEqual(t, -1, emptyPrefix, "should find an empty bucket between non-empty ones")

	// Craft a key whose prefix falls in the empty bucket.
	betweenKey := make([]byte, 10)
	betweenKey[0] = byte(emptyPrefix >> 8)
	betweenKey[1] = byte(emptyPrefix & 0xff)

	c, err := pi.Seek(g, betweenKey)
	require.NoError(t, err)
	if c != nil {
		require.True(t, bytes.Compare(c.Key(), betweenKey) >= 0,
			"seek(%x) returned key %x which is less", betweenKey, c.Key())
		// Verify against sorted keys.
		idx := sort.Search(len(keys), func(i int) bool {
			return bytes.Compare(keys[i], betweenKey) >= 0
		})
		require.Less(t, idx, len(keys))
		require.Equal(t, keys[idx], c.Key())
		c.Close()
	}
}

func TestPrefixIndex_CompareWithBpsTree(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 1000
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	// Build PrefixIndex.
	decomp1, pi, efi, g1 := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp1.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	// Build BpsTree from the same data.
	decomp2, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	defer decomp2.Close()
	g2 := seg.NewReader(decomp2.MakeGetter(), compressFlags)
	ir2 := NewMockIndexReader(efi)
	bp := NewBpsTree(g2, efi, DefaultBtreeM, ir2.dataLookup, ir2.keyCmp)
	bp.cursorGetter = ir2.newCursor

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	// Compare Get for all actual keys.
	for i, k := range keys {
		v1, ok1, _, err1 := pi.Get(g1, k)
		v2, ok2, _, err2 := bp.Get(g2, k)
		require.NoError(t, err1, "pi.Get i=%d", i)
		require.NoError(t, err2, "bp.Get i=%d", i)
		require.Equal(t, ok2, ok1, "found mismatch i=%d key=%x", i, k)
		if ok1 {
			require.Equal(t, v2, v1, "value mismatch i=%d key=%x", i, k)
		}
	}

	// Compare Seek for all actual keys.
	for i, k := range keys {
		c1, err1 := pi.Seek(g1, k)
		c2, err2 := bp.Seek(g2, k)
		require.NoError(t, err1, "pi.Seek i=%d", i)
		require.NoError(t, err2, "bp.Seek i=%d", i)
		if c1 == nil && c2 == nil {
			continue
		}
		require.NotNilf(t, c1, "pi nil but bp non-nil i=%d key=%x", i, k)
		require.NotNilf(t, c2, "bp nil but pi non-nil i=%d key=%x", i, k)
		require.Equalf(t, c2.Key(), c1.Key(), "seek key mismatch i=%d key=%x", i, k)
		c1.Close()
		c2.Close()
	}

	// Compare 1000 random keys (mix of existing and non-existing).
	// Note: BpsTree has the same last-bucket boundary bug (F3) that we fixed in PrefixIndex.
	// When BpsTree returns ErrBtIndexLookupBounds, PrefixIndex should return (nil, false) — not an error.
	rnd := newRnd(42)
	for i := 0; i < 1000; i++ {
		var testKey []byte
		if rnd.IntN(2) == 0 && len(keys) > 0 {
			testKey = keys[rnd.IntN(len(keys))]
		} else {
			testKey = make([]byte, 10)
			rnd.Read(testKey)
		}

		v1, ok1, _, err1 := pi.Get(g1, testKey)
		v2, ok2, _, err2 := bp.Get(g2, testKey)
		require.NoError(t, err1, "pi.Get random i=%d key=%x", i, testKey)
		if err2 != nil {
			// BpsTree hit the known last-bucket bounds bug; PrefixIndex should return not-found.
			require.Truef(t, errors.Is(err2, ErrBtIndexLookupBounds),
				"unexpected bp.Get error random i=%d key=%x: %v", i, testKey, err2)
			require.False(t, ok1, "pi should return not-found when bp hits bounds error i=%d key=%x", i, testKey)
			continue
		}
		require.Equalf(t, ok2, ok1, "found mismatch random i=%d key=%x", i, testKey)
		if ok1 {
			require.Equalf(t, v2, v1, "value mismatch random i=%d key=%x", i, testKey)
		}

		c1, err1 := pi.Seek(g1, testKey)
		c2, err2 := bp.Seek(g2, testKey)
		require.NoError(t, err1, "pi.Seek random i=%d key=%x", i, testKey)
		if err2 != nil {
			if c1 != nil {
				c1.Close()
			}
			continue // BpsTree bounds error; PrefixIndex should handle gracefully.
		}
		if c1 == nil && c2 == nil {
			continue
		}
		require.NotNilf(t, c1, "pi nil but bp non-nil random i=%d key=%x", i, testKey)
		require.NotNilf(t, c2, "bp nil but pi non-nil random i=%d key=%x", i, testKey)
		require.Equalf(t, c2.Key(), c1.Key(), "seek key mismatch random i=%d key=%x", i, testKey)
		c1.Close()
		c2.Close()
	}
}

func TestPrefixIndex_ConcurrentReads(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	logger := log.New()

	keyCount := 500
	compressFlags := seg.CompressNone
	kvPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlags)

	decomp, pi, _, _ := buildTestPrefixIndex(t, kvPath, compressFlags)
	defer decomp.Close()
	require.NotNil(t, pi)
	defer pi.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)

	const goroutines = 8
	const opsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for w := 0; w < goroutines; w++ {
		go func(id int) {
			defer wg.Done()
			g := seg.NewReader(decomp.MakeGetter(), compressFlags)
			rnd := newRnd(uint64(id))

			for i := 0; i < opsPerGoroutine; i++ {
				k := keys[rnd.IntN(len(keys))]
				if i%2 == 0 {
					c, err := pi.Seek(g, k)
					require.NoError(t, err)
					require.NotNil(t, c)
					require.Equal(t, k, c.Key())
					c.Close()
				} else {
					v, ok, _, err := pi.Get(g, k)
					require.NoError(t, err)
					require.True(t, ok)
					require.NotEmpty(t, v)
				}
			}
		}(w)
	}
	wg.Wait()
}

