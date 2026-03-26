package btindex

import (
	"bytes"
	"sort"
	"testing"
	"strings"
	"math"

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

