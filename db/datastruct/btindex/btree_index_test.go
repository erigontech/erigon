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

package btindex

import (
	"bytes"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

func Test_BtreeIndex_Init(t *testing.T) {
	t.Parallel()

	logger := log.New()
	tmp := t.TempDir()

	keyCount, M := 100, uint64(4)
	compPath := generateKV(t, tmp, 52, 300, keyCount, logger, 0)
	decomp, err := seg.NewDecompressor(compPath)
	require.NoError(t, err)
	defer decomp.Close()

	r := seg.NewReader(decomp.MakeGetter(), seg.CompressNone)
	err = BuildBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), r, background.NewProgressSet(), tmp, 1, logger, true, statecfg.AccessorBTree|statecfg.AccessorExistence)
	require.NoError(t, err)

	bt, err := OpenBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), M, r)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	bt.Close()
}

func Test_BtreeIndex_Seek(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 120, 30
	compressFlags := seg.CompressKeys | seg.CompressVals

	t.Run("empty index", func(t *testing.T) {
		dataPath := generateKV(t, tmp, 52, 180, 0, logger, 0)
		indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
		buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
		require.NoError(t, err)
		require.EqualValues(t, 0, bt.KeyCount())
		bt.Close()
		kv.Close()
	})
	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, _, err := bt.dataLookup(bt.ef.Count()+1, getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, _, err = bt.dataLookup(bt.ef.Count(), getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, _, err = bt.dataLookup(bt.ef.Count()-1, getter)
		require.NoError(t, err)

		cur, err := bt.Seek(getter, common.FromHex("0xffffffffffffff")) //seek beyeon the last key
		require.NoError(t, err)
		require.Nil(t, cur)
		cur.Close()
	})

	c, err := bt.Seek(getter, nil)
	require.NoError(t, err)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		require.Equal(t, keys[i], k)
		c.Next()
	}
	c.Close()

	for i := 0; i < len(keys); i++ {
		cur, err := bt.Seek(getter, keys[i])
		require.NoErrorf(t, err, "i=%d", i)
		require.Equalf(t, keys[i], cur.key, "i=%d", i)
		require.NotEmptyf(t, cur.Value(), "i=%d", i)
		cur.Close()
		// require.EqualValues(t, uint64(i), cur.Value())
	}
	for i := 1; i < len(keys); i++ {
		alt := common.Copy(keys[i])
		for j := len(alt) - 1; j >= 0; j-- {
			if alt[j] > 0 {
				alt[j] -= 1
				break
			}
		}
		cur, err := bt.Seek(getter, keys[i])
		require.NoError(t, err)
		require.Equal(t, keys[i], cur.Key())
		cur.Close()
	}
}

func Test_BtreeIndex_Build(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 20000, 510

	compressFlags := seg.CompressKeys | seg.CompressVals
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressFlags)
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)
	require.NoError(t, err)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)

	c, err := bt.Seek(getter, nil)
	require.NoError(t, err)
	require.NotNil(t, c)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		if !bytes.Equal(keys[i], k) {
			fmt.Printf("\tinvalid, want %x\n", keys[i])
		}
		c.Next()
	}
	c.Close()

	for i := 0; i < 10000; i++ {
		c, err := bt.Seek(getter, keys[i])
		require.NoError(t, err)
		require.Equal(t, keys[i], c.Key())
		c.Close()
	}
}

// Opens .kv at dataPath and generates index over it to file 'indexPath'
func buildBtreeIndex(tb testing.TB, dataPath, indexPath string, compressed seg.FileCompression, seed uint32, logger log.Logger, noFsync bool) {
	tb.Helper()
	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()

	r := seg.NewReader(decomp.MakeGetter(), compressed)
	err = BuildBtreeIndexWithDecompressor(indexPath, r, background.NewProgressSet(), filepath.Dir(indexPath), seed, logger, noFsync, statecfg.AccessorBTree|statecfg.AccessorExistence)
	require.NoError(tb, err)
}

func Test_BtreeIndex_Seek2(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	M := 1024
	keyCount := 3 * M

	compressFlags := seg.CompressKeys | seg.CompressVals
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressFlags)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	getter := seg.NewReader(kv.MakeGetter(), compressFlags)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, _, err := bt.dataLookup(bt.ef.Count()+1, getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, _, err = bt.dataLookup(bt.ef.Count(), getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, _, err = bt.dataLookup(bt.ef.Count()-1, getter)
		require.NoError(t, err)

		cur, err := bt.Seek(getter, common.FromHex("0xffffffffffffff")) //seek beyeon the last key
		require.NoError(t, err)
		require.Nil(t, cur)
		cur.Close()
	})

	t.Run("checkNextAgainstGetter", func(t *testing.T) {
		cur, err := bt.Seek(getter, nil)
		require.NoError(t, err)
		defer cur.Close()

		require.NoError(t, err)
		require.Equal(t, keys[0], cur.Key())
		require.NotEmptyf(t, cur.Value(), "i=%d", 0)

		k, v, _, err := bt.dataLookup(0, getter)
		require.NoError(t, err)
		cur.Reset(0, getter)

		require.Equal(t, k, cur.Key())
		require.Equal(t, v, cur.Value())

		totalKeys := kv.Count() / 2

		for i := 1; i < totalKeys; i++ {
			k, v, _, err = bt.dataLookup(uint64(i), getter)
			require.NoError(t, err)

			b := cur.Next()
			require.True(t, b)

			require.Equalf(t, k, cur.Key(), "i=%d", i)
			require.Equalf(t, v, cur.Value(), "i=%d", i)

			curS, err := bt.Seek(getter, cur.Key())
			require.NoError(t, err)

			require.Equalf(t, cur.Key(), curS.Key(), "i=%d", i)
			require.Equalf(t, cur.Value(), curS.Value(), "i=%d", i)
			require.Equal(t, cur.d, curS.d)
			require.Equal(t, cur.getter, curS.getter)
			curS.Close()
		}
	})

	for i := 1; i < len(keys); i++ {
		cur, err := bt.Seek(getter, keys[i])
		require.NoError(t, err)
		require.Equal(t, keys[i], cur.Key())
		cur.Close()
	}
}

func TestBpsTree_Seek(t *testing.T) {
	t.Parallel()

	keyCount, M := 48, 4
	tmp := t.TempDir()

	logger := log.New()

	compressFlag := seg.CompressNone
	dataPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlag)

	kv, err := seg.NewDecompressor(dataPath)
	require.NoError(t, err)
	defer kv.Close()

	g := seg.NewReader(kv.MakeGetter(), compressFlag)

	g.Reset(0)
	ps := make([]uint64, 0, keyCount)
	keys := make([][]byte, 0, keyCount)

	p := uint64(0)
	i := 0
	for g.HasNext() {
		ps = append(ps, p)
		k, _ := g.Next(nil)
		_, p = g.Next(nil)
		keys = append(keys, k)
		//fmt.Printf("%2d k=%x, p=%v\n", i, k, p)
		i++
	}

	//tr := newTrie()
	ef := eliasfano32.NewEliasFano(uint64(keyCount), ps[len(ps)-1])
	for i := 0; i < len(ps); i++ {
		//tr.insert(Node{i: uint64(i), key: common.Copy(keys[i]), off: ps[i]})
		ef.AddOffset(ps[i])
	}
	ef.Build()

	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))

	ir := NewMockIndexReader(efi)
	bp := NewBpsTree(g, efi, uint64(M), ir.dataLookup, ir.keyCmp)
	bp.cursorGetter = ir.newCursor
	bp.trace = false

	for i := 0; i < len(keys); i++ {
		sk := keys[i]
		c, err := bp.Seek(g, sk[:len(sk)/2])
		require.NoError(t, err)
		require.NotNil(t, c)
		require.NotNil(t, c.Key())

		//k, _, err := it.KVFromGetter(g)
		//require.NoError(t, err)
		require.Equal(t, keys[i], c.Key())
		c.Close()
	}
}

func NewMockIndexReader(ef *eliasfano32.EliasFano) *mockIndexReader {
	return &mockIndexReader{ef: ef}
}

type mockIndexReader struct {
	ef *eliasfano32.EliasFano
}

func (b *mockIndexReader) newCursor(k, v []byte, di uint64, g *seg.Reader) *Cursor {
	return &Cursor{
		ef:     b.ef,
		getter: g,
		key:    common.Copy(k),
		value:  common.Copy(v),
		d:      di,
	}
}

func (b *mockIndexReader) dataLookup(di uint64, g *seg.Reader) (k, v []byte, offset uint64, err error) {
	if di >= b.ef.Count() {
		return nil, nil, 0, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di, g.FileName())
	}

	offset = b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d key not found, file: %s", di, b.ef.Count(), g.FileName())
	}

	k, _ = g.Next(nil)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d value not found, file: %s", di, b.ef.Count(), g.FileName())
	}
	v, _ = g.Next(nil)
	return k, v, offset, nil
}

// comparing `k` with item of index `di`. using buffer `kBuf` to avoid allocations
func (b *mockIndexReader) keyCmp(k []byte, di uint64, g *seg.Reader, resBuf []byte) (int, []byte, error) {
	if di >= b.ef.Count() {
		return 0, resBuf, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di+1, g.FileName())
	}

	offset := b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return 0, resBuf, fmt.Errorf("key at %d/%d not found, file: %s", di, b.ef.Count(), g.FileName())
	}

	resBuf, _ = g.Next(resBuf)

	//TODO: use `b.getter.Match` after https://github.com/erigontech/erigon/issues/7855
	return bytes.Compare(resBuf, k), resBuf, nil
	//return b.getter.Match(k), result, nil
}

func TestNewBtIndex(t *testing.T) {
	t.Parallel()

	t.Run("small file uses mx", func(t *testing.T) {
		t.Parallel()
		keyCount := 10000
		kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, log.New(), seg.CompressNone)

		indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"

		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, seg.CompressNone, false)
		require.NoError(t, err)
		defer bt.Close()
		defer kv.Close()
		require.NotNil(t, kv)
		require.NotNil(t, bt)
		bplus := bt.bplus
		require.Nil(t, bplus.prefix, "small files should not have prefix index")
		require.GreaterOrEqual(t, len(bplus.mx), keyCount/int(DefaultBtreeM))

		for i := 1; i < len(bt.bplus.mx); i++ {
			require.NotZero(t, bt.bplus.mx[i].di)
			require.NotZero(t, bt.bplus.mx[i].off)
			require.NotEmpty(t, bt.bplus.mx[i].key)
		}
	})

	t.Run("large file uses prefix buckets", func(t *testing.T) {
		if testing.Short() {
			t.Skip("slow test")
		}
		t.Parallel()
		keyCount := 65_000 // above minKeysForPrefixIndex threshold
		kvPath := generateKV(t, t.TempDir(), 20, 10, keyCount, log.New(), seg.CompressNone)

		indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"

		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, DefaultBtreeM, seg.CompressNone, false)
		require.NoError(t, err)
		defer bt.Close()
		defer kv.Close()
		require.NotNil(t, kv)
		require.NotNil(t, bt)
		bplus := bt.bplus
		require.NotNil(t, bplus.prefix, "large files should have prefix index")
		require.Empty(t, bplus.mx, "large files should not populate mx")

		// Verify prefix buckets contain nodes
		totalNodes := 0
		nonEmptyBuckets := 0
		for i := range bplus.prefix.buckets {
			b := &bplus.prefix.buckets[i]
			if b.firstDI != math.MaxUint64 {
				nonEmptyBuckets++
				require.Less(t, b.firstDI, b.endDI, "bucket %d: firstDI must be < endDI", i)
			}
			totalNodes += len(b.nodes)
			require.LessOrEqual(t, len(b.nodes), maxNodesPerBucket, "bucket %d exceeds max nodes", i)
		}
		require.Greater(t, nonEmptyBuckets, 0, "should have non-empty buckets")
		require.Greater(t, totalNodes, 0, "should have cached nodes in buckets")

		// Verify L1 tables are populated
		hasL1 := false
		for i := range bplus.prefix.l1First {
			if bplus.prefix.l1First[i] != math.MaxUint64 {
				hasL1 = true
				require.Less(t, bplus.prefix.l1First[i], bplus.prefix.l1End[i],
					"L1[%d]: l1First must be < l1End", i)
			}
		}
		require.True(t, hasL1, "should have non-empty L1 entries")
	})
}

func TestPrefixIndexLookup(t *testing.T) {
	t.Parallel()

	p := newPrefixIndex()
	// Record keys with known prefixes and di values
	// Prefix 0x01xx starts at di=100
	p.record([]byte{0x01, 0x00, 0xaa}, 100)
	p.record([]byte{0x01, 0x10, 0xbb}, 200)
	p.record([]byte{0x01, 0x20, 0xcc}, 300)
	// Prefix 0x02xx starts at di=500
	p.record([]byte{0x02, 0xab, 0xdd}, 500)
	p.record([]byte{0x02, 0xac, 0xee}, 600)
	// Prefix 0x05xx starts at di=1000
	p.record([]byte{0x05, 0xff, 0x11}, 1000)

	p.computeL1()
	totalCount := uint64(2000)

	// Test L2 lookup: key 0x02ab has only di=500, so range is [500, 501)
	l, r := p.lookup([]byte{0x02, 0xab, 0x00}, totalCount)
	require.Equal(t, uint64(500), l, "L2 left bound")
	require.Equal(t, uint64(501), r, "L2 right bound (maxDi+1)")

	// Test L2 lookup: key 0x02ac has only di=600, so range is [600, 601)
	l, r = p.lookup([]byte{0x02, 0xac, 0x00}, totalCount)
	require.Equal(t, uint64(600), l, "L2 left bound for 0x02ac")
	require.Equal(t, uint64(601), r, "L2 right bound for 0x02ac (maxDi+1)")

	// Test L1 lookup: key with only 1 byte 0x02 should use L1 bounds [500, 601)
	l, r = p.lookup([]byte{0x02}, totalCount)
	require.Equal(t, uint64(500), l, "L1 left bound")
	require.Equal(t, uint64(601), r, "L1 right bound (maxDi+1)")

	// Test L1 lookup: last prefix 0x05 has only di=1000, range is [1000, 1001)
	l, r = p.lookup([]byte{0x05, 0xff}, totalCount)
	require.Equal(t, uint64(1000), l, "last prefix left bound")
	require.Equal(t, uint64(1001), r, "last prefix right bound (maxDi+1)")

	// Test non-existent prefix: 0x03 has no keys
	l, r = p.lookup([]byte{0x03, 0x00}, totalCount)
	require.Equal(t, uint64(0), l, "non-existent prefix should return 0")
	require.Equal(t, uint64(0), r, "non-existent prefix should return 0")

	// Test empty key: no narrowing
	l, r = p.lookup([]byte{}, totalCount)
	require.Equal(t, uint64(0), l)
	require.Equal(t, totalCount, r)

	// Test L2 with non-existent second byte: 0x01,0x05 doesn't exist, should fall back to L1 bounds
	l, r = p.lookup([]byte{0x01, 0x05}, totalCount)
	require.Equal(t, uint64(100), l, "should use L1 left when L2 entry missing")
	require.Equal(t, uint64(301), r, "should use L1 right when L2 entry missing (maxDi+1)")

	// Test record with duplicate lower di: should keep minimum
	p.record([]byte{0x02, 0xab, 0x00}, 450)
	l, _ = p.lookup([]byte{0x02, 0xab, 0x00}, totalCount)
	require.Equal(t, uint64(450), l, "record should keep minimum di")
}

func TestPrefixIndexNarrowWithNodes(t *testing.T) {
	t.Parallel()

	p := newPrefixIndex()

	// Set up bucket 0x0200 with range [100, 200)
	p.buckets[0x0200].firstDI = 100
	p.buckets[0x0200].endDI = 200

	// Add cached nodes (must be sorted by key)
	p.buckets[0x0200].nodes = []Node{
		{key: []byte{0x02, 0x00, 0x10}, di: 110},
		{key: []byte{0x02, 0x00, 0x30}, di: 130},
		{key: []byte{0x02, 0x00, 0x50}, di: 150},
		{key: []byte{0x02, 0x00, 0x70}, di: 170},
	}

	// Test exact match
	l, r, exactDI, found := p.narrowWithNodes([]byte{0x02, 0x00, 0x30})
	require.True(t, found, "should find exact match")
	require.Equal(t, uint64(130), exactDI)
	_ = l
	_ = r

	// Test narrowing: key between nodes[1] and nodes[2]
	l, r, _, found = p.narrowWithNodes([]byte{0x02, 0x00, 0x40})
	require.False(t, found, "should not find exact match")
	require.Equal(t, uint64(131), l, "left should be di of node[1]+1")
	require.Equal(t, uint64(150), r, "right should be di of node[2]")

	// Test narrowing: key before all nodes
	l, r, _, found = p.narrowWithNodes([]byte{0x02, 0x00, 0x05})
	require.False(t, found)
	require.Equal(t, uint64(100), l, "left should be bucket firstDI")
	require.Equal(t, uint64(110), r, "right should be di of first node")

	// Test narrowing: key after all nodes
	l, r, _, found = p.narrowWithNodes([]byte{0x02, 0x00, 0x80})
	require.False(t, found)
	require.Equal(t, uint64(171), l, "left should be di of last node + 1")
	require.Equal(t, uint64(200), r, "right should be bucket endDI")

	// Test bucket with no nodes: returns (0, 0, 0, false)
	p.buckets[0x0300].firstDI = 500
	p.buckets[0x0300].endDI = 600
	l, r, _, found = p.narrowWithNodes([]byte{0x03, 0x00, 0x10})
	require.False(t, found)
	require.Equal(t, uint64(0), l)
	require.Equal(t, uint64(0), r)

	// Test short key: returns (0, 0, 0, false)
	l, r, _, found = p.narrowWithNodes([]byte{0x02})
	require.False(t, found)
	require.Equal(t, uint64(0), l)
	require.Equal(t, uint64(0), r)
}

func TestPrefixIndexComputeL1(t *testing.T) {
	t.Parallel()

	p := newPrefixIndex()

	// Set up buckets for first byte 0x01
	p.buckets[0x0100].firstDI = 50
	p.buckets[0x0100].endDI = 60
	p.buckets[0x010A].firstDI = 100
	p.buckets[0x010A].endDI = 200
	p.buckets[0x01FF].firstDI = 300
	p.buckets[0x01FF].endDI = 350

	// Set up buckets for first byte 0x02
	p.buckets[0x0200].firstDI = 400
	p.buckets[0x0200].endDI = 500

	// First byte 0x03 has no buckets (all sentinel)

	p.computeL1()

	// L1 for 0x01: min firstDI = 50, max endDI = 350
	require.Equal(t, uint64(50), p.l1First[0x01])
	require.Equal(t, uint64(350), p.l1End[0x01])

	// L1 for 0x02: single bucket
	require.Equal(t, uint64(400), p.l1First[0x02])
	require.Equal(t, uint64(500), p.l1End[0x02])

	// L1 for 0x03: all sentinel (no buckets)
	require.Equal(t, uint64(math.MaxUint64), p.l1First[0x03])
	require.Equal(t, uint64(0), p.l1End[0x03])

	// Verify lookup uses computed L1
	l, r := p.lookup([]byte{0x01}, 1000)
	require.Equal(t, uint64(50), l)
	require.Equal(t, uint64(350), r)

	l, r = p.lookup([]byte{0x03}, 1000)
	require.Equal(t, uint64(0), l, "empty L1 prefix returns (0,0)")
	require.Equal(t, uint64(0), r, "empty L1 prefix returns (0,0)")
}

func TestPrefixIndexAddNode(t *testing.T) {
	t.Parallel()

	p := newPrefixIndex()

	// Add nodes to a bucket
	for i := 0; i < maxNodesPerBucket+3; i++ {
		p.addNode([]byte{0x01, 0x00, byte(i)}, Node{
			key: []byte{0x01, 0x00, byte(i)},
			di:  uint64(i * 10),
		})
	}

	// Should cap at maxNodesPerBucket
	require.Equal(t, maxNodesPerBucket, len(p.buckets[0x0100].nodes))

	// Short key should be ignored
	p.addNode([]byte{0x01}, Node{key: []byte{0x01}, di: 999})
	require.Equal(t, maxNodesPerBucket, len(p.buckets[0x0100].nodes))
}

func TestWarmUpLargeFilePopulatesPrefixBuckets(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount := 65_000 // above minKeysForPrefixIndex (60_000)
	M := uint64(256)

	compressFlag := seg.CompressNone
	dataPath := generateKV(t, tmp, 10, 16, keyCount, logger, compressFlag)

	kv, err := seg.NewDecompressor(dataPath)
	require.NoError(t, err)
	defer kv.Close()

	g := seg.NewReader(kv.MakeGetter(), compressFlag)

	// Read all keys and offsets
	g.Reset(0)
	ps := make([]uint64, 0, keyCount)
	keys := make([][]byte, 0, keyCount)
	p := uint64(0)
	for g.HasNext() {
		ps = append(ps, p)
		k, _ := g.Next(nil)
		_, p = g.Next(nil)
		keys = append(keys, common.Copy(k))
	}
	require.Equal(t, keyCount, len(keys))

	ef := eliasfano32.NewEliasFano(uint64(keyCount), ps[len(ps)-1])
	for i := 0; i < len(ps); i++ {
		ef.AddOffset(ps[i])
	}
	ef.Build()
	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))

	ir := NewMockIndexReader(efi)
	bp := NewBpsTree(g, efi, M, ir.dataLookup, ir.keyCmp)

	// Prefix index should be active
	require.NotNil(t, bp.prefix, "prefix index should be built for large files")
	// mx should NOT be populated for large files
	require.Empty(t, bp.mx, "mx should not be populated when prefix index is active")

	// Spot-check: at least some buckets should have non-sentinel firstDI
	nonEmptyBuckets := 0
	totalNodes := 0
	for i := range bp.prefix.buckets {
		if bp.prefix.buckets[i].firstDI != math.MaxUint64 {
			nonEmptyBuckets++
			require.Less(t, bp.prefix.buckets[i].firstDI, bp.prefix.buckets[i].endDI,
				"firstDI must be < endDI for non-empty bucket %d", i)
			totalNodes += len(bp.prefix.buckets[i].nodes)
			require.LessOrEqual(t, len(bp.prefix.buckets[i].nodes), maxNodesPerBucket,
				"bucket %d has too many nodes", i)
		}
	}
	require.Greater(t, nonEmptyBuckets, 0, "should have non-empty buckets")
	require.Greater(t, totalNodes, 0, "should have cached nodes in buckets")

	// Verify L1 aggregation: at least some L1 entries should be set
	nonEmptyL1 := 0
	for i := range bp.prefix.l1First {
		if bp.prefix.l1First[i] != math.MaxUint64 {
			nonEmptyL1++
			require.Less(t, bp.prefix.l1First[i], bp.prefix.l1End[i],
				"l1First must be < l1End for non-empty L1 entry %d", i)
		}
	}
	require.Greater(t, nonEmptyL1, 0, "should have non-empty L1 entries")

	// Verify a specific bucket: find the bucket for keys[0] and check it contains di=0
	if len(keys[0]) >= 2 {
		prefix := uint16(keys[0][0])<<8 | uint16(keys[0][1])
		bkt := &bp.prefix.buckets[prefix]
		require.Equal(t, uint64(0), bkt.firstDI, "first key should set firstDI=0")
	}
}

func TestNewBpsTreeWithNodesPrefixIndex(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount := 65_000 // above minKeysForPrefixIndex (60_000)
	M := uint64(256)

	compressFlag := seg.CompressNone
	dataPath := generateKV(t, tmp, 10, 16, keyCount, logger, compressFlag)

	kv, err := seg.NewDecompressor(dataPath)
	require.NoError(t, err)
	defer kv.Close()

	g := seg.NewReader(kv.MakeGetter(), compressFlag)

	// Read all keys and offsets
	g.Reset(0)
	ps := make([]uint64, 0, keyCount)
	keys := make([][]byte, 0, keyCount)
	p := uint64(0)
	for g.HasNext() {
		ps = append(ps, p)
		k, _ := g.Next(nil)
		_, p = g.Next(nil)
		keys = append(keys, common.Copy(k))
	}
	require.Equal(t, keyCount, len(keys))

	ef := eliasfano32.NewEliasFano(uint64(keyCount), ps[len(ps)-1])
	for i := 0; i < len(ps); i++ {
		ef.AddOffset(ps[i])
	}
	ef.Build()
	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))

	// Build nodes every M-th key (same sampling as WarmUp)
	var nodes []Node
	step := M
	for i := step; i < uint64(keyCount); i += step {
		di := i - 1
		nodes = append(nodes, Node{key: common.Copy(keys[di]), di: di})
	}

	ir := NewMockIndexReader(efi)
	bp := NewBpsTreeWithNodes(g, efi, M, ir.dataLookup, ir.keyCmp, nodes)

	// Prefix index should be active
	require.NotNil(t, bp.prefix, "prefix index should be built for large files")
	// mx should NOT be populated for large files
	require.Empty(t, bp.mx, "mx should not be populated when prefix index is active")

	// Spot-check: at least some buckets should have non-sentinel firstDI
	nonEmptyBuckets := 0
	totalNodes := 0
	for i := range bp.prefix.buckets {
		if bp.prefix.buckets[i].firstDI != math.MaxUint64 {
			nonEmptyBuckets++
			require.Less(t, bp.prefix.buckets[i].firstDI, bp.prefix.buckets[i].endDI,
				"firstDI must be < endDI for non-empty bucket %d", i)
			totalNodes += len(bp.prefix.buckets[i].nodes)
			require.LessOrEqual(t, len(bp.prefix.buckets[i].nodes), maxNodesPerBucket,
				"bucket %d has too many nodes", i)
		}
	}
	require.Greater(t, nonEmptyBuckets, 0, "should have non-empty buckets")
	require.Greater(t, totalNodes, 0, "should have cached nodes in buckets")

	// Verify L1 aggregation
	nonEmptyL1 := 0
	for i := range bp.prefix.l1First {
		if bp.prefix.l1First[i] != math.MaxUint64 {
			nonEmptyL1++
			require.Less(t, bp.prefix.l1First[i], bp.prefix.l1End[i],
				"l1First must be < l1End for non-empty L1 entry %d", i)
		}
	}
	require.Greater(t, nonEmptyL1, 0, "should have non-empty L1 entries")

	// Verify nodes ended up in correct buckets: check a sampled node
	for _, n := range nodes {
		if len(n.key) >= 2 {
			prefix := uint16(n.key[0])<<8 | uint16(n.key[1])
			bkt := &bp.prefix.buckets[prefix]
			found := false
			for _, bn := range bkt.nodes {
				if bn.di == n.di {
					found = true
					break
				}
			}
			// Node should be in its prefix bucket (unless bucket was full)
			if len(bkt.nodes) < maxNodesPerBucket || found {
				require.True(t, found || len(bkt.nodes) == maxNodesPerBucket,
					"node di=%d should be in bucket %d or bucket should be full", n.di, prefix)
			}
			break // just check one
		}
	}
}

func BenchmarkBtIndex_Get(b *testing.B) {
	keyCount := 1_000_000
	if testing.Short() {
		keyCount = 10_000
	}
	compress := seg.CompressKeys

	for _, M := range []uint64{256, 128, 64, 32} {
		kvPath := generateKV(b, b.TempDir(), 20, 10, keyCount, log.New(), compress)
		keys, err := pivotKeysFromKV(kvPath)
		require.NoError(b, err)

		indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"

		b.Run(fmt.Sprintf("M%d", M), func(b *testing.B) {
			decomp, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, M, compress, false)
			require.NoError(b, err)
			defer bt.Close()
			defer decomp.Close()

			getter := seg.NewReader(decomp.MakeGetter(), compress)
			rnd := newRnd(uint64(b.N))

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				p := rnd.IntN(len(keys))
				k, _, _, found, err := bt.Get(keys[p], getter)
				if err != nil {
					b.Fatal(err)
				}
				if !found || !bytes.Equal(keys[p], k) {
					b.Fatal("key not found or mismatch")
				}
			}
		})
	}
}
