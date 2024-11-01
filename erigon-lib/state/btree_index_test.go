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

package state

import (
	"bytes"
	"fmt"
	"math/rand"
	"path"
	"path/filepath"
	"sort"
	"testing"

	bloomfilter "github.com/holiman/bloomfilter/v2"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
)

func Test_BtreeIndex_Init2(t *testing.T) {
	t.Parallel()

	//mainnet: storage.128-160.kv  110mil keys, 100mb bloomfilter of 0.01 (1%) miss-probability
	//no much reason to merge bloomfilter - can merge them on startup
	//1B keys: 1Gb

	sizes := []int{54, 74, 135, 139, 109, 105, 144}
	sum := 0
	sumB := 0
	for _, sz := range sizes {
		sum += sz
		sumB += int(bloomfilter.OptimalM(uint64(sz*1_000_000), 0.001))
	}
	large := bloomfilter.OptimalM(uint64(sum*1_000_000), 0.001)
	fmt.Printf("see: %d\n", bloomfilter.OptimalM(uint64(1_000_000_000), 0.001)/8/1024/1024)
	fmt.Printf("see: %d vs %d\n", sumB/8/1024/1024, large/8/1024/1024)

}
func Test_BtreeIndex_Init(t *testing.T) {
	t.Parallel()

	logger := log.New()
	tmp := t.TempDir()

	keyCount, M := 100, uint64(4)
	compPath := generateKV(t, tmp, 52, 300, keyCount, logger, 0)
	decomp, err := seg.NewDecompressor(compPath)
	require.NoError(t, err)
	defer decomp.Close()

	err = BuildBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), decomp, seg.CompressNone, background.NewProgressSet(), tmp, 1, logger, true)
	require.NoError(t, err)

	bt, err := OpenBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), M, decomp, seg.CompressKeys|seg.CompressVals)
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
	//UseBpsTree = true

	t.Run("empty index", func(t *testing.T) {
		dataPath := generateKV(t, tmp, 52, 180, 0, logger, 0)
		indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
		buildBtreeIndex(t, dataPath, indexPath, compressFlags, 1, logger, true)

		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressFlags, false)
		require.NoError(t, err)
		require.EqualValues(t, 0, bt.KeyCount())
		bt.Close()
		kv.Close()
	})
	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
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
	})

	c, err := bt.Seek(getter, nil)
	require.NoError(t, err)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		//if !bytes.Equal(keys[i], k) {
		//	fmt.Printf("\tinvalid, want %x, got %x\n", keys[i], k)
		//}
		require.EqualValues(t, keys[i], k)
		c.Next()
	}

	for i := 0; i < len(keys); i++ {
		cur, err := bt.Seek(getter, keys[i])
		require.NoErrorf(t, err, "i=%d", i)
		require.NotNilf(t, cur, "i=%d", i)
		require.EqualValuesf(t, keys[i], cur.key, "i=%d", i)
		require.NotEmptyf(t, cur.Value(), "i=%d", i)
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
		require.EqualValues(t, keys[i], cur.Key())
	}
}

func Test_BtreeIndex_Build(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 20000, 510

	compressFlags := seg.CompressKeys | seg.CompressVals
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressFlags)
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
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
	for i := 0; i < 10000; i++ {
		c, err := bt.Seek(getter, keys[i])
		require.NoError(t, err)
		require.EqualValuesf(t, keys[i], c.Key(), "i=%d", i)
	}
}

// Opens .kv at dataPath and generates index over it to file 'indexPath'
func buildBtreeIndex(tb testing.TB, dataPath, indexPath string, compressed seg.FileCompression, seed uint32, logger log.Logger, noFsync bool) {
	tb.Helper()
	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()

	err = BuildBtreeIndexWithDecompressor(indexPath, decomp, compressed, background.NewProgressSet(), filepath.Dir(indexPath), seed, logger, noFsync)
	require.NoError(tb, err)
}

func Test_BtreeIndex_Seek2(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 1_200_0, 1024

	compressFlags := seg.CompressKeys | seg.CompressVals
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressFlags)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
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
	})

	c, err := bt.Seek(getter, nil)
	require.NoError(t, err)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		if !bytes.Equal(keys[i], k) {
			fmt.Printf("\tinvalid, want %x\n", keys[i])
		}
		c.Next()
	}

	for i := 0; i < len(keys); i++ {
		cur, err := bt.Seek(getter, keys[i])
		require.NoErrorf(t, err, "i=%d", i)
		require.EqualValues(t, keys[i], cur.key)
		require.NotEmptyf(t, cur.Value(), "i=%d", i)
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
		require.EqualValues(t, keys[i], cur.Key())
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
	bp.trace = false

	for i := 0; i < len(keys); i++ {
		sk := keys[i]
		k, _, di, found, err := bp.Seek(g, sk[:len(sk)/2])
		_ = di
		_ = found
		require.NoError(t, err)
		require.NotNil(t, k)
		require.False(t, found) // we are looking up by half of key, while FOUND=true when exact match found.

		//k, _, err := it.KVFromGetter(g)
		//require.NoError(t, err)
		require.EqualValues(t, keys[i], k)
	}
}

func NewMockIndexReader(ef *eliasfano32.EliasFano) *mockIndexReader {
	return &mockIndexReader{ef: ef}
}

type mockIndexReader struct {
	ef *eliasfano32.EliasFano
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
		return 0, nil, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di+1, g.FileName())
	}

	offset := b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return 0, nil, fmt.Errorf("key at %d/%d not found, file: %s", di, b.ef.Count(), g.FileName())
	}

	resBuf, _ = g.Next(resBuf)

	//TODO: use `b.getter.Match` after https://github.com/erigontech/erigon/issues/7855
	return bytes.Compare(resBuf, k), resBuf, nil
	//return b.getter.Match(k), result, nil
}

func Test_Btrie_Insert(t *testing.T) {

	// keys := [][]byte{
	// 	{0xce, 0xad},
	// 	{0xde, 0xad},
	// 	{0xde, 1, 2, 3, 4, 0xbd},
	// 	{0xde, 1, 2, 3, 5, 0xbd},
	// 	{0xde, 1, 2, 4, 5, 0xbd},
	// 	{0xde, 1, 2, 0xbd},
	// }

	keys := make([][]byte, 0)
	rnd := rand.New(rand.NewSource(0))
	for i := 0; i < 400; i++ {
		b := make([]byte, 10)
		_, _ = rnd.Read(b)
		keys = append(keys, b)
	}

	bt := NewBtrie(uint64(len(keys)))

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) < 0
	})

	// bt.trace = true
	for i, key := range keys {
		// t.Logf("Inserting %x - %d", key, i)
		bt.Insert(key, uint64(i))
	}

	if bt.trace {
		bt.printRoot()
	}

	for ki, key := range keys {
		li, ri := bt.SeekLR(key)
		// fmt.Printf("%x -> [%d, %d]\n", key, li, ri)
		// require.Truef(t, found, "key=%x", key)
		require.EqualValuesf(t, ki, li, "key=%x", key)
		require.InDelta(t, ki, ri, 1, "key=%x", key)

		key[len(key)-1]++
		li, ri = bt.SeekLR(key)
		// require.True(t, found)
		require.Truef(t, li <= uint64(ki), "key=%x li=%d ki=%d", key, li, ki)
		require.Truef(t, ri >= uint64(ki), "key=%x li=%d ri=%d ki=%d", key, li, ri, ki)
		// require.GreaterOrEqual(t, li, uint64(ki))
		// require.GreaterOrEqual(t, ri, uint64(ki))
	}

	// keysNotExist := [][]byte{
	// 	// []byte{0xde, 0xad},
	// 	// []byte{0xce, 0xad},
	// 	{0xfe, 0xad},
	// 	{0xde, 0, 0, 0, 0, 0xbd},
	// }
	// for _, key := range keysNotExist {
	// 	found, li, ri := bt.SeekLR(key)
	// 	// require.False(t, found)
	// 	// require.EqualValues(t, 0, li)
	// 	// require.EqualValues(t, 0, ri)

	// 	fmt.Printf("%x -> %v [%d, %d]\n", key, found, li, ri)
	// }
}
