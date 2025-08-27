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
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/background"
	"github.com/erigontech/erigon-lib/log/v3"
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
		t.Skip()
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
	t.Skip("issue #15028")

	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 1_200_000, 1024

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
		}
	})

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
