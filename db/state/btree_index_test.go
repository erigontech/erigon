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
	"github.com/erigontech/erigon-lib/recsplit/eliasfano32"
	"github.com/erigontech/erigon-lib/seg"
)

func Test_BtreeIndex_Init(t *testing.T) {
	t.Parallel()

	logger := log.New()
	tmp := t.TempDir()

	keyCount, M := 100, uint64(4)
	comp := seg.Cfg{WordLvl: seg.CompressNone, WordLvlCfg: seg.DefaultWordLvlCfg}
	compPath := generateKV(t, tmp, 52, 300, keyCount, logger, comp)
	decomp, err := seg.NewDecompressor(compPath)
	require.NoError(t, err)
	defer decomp.Close()

	r := seg.NewPagedReader(seg.NewReader(decomp.MakeGetter(), comp.WordLvl), comp.PageLvl)
	err = BuildBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), r, background.NewProgressSet(), tmp, 1, logger, true, AccessorBTree|AccessorExistence)
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
	compressCfg := seg.Cfg{WordLvl: seg.CompressNone, WordLvlCfg: seg.DefaultWordLvlCfg} //, PageSize: 16, PageLvl: true}
	t.Run("empty index", func(t *testing.T) {
		dataPath := generateKV(t, tmp, 8, 16, 0, logger, compressCfg)
		indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
		buildBtreeIndex(t, dataPath, indexPath, compressCfg, 1, logger, true)

		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressCfg, false)
		require.NoError(t, err)
		require.EqualValues(t, 0, bt.KeyCount())
		bt.Close()
		kv.Close()
	})
	dataPath := generateKV(t, tmp, 8, 16, keyCount, logger, compressCfg)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressCfg, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressCfg, false)
	require.NoError(t, err)
	require.EqualValues(t, keyCount, int(bt.KeyCount()))
	defer bt.Close()
	defer kv.Close()

	getter := seg.NewPagedReader(seg.NewReader(kv.MakeGetter(), compressCfg.WordLvl), compressCfg.PageLvl)
	keys, err := pivotKeysFromKV(getter)
	require.NoError(t, err)

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
		require.Equal(t, fmt.Sprintf("%x", keys[i]), fmt.Sprintf("%x", k), i)
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

	compressCfg := seg.Cfg{WordLvl: seg.CompressKeys | seg.CompressVals, WordLvlCfg: seg.DefaultWordLvlCfg}
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressCfg)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressCfg, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressCfg, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	getter := seg.NewPagedReader(seg.NewReader(kv.MakeGetter(), compressCfg.WordLvl), compressCfg.PageLvl)
	keys, err := pivotKeysFromKV(getter)
	require.NoError(t, err)

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
func buildBtreeIndex(tb testing.TB, dataPath, indexPath string, compressCfg seg.Cfg, seed uint32, logger log.Logger, noFsync bool) {
	tb.Helper()
	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()

	r := seg.NewPagedReader(seg.NewReader(decomp.MakeGetter(), compressCfg.WordLvl), compressCfg.PageLvl)
	err = BuildBtreeIndexWithDecompressor(indexPath, r, background.NewProgressSet(), filepath.Dir(indexPath), seed, logger, noFsync, AccessorBTree|AccessorExistence)
	require.NoError(tb, err)
}

func TestBtree_Seek2(t *testing.T) {
	t.Skip("issue #15028")

	t.Parallel()

	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 1_200_000, 1024

	compressCfg := seg.Cfg{WordLvl: seg.CompressKeys | seg.CompressVals, WordLvlCfg: seg.DefaultWordLvlCfg}
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressCfg)

	indexPath := filepath.Join(tmp, filepath.Base(dataPath)+".bti")
	buildBtreeIndex(t, dataPath, indexPath, compressCfg, 1, logger, true)

	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, dataPath, uint64(M), compressCfg, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	defer bt.Close()
	defer kv.Close()

	getter := seg.NewPagedReader(seg.NewReader(kv.MakeGetter(), compressCfg.WordLvl), compressCfg.PageLvl)
	keys, err := pivotKeysFromKV(getter)
	require.NoError(t, err)

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
		cur.Reset(0, getter, nil)

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
	pageLvlOn := seg.PageLvlCfg{PageSize: 4, Compress: true}
	pageLvlOff := seg.PageLvlCfg{PageSize: 0, Compress: false}
	cases := []struct {
		name string
		c    seg.Cfg
	}{
		{"no_comp", seg.Cfg{WordLvl: seg.CompressNone, PageLvl: pageLvlOff}},
		{"only_word_lvl", seg.Cfg{WordLvl: seg.CompressKeys | seg.CompressVals, WordLvlCfg: seg.DefaultWordLvlCfg, PageLvl: pageLvlOff}},
		{"only_page_lvl", seg.Cfg{WordLvl: seg.CompressNone, WordLvlCfg: seg.DefaultWordLvlCfg, PageLvl: pageLvlOn}},
		{"both", seg.Cfg{WordLvl: seg.CompressKeys | seg.CompressVals, WordLvlCfg: seg.DefaultWordLvlCfg, PageLvl: pageLvlOn}},
	}
	for _, tc := range cases {
		compressCfg := tc.c
		t.Run(tc.name, func(t *testing.T) {
			dataPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressCfg)

			kv, err := seg.NewDecompressor(dataPath)
			require.NoError(t, err)
			defer kv.Close()

			g := seg.NewPagedReader(seg.NewReader(kv.MakeGetter(), compressCfg.WordLvl), compressCfg.PageLvl)
			//g.PrintPages()
			g.Reset(0)
			ps := make([]uint64, 0, keyCount)
			keys := make([][]byte, 0, keyCount)

			p := uint64(0)
			var k []byte
			for g.HasNext() {
				ps = append(ps, p)
				k, _, p = g.NextKey(nil)
				keys = append(keys, common.Copy(k))
			}

			ef := eliasfano32.NewEliasFano(uint64(keyCount), ps[len(ps)-1])
			for i := 0; i < len(ps); i++ {
				ef.AddOffset(ps[i])
			}
			ef.Build()

			efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))

			ir := NewMockIndexReader(efi)

			{
				//cmp, kk, _ := ir.keyCmp(nil, 1, g, nil)
				//require.Equal(t, -1, cmp)
				//if compressCfg.PageSize > 0 {
				//	require.Equal(t, fmt.Sprintf("%x", keys[compressCfg.PageSize-1]), fmt.Sprintf("%x", kk))
				//}
			}

			bp := NewBpsTree(g, efi, uint64(M), ir.dataLookup, ir.keyCmp)
			bp.cursorGetter = ir.newCursor
			bp.trace = false

			ssk := keys[len(keys)-1]
			c, err := bp.Seek(g, ssk[:len(ssk)/2])
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("%x", ssk), fmt.Sprintf("%x", c.Key()))

			for i := 0; i < len(keys); i++ {
				sk := keys[i]

				c, err := bp.Seek(g, sk[:len(sk)/2])
				require.NoError(t, err, i)
				require.NotNil(t, c, i)
				require.NotNil(t, c.Key(), i)

				//k, _, err := it.KVFromGetter(g)
				//require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("%x", keys[i]), fmt.Sprintf("%x", c.Key()), i)
			}
		})
	}

}

func NewMockIndexReader(ef *eliasfano32.EliasFano) *mockIndexReader {
	return &mockIndexReader{ef: ef}
}

type mockIndexReader struct {
	ef *eliasfano32.EliasFano
}

func (b *mockIndexReader) newCursor(k, v []byte, di uint64, g *seg.PagedReader) *Cursor {
	return &Cursor{
		ef:     b.ef,
		getter: g,
		key:    common.Copy(k),
		value:  common.Copy(v),
		d:      di,
	}
}

func (b *mockIndexReader) dataLookup(di uint64, g *seg.PagedReader) (k, v []byte, offset uint64, err error) {
	if di >= b.ef.Count() {
		return nil, nil, 0, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di, g.FileName())
	}

	offset = b.ef.Get(di)
	g.Reset(offset)
	if !g.HasNext() {
		return nil, nil, 0, fmt.Errorf("pair %d/%d key not found, file: %s", di, b.ef.Count(), g.FileName())
	}
	k, v, _, _, _ = g.Next2(nil, nil)
	return k, v, offset, nil
}

// comparing `k` with item of index `di`. using buffer `kBuf` to avoid allocations
func (b *mockIndexReader) keyCmp(k []byte, di uint64, g *seg.PagedReader, resBuf []byte) (int, []byte, error) {
	if di >= b.ef.Count() {
		return 0, nil, fmt.Errorf("%w: keyCount=%d, but key %d requested. file: %s", ErrBtIndexLookupBounds, b.ef.Count(), di+1, g.FileName())
	}

	cmp, k := g.Cmp(k, b.ef.Get(di))
	return cmp, k, nil
}
