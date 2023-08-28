package state

import (
	"bytes"
	"fmt"
	"path"
	"path/filepath"
	"testing"

	bloomfilter "github.com/holiman/bloomfilter/v2"
	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/compress"
	"github.com/ledgerwatch/erigon-lib/recsplit/eliasfano32"
)

func Test_BtreeIndex_Init2(t *testing.T) {
	//mainnnet: storage.128-160.kv  110mil keys, 100mb bloomfilter of 0.01 (1%) miss-probability
	//no much reason to merge bloomfilter - can merge them on starup
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
	logger := log.New()
	tmp := t.TempDir()

	keyCount, M := 100, uint64(4)
	compPath := generateKV(t, tmp, 52, 300, keyCount, logger, 0)
	decomp, err := compress.NewDecompressor(compPath)
	require.NoError(t, err)
	defer decomp.Close()

	err = BuildBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), decomp, CompressNone, background.NewProgressSet(), tmp, 1, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), M, decomp, CompressKeys|CompressVals)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	bt.Close()
}

func Test_BtreeIndex_Seek(t *testing.T) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 120, 30
	compressFlags := FileCompression(CompressKeys | CompressVals)

	t.Run("empty index", func(t *testing.T) {
		dataPath := generateKV(t, tmp, 52, 180, 0, logger, 0)
		indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
		err := BuildBtreeIndex(dataPath, indexPath, compressFlags, 1, logger)
		require.NoError(t, err)

		bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M), compressFlags, false)
		require.NoError(t, err)
		require.EqualValues(t, 0, bt.KeyCount())
	})
	dataPath := generateKV(t, tmp, 52, 180, keyCount, logger, 0)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err := BuildBtreeIndex(dataPath, indexPath, compressFlags, 1, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	getter := NewArchiveGetter(bt.decompressor.MakeGetter(), compressFlags)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, err := bt.dataLookup(bt.ef.Count()+1, getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, err = bt.dataLookup(bt.ef.Count(), getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, err = bt.dataLookup(bt.ef.Count()-1, getter)
		require.NoError(t, err)

		cur, err := bt.Seek(common.FromHex("0xffffffffffffff")) //seek beyeon the last key
		require.NoError(t, err)
		require.Nil(t, cur)
	})

	c, err := bt.Seek(nil)
	require.NoError(t, err)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		if !bytes.Equal(keys[i], k) {
			fmt.Printf("\tinvalid, want %x\n", keys[i])
		}
		c.Next()
	}

	for i := 0; i < len(keys); i++ {
		cur, err := bt.Seek(keys[i])
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
		cur, err := bt.Seek(keys[i])
		require.NoError(t, err)
		require.EqualValues(t, keys[i], cur.Key())
	}

	bt.Close()
}

func Test_BtreeIndex_Build(t *testing.T) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 20000, 510

	compressFlags := FileCompression(CompressKeys | CompressVals)
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressFlags)
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err = BuildBtreeIndex(dataPath, indexPath, compressFlags, 1, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)

	c, err := bt.Seek(nil)
	require.NoError(t, err)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		if !bytes.Equal(keys[i], k) {
			fmt.Printf("\tinvalid, want %x\n", keys[i])
		}
		c.Next()
	}
	for i := 0; i < 10000; i++ {
		c, err := bt.Seek(keys[i])
		require.NoError(t, err)
		require.EqualValues(t, keys[i], c.Key())
	}
	defer bt.Close()
}

func Test_BtreeIndex_Seek2(t *testing.T) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 1_200_000, 1024
	UseBpsTree = false

	compressFlags := FileCompression(CompressKeys | CompressVals)
	dataPath := generateKV(t, tmp, 52, 48, keyCount, logger, compressFlags)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err := BuildBtreeIndex(dataPath, indexPath, compressFlags, 1, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M), compressFlags, false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	getter := NewArchiveGetter(bt.decompressor.MakeGetter(), compressFlags)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, err := bt.dataLookup(bt.ef.Count()+1, getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, err = bt.dataLookup(bt.ef.Count(), getter)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, err = bt.dataLookup(bt.ef.Count()-1, getter)
		require.NoError(t, err)

		cur, err := bt.Seek(common.FromHex("0xffffffffffffff")) //seek beyeon the last key
		require.NoError(t, err)
		require.Nil(t, cur)
	})

	c, err := bt.Seek(nil)
	require.NoError(t, err)
	for i := 0; i < len(keys); i++ {
		k := c.Key()
		if !bytes.Equal(keys[i], k) {
			fmt.Printf("\tinvalid, want %x\n", keys[i])
		}
		c.Next()
	}

	for i := 0; i < len(keys); i++ {
		cur, err := bt.Seek(keys[i])
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
		cur, err := bt.Seek(keys[i])
		require.NoError(t, err)
		require.EqualValues(t, keys[i], cur.Key())
	}

	bt.Close()
}

func TestBpsTree_Seek(t *testing.T) {
	keyCount, M := 48, 4
	tmp := t.TempDir()

	logger := log.New()

	compressFlag := CompressNone
	dataPath := generateKV(t, tmp, 10, 48, keyCount, logger, compressFlag)

	kv, err := compress.NewDecompressor(dataPath)
	require.NoError(t, err)
	defer kv.Close()

	g := NewArchiveGetter(kv.MakeGetter(), compressFlag)

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
		//tr.insert(Node{i: uint64(i), prefix: common.Copy(keys[i]), off: ps[i]})
		ef.AddOffset(ps[i])
	}
	ef.Build()

	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))

	bp := NewBpsTree(g, efi, uint64(M))
	bp.trace = true

	for i := 0; i < len(keys); i++ {
		sk := keys[i]
		it, err := bp.SeekWithGetter(g, sk[:len(sk)/2])
		require.NoError(t, err)
		require.NotNil(t, it)

		k, _, err := it.KVFromGetter(g)
		require.NoError(t, err)
		require.EqualValues(t, keys[i], k)
	}
}
