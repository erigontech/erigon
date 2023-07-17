package state

import (
	"bytes"
	"fmt"
	"path"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/background"
	"github.com/ledgerwatch/erigon-lib/compress"
)

func Test_BtreeIndex_Init(t *testing.T) {
	logger := log.New()
	tmp := t.TempDir()

	keyCount, M := 100, uint64(4)
	compPath := generateCompressedKV(t, tmp, 52, 300, keyCount, logger)
	decomp, err := compress.NewDecompressor(compPath)
	require.NoError(t, err)
	defer decomp.Close()

	err = BuildBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), decomp, &background.Progress{}, tmp, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndexWithDecompressor(filepath.Join(tmp, "a.bt"), M, decomp)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	bt.Close()
}

func Test_BtreeIndex_Seek(t *testing.T) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 120, 30

	t.Run("empty index", func(t *testing.T) {
		dataPath := generateCompressedKV(t, tmp, 52, 180 /*val size*/, 0, logger)
		indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
		err := BuildBtreeIndex(dataPath, indexPath, logger)
		require.NoError(t, err)

		bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M), false)
		require.NoError(t, err)
		require.EqualValues(t, 0, bt.KeyCount())
	})
	dataPath := generateCompressedKV(t, tmp, 52, 180 /*val size*/, keyCount, logger)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err := BuildBtreeIndex(dataPath, indexPath, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M), false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, err := bt.dataLookup(nil, nil, bt.keyCount+1)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, err = bt.dataLookup(nil, nil, bt.keyCount)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, err = bt.dataLookup(nil, nil, bt.keyCount-1)
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
	dataPath := generateCompressedKV(t, tmp, 52, 48 /*val size*/, keyCount, logger)
	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err = BuildBtreeIndex(dataPath, indexPath, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M), false)
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
	defer bt.Close()
}

func Test_BtreeIndex_Seek2(t *testing.T) {
	tmp := t.TempDir()
	logger := log.New()
	keyCount, M := 1_200_000, 1024

	dataPath := generateCompressedKV(t, tmp, 52, 48 /*val size*/, keyCount, logger)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err := BuildBtreeIndex(dataPath, indexPath, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M), false)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

	t.Run("seek beyond the last key", func(t *testing.T) {
		_, _, err := bt.dataLookup(nil, nil, bt.keyCount+1)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)

		_, _, err = bt.dataLookup(nil, nil, bt.keyCount)
		require.ErrorIs(t, err, ErrBtIndexLookupBounds)
		require.Error(t, err)

		_, _, err = bt.dataLookup(nil, nil, bt.keyCount-1)
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
