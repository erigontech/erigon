package state

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/ledgerwatch/log/v3"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon-lib/common/background"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/seg"
)

func Test_EncodeCommitmentState(t *testing.T) {
	cs := commitmentState{
		txNum:     rand.Uint64(),
		trieState: make([]byte, 1024),
	}
	n, err := rand.Read(cs.trieState)
	require.NoError(t, err)
	require.EqualValues(t, len(cs.trieState), n)

	buf, err := cs.Encode()
	require.NoError(t, err)
	require.NotEmpty(t, buf)

	var dec commitmentState
	err = dec.Decode(buf)
	require.NoError(t, err)
	require.EqualValues(t, cs.txNum, dec.txNum)
	require.EqualValues(t, cs.trieState, dec.trieState)
}

func Test_BtreeIndex_Seek(t *testing.T) {
	tmp := t.TempDir()
	logger := log.New()

	keyCount, M := 120000, 1024
	dataPath := generateCompressedKV(t, tmp, 52, 180 /*val size*/, keyCount, logger)
	defer os.RemoveAll(tmp)

	indexPath := path.Join(tmp, filepath.Base(dataPath)+".bti")
	err := BuildBtreeIndex(dataPath, indexPath, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndex(indexPath, dataPath, uint64(M))
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)

	keys, err := pivotKeysFromKV(dataPath)
	require.NoError(t, err)

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

func pivotKeysFromKV(dataPath string) ([][]byte, error) {
	decomp, err := seg.NewDecompressor(dataPath)
	if err != nil {
		return nil, err
	}

	getter := decomp.MakeGetter()
	getter.Reset(0)

	key := make([]byte, 0, 64)

	listing := make([][]byte, 0, 1000)

	for getter.HasNext() {
		if len(listing) > 100000 {
			break
		}
		key, _ := getter.Next(key[:0])
		listing = append(listing, common.Copy(key))
		getter.Skip()
	}
	decomp.Close()

	return listing, nil
}

func generateCompressedKV(tb testing.TB, tmp string, keySize, valueSize, keyCount int, logger log.Logger) string {
	tb.Helper()

	args := BtIndexWriterArgs{
		IndexFile: path.Join(tmp, fmt.Sprintf("%dk.bt", keyCount/1000)),
		TmpDir:    tmp,
		KeyCount:  12,
	}

	iw, err := NewBtIndexWriter(args, logger)
	require.NoError(tb, err)

	defer iw.Close()
	rnd := rand.New(rand.NewSource(0))
	values := make([]byte, valueSize)

	dataPath := path.Join(tmp, fmt.Sprintf("%dk.kv", keyCount/1000))
	comp, err := seg.NewCompressor(context.Background(), "cmp", dataPath, tmp, seg.MinPatternScore, 1, log.LvlDebug, logger)
	require.NoError(tb, err)

	for i := 0; i < keyCount; i++ {
		key := make([]byte, keySize)
		n, err := rnd.Read(key[:])
		require.EqualValues(tb, keySize, n)
		binary.BigEndian.PutUint64(key[keySize-8:], uint64(i))
		require.NoError(tb, err)
		err = comp.AddWord(key[:])
		require.NoError(tb, err)

		n, err = rnd.Read(values[:rnd.Intn(valueSize)+1])
		require.NoError(tb, err)

		err = comp.AddWord(values[:n])
		require.NoError(tb, err)
	}

	err = comp.Compress()
	require.NoError(tb, err)
	comp.Close()

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)

	getter := decomp.MakeGetter()
	getter.Reset(0)

	var pos uint64
	key := make([]byte, keySize)
	for i := 0; i < keyCount; i++ {
		if !getter.HasNext() {
			tb.Fatalf("not enough values at %d", i)
			break
		}

		keys, _ := getter.Next(key[:0])
		err = iw.AddKey(keys[:], pos)

		pos, _ = getter.Skip()
		require.NoError(tb, err)
	}
	decomp.Close()

	require.NoError(tb, iw.Build())
	iw.Close()

	return decomp.FilePath()
}

func Test_InitBtreeIndex(t *testing.T) {
	logger := log.New()
	tmp := t.TempDir()

	keyCount, M := 100, uint64(4)
	compPath := generateCompressedKV(t, tmp, 52, 300, keyCount, logger)
	decomp, err := seg.NewDecompressor(compPath)
	require.NoError(t, err)
	defer decomp.Close()

	err = BuildBtreeIndexWithDecompressor(tmp+".bt", decomp, &background.Progress{}, tmp, logger)
	require.NoError(t, err)

	bt, err := OpenBtreeIndexWithDecompressor(tmp+".bt", M, decomp)
	require.NoError(t, err)
	require.EqualValues(t, bt.KeyCount(), keyCount)
	bt.Close()
}
