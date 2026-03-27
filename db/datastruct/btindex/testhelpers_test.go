package btindex

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	randOld "math/rand"
	"math/rand/v2"
	"path/filepath"
	"sort"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/background"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/etl"
	"github.com/erigontech/erigon/db/seg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// takes first 100k keys from file
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

type rndGen struct {
	*rand.Rand
	oldGen *randOld.Rand
}

func newRnd(seed uint64) *rndGen {
	return &rndGen{
		Rand:   rand.New(rand.NewChaCha8([32]byte{byte(seed)})),
		oldGen: randOld.New(randOld.NewSource(int64(seed))),
	}
}
func (r *rndGen) IntN(n int) int                   { return int(r.Uint64N(uint64(n))) }
func (r *rndGen) Read(p []byte) (n int, err error) { return r.oldGen.Read(p) }

func generateKV(tb testing.TB, tmp string, keySize, valueSize, keyCount int, logger log.Logger, compressFlags seg.FileCompression) string {
	tb.Helper()

	rnd := newRnd(0)
	values := make([]byte, valueSize)

	dataPath := filepath.Join(tmp, fmt.Sprintf("%dk.kv", keyCount/1000))
	comp, err := seg.NewCompressor(context.Background(), "cmp", dataPath, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(tb, err)

	bufSize := 8 * datasize.KB
	if keyCount > 1000 { // windows CI can't handle much small parallel disk flush
		bufSize = 1 * datasize.MB
	}
	collector := etl.NewCollector(BtreeLogPrefix+" genCompress", tb.TempDir(), etl.NewSortableBuffer(bufSize), logger)
	defer collector.Close()

	for i := 0; i < keyCount; i++ {
		key := make([]byte, keySize)
		n, err := rnd.Read(key)
		require.Equal(tb, keySize, n)
		binary.BigEndian.PutUint64(key[keySize-8:], uint64(i))
		require.NoError(tb, err)

		n, err = rnd.Read(values[:rnd.IntN(valueSize)+1])
		require.NoError(tb, err)

		err = collector.Collect(key, values[:n])
		require.NoError(tb, err)
	}

	writer := seg.NewWriter(comp, compressFlags)

	loader := func(k, v []byte, _ etl.CurrentTableReader, _ etl.LoadNextFunc) error {
		_, err = writer.Write(k)
		require.NoError(tb, err)
		_, err = writer.Write(v)
		require.NoError(tb, err)
		return nil
	}

	err = collector.Load(nil, "", loader, etl.TransformArgs{})
	require.NoError(tb, err)

	collector.Close()

	err = comp.Compress()
	require.NoError(tb, err)
	comp.Close()

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()
	compPath := decomp.FilePath()
	ps := background.NewProgressSet()

	IndexFile := filepath.Join(tmp, fmt.Sprintf("%dk.bt", keyCount/1000))
	r := seg.NewReader(decomp.MakeGetter(), compressFlags)
	err = BuildBtreeIndexWithDecompressor(IndexFile, r, ps, tb.TempDir(), 777, logger, true, statecfg.AccessorBTree|statecfg.AccessorExistence)
	require.NoError(tb, err)

	return compPath
}

// generateControlledKV creates a .kv file with keys that have specific 2-byte prefixes.
// For each prefix, keysPerPrefix sorted keys are generated: prefix + 6-byte suffix.
// Returns the path to the .kv file and the list of sorted (key, value) pairs.
func generateControlledKV(tb testing.TB, tmp string, prefixes [][]byte, keysPerPrefix int, compressFlags seg.FileCompression) (string, [][]byte, [][]byte) {
	tb.Helper()
	logger := log.New()
	rnd := newRnd(42)

	type kv struct {
		key, val []byte
	}
	var pairs []kv
	for _, pfx := range prefixes {
		for i := 0; i < keysPerPrefix; i++ {
			suffix := make([]byte, 6)
			binary.BigEndian.PutUint32(suffix, uint32(i))
			suffix[4] = byte(rnd.IntN(256))
			suffix[5] = byte(rnd.IntN(256))

			key := make([]byte, len(pfx)+len(suffix))
			copy(key, pfx)
			copy(key[len(pfx):], suffix)

			val := make([]byte, 8)
			_, _ = rnd.Read(val)
			pairs = append(pairs, kv{key: key, val: val})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return string(pairs[i].key) < string(pairs[j].key)
	})

	// Deduplicate keys
	deduped := pairs[:0]
	for i, p := range pairs {
		if i > 0 && bytes.Equal(p.key, pairs[i-1].key) {
			continue
		}
		deduped = append(deduped, p)
	}
	pairs = deduped

	dataPath := filepath.Join(tmp, fmt.Sprintf("controlled_%d.kv", len(pairs)))
	comp, err := seg.NewCompressor(context.Background(), "cmp", dataPath, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(tb, err)

	writer := seg.NewWriter(comp, compressFlags)
	for _, p := range pairs {
		_, err = writer.Write(p.key)
		require.NoError(tb, err)
		_, err = writer.Write(p.val)
		require.NoError(tb, err)
	}
	err = comp.Compress()
	require.NoError(tb, err)
	comp.Close()

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()

	// Build .bt index alongside
	btPath := filepath.Join(tmp, fmt.Sprintf("controlled_%d.bt", len(pairs)))
	ps := background.NewProgressSet()
	r := seg.NewReader(decomp.MakeGetter(), compressFlags)
	err = BuildBtreeIndexWithDecompressor(btPath, r, ps, tb.TempDir(), 777, logger, true, statecfg.AccessorBTree|statecfg.AccessorExistence)
	require.NoError(tb, err)

	keys := make([][]byte, len(pairs))
	vals := make([][]byte, len(pairs))
	for i, p := range pairs {
		keys[i] = common.Copy(p.key)
		vals[i] = common.Copy(p.val)
	}
	return decomp.FilePath(), keys, vals
}

// generateMinimalKV creates a .kv file with exactly the given key-value pairs (already sorted).
// Returns the path to the .kv file.
func generateMinimalKV(tb testing.TB, tmp string, keys, values [][]byte, compressFlags seg.FileCompression) string {
	tb.Helper()
	logger := log.New()

	dataPath := filepath.Join(tmp, fmt.Sprintf("minimal_%d.kv", len(keys)))
	comp, err := seg.NewCompressor(context.Background(), "cmp", dataPath, tmp, seg.DefaultCfg, log.LvlDebug, logger)
	require.NoError(tb, err)

	writer := seg.NewWriter(comp, compressFlags)
	for i := range keys {
		_, err = writer.Write(keys[i])
		require.NoError(tb, err)
		_, err = writer.Write(values[i])
		require.NoError(tb, err)
	}
	err = comp.Compress()
	require.NoError(tb, err)
	comp.Close()

	decomp, err := seg.NewDecompressor(dataPath)
	require.NoError(tb, err)
	defer decomp.Close()

	return decomp.FilePath()
}

// gapKey returns a key that is just past k (last byte incremented).
// Handles 0xFF overflow by incrementing higher bytes.
func gapKey(k []byte) []byte {
	g := common.Copy(k)
	for i := len(g) - 1; i >= 0; i-- {
		if g[i] < 0xFF {
			g[i]++
			return g
		}
		g[i] = 0x00
	}
	// All bytes were 0xFF; append 0x00 to make it longer (still > k)
	return append(g, 0x00)
}
