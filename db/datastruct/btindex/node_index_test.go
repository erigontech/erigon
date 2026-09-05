package btindex

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
)

func TestNodeOfftMatchesEliasFano(t *testing.T) {
	for _, compress := range []seg.FileCompression{0, seg.CompressKeys} {
		kvPath := generateVarLenKV(t, t.TempDir(), 20000, log.New(), compress)
		indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
		buildBtreeIndex(t, kvPath, indexPath, compress, 1, log.New(), true)
		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
		require.NoError(t, err)

		b := bt.bplus
		require.NotNil(t, b.nodeOfft)
		require.Equal(t, b.numNodes(), len(b.nodeOfft))
		for i := range b.numNodes() {
			require.Equalf(t, b.nodeOfftEF.Get(uint64(i)), uint64(b.nodeOfft[i]), "offset %d", i)

			withCache := append([]byte(nil), b.nodeKey(i)...)
			saved := b.nodeOfft
			b.nodeOfft = nil
			withoutCache := append([]byte(nil), b.nodeKey(i)...)
			b.nodeOfft = saved
			require.Equalf(t, withoutCache, withCache, "nodeKey(%d)", i)
		}
		bt.Close()
		kv.Close()
	}
}

func TestSeekExactHitSurvivesPooledCursor(t *testing.T) {
	const compress = seg.FileCompression(0)
	kvPath := generateKV(t, t.TempDir(), 20, 10, 20000, log.New(), compress)
	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	buildBtreeIndex(t, kvPath, indexPath, compress, 1, log.New(), true)
	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
	require.NoError(t, err)
	defer bt.Close()
	defer kv.Close()

	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	g := seg.NewReader(kv.MakeGetter(), compress)
	for range 4 {
		for _, k := range keys {
			c, err := bt.Seek(g, k)
			require.NoError(t, err)
			if c == nil {
				continue
			}
			require.Equal(t, k, c.Key())
			c.Close()
		}
	}
}

func TestBuildNodeIndexIgnoresBlobLayout(t *testing.T) {
	keys := [][]byte{{0x01, 0x02}, {0x03, 0x04, 0x05}, {0x07}, {0x09, 0x0a}}

	var blob []byte
	offs := make([]uint64, 0, len(keys))
	blob = append(blob, make([]byte, 8)...)
	for i, k := range keys {
		blob = append(blob, make([]byte, 8)...)
		offs = append(offs, uint64(len(blob)))
		blob = append(blob, byte(len(k)>>8), byte(len(k)))
		blob = append(blob, k...)
		_ = i
	}

	ef := eliasfano32.NewEliasFano(uint64(len(offs)), offs[len(offs)-1])
	for _, o := range offs {
		ef.AddOffset(o)
	}
	ef.Build()

	b := &BpsTree{keysBlob: blob, nodeOfftEF: ef, nodeStride: 1, offt: ef}
	b.buildNodeIndex()

	require.NotNil(t, b.nodeOfft)
	for i := range b.numNodes() {
		require.Equalf(t, offs[i], uint64(b.nodeOfft[i]), "offset %d", i)
		require.Equalf(t, keys[i], b.nodeKey(i), "nodeKey(%d)", i)
	}
}
