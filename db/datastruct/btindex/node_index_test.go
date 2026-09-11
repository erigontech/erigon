package btindex

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
)

func TestNodeOfftPointsAtPivotKeys(t *testing.T) {
	for _, compress := range []seg.FileCompression{0, seg.CompressKeys} {
		kvPath := generateVarLenKV(t, t.TempDir(), 20000, log.New(), compress)
		indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
		buildBtreeIndex(t, kvPath, indexPath, compress, 1, log.New(), true)
		kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
		require.NoError(t, err)

		b := bt.bplus
		require.NotEmpty(t, b.nodeOfft)
		g := seg.NewReader(kv.MakeGetter(), compress)
		for i := range b.numNodes() {
			require.Zerof(t, b.compareKey(g, b.nodeKey(i), b.nodeDi(i)), "pivot %d", i)
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

func TestDecodeListNodesV0SkipsStoredDi(t *testing.T) {
	keys := [][]byte{{0x01, 0x02}, {0x03, 0x04, 0x05}, {0x07}, {0x09, 0x0a}}
	const stride = uint64(32)

	var blob []byte
	var u8 [8]byte
	binary.BigEndian.PutUint64(u8[:], uint64(len(keys)))
	blob = append(blob, u8[:]...)
	for i, k := range keys {
		binary.BigEndian.PutUint64(u8[:], uint64(i)*stride)
		blob = append(blob, u8[:]...)
		blob = append(blob, byte(len(k)>>8), byte(len(k)))
		blob = append(blob, k...)
	}

	nd, end, err := decodeListNodesV0(blob)
	require.NoError(t, err)
	require.Equal(t, stride, nd.stride)
	require.Len(t, nd.nodeOfft, len(keys))
	require.Equal(t, len(blob), end)

	b := &BpsTree{keysBlob: blob, nodeOfft: nd.nodeOfft, nodeStride: stride}
	for i := range keys {
		require.Equalf(t, keys[i], b.nodeKey(i), "nodeKey(%d)", i)
		require.Equalf(t, uint64(i)*stride, b.nodeDi(i), "nodeDi(%d)", i)
	}
}

func TestDecodedNodeOfftExactCapacity(t *testing.T) {
	count := nodePreallocCap + 1

	t.Run("footer", func(t *testing.T) {
		nd, _, err := decodeNodes(make([]byte, 2*count), uint64(count))
		require.NoError(t, err)
		require.Len(t, nd.nodeOfft, count)
		require.Equal(t, count, cap(nd.nodeOfft))
	})

	t.Run("legacy", func(t *testing.T) {
		blob := binary.BigEndian.AppendUint64(make([]byte, 0, 8+10*count), uint64(count))
		for i := range count {
			blob = binary.BigEndian.AppendUint64(blob, uint64(i))
			blob = append(blob, 0, 0)
		}
		nd, _, err := decodeListNodesV0(blob)
		require.NoError(t, err)
		require.Len(t, nd.nodeOfft, count)
		require.Equal(t, count, cap(nd.nodeOfft))
	})
}

func TestResetNoReadBoundsTargetDi(t *testing.T) {
	ef := eliasfano32.NewEliasFano(2, 128)
	ef.AddOffset(0)
	ef.AddOffset(128)
	ef.Build()

	c := &Cursor{ef: ef}
	require.ErrorIs(t, c.resetNoRead(2, nil), ErrBtIndexLookupBounds)
	require.ErrorIs(t, c.resetNoRead(99, nil), ErrBtIndexLookupBounds)
	require.Zero(t, c.d)
	require.Nil(t, c.getter)
}
