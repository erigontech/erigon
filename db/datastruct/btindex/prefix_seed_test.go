package btindex

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/seg"
)

type bsResult struct {
	dl, dr   uint64
	klo, khi []byte
}

func bsWith(b *BpsTree, seed, offt bool, x []byte) bsResult {
	saveLo, saveHi, saveOfft := b.prefixLo, b.prefixHi, b.nodeOfft
	defer func() { b.prefixLo, b.prefixHi, b.nodeOfft = saveLo, saveHi, saveOfft }()
	if !seed {
		b.prefixLo, b.prefixHi = nil, nil
	}
	if !offt {
		b.nodeOfft = nil
	}
	dl, dr, klo, khi := b.bs(x)
	return bsResult{dl: dl, dr: dr, klo: append([]byte(nil), klo...), khi: append([]byte(nil), khi...)}
}

func openFixture(t *testing.T, kvPath string, compress seg.FileCompression) (*BtIndex, *seg.Reader, func()) {
	t.Helper()
	indexPath := strings.TrimSuffix(kvPath, ".kv") + ".bt"
	buildBtreeIndex(t, kvPath, indexPath, compress, 1, log.New(), true)
	kv, bt, err := OpenBtreeIndexAndDataFile(indexPath, kvPath, compress, false)
	require.NoError(t, err)
	require.NotNil(t, bt.bplus.prefixLo, "prefix seed table must be built")
	require.NotNil(t, bt.bplus.nodeOfft, "node offset cache must be built")
	return bt, seg.NewReader(kv.MakeGetter(), compress), func() { bt.Close(); kv.Close() }
}

func probeSet(t *testing.T, kvPath string) [][]byte {
	t.Helper()
	keys, err := pivotKeysFromKV(kvPath)
	require.NoError(t, err)
	require.NotEmpty(t, keys)

	probes := [][]byte{nil, {}, {0x00}, {0xff}, {0x00, 0x00}, {0xff, 0xff}}
	for _, k := range keys {
		probes = append(probes, k)
		if len(k) > 0 {
			probes = append(probes, k[:1])
		}
		if len(k) > 1 {
			probes = append(probes, k[:2])
		}
		miss := append([]byte(nil), k...)
		miss[len(miss)/2] ^= 0xff
		probes = append(probes, miss)
		lo := append([]byte(nil), k...)
		lo[len(lo)-1] ^= 0x01
		probes = append(probes, lo)
	}
	return probes
}

func seekSnapshot(t *testing.T, bt *BtIndex, g *seg.Reader, x []byte) string {
	t.Helper()
	c, err := bt.bplus.Seek(g, x)
	if err != nil {
		return "err:" + err.Error()
	}
	if c == nil {
		return "nil"
	}
	return fmt.Sprintf("%d|%x|%x", c.Di(), c.Key(), c.Value())
}

func TestPrefixSeedMatchesFullBinarySearch(t *testing.T) {
	for _, tc := range []struct {
		name     string
		build    func(*testing.T, seg.FileCompression) string
		compress seg.FileCompression
	}{
		{"fixed-len", func(t *testing.T, c seg.FileCompression) string {
			return generateKV(t, t.TempDir(), 20, 10, 30000, log.New(), c)
		}, seg.CompressKeys},
		{"var-len-short-keys", func(t *testing.T, c seg.FileCompression) string {
			return generateVarLenKV(t, t.TempDir(), 20000, log.New(), c)
		}, seg.CompressKeys},
		{"uncompressed", func(t *testing.T, c seg.FileCompression) string {
			return generateKV(t, t.TempDir(), 20, 10, 20000, log.New(), c)
		}, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvPath := tc.build(t, tc.compress)
			bt, g, done := openFixture(t, kvPath, tc.compress)
			defer done()

			for i, x := range probeSet(t, kvPath) {
				base := bsWith(bt.bplus, false, false, x)
				for _, arm := range []struct {
					name       string
					seed, offt bool
				}{{"seed", true, false}, {"offt", false, true}, {"seed+offt", true, true}} {
					got := bsWith(bt.bplus, arm.seed, arm.offt, x)
					require.Equalf(t, base.dl, got.dl, "%s probe %d (%x): dl", arm.name, i, x)
					require.Equalf(t, base.dr, got.dr, "%s probe %d (%x): dr", arm.name, i, x)
					require.Equalf(t, base.klo, got.klo, "%s probe %d (%x): klo", arm.name, i, x)
					require.Equalf(t, base.khi, got.khi, "%s probe %d (%x): khi", arm.name, i, x)
				}

				wantV, wantOK, wantOff, err := bt.bplus.Get(g, x)
				require.NoError(t, err)
				wantSeek := seekSnapshot(t, bt, g, x)

				saveLo, saveHi, saveOfft := bt.bplus.prefixLo, bt.bplus.prefixHi, bt.bplus.nodeOfft
				bt.bplus.prefixLo, bt.bplus.prefixHi, bt.bplus.nodeOfft = nil, nil, nil
				gotV, gotOK, gotOff, err := bt.bplus.Get(g, x)
				require.NoError(t, err)
				gotSeek := seekSnapshot(t, bt, g, x)
				bt.bplus.prefixLo, bt.bplus.prefixHi, bt.bplus.nodeOfft = saveLo, saveHi, saveOfft

				require.Equalf(t, gotOK, wantOK, "probe %d (%x): Get ok", i, x)
				require.Equalf(t, gotV, wantV, "probe %d (%x): Get value", i, x)
				require.Equalf(t, gotOff, wantOff, "probe %d (%x): Get offset", i, x)
				require.Equalf(t, gotSeek, wantSeek, "probe %d (%x): Seek", i, x)
			}
		})
	}
}

func TestPrefixSeedBucketBoundsCoverEveryPivot(t *testing.T) {
	kvPath := generateVarLenKV(t, t.TempDir(), 20000, log.New(), seg.CompressKeys)
	bt, _, done := openFixture(t, kvPath, seg.CompressKeys)
	defer done()

	b := bt.bplus
	n := b.numNodes()
	for i := range n {
		p := nodePrefix(b.nodeKey(i)) >> (16 - b.prefixBits)
		require.LessOrEqualf(t, int(b.prefixLo[p]), i, "pivot %d prefix %04x below bucket lo", i, p)
		require.Greaterf(t, int(b.prefixHi[p]), i, "pivot %d prefix %04x at or above bucket hi", i, p)
	}
	require.Equal(t, uint32(0), b.prefixLo[0])
	require.Equal(t, uint32(n), b.prefixHi[len(b.prefixHi)-1])

	for i := 1; i < n; i++ {
		require.LessOrEqualf(t, nodePrefix(b.nodeKey(i-1)), nodePrefix(b.nodeKey(i)),
			"pivot prefixes must be non-decreasing at %d", i)
		require.LessOrEqualf(t, bytes.Compare(b.nodeKey(i-1), b.nodeKey(i)), 0, "pivots must be sorted at %d", i)
	}
}
