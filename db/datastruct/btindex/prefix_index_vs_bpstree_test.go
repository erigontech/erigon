package btindex

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/recsplit/eliasfano32"
	"github.com/erigontech/erigon/db/seg"
)

// mixedLengthKeys returns a sorted, deduplicated key set with 1..33-byte keys,
// so short commitment-style paths and long storage-style keys share buckets.
func mixedLengthKeys(n int) (keys, vals [][]byte) {
	rnd := newRnd(7)
	seen := map[string]struct{}{}
	for len(keys) < n {
		l := 1 + rnd.IntN(33)
		k := make([]byte, l)
		_, _ = rnd.Read(k)
		k[0] &= 0x0f // crowd the keys into few first-byte buckets
		if _, ok := seen[string(k)]; ok {
			continue
		}
		seen[string(k)] = struct{}{}
		keys = append(keys, k)
	}
	slicesSortBytes(keys)
	vals = make([][]byte, len(keys))
	for i := range keys {
		vals[i] = append([]byte("v"), keys[i]...)
	}
	return keys, vals
}

func slicesSortBytes(s [][]byte) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && bytes.Compare(s[j-1], s[j]) > 0; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func buildEF(t testing.TB, g *seg.Reader) *eliasfano32.EliasFano {
	t.Helper()
	g.Reset(0)
	var offsets []uint64
	pos := uint64(0)
	for g.HasNext() {
		offsets = append(offsets, pos)
		_, _ = g.Next(nil)
		pos, _ = g.Skip()
	}
	require.NotEmpty(t, offsets)
	ef := eliasfano32.NewEliasFano(uint64(len(offsets)), offsets[len(offsets)-1])
	for _, o := range offsets {
		ef.AddOffset(o)
	}
	ef.Build()
	efi, _ := eliasfano32.ReadEliasFano(ef.AppendBytes(nil))
	return efi
}

// TestPrefixIndexMatchesBpsTree pins PrefixIndex as a drop-in replacement: for the
// same file, Get and Seek must answer identically to BpsTree on hits and misses.
func TestPrefixIndexMatchesBpsTree(t *testing.T) {
	t.Parallel()
	keys, vals := mixedLengthKeys(3000)
	compressFlags := seg.CompressNone
	kvPath := generateMinimalKV(t, t.TempDir(), keys, vals, compressFlags)

	decomp, err := seg.NewDecompressor(kvPath)
	require.NoError(t, err)
	defer decomp.Close()

	g := seg.NewReader(decomp.MakeGetter(), compressFlags)
	efi := buildEF(t, g)
	ir := NewMockIndexReader(efi)

	bps := NewBpsTree(g, efi, DefaultBtreeM, ir.dataLookup)
	bps.cursorGetter = ir.newCursor
	defer bps.Close()

	for _, tc := range []struct {
		name string
		make func() *PrefixIndex
	}{
		{"scan", func() *PrefixIndex { return NewPrefixIndex(g, efi, ir.dataLookup) }},
		{"withNodes", func() *PrefixIndex {
			return NewPrefixIndexWithNodes(g, efi, ir.dataLookup, btPivots(g, DefaultBtreeM))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pi := tc.make()
			pi.cursorGetter = ir.newCursor
			defer pi.Close()
			comparePrefixIndexToBpsTree(t, g, bps, pi, keys)
		})
	}
}

// btPivots collects every stride-th key, the way BtIndexWriter lays out .bt nodes.
func btPivots(g *seg.Reader, stride uint64) []prefixNode {
	var nodes []prefixNode
	var key []byte
	g.Reset(0)
	for di := uint64(0); g.HasNext(); di++ {
		key, _ = g.Next(key[:0])
		g.Skip()
		if di%stride == 0 {
			nodes = append(nodes, prefixNode{key: bytes.Clone(key), di: di})
		}
	}
	return nodes
}

func comparePrefixIndexToBpsTree(t *testing.T, g *seg.Reader, bps *BpsTree, pi *PrefixIndex, keys [][]byte) {
	t.Helper()
	probes := make([][]byte, 0, len(keys)*2)
	for _, k := range keys {
		probes = append(probes, k, gapKey(k))
	}
	probes = append(probes, []byte{0x00}, []byte{0xff}, []byte{0x0f, 0xff, 0xff})

	for _, probe := range probes {
		wantV, wantOK, _, wantErr := bps.Get(g, probe)
		gotV, gotOK, _, gotErr := pi.Get(g, probe)
		require.Equal(t, wantErr == nil, gotErr == nil, "Get err mismatch for %x", probe)
		require.Equalf(t, wantOK, gotOK, "Get found mismatch for %x", probe)
		require.Equalf(t, wantV, gotV, "Get value mismatch for %x", probe)

		// BtIndex.Seek maps ErrBtIndexLookupBounds to a nil cursor; compare at that level.
		wantC, wantErr := bps.Seek(g, probe)
		if errors.Is(wantErr, ErrBtIndexLookupBounds) {
			wantC, wantErr = nil, nil
		}
		gotC, gotErr := pi.Seek(g, probe)
		if errors.Is(gotErr, ErrBtIndexLookupBounds) {
			gotC, gotErr = nil, nil
		}
		require.NoError(t, wantErr)
		require.NoError(t, gotErr)
		if wantC == nil {
			if gotC != nil {
				t.Fatalf("Seek %x: BpsTree nil, PrefixIndex %x", probe, gotC.Key())
			}
			continue
		}
		require.NotNilf(t, gotC, "Seek %x: BpsTree %x, PrefixIndex nil", probe, wantC.Key())
		require.Equalf(t, wantC.Key(), gotC.Key(), "Seek key mismatch for %x", probe)
		wantC.Close()
		gotC.Close()
	}
}
