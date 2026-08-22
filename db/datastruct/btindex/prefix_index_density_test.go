package btindex

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/seg"
)

// densityKeys builds keyCount sorted keys spread over exactly nPrefixes distinct
// 2-byte prefixes, so keys-per-bucket is controlled directly.
func densityKeys(nPrefixes, keyCount int) (keys, vals [][]byte) {
	keys = make([][]byte, 0, keyCount)
	for i := range keyCount {
		k := make([]byte, 20)
		binary.BigEndian.PutUint16(k, uint16(i%nPrefixes))
		binary.BigEndian.PutUint64(k[12:], uint64(i))
		keys = append(keys, k)
	}
	slices.SortFunc(keys, bytes.Compare)
	vals = make([][]byte, len(keys))
	for i := range keys {
		vals[i] = []byte{byte(i)}
	}
	return keys, vals
}

// TestNarrowingWindowByDensity pins how far each engine narrows before it starts
// probing the .kv file. BpsTree's window is bounded by M; PrefixIndex's is bounded
// by bucket size divided by the per-bucket node cap, so it widens as keys-per-bucket
// grows. Both windows must still contain the key.
func TestNarrowingWindowByDensity(t *testing.T) {
	t.Parallel()
	for _, nPrefixes := range []int{4096, 256} {
		t.Run(fmt.Sprintf("prefixes=%d", nPrefixes), func(t *testing.T) {
			t.Parallel()
			const keyCount = 200_000
			keys, vals := densityKeys(nPrefixes, keyCount)
			compressFlags := seg.CompressNone
			kvPath := generateMinimalKV(t, t.TempDir(), keys, vals, compressFlags)

			decomp, err := seg.NewDecompressor(kvPath)
			require.NoError(t, err)
			defer decomp.Close()
			g := seg.NewReader(decomp.MakeGetter(), compressFlags)
			efi := buildEF(t, g)
			ir := NewMockIndexReader(efi)

			bps := NewBpsTree(g, efi, DefaultBtreeM, ir.dataLookup)
			defer bps.Close()
			pi := NewPrefixIndexWithNodes(g, efi, ir.dataLookup, btPivots(g, DefaultBtreeM))
			defer pi.Close()

			var bpsTotal, piTotal, bpsMax, piMax uint64
			const samples = 512
			for i := range samples {
				di := uint64(i) * uint64(len(keys)) / samples
				key := keys[di]

				bl, br, _, _ := bps.bs(key)
				require.LessOrEqual(t, bl, di, "BpsTree window starts past key")
				require.GreaterOrEqual(t, br, di, "BpsTree window ends before key")
				bpsTotal += br - bl
				bpsMax = max(bpsMax, br-bl)

				l, r := pi.lookup(key)
				nl, nr, _, found := pi.narrowWithNodes(key, l, r)
				if found {
					continue
				}
				require.LessOrEqual(t, nl, di, "PrefixIndex window starts past key")
				require.GreaterOrEqual(t, nr, di, "PrefixIndex window ends before key")
				piTotal += nr - nl
				piMax = max(piMax, nr-nl)
			}
			t.Logf("keys=%d prefixes=%d keysPerBucket=%d M=%d | window avg: bpsTree=%d prefixIndex=%d | max: bpsTree=%d prefixIndex=%d",
				keyCount, nPrefixes, keyCount/nPrefixes, DefaultBtreeM,
				bpsTotal/samples, piTotal/samples, bpsMax, piMax)
		})
	}
}
