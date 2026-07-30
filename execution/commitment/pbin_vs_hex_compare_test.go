package commitment

import (
	"context"
	"fmt"
	"math/bits"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/length"
)

// Structural comparison of the hex and binary commitment engines over one
// corpus. Roots differ by construction — the trees, keys and node preimages all
// differ — so this measures shape and footprint, not equality.

type pbinEngineShape struct {
	name       string
	root       []byte
	records    int
	recordByte int
	depthBits  []int // path length to each stored branch, in key bits
}

func (s pbinEngineShape) depthStats() (maxD, p50, mean int) {
	if len(s.depthBits) == 0 {
		return 0, 0, 0
	}
	d := append([]int(nil), s.depthBits...)
	sort.Ints(d)
	sum := 0
	for _, v := range d {
		sum += v
	}
	return d[len(d)-1], d[len(d)/2], sum / len(d)
}

// pbinHexPathBits converts a HexToCompact-encoded branch key to a path length in
// key bits so the two radices are comparable: one nibble is four bits.
func pbinHexPathBits(compact string) int {
	if len(compact) == 0 {
		return 0
	}
	nibbles := (len(compact)-1)*2 + 1
	if compact[0]&0x10 == 0 {
		nibbles--
	}
	return nibbles * 4
}

func pbinPathBits(key string) int {
	p, err := pbinDecodeBitPath([]byte(key))
	if err != nil {
		return -1
	}
	return int(p.bitLen)
}

func pbinRunHex(t *testing.T, plainKeys [][]byte, updates []Update) pbinEngineShape {
	t.Helper()
	ms := NewMockState(t)
	// PBin derives its zone from the plain-key length, so a comparison corpus
	// must use real 20-byte addresses; the hex engine has to be told the same.
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	upds := WrapKeyUpdates(t, ModeDirect, KeyToHexNibbleHash, plainKeys, updates)
	defer upds.Close()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

	root, err := hph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	s := pbinEngineShape{name: "hex", root: root}
	for k, v := range ms.cm {
		s.records++
		s.recordByte += len(v)
		s.depthBits = append(s.depthBits, pbinHexPathBits(k))
	}
	return s
}

func pbinRunBin(t *testing.T, plainKeys [][]byte, updates []Update) (pbinEngineShape, pbinCounters) {
	t.Helper()
	ms := NewMockState(t)
	pph := NewPBinPatriciaHashed(ms)
	upds := WrapKeyUpdates(t, ModeDirect, pbinKeyHasher(), plainKeys, updates)
	defer upds.Close()
	require.NoError(t, ms.applyPlainUpdates(plainKeys, updates))

	root, err := pph.Process(context.Background(), upds, "", nil, WarmupConfig{})
	require.NoError(t, err)

	s := pbinEngineShape{name: "bin", root: root}
	for k, v := range ms.cm {
		s.records++
		s.recordByte += len(v)
		s.depthBits = append(s.depthBits, pbinPathBits(k))
	}
	return s, pph.counters
}

// pbinClusteredCorpus gives every contract slots that share a storage group, which
// is what EIP-8297's raw sub-index co-locates. pbinScatteredCorpus spreads slots so
// no two share a group — the mapping-style access random corpora produce.
func pbinClusteredCorpus(contracts, slotsPer int) ([][]byte, []Update) {
	ub := NewUpdateBuilder()
	for c := range contracts {
		addr := fmt.Sprintf("%040x", c+1)
		ub.Balance(addr, uint64(c+1))
		for s := range slotsPer {
			ub.Storage(addr, fmt.Sprintf("%064x", 0x100+s), fmt.Sprintf("%064x", s+1))
		}
	}
	return ub.Build()
}

func pbinScatteredCorpus(contracts, slotsPer int) ([][]byte, []Update) {
	ub := NewUpdateBuilder()
	for c := range contracts {
		addr := fmt.Sprintf("%040x", c+1)
		ub.Balance(addr, uint64(c+1))
		for s := range slotsPer {
			// one slot per group: step by STEM_SUBTREE_WIDTH
			ub.Storage(addr, fmt.Sprintf("%064x", (s+1)*256), fmt.Sprintf("%064x", s+1))
		}
	}
	return ub.Build()
}

func TestPBinVsHexStructure(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		build func(int, int) ([][]byte, []Update)
	}{
		{"clustered", pbinClusteredCorpus},
		{"scattered", pbinScatteredCorpus},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plainKeys, updates := tc.build(16, 16)

			hex := pbinRunHex(t, plainKeys, updates)
			bin, counters := pbinRunBin(t, plainKeys, updates)

			require.NotEqual(t, hex.root, bin.root,
				"hex and binary trees must not agree on a root; equality means one of them is not the tree it claims to be")

			hMax, hP50, hMean := hex.depthStats()
			bMax, bP50, bMean := bin.depthStats()

			t.Logf("corpus=%s accounts=%d storage=%d", tc.name, 16, 16*16)
			t.Logf("  %-4s records=%4d bytes=%7d depthBits max=%3d p50=%3d mean=%3d",
				hex.name, hex.records, hex.recordByte, hMax, hP50, hMean)
			t.Logf("  %-4s records=%4d bytes=%7d depthBits max=%3d p50=%3d mean=%3d",
				bin.name, bin.records, bin.recordByte, bMax, bP50, bMean)
			t.Logf("  bin/hex records=%.2fx bytes=%.2fx",
				float64(bin.records)/float64(hex.records),
				float64(bin.recordByte)/float64(hex.recordByte))
			t.Logf("  bin splitsInsidePrefix=%d materializeReads=%d",
				counters.splitsInsidePrefix, counters.materializeReads)
		})
	}
}

// TestPBinStemCoLocation pins the storage behaviour that distinguishes
// EIP-8297: slots sharing a tree_index differ only in the last key byte, so
// they hang off one stem. Random 32-byte slots never collide in a group, so
// without a deliberate corpus this path goes untested.
func TestPBinStemCoLocation(t *testing.T) {
	t.Parallel()

	addr := make([]byte, 20)
	addr[19] = 0xAB

	slotOf := func(n uint64) []byte {
		s := make([]byte, 32)
		s[31] = byte(n)
		s[30] = byte(n >> 8)
		return s
	}

	var c pbinDigestCache
	// slots 256..511 share tree_index 1 and differ only in sub_index.
	base := c.storageKey(addr, slotOf(256))
	require.Len(t, base, pbinStorageKeyLength)

	for _, n := range []uint64{257, 300, 511} {
		k := c.storageKey(addr, slotOf(n))
		require.Equal(t, base[:pbinStorageKeyLength-1], k[:pbinStorageKeyLength-1],
			"slots in one group must share every byte but the sub-index")
		require.Equal(t, byte(n%256), k[pbinStorageKeyLength-1], "sub-index is the raw low byte")
	}

	// crossing into the next group must change the second digest
	next := c.storageKey(addr, slotOf(512))
	require.NotEqual(t, base[33:65], next[33:65], "a new tree_index must move the group digest")

	// a co-located pair shares a long prefix; a cross-group pair does not
	sharedBits := pbinCommonPrefixBitsOfKeys(base, c.storageKey(addr, slotOf(257)))
	crossBits := pbinCommonPrefixBitsOfKeys(base, next)
	require.Greater(t, sharedBits, crossBits,
		"co-located slots must share a longer key prefix than cross-group slots")
	t.Logf("co-located slots share %d bits; cross-group share %d bits", sharedBits, crossBits)
}

func pbinCommonPrefixBitsOfKeys(a, b []byte) int {
	n := min(len(a), len(b))
	for i := range n {
		if a[i] != b[i] {
			return i*8 + bits.LeadingZeros8(a[i]^b[i])
		}
	}
	return n * 8
}
