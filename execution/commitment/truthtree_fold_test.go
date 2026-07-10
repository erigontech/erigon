// Copyright 2026 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package commitment

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// whaleStorageNode builds the touched prefix trie for the whale corpus and returns the depth-64
// account node (whose children are the storage subtree) and the 64-nibble account prefix. All
// keys share the account hash, so the account node is a single walk down.
func whaleStorageNode(pk [][]byte, upds []Update, accHash []byte) (*prefixNode, []byte) {
	tr := newPrefixTrie()
	for i, k := range pk {
		tr.Insert(KeyToHexNibbleHash(k), k, &upds[i])
	}
	node := tr.root
	depth := 0
	for {
		depth += len(node.ext)
		if depth >= 64 {
			break
		}
		nib := accHash[depth]
		idx, ok := childIndex(node, nib)
		if !ok {
			panic(fmt.Sprintf("whaleStorageNode: account path missing at depth %d", depth))
		}
		depth++ // consume the branch nibble
		node = node.children[idx]
	}
	if depth != 64 {
		panic(fmt.Sprintf("whaleStorageNode: account node at depth %d, want 64", depth))
	}
	return node, append([]byte(nil), accHash[:64]...)
}

func makeFoldPool(ms *MockState, workers int) *foldPool {
	return &foldPool{
		numWorkers: workers,
		ctxFactory: mockTrieCtxFactory(ms),
		workerPool: &sync.Pool{New: func() any { return NewHexPatriciaHashed(length.Addr, nil, DefaultTrieConfig()) }},
	}
}

// oracleRoot computes the reference account state root via the sequential ModeDirect engine.
func oracleRoot(tb testing.TB, pk [][]byte, upds []Update) []byte {
	ms := NewMockState(tb)
	require.NoError(tb, ms.applyPlainUpdates(pk, upds))
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	u := WrapKeyUpdates(tb, ModeDirect, KeyToHexNibbleHash, pk, upds)
	root, err := hph.Process(context.Background(), u, "", nil, WarmupConfig{})
	require.NoError(tb, err)
	u.Close()
	hph.Release()
	return root
}

// wrapAccountRoot builds the single-account state root from a storage-subtree root hash by hashing
// the account leaf at depth 0 (its full 64-nibble key is re-derived from the address).
func wrapAccountRoot(ms *MockState, addr []byte, accUpd Update, sr common.Hash) ([]byte, error) {
	hph := NewHexPatriciaHashed(length.Addr, ms, DefaultTrieConfig())
	defer hph.Release()
	var ac cell
	ac.accountAddrLen = int16(len(addr))
	copy(ac.accountAddr[:], addr)
	ac.CodeHash = empty.CodeHash
	ac.setFromUpdate(&accUpd)
	ac.hash = sr
	ac.hashLen = 32
	h, err := hph.computeCellHash(&ac, 0, nil)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), h[1:]...), nil
}

// TestTruthtreeFold_FreshStoragePlane pins the direct fold's storage root byte-for-byte against
// both the current fresh-whale fold path and the sequential oracle, on fresh storage subtrees.
func TestTruthtreeFold_FreshStoragePlane(t *testing.T) {
	for _, slots := range []int{5_000, 50_000} {
		t.Run(fmt.Sprintf("slots=%d", slots), func(t *testing.T) {
			addr, accHash, _, accUpd, pk, upds, _ := whaleByNibble(slots)
			ctx := context.Background()

			oracle := oracleRoot(t, pk, upds)

			ms := NewMockState(t)
			require.NoError(t, ms.applyPlainUpdates(pk, upds))
			node, accPrefix := whaleStorageNode(pk, upds, accHash)

			fp := makeFoldPool(ms, runtime.NumCPU())
			srCur, _, err := fp.foldFreshStorage(ctx, node, accPrefix)
			require.NoError(t, err)

			sr, err := foldFreshStorageRoot(node)
			require.NoError(t, err)
			require.Equal(t, srCur, sr, "direct fold storage root != current fresh-whale fold")

			root, err := wrapAccountRoot(ms, addr, accUpd, sr)
			require.NoError(t, err)
			require.Equal(t, oracle, root, "direct fold account root != sequential oracle")
		})
	}
}

// truthtreeFoldAllocCeiling caps the direct fold's per-op allocation on the 750k fresh-whale
// storage subtree. Buffer reuse keeps it near the proto's ~44 MB serial figure; the naive
// per-node-cell fold the proto rejected sits at ~575 MB (~331 MB for the current copy-replay fold).
// The ceiling sits well above the former and far below the latter, so it catches a buffer-reuse
// regression without pinning an exact byte count.
const truthtreeFoldAllocCeiling = 96 << 20

func freshWhaleFoldNode(tb testing.TB, slots int) *prefixNode {
	tb.Helper()
	_, accHash, _, _, pk, upds, _ := whaleByNibble(slots)
	node, _ := whaleStorageNode(pk, upds, accHash)
	return node
}

func foldFreshWhale(b *testing.B, node *prefixNode) {
	b.ReportAllocs()
	var sink common.Hash
	for b.Loop() {
		h, err := foldFreshStorageRoot(node)
		if err != nil {
			b.Fatal(err)
		}
		sink = h
	}
	runtime.KeepAlive(sink)
}

func Benchmark_TruthtreeFold_FreshWhaleAlloc(b *testing.B) {
	foldFreshWhale(b, freshWhaleFoldNode(b, 750_000))
}

// TestTruthtreeFold_AllocCeiling is the buffer-reuse definition-of-done gate: the direct fold of the
// 750k fresh-whale storage subtree must stay near the proto's ~44 MB figure and never regress toward
// the ~575 MB naive per-node-cell fold.
func TestTruthtreeFold_AllocCeiling(t *testing.T) {
	node := freshWhaleFoldNode(t, 750_000)
	res := testing.Benchmark(func(b *testing.B) { foldFreshWhale(b, node) })
	require.NotZero(t, res.N, "alloc-ceiling bench did not run")
	got := res.AllocedBytesPerOp()
	t.Logf("truthtree fold 750k fresh-whale: %.1f MB/op, %d allocs/op", float64(got)/(1<<20), res.AllocsPerOp())
	require.Lessf(t, got, int64(truthtreeFoldAllocCeiling),
		"fold alloc %.1f MB/op exceeds %.0f MB ceiling — buffer-reuse regression toward the ~575 MB naive fold",
		float64(got)/(1<<20), float64(truthtreeFoldAllocCeiling)/(1<<20))
}

func TestTruthtreeFold_ErrorPaths(t *testing.T) {
	t.Run("nil subtree", func(t *testing.T) {
		h, err := foldFreshStorageRoot(nil)
		require.NoError(t, err)
		require.Equal(t, empty.RootHash, h)
	})
	t.Run("childless subtree", func(t *testing.T) {
		h, err := foldFreshStorageRoot(&prefixNode{})
		require.NoError(t, err)
		require.Equal(t, empty.RootHash, h)
	})
	t.Run("malformed leaf without plainKey", func(t *testing.T) {
		leaf := &prefixNode{} // bitmap 0, plainKey nil -> a leaf that terminates no key
		parent := &prefixNode{bitmap: uint16(1) << 3, children: []*prefixNode{leaf}}
		_, err := foldFreshStorageRoot(parent)
		require.Error(t, err)
	})
}
