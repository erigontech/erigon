// Copyright 2020 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package gasprice_test

import (
	"container/heap"
	"context"
	"math"
	"math/big"
	"math/rand"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/gasprice/gaspricecfg"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

func newTestBackend(t *testing.T) *mock.MockSentry {

	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &types.Genesis{
			Config: chain.TestChainConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		signer = types.LatestSigner(gspec.Config)
	)
	m := mock.MockWithGenesis(t, gspec, key)

	// Generate testing blocks
	chain, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 32, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		tx, txErr := types.SignTx(types.NewTransaction(b.TxNonce(addr), common.HexToAddress("deadbeef"), uint256.NewInt(100), 21000, uint256.NewInt(uint64(int64(i+1)*common.GWei)), nil), *signer, key)
		if txErr != nil {
			t.Fatalf("failed to create tx: %v", txErr)
		}
		b.AddTx(tx)
	})
	if err != nil {
		t.Error(err)
	}
	// Construct testing chain
	if err = m.InsertChain(chain); err != nil {
		t.Error(err)
	}
	return m
}

func TestSuggestPrice(t *testing.T) {
	config := gaspricecfg.Config{
		Blocks:     2,
		Percentile: 60,
		Default:    uint256.NewInt(common.GWei),
	}

	m := newTestBackend(t) //, big.NewInt(16), c.pending)
	baseApi := jsonrpc.NewBaseApi(nil, kvcache.NewDummy(), m.BlockReader, false, rpccfg.DefaultEvmCallTimeout, m.Engine, m.Dirs, nil, 0)

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	cache := jsonrpc.NewGasPriceCache()
	oracle := gasprice.NewOracle(jsonrpc.NewGasPriceOracleBackend(tx, baseApi), config, cache, log.New())

	// The gas price sampled is: 32G, 31G, 30G, 29G, 28G, 27G
	got, err := oracle.SuggestTipCap(context.Background())
	if err != nil {
		t.Fatalf("Failed to retrieve recommended gas price: %v", err)
	}
	expect := common.GWei * uint64(30)
	if got.CmpUint64(expect) != 0 {
		t.Fatalf("Gas price mismatch, want %d, got %d", expect, got)
	}
}

const (
	sliceSizeSmall = 20
	sliceSizeLarge = 3600
	percentile     = 60
	iterations     = 20
)

func generateUint256Slice(n int) []*uint256.Int {
	out := make([]*uint256.Int, n)
	for i := 0; i < n; i++ {
		out[i] = uint256.NewInt(uint64(rand.Int63()))
	}
	return out
}

func copyUint256Slice(src []*uint256.Int) []*uint256.Int {
	dst := make([]*uint256.Int, len(src))
	for i, v := range src {
		dst[i] = new(uint256.Int).Set(v)
	}
	return dst
}

type sortingHeap []*uint256.Int

func (s sortingHeap) Len() int           { return len(s) }
func (s sortingHeap) Less(i, j int) bool { return s[i].Lt(s[j]) }
func (s sortingHeap) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

func (s *sortingHeap) Push(x any) {
	*s = append(*s, x.(*uint256.Int))
}

func (s *sortingHeap) Pop() any {
	old := *s
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*s = old[:n-1]
	return x
}

func heapPercentile(values []*uint256.Int, percentile int) *uint256.Int {
	h := sortingHeap(values)
	heap.Init(&h)
	pos := (h.Len() - 1) * percentile / 100
	for i := 0; i < pos; i++ {
		heap.Pop(&h)
	}
	return h[0]
}

func partitionUint256(values []*uint256.Int, left, right int) int {
	pivot := values[right]
	i := left
	for j := left; j < right; j++ {
		if values[j].Lt(pivot) {
			values[i], values[j] = values[j], values[i]
			i++
		}
	}
	values[i], values[right] = values[right], values[i]
	return i
}

func findKthUint256(values []*uint256.Int, k int) *uint256.Int {
	left, right := 0, len(values)-1
	for left < right {
		pivot := left + rand.Intn(right-left+1)
		values[pivot], values[right] = values[right], values[pivot]
		pos := partitionUint256(values, left, right)
		if pos == k {
			return values[k]
		} else if pos < k {
			left = pos + 1
		} else {
			right = pos - 1
		}
	}
	return values[left]
}

func TestKthAlgorithmCorrectness(t *testing.T) {
	for i := 0; i < iterations; i++ {
		original := generateUint256Slice(sliceSizeSmall)

		// Create independent copies
		heapCopy := copyUint256Slice(original)
		kthCopy := copyUint256Slice(original)

		// Heap-based percentile (current Erigon behavior)
		heapResult := heapPercentile(heapCopy, percentile)

		// K-th / QuickSelect percentile (optimized behavior)
		index := (len(kthCopy) - 1) * percentile / 100
		kthResult := findKthUint256(kthCopy, index)

		// Verify results match
		if heapResult.Cmp(kthResult) != 0 {
			t.Fatalf(
				"Iteration %d: percentile mismatch\nheap=%s\nkth =%s",
				i,
				heapResult.String(),
				kthResult.String(),
			)
		}
	}
}

func BenchmarkHeapPercentile_N20(b *testing.B) {
	testData := make([][]*uint256.Int, iterations)
	for i := 0; i < iterations; i++ {
		testData[i] = generateUint256Slice(sliceSizeSmall)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < iterations; j++ {
			values := copyUint256Slice(testData[j])
			_ = heapPercentile(values, percentile)
		}
	}
}

func BenchmarkKthPercentile_N20(b *testing.B) {
	testData := make([][]*uint256.Int, iterations)
	for i := 0; i < iterations; i++ {
		testData[i] = generateUint256Slice(sliceSizeSmall)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < iterations; j++ {
			values := copyUint256Slice(testData[j])
			index := (len(values) - 1) * percentile / 100
			_ = findKthUint256(values, index)
		}
	}
}

func BenchmarkHeapPercentile(b *testing.B) {
	testData := make([][]*uint256.Int, b.N)
	for i := 0; i < b.N; i++ {
		testData[i] = generateUint256Slice(sliceSizeLarge)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values := copyUint256Slice(testData[i])
		_ = heapPercentile(values, percentile)
	}
}

func BenchmarkKthPercentile(b *testing.B) {
	testData := make([][]*uint256.Int, b.N)
	for i := 0; i < b.N; i++ {
		testData[i] = generateUint256Slice(sliceSizeLarge)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		values := copyUint256Slice(testData[i])
		index := (len(values) - 1) * percentile / 100
		_ = findKthUint256(values, index)
	}
}
