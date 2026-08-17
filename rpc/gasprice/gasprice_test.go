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
	"encoding/binary"
	"errors"
	"math"
	"math/big"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv/kvcache"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/gasprice"
	"github.com/erigontech/erigon/rpc/gasprice/gaspricecfg"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

func newTestBackend(t *testing.T) *execmoduletester.ExecModuleTester {

	var (
		key, _ = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr   = crypto.PubkeyToAddress(key.PublicKey)
		gspec  = &types.Genesis{
			Config: chain.TestChainBerlinConfig,
			Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
		}
		signer = types.LatestSigner(gspec.Config)
	)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	// Generate testing blocks
	chain, err := m.GenerateChain(32, func(i int, b *blockgen.BlockGen) {
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
	if testing.Short() {
		t.Skip("slow test")
	}
	config := gaspricecfg.Config{
		Blocks:     2,
		Percentile: 60,
		Default:    uint256.NewInt(common.GWei),
	}

	m := newTestBackend(t) //, big.NewInt(16), c.pending)
	baseApi := jsonrpc.NewBaseApi(nil, kvcache.NewLatestBatchCache(), m.BlockReader, m.Engine, nil, &rpccfg.BaseApiConfig{Dirs: m.Dirs})

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	cache := jsonrpc.NewGasPriceCache()
	oracle := gasprice.NewOracle(jsonrpc.NewGasPriceOracleBackend(nil, rpchelper.PinToOverlay(tx, nil), baseApi), config, cache, nil, log.New())

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
	for i := range n {
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
	for range pos {
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
		switch {
		case pos == k:
			return values[k]
		case pos < k:
			left = pos + 1
		default:
			right = pos - 1
		}
	}
	return values[left]
}

func TestKthAlgorithmCorrectness(t *testing.T) {
	for i := range iterations {
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
	for i := range iterations {
		testData[i] = generateUint256Slice(sliceSizeSmall)
	}

	for b.Loop() {
		for j := range iterations {
			values := copyUint256Slice(testData[j])
			_ = heapPercentile(values, percentile)
		}
	}
}

func BenchmarkKthPercentile_N20(b *testing.B) {
	testData := make([][]*uint256.Int, iterations)
	for i := range iterations {
		testData[i] = generateUint256Slice(sliceSizeSmall)
	}

	for b.Loop() {
		for j := range iterations {
			values := copyUint256Slice(testData[j])
			index := (len(values) - 1) * percentile / 100
			_ = findKthUint256(values, index)
		}
	}
}

func BenchmarkHeapPercentile(b *testing.B) {
	testData := generateUint256Slice(sliceSizeLarge)

	for b.Loop() {
		values := copyUint256Slice(testData)
		_ = heapPercentile(values, percentile)
	}
}

func BenchmarkKthPercentile(b *testing.B) {
	testData := generateUint256Slice(sliceSizeLarge)

	for b.Loop() {
		values := copyUint256Slice(testData)
		index := (len(values) - 1) * percentile / 100
		_ = findKthUint256(values, index)
	}
}

// mockOracleBackend is a minimal OracleBackend for unit tests.
// HeaderByNumber intentionally ignores ctx cancellation so the oracle can
// proceed past the head-lookup even when the caller's context is already
// cancelled, allowing us to verify that cancellation propagates correctly
// through fetchBlockPricesParallel.
type mockOracleBackend struct {
	head           *types.Header
	frozen         uint64
	canonicalErr   error
	headerCalls    atomic.Int32
	safeBlock      uint64
	finalizedBlock uint64

	canonicalMu     sync.Mutex
	canonicalRanges [][2]uint64
}

// mockHeightHash derives a distinct hash per height, honoring the "one hash
// per block" contract the fee-history cache keys rely on.
func mockHeightHash(number uint64) common.Hash {
	var h common.Hash
	binary.BigEndian.PutUint64(h[:8], number+1)
	return h
}

func (m *mockOracleBackend) HeaderByNumber(_ context.Context, number rpc.BlockNumber) (*types.Header, error) {
	m.headerCalls.Add(1)
	header := types.CopyHeader(m.head)
	switch number {
	case rpc.SafeBlockNumber:
		header.Number.SetUint64(m.safeBlock)
	case rpc.FinalizedBlockNumber:
		header.Number.SetUint64(m.finalizedBlock)
	}
	return header, nil
}

func (m *mockOracleBackend) BlockByNumber(ctx context.Context, _ rpc.BlockNumber) (*types.Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return types.NewBlock(m.head, nil, nil, nil, nil), nil
}

func (m *mockOracleBackend) ChainConfig() *chain.Config { return chain.AllProtocolChanges }

func (m *mockOracleBackend) GetLatestBlockNumber() (uint64, error) {
	return m.head.Number.Uint64(), nil
}

func (m *mockOracleBackend) GetReceiptsGasUsed(_ context.Context, _ *types.Block) (types.Receipts, error) {
	return nil, nil
}

func (m *mockOracleBackend) PendingBlockAndReceipts() (*types.Block, types.Receipts) {
	return nil, nil
}

func (m *mockOracleBackend) CanonicalHashes(_ context.Context, from, to uint64) ([]common.Hash, error) {
	m.canonicalMu.Lock()
	m.canonicalRanges = append(m.canonicalRanges, [2]uint64{from, to})
	m.canonicalMu.Unlock()
	if m.canonicalErr != nil {
		return nil, m.canonicalErr
	}
	hashes := make([]common.Hash, to-from+1)
	for number := from; number <= min(to, m.head.Number.Uint64()); number++ {
		hashes[number-from] = mockHeightHash(number)
	}
	return hashes, nil
}

func (m *mockOracleBackend) resolvedRanges() [][2]uint64 {
	m.canonicalMu.Lock()
	defer m.canonicalMu.Unlock()
	return slices.Clone(m.canonicalRanges)
}

func (m *mockOracleBackend) FrozenBlocks() (uint64, error) {
	return m.frozen, nil
}

func (m *mockOracleBackend) HeaderByHashNumber(ctx context.Context, _ common.Hash, _ uint64) (*types.Header, error) {
	return m.HeaderByNumber(ctx, 0)
}

func (m *mockOracleBackend) BlockByHashNumber(ctx context.Context, _ common.Hash, _ uint64) (*types.Block, error) {
	return m.BlockByNumber(ctx, 0)
}

func (m *mockOracleBackend) Fork(_ context.Context) (gasprice.OracleBackend, func(), error) {
	return nil, nil, nil // sequential mode
}

// TestFeeHistory_CanonicalHashErrorDegradesToUncached pins that a transient
// error on the auxiliary cache-key resolution degrades the block to uncached
// instead of failing the whole request — the number-keyed hit path had no
// failure mode at all.
func TestFeeHistory_CanonicalHashErrorDegradesToUncached(t *testing.T) {
	head := types.NewEmptyHeaderForAssembling()
	head.Number.SetUint64(10)
	head.GasLimit = 30_000_000
	head.BaseFee = uint256.NewInt(1_000_000_000)

	backend := &mockOracleBackend{head: head, canonicalErr: errors.New("transient lookup failure")}
	oracle := gasprice.NewOracle(backend, gaspricecfg.Config{Blocks: 2, Percentile: 60}, jsonrpc.NewGasPriceCache(), gasprice.NewFeeHistoryCache(), log.New())

	_, _, baseFee, _, _, _, err := oracle.FeeHistory(context.Background(), 3, rpc.LatestBlockNumber, nil)
	require.NoError(t, err, "a cache-key resolution error must degrade to uncached, not fail the request")
	require.Len(t, baseFee, 4)
}

// TestFeeHistory_FrozenRangeCachesByNumberWithoutResolution pins that at or
// below the frozen boundary — where the number-to-hash mapping is immutable —
// no per-block hash resolution happens and repeat requests hit the cache.
func TestFeeHistory_FrozenRangeCachesByNumberWithoutResolution(t *testing.T) {
	head := types.NewEmptyHeaderForAssembling()
	head.Number.SetUint64(10)
	head.GasLimit = 30_000_000
	head.BaseFee = uint256.NewInt(1_000_000_000)

	backend := &mockOracleBackend{head: head, frozen: 10}
	oracle := gasprice.NewOracle(backend, gaspricecfg.Config{Blocks: 2, Percentile: 60}, jsonrpc.NewGasPriceCache(), gasprice.NewFeeHistoryCache(), log.New())

	_, _, _, _, _, _, err := oracle.FeeHistory(context.Background(), 4, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Empty(t, backend.resolvedRanges(),
		"the frozen range must not resolve hashes: the mapping is immutable")
	fetchesAfterFirst := backend.headerCalls.Load()

	_, _, _, _, _, _, err = oracle.FeeHistory(context.Background(), 4, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Empty(t, backend.resolvedRanges())
	require.Equal(t, fetchesAfterFirst, backend.headerCalls.Load(),
		"the second request must be served from number-keyed cache entries")
}

// TestFeeHistory_HotRangeResolvedInOneScan pins the cost of the cache-key
// resolution above the frozen boundary: one range resolution per request,
// whatever the window size. Per-block resolution would turn a memoized
// eth_feeHistory into one remote round trip per block in rpcdaemon mode.
func TestFeeHistory_HotRangeResolvedInOneScan(t *testing.T) {
	head := types.NewEmptyHeaderForAssembling()
	head.Number.SetUint64(20)
	head.GasLimit = 30_000_000
	head.BaseFee = uint256.NewInt(1_000_000_000)

	backend := &mockOracleBackend{head: head}
	oracle := gasprice.NewOracle(backend, gaspricecfg.Config{Blocks: 2, Percentile: 60}, jsonrpc.NewGasPriceCache(), gasprice.NewFeeHistoryCache(), log.New())

	_, _, _, _, _, _, err := oracle.FeeHistory(context.Background(), 8, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Equal(t, [][2]uint64{{13, 20}}, backend.resolvedRanges(),
		"the whole hot window must be resolved by a single scan")
	fetchesAfterFirst := backend.headerCalls.Load()

	_, _, _, _, _, _, err = oracle.FeeHistory(context.Background(), 8, rpc.LatestBlockNumber, nil)
	require.NoError(t, err)
	require.Equal(t, [][2]uint64{{13, 20}, {13, 20}}, backend.resolvedRanges(),
		"a warm request must not cost more than its one scan")
	require.Equal(t, fetchesAfterFirst, backend.headerCalls.Load(),
		"the second request must be served from hash-keyed cache entries")
}

func TestFeeHistoryResolvesSafeAndFinalizedBlocks(t *testing.T) {
	head := types.NewEmptyHeaderForAssembling()
	head.Number.SetUint64(25)
	head.BaseFee = uint256.NewInt(1)
	head.GasLimit = 30_000_000
	head.GasUsed = 15_000_000

	backend := &mockOracleBackend{
		head:           head,
		safeBlock:      20,
		finalizedBlock: 18,
	}
	oracle := gasprice.NewOracle(backend, gaspricecfg.Config{
		Blocks:      1,
		MaxPrice:    gaspricecfg.DefaultMaxPrice,
		IgnorePrice: gaspricecfg.DefaultIgnorePrice,
	}, jsonrpc.NewGasPriceCache(), gasprice.NewFeeHistoryCache(), log.New())

	for _, tc := range []struct {
		name        string
		newestBlock rpc.BlockNumber
		wantOldest  uint64
	}{
		{name: "safe", newestBlock: rpc.SafeBlockNumber, wantOldest: 17},
		{name: "finalized", newestBlock: rpc.FinalizedBlockNumber, wantOldest: 15},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				oldest, reward, baseFee, gasUsedRatio, _, _, err := oracle.FeeHistory(t.Context(), 4, tc.newestBlock, []float64{25})
				require.NoError(t, err)
				require.Equal(t, tc.wantOldest, oldest.Uint64())
				require.Len(t, reward, 4)
				require.Len(t, baseFee, 5)
				require.Len(t, gasUsedRatio, 4)
			})
		})
	}
}

func TestFeeHistoryRejectsUnknownNegativeBlockNumber(t *testing.T) {
	head := types.NewEmptyHeaderForAssembling()
	head.Number.SetUint64(25)
	head.BaseFee = uint256.NewInt(1)
	head.GasLimit = 30_000_000
	head.GasUsed = 15_000_000

	backend := &mockOracleBackend{head: head}
	oracle := gasprice.NewOracle(backend, gaspricecfg.Config{
		Blocks:      1,
		MaxPrice:    gaspricecfg.DefaultMaxPrice,
		IgnorePrice: gaspricecfg.DefaultIgnorePrice,
	}, jsonrpc.NewGasPriceCache(), gasprice.NewFeeHistoryCache(), log.New())

	_, _, _, _, _, _, err := oracle.FeeHistory(t.Context(), 4, rpc.BlockNumber(-6), nil)
	require.EqualError(t, err, "invalid block number -6")
}

// TestSuggestTipCap_EmptyBlocksFallbackMatchesGeth verifies that on a chain
// where all sampled blocks are empty, the oracle uses GWei/1000 as the
// fallback price (matching Geth's miner.DefaultConfig.GasPrice) rather than
// 0 from an uninitialized cache.
func TestSuggestTipCap_EmptyBlocksFallbackMatchesGeth(t *testing.T) {
	head := types.NewEmptyHeaderForAssembling()
	head.Number.SetUint64(25)

	backend := &mockOracleBackend{head: head}
	cfg := gaspricecfg.Config{
		Blocks:      20,
		Percentile:  60,
		MaxPrice:    gaspricecfg.DefaultMaxPrice,
		IgnorePrice: gaspricecfg.DefaultIgnorePrice,
	}

	cache := jsonrpc.NewGasPriceCache()
	oracle := gasprice.NewOracle(backend, cfg, cache, nil, log.New())

	got, err := oracle.SuggestTipCap(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, uint64(common.GWei/1000), got.Uint64(),
		"empty-block fallback must be GWei/1000 to match Geth's default start price")
}

// TestSuggestTipCap_ContextCancelled verifies that a cancelled caller context is
// propagated as an error rather than silently returning partial/stale results.
func TestSuggestTipCap_ContextCancelled(t *testing.T) {
	head := types.NewEmptyHeaderForAssembling()
	head.Number.SetUint64(10)

	backend := &mockOracleBackend{head: head}
	cfg := gaspricecfg.Config{
		Blocks:     5,
		Percentile: 60,
		Default:    uint256.NewInt(common.GWei),
	}

	cache := jsonrpc.NewGasPriceCache()
	oracle := gasprice.NewOracle(backend, cfg, cache, nil, log.New())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the call

	_, err := oracle.SuggestTipCap(ctx)
	require.Error(t, err, "cancelled context must propagate as an error, not return partial results")
}

// TestSuggestTipCap_SparseBlocks verifies that the oracle does not panic or error
// on a chain where most blocks are empty and only one has transactions.
// This exercises the two-phase scan and empty-block fallback (fix for #17617).
func TestSuggestTipCap_SparseBlocks(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	key, _ := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	addr := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc:  types.GenesisAlloc{addr: {Balance: big.NewInt(math.MaxInt64)}},
	}
	signer := types.LatestSigner(gspec.Config)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))

	// 10 blocks: only the last one (index 9) has a transaction; all others are empty.
	const totalBlocks = 10
	ch, err := m.GenerateChain(totalBlocks, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		if i == totalBlocks-1 {
			tx, txErr := types.SignTx(
				types.NewTransaction(b.TxNonce(addr), common.HexToAddress("deadbeef"),
					uint256.NewInt(100), 21000, uint256.NewInt(42*common.GWei), nil),
				*signer, key,
			)
			if txErr != nil {
				t.Fatalf("failed to create tx: %v", txErr)
			}
			b.AddTx(tx)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(ch))

	cfg := gaspricecfg.Config{
		Blocks:     5,
		Percentile: 60,
		Default:    uint256.NewInt(common.GWei),
	}
	baseApi := jsonrpc.NewBaseApi(nil, kvcache.NewLatestBatchCache(), m.BlockReader, m.Engine, nil, &rpccfg.BaseApiConfig{Dirs: m.Dirs})

	dbTx, txErr := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, txErr)
	defer dbTx.Rollback()

	cache := jsonrpc.NewGasPriceCache()
	oracle := gasprice.NewOracle(jsonrpc.NewGasPriceOracleBackend(nil, rpchelper.PinToOverlay(dbTx, nil), baseApi), cfg, cache, nil, log.New())

	got, err := oracle.SuggestTipCap(context.Background())
	require.NoError(t, err)
	require.NotNil(t, got, "oracle must return a non-nil price for a sparse chain with at least one transaction")
}

// TestSuggestTipCap_AllEmptyBlocks verifies that the oracle handles a fully empty chain
// (no transactions in any block) without panicking or returning an error.
func TestSuggestTipCap_AllEmptyBlocks(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	gspec := &types.Genesis{Config: chain.AllProtocolChanges}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec))

	const totalBlocks = 5
	ch, err := m.GenerateChain(totalBlocks, func(_ int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		// no transactions — all blocks are empty
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(ch))

	cfg := gaspricecfg.Config{
		Blocks:     5,
		Percentile: 60,
		Default:    uint256.NewInt(common.GWei),
	}
	baseApi := jsonrpc.NewBaseApi(nil, kvcache.NewLatestBatchCache(), m.BlockReader, m.Engine, nil, &rpccfg.BaseApiConfig{Dirs: m.Dirs})

	dbTx, txErr := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, txErr)
	defer dbTx.Rollback()

	cache := jsonrpc.NewGasPriceCache()
	oracle := gasprice.NewOracle(jsonrpc.NewGasPriceOracleBackend(nil, rpchelper.PinToOverlay(dbTx, nil), baseApi), cfg, cache, nil, log.New())

	// With no transactions anywhere, the oracle returns (nil, nil): no price, no error.
	_, err = oracle.SuggestTipCap(context.Background())
	require.NoError(t, err)
}
