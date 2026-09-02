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

package execmodule_test

import (
	"crypto/ecdsa"
	"encoding/binary"
	"flag"
	"fmt"
	"math/big"
	"os"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

// benchCoinbase replaces blockgen's default zero address and is funded in
// genesis. Built from benchAddress rather than a hex literal so it cannot
// collide with a destination and cannot silently decode to the zero address,
// which is the one value that would put the shared coinbase write back.
var benchCoinbase = benchAddress("fees", 0)

const (
	benchGasLimit   = 36_000_000
	benchTxsPerBlok = 1000 // 21M gas, comfortably under the limit
)

// workload decides where a block's value transfers are sent. Every workload
// moves the same value with the same gas and the same transaction count, so
// the destination keys are the only variable.
type workload string

const (
	// warmAccounts pays accounts genesis already created, a fresh slice of the
	// pre-state per block. Reusing one set would rewrite the same keys every
	// block and pile up versions per key, which is a cost that grows with the
	// block count in this arm and not in cold — the two would then only be
	// comparable at one -benchtime.
	warmAccounts workload = "warm"
	// coldAccounts pays an address never seen before, so every transfer
	// inserts a trie key that was not there. It draws from the same address
	// space as warm, past the range genesis allocated: a separate space would
	// put the two arms in different regions of the accounts domain and make
	// key locality a second variable alongside novelty.
	coldAccounts workload = "cold"
	// hotAccount pays one account for the whole block, so it is the arm whose
	// destination conflicts. Senders are distinct everywhere and the coinbase is
	// funded, so the other arms have no shared write for it to be compared
	// against.
	hotAccount workload = "hot"
)

// stateSizes are the accounts genesis creates before the first block runs.
// This is the knob that decides whether the trie is deep enough for an insert
// to split branches; at the small end it is not. Sizes under 100k are absent
// deliberately: stateBudget leaves them too few blocks to measure.
var stateSizes = []int{100_000, 1_000_000}

var windowProfile = flag.String("windowprofile", "",
	"path prefix for a CPU profile covering only the timed block loop; incompatible with -cpuprofile")

var windowProfileSeq atomic.Int64

// BenchmarkValidatePayload times a payload end to end and splits the result:
// ns/op covers InsertBlocks + ValidateChain + UpdateForkChoice, newPayload_ms
// is the first two, fcu_ms the flush. Erigon computes the state root under
// engine_newPayload but flushes under engine_forkchoiceUpdated, and the
// cross-client harnesses time only the first, so newPayload_ms is the number
// comparable to theirs. Splitting by report rather than by toggling the
// benchmark timer keeps testing's ReadMemStats stop-the-world out of the loop,
// where its cost would grow with the heap and land on one window only.
//
// Compare newPayload_ms medians, never ns/op: a single flush excursion of
// 11 ms has been observed against a warm-to-cold difference of 0.8 ms, so
// ns/op reports the flush, not the effect.
//
//	go test -run='^$' -bench=BenchmarkValidatePayload -benchtime=10x -count=10 -cpu=1
//
// Take power from -count, not -benchtime: every repeat rebuilds genesis, so
// repeats add samples without letting one run drift further from its
// pre-state. -benchtime=1x is not usable — first-block page faults land on the
// only timed block and cost cold more than warm.
//
// -cpu=1 is deliberate. The destination cost is fixed CPU work per block, so
// more cores absorb it into parallel slack: at a 100k pre-state it reads ~10%
// on one core and ~3-4% on eighteen. Either is a real number, but only one of
// them is the same number as last week's.
func BenchmarkValidatePayload(b *testing.B) {
	for _, w := range []workload{warmAccounts, coldAccounts, hotAccount} {
		for _, size := range stateSizes {
			b.Run(fmt.Sprintf("%s/%s", w, accountCount(size)), func(b *testing.B) {
				benchmarkValidatePayload(b, w, size)
			})
		}
	}
}

// stateBudget is how much of the pre-state one run may consume, as a fraction.
// Trie depth goes with log16(keys), so holding growth under a tenth keeps every
// point on the state-size sweep within ~0.03 nibbles of its own pre-state and
// of the other points — without it, a fixed -benchtime grows the trie by half
// at one size and a twentieth at the next, and the sweep measures the growth
// rather than the size.
const stateBudget = 0.1

func benchmarkValidatePayload(b *testing.B, w workload, stateSize int) {
	// warm needs a distinct pre-existing account per transaction per block and
	// cold grows the state by the same amount. hot needs neither, but it is
	// bounded too: it rewrites one key every block, so its cost climbs with the
	// block count, and a ratio against warm only means something when both arms
	// ran the same number of blocks.
	if used := float64(b.N*benchTxsPerBlok) / float64(stateSize); used > stateBudget {
		b.Fatalf("%s needs %.2f× the %d-account pre-state over %d blocks (budget %.2f); "+
			"lower -benchtime and raise -count, or raise the state size", w, used, stateSize, b.N, stateBudget)
	}

	// One sender per transaction slot. A single sender would chain every
	// transaction in the block through its own nonce and balance, serialising
	// the parallel executor in every arm and leaving the destination work as a
	// residual on top of it.
	senders := make([]*ecdsa.PrivateKey, benchTxsPerBlok)
	alloc := make(types.GenesisAlloc, stateSize+benchTxsPerBlok)
	senderBalance := new(big.Int).Exp(big.NewInt(10), big.NewInt(22), nil)
	for i := range senders {
		key, err := crypto.GenerateKey()
		require.NoError(b, err)
		senders[i] = key
		alloc[crypto.PubkeyToAddress(key.PublicKey)] = types.GenesisAccount{Balance: senderBalance}
	}
	for i := range stateSize {
		alloc[benchAddress("acct", uint64(i))] = types.GenesisAccount{Balance: big.NewInt(1)}
	}
	// A zero tip still calls AddBalance(coinbase, 0), which on an absent account
	// falls through to a create and records a versioned write. Left as blockgen's
	// unfunded zero address that is a write every transaction in the block shares,
	// which serialises every arm and buries the destination cost under it.
	alloc[benchCoinbase] = types.GenesisAccount{Balance: senderBalance}

	m := execmoduletester.New(b,
		execmoduletester.WithGenesisSpec(&types.Genesis{
			Config:   benchChainConfig(),
			GasLimit: benchGasLimit,
			Alloc:    alloc,
		}),
		execmoduletester.WithKey(senders[0]),
	)
	ctx := b.Context()
	signer := *types.LatestSignerForChainID(nil)

	var coldSeq uint64
	chainResult, err := m.GenerateChain(b.N, func(blockIdx int, bg *blockgen.BlockGen) {
		bg.SetCoinbase(benchCoinbase)
		// Full blocks push the base fee up every block, so the fee cap has to
		// track the header being built rather than the genesis value.
		gasPrice := bg.GetHeader().BaseFee
		for i := range benchTxsPerBlok {
			var to common.Address
			switch w {
			case warmAccounts:
				to = benchAddress("acct", uint64(blockIdx*benchTxsPerBlok+i))
			case coldAccounts:
				to = benchAddress("acct", uint64(stateSize)+coldSeq)
			case hotAccount:
				to = benchAddress("acct", 0)
			}
			coldSeq++
			// Each sender posts one transaction per block, so its nonce is the
			// block index and no two transactions in a block share an account.
			txn, err := types.SignTx(
				types.NewTransaction(uint64(blockIdx), to, uint256.NewInt(1), params.TxGas, gasPrice, nil),
				signer, senders[i],
			)
			require.NoError(b, err)
			bg.AddTx(txn)
		}
	})
	require.NoError(b, err)
	requireAllSucceeded(b, chainResult)

	var totalGas uint64
	var newPayload, fcu time.Duration

	b.ReportAllocs()
	stopWindowProfile := startWindowProfile(b)
	defer stopWindowProfile()
	b.ResetTimer()
	for _, block := range chainResult.Blocks {
		start := time.Now()
		status, err := m.InsertBlocks(ctx, []*types.Block{block})
		require.NoError(b, err)
		require.Equal(b, execmodule.ExecutionStatusSuccess, status)

		result, err := m.ValidateChain(ctx, block.Header())
		require.NoError(b, err)
		require.Equal(b, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
		newPayload += time.Since(start)

		start = time.Now()
		_, err = m.UpdateForkChoice(ctx, block.Header())
		require.NoError(b, err)
		fcu += time.Since(start)

		totalGas += block.GasUsed()
	}
	b.StopTimer()

	perBlock := float64(len(chainResult.Blocks))
	b.ReportMetric(float64(newPayload.Nanoseconds())/perBlock/1e6, "newPayload_ms")
	b.ReportMetric(float64(fcu.Nanoseconds())/perBlock/1e6, "fcu_ms")
	if secs := newPayload.Seconds(); secs > 0 {
		b.ReportMetric((float64(totalGas)/1e6)/secs, "Mgas/s")
	}
	// The executor mode changes what every arm measures and leaves no other
	// trace in the output.
	b.ReportMetric(boolMetric(dbg.Exec3Parallel), "parallelExec")
	b.ReportMetric(gogcPercent(), "gogc")
	b.ReportMetric(float64(b.N*benchTxsPerBlok)/float64(stateSize), "stateGrowth")
}

func gogcPercent() float64 {
	v := os.Getenv("GOGC")
	if v == "off" {
		return -1
	}
	if strings.HasPrefix(v, "+") {
		return 100
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return 100
	}
	return float64(n)
}

func startWindowProfile(b *testing.B) func() {
	if *windowProfile == "" {
		return func() {}
	}
	name := strings.ReplaceAll(b.Name(), "/", "_")
	path := fmt.Sprintf("%s-%s-%d.prof", *windowProfile, name, windowProfileSeq.Add(1))
	f, err := os.Create(path)
	require.NoError(b, err)
	require.NoError(b, pprof.StartCPUProfile(f))
	b.Logf("window cpu profile: %s (b.N=%d)", path, b.N)
	return func() {
		pprof.StopCPUProfile()
		require.NoError(b, f.Close())
	}
}

func boolMetric(v bool) float64 {
	if v {
		return 1
	}
	return 0
}

// benchChainConfig is AllProtocolChanges without Amsterdam, which is where
// mainnet is. Under Amsterdam a transfer to an account that does not exist
// reverts and burns the whole gas limit, which would empty the cold arm.
func benchChainConfig() *chain.Config {
	cfg := chain.AllProtocolChanges.Copy()
	cfg.AmsterdamTime = nil
	return cfg
}

// requireAllSucceeded fails the benchmark if any transaction reverted. A
// reverted transfer still burns gas and still moves the block through
// validation, so the arm reports a plausible number for work it never did.
func requireAllSucceeded(b *testing.B, chainResult *blockgen.ChainPack) {
	b.Helper()
	for blockIdx, receipts := range chainResult.Receipts {
		if len(receipts) != benchTxsPerBlok {
			b.Fatalf("block %d has %d receipts, want %d", blockIdx, len(receipts), benchTxsPerBlok)
		}
		for txIdx, receipt := range receipts {
			if receipt.Status != types.ReceiptStatusSuccessful {
				b.Fatalf("block %d txn %d reverted (gas used %d of %d): the workload is not doing what the arm claims",
					blockIdx, txIdx, receipt.GasUsed, params.TxGas)
			}
		}
	}
}

// benchAddress numbers one address space. Genesis fills indices below
// stateSize, so an index at or above it has never existed — which is the only
// thing that separates a cold destination from a warm one.
func benchAddress(space string, i uint64) common.Address {
	var a common.Address
	copy(a[:4], space)
	binary.BigEndian.PutUint64(a[12:], i+1)
	return a
}

func accountCount(n int) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%dMaccounts", n/1_000_000)
	case n >= 1_000:
		return fmt.Sprintf("%dkaccounts", n/1_000)
	default:
		return fmt.Sprintf("%daccounts", n)
	}
}
