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
	"runtime/debug"
	"runtime/pprof"
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

var benchCoinbase = benchAddress("fees", 0)

const (
	benchGasLimit    = 36_000_000
	benchTxsPerBlock = 1000 // 21M gas, comfortably under the limit
)

type workload string

const (
	warmAccounts workload = "warm"
	coldAccounts workload = "cold"
	hotAccount   workload = "hot"
)

var stateSizes = []struct {
	n    int
	name string
}{
	{100_000, "100kaccounts"},
	{1_000_000, "1Maccounts"},
}

var windowProfile = flag.String("windowprofile", "",
	"path prefix for a CPU profile covering only the timed block loop; incompatible with -cpuprofile")

var windowProfileSeq atomic.Int64

func BenchmarkValidatePayload(b *testing.B) {
	sizes := stateSizes
	if testing.Short() {
		sizes = sizes[:1]
	}
	for _, w := range []workload{warmAccounts, coldAccounts, hotAccount} {
		for _, size := range sizes {
			b.Run(fmt.Sprintf("%s/%s", w, size.name), func(b *testing.B) {
				benchmarkValidatePayload(b, w, size.n)
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
	if used := float64(b.N*benchTxsPerBlock) / float64(stateSize); used > stateBudget {
		b.Fatalf("%s needs %.2f× the %d-account pre-state over %d blocks (budget %.2f); "+
			"lower -benchtime and raise -count, or raise the state size", w, used, stateSize, b.N, stateBudget)
	}

	senders := make([]*ecdsa.PrivateKey, benchTxsPerBlock)
	alloc := make(types.GenesisAlloc, stateSize+benchTxsPerBlock)
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
		gasPrice := bg.GetHeader().BaseFee
		for i := range benchTxsPerBlock {
			var to common.Address
			switch w {
			case warmAccounts:
				to = benchAddress("acct", uint64(blockIdx*benchTxsPerBlock+i))
			case coldAccounts:
				to = benchAddress("acct", uint64(stateSize)+coldSeq)
				coldSeq++
			case hotAccount:
				to = benchAddress("acct", 0)
			}
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
		fcuResult, err := m.UpdateForkChoice(ctx, block.Header())
		require.NoError(b, err)
		require.Equal(b, execmodule.ExecutionStatusSuccess, fcuResult.Status)
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
	b.ReportMetric(boolMetric(dbg.Exec3Parallel), "parallelExec")
	b.ReportMetric(gogcPercent(), "gogc")
	b.ReportMetric(float64(b.N*benchTxsPerBlock)/float64(stateSize), "stateGrowth")
}

func gogcPercent() float64 {
	p := debug.SetGCPercent(100)
	debug.SetGCPercent(p)
	return float64(p)
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

func requireAllSucceeded(b *testing.B, chainResult *blockgen.ChainPack) {
	b.Helper()
	for blockIdx, receipts := range chainResult.Receipts {
		if len(receipts) != benchTxsPerBlock {
			b.Fatalf("block %d has %d receipts, want %d", blockIdx, len(receipts), benchTxsPerBlock)
		}
		for txIdx, receipt := range receipts {
			if receipt.Status != types.ReceiptStatusSuccessful {
				b.Fatalf("block %d txn %d reverted (gas used %d of %d): the workload is not doing what the arm claims",
					blockIdx, txIdx, receipt.GasUsed, params.TxGas)
			}
		}
	}
}

func benchAddress(space string, i uint64) common.Address {
	var a common.Address
	copy(a[:4], space)
	binary.BigEndian.PutUint64(a[12:], i+1)
	return a
}
