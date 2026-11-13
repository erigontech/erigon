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

package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/program"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonstream"
)

// withReplayCPUs makes the block replays of this test see n cores: one core keeps the
// sequential path, more cores take the parallel one.
func withReplayCPUs(t *testing.T, n int) {
	prev := runtime.GOMAXPROCS(n)
	t.Cleanup(func() { runtime.GOMAXPROCS(prev) })
}

// requireSameTraceOnAnyCores traces block 1 with one core and with 16 cores and requires
// the same bytes.
func requireSameTraceOnAnyCores(t *testing.T, m *execmoduletester.ExecModuleTester, config func() *tracersConfig.TraceConfig) {
	api := newDebugApiForTest(m)
	trace := func(cpus int) string {
		withReplayCPUs(t, cpus)
		var buf bytes.Buffer
		stream := jsonstream.New(&buf)
		ctx, cancel := context.WithTimeout(m.Ctx, time.Minute)
		defer cancel()
		require.NoError(t, api.TraceBlockByNumber(ctx, rpc.BlockNumber(1), config(), stream))
		require.NoError(t, stream.Flush())
		return buf.String()
	}
	sequential := trace(1)
	parallel := trace(16)
	require.Equal(t, sequential, parallel)
}

func prestateDiffTracer() *tracersConfig.TraceConfig {
	prestate := "prestateTracer"
	diffMode := json.RawMessage(`{"diffMode":true}`)
	return &tracersConfig.TraceConfig{Tracer: &prestate, TracerConfig: &diffMode}
}

// TestTraceBlockParallelMatchesSequential pins that tracing a block's transactions on
// parallel workers, each on the history state before it, writes the same bytes as replaying
// them one after another on one state.
func TestTraceBlockParallelMatchesSequential(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	configs := map[string]func() *tracersConfig.TraceConfig{
		"callTracer withLog":      withLogCallTracer,
		"prestateTracer diffMode": prestateDiffTracer,
	}
	for name, config := range configs {
		t.Run(name, func(t *testing.T) {
			requireSameTraceOnAnyCores(t, m, config)
		})
	}
}

var clearSlotAddr = common.HexToAddress("0x00000000000000000000000000000000000066ff")

// createClearSlotTestModule builds a one-block chain with two transactions of one sender
// to a contract that clears its storage slot 0: the first one earns a storage refund.
func createClearSlotTestModule(t *testing.T) *execmoduletester.ExecModuleTester {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)
	gspec := &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			sender: {Balance: big.NewInt(1000000000)},
			clearSlotAddr: {
				Code:    program.New().Push(0).Push(0).Op(vm.SSTORE).Op(vm.STOP).Bytes(),
				Storage: map[common.Hash]common.Hash{{}: common.BytesToHash([]byte{1})},
				Nonce:   1,
			},
		},
	}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	signer := *types.LatestSignerForChainID(nil)
	chainPack, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for nonce := range 2 {
			txn, err := types.SignTx(types.NewTransaction(uint64(nonce), clearSlotAddr, &u256.Num0, 100000, &u256.Num1, nil), signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	return m
}

// TestTraceBlockParallelKeepsCarriedState pins that a request whose options change the state
// one transaction leaves to the next is answered as a replay on one state.
func TestTraceBlockParallelKeepsCarriedState(t *testing.T) {
	m := createClearSlotTestModule(t)
	configs := map[string]func() *tracersConfig.TraceConfig{
		"blockOverrides feeRecipient": func() *tracersConfig.TraceConfig {
			config := prestateDiffTracer()
			feeRecipient := common.HexToAddress("0x00000000000000000000000000000000000077ff")
			config.BlockOverrides = &ethapi.BlockOverrides{FeeRecipient: &feeRecipient}
			return config
		},
		"noRefunds": func() *tracersConfig.TraceConfig {
			config := prestateDiffTracer()
			noRefunds := true
			config.NoRefunds = &noRefunds
			return config
		},
	}
	for name, config := range configs {
		t.Run(name, func(t *testing.T) {
			requireSameTraceOnAnyCores(t, m, config)
		})
	}
}

// createManyTxnsTestModule builds a one-block chain of 12 transactions to the log emitting
// contracts: more than the window of the 4 workers that 16 cores give, so they wait for
// the writer.
func createManyTxnsTestModule(t *testing.T) *execmoduletester.ExecModuleTester {
	t.Helper()
	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(logIndexTestGenesis(crypto.PubkeyToAddress(key.PublicKey))), execmoduletester.WithKey(key))
	signer := *types.LatestSignerForChainID(nil)
	contracts := []common.Address{emitTwoAddr, emitOneAddr, emitAroundAddr, emitAroundRevertAddr}
	chainPack, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for nonce := range 12 {
			txn, err := types.SignTx(types.NewTransaction(uint64(nonce), contracts[nonce%len(contracts)], &u256.Num0, 200000, &u256.Num1, nil), signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))
	return m
}

// TestTraceBlockParallelMatchesSequentialBeyondWindow pins the output of a block whose
// workers are held back by the window.
func TestTraceBlockParallelMatchesSequentialBeyondWindow(t *testing.T) {
	m := createManyTxnsTestModule(t)
	withReplayCPUs(t, 16)
	require.Less(t, 2*debugTraceBlockWorkers(12), 12, "the window must be smaller than the block")
	requireSameTraceOnAnyCores(t, m, withLogCallTracer)
}
