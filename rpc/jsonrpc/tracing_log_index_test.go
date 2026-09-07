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
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
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

	// Force-load the native package, to trigger registration
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
)

var (
	// emitOne emits one log and stops.
	emitOneAddr = common.HexToAddress("0x00000000000000000000000000000000000011ff")
	// emitTwo emits two logs and stops.
	emitTwoAddr = common.HexToAddress("0x00000000000000000000000000000000000022ff")
	// emitAround emits a log, calls emitOne, then emits another log, so the
	// callee's log falls between the caller's two.
	emitAroundAddr = common.HexToAddress("0x00000000000000000000000000000000000033ff")
	// emitAndRevert emits a log and then reverts, so the log never survives.
	emitAndRevertAddr = common.HexToAddress("0x00000000000000000000000000000000000044ff")
	// emitAroundRevert is emitAround over emitAndRevert: the callee takes an
	// index and gives it back, so the caller's second log takes it instead.
	emitAroundRevertAddr = common.HexToAddress("0x00000000000000000000000000000000000055ff")
)

// logIndexTestGenesis allocates the log-emitting contracts the numbering tests
// call.
func logIndexTestGenesis(sender common.Address) *types.Genesis {
	log0 := func(p *program.Program) *program.Program {
		return p.Push(0).Push(0).Op(vm.LOG0)
	}
	// The success flag is dropped so a reverting callee does not stop the caller.
	callTo := func(p *program.Program, addr common.Address) *program.Program {
		return p.Call(nil, addr, 0, 0, 0, 0, 0).Op(vm.POP)
	}
	emitter := func(code []byte) types.GenesisAccount {
		return types.GenesisAccount{Code: code, Nonce: 1}
	}
	return &types.Genesis{
		Config: chain.TestChainBerlinConfig,
		Alloc: types.GenesisAlloc{
			sender:      {Balance: big.NewInt(1000000000)},
			emitOneAddr: emitter(log0(program.New()).Op(vm.STOP).Bytes()),
			emitTwoAddr: emitter(log0(log0(program.New())).Op(vm.STOP).Bytes()),
			emitAroundAddr: emitter(
				log0(callTo(log0(program.New()), emitOneAddr)).Op(vm.STOP).Bytes()),
			emitAndRevertAddr: emitter(
				log0(program.New()).Push(0).Push(0).Op(vm.REVERT).Bytes()),
			emitAroundRevertAddr: emitter(
				log0(callTo(log0(program.New()), emitAndRevertAddr)).Op(vm.STOP).Bytes()),
		},
	}
}

// createLogIndexTestModule builds a one-block chain whose four transactions emit
// 2, 1, 3 and 2 surviving logs, across nested and reverting calls. Returns the
// module and the transaction hashes in block order.
func createLogIndexTestModule(t *testing.T) (*execmoduletester.ExecModuleTester, []common.Hash) {
	t.Helper()

	key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
	require.NoError(t, err)
	sender := crypto.PubkeyToAddress(key.PublicKey)

	gspec := logIndexTestGenesis(sender)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(gspec), execmoduletester.WithKey(key))
	signer := *types.LatestSignerForChainID(nil)
	var hashes []common.Hash
	chainPack, err := m.GenerateChain(1, func(i int, b *blockgen.BlockGen) {
		b.SetCoinbase(common.Address{1})
		for nonce, to := range []common.Address{emitTwoAddr, emitOneAddr, emitAroundAddr, emitAroundRevertAddr} {
			txn, err := types.SignTx(
				types.NewTransaction(uint64(nonce), to, &u256.Num0, 200000, &u256.Num1, nil), signer, key)
			require.NoError(t, err)
			b.AddTx(txn)
			hashes = append(hashes, txn.Hash())
		}
	})
	require.NoError(t, err)
	require.NoError(t, m.InsertChain(chainPack))

	return m, hashes
}

type traceLog struct {
	Address  common.Address `json:"address"`
	Index    hexutil.Uint64 `json:"index"`
	Position hexutil.Uint   `json:"position"`
}

type traceFrame struct {
	Error string       `json:"error"`
	Logs  []traceLog   `json:"logs"`
	Calls []traceFrame `json:"calls"`
}

// emittedLogs collects a frame's logs and its children's. The order is the
// tree's, not the block's - each log carries the index and position it was
// given, which is what the expectations pin.
func emittedLogs(frame traceFrame) []traceLog {
	out := frame.Logs
	for i := range frame.Calls {
		out = append(out, emittedLogs(frame.Calls[i])...)
	}
	return out
}

func withLogCallTracer() *tracersConfig.TraceConfig {
	name := "callTracer"
	cfg := json.RawMessage(`{"withLog":true}`)
	return &tracersConfig.TraceConfig{Tracer: &name, TracerConfig: &cfg}
}

// wantByTxn is the block's log numbering, per transaction: the eight surviving
// logs the four transactions emit take 0x0..0x7 over the whole block. Each entry
// is what emittedLogs returns for that transaction, so the entries concatenate
// to what the whole block's trace returns.
var wantByTxn = []struct {
	name string
	want []traceLog
}{
	{
		name: "first transaction starts at zero",
		want: []traceLog{
			{Address: emitTwoAddr, Index: 0, Position: 0},
			{Address: emitTwoAddr, Index: 1, Position: 0},
		},
	},
	{
		name: "two logs already emitted by the block",
		want: []traceLog{
			{Address: emitOneAddr, Index: 2, Position: 0},
		},
	},
	{
		// The callee ran between the caller's two logs, so it holds 0x4 while the
		// caller holds 0x3 and 0x5.
		name: "nested call keeps the block-wide order",
		want: []traceLog{
			{Address: emitAroundAddr, Index: 3, Position: 0},
			{Address: emitAroundAddr, Index: 5, Position: 1},
			{Address: emitOneAddr, Index: 4, Position: 0},
		},
	},
	{
		// The callee's log took 0x7 and the revert gave it back, so the caller's
		// second log takes it and the surviving numbering stays contiguous.
		name: "reverted frame gives its index back",
		want: []traceLog{
			{Address: emitAroundRevertAddr, Index: 6, Position: 0},
			{Address: emitAroundRevertAddr, Index: 7, Position: 1},
		},
	},
}

// TestCallTracerWithLogBlockWideIndex pins that tracing a whole block numbers
// the logs over the block: the second transaction's only log is 0x2 because the
// first emitted two, not 0x0.
func TestCallTracerWithLogBlockWideIndex(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	api := newDebugApiForTest(m)

	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	require.NoError(t, api.TraceBlockByNumber(m.Ctx, rpc.BlockNumber(1), withLogCallTracer(), stream))
	require.NoError(t, stream.Flush())

	var traces []struct {
		Result traceFrame `json:"result"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &traces))
	require.Len(t, traces, len(wantByTxn))

	for i, trace := range traces {
		require.Equal(t, wantByTxn[i].want, emittedLogs(trace.Result), "txn %d", i)
	}

	// Without this the last transaction's numbering would also hold for a callee
	// that emitted nothing: reaching REVERT is what proves its LOG0 ran, and the
	// index it took is the one the caller's second log then takes.
	callee := traces[3].Result.Calls
	require.Len(t, callee, 1)
	require.Equal(t, "execution reverted", callee[0].Error)
	require.Empty(t, callee[0].Logs)
}

// TestCallTracerWithLogIndexOfSingleTransaction pins the same numbering when one
// transaction is traced on its own. The state is built out of history at the
// transaction boundary rather than by replaying the block, so the count the
// earlier transactions left has to be carried in.
func TestCallTracerWithLogIndexOfSingleTransaction(t *testing.T) {
	m, hashes := createLogIndexTestModule(t)
	api := newDebugApiForTest(m)

	for i, tc := range wantByTxn {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			stream := jsonstream.New(&buf)
			require.NoError(t, api.TraceTransaction(m.Ctx, hashes[i], withLogCallTracer(), stream))
			require.NoError(t, stream.Flush())

			var frame traceFrame
			require.NoError(t, json.Unmarshal(buf.Bytes(), &frame))
			require.Equal(t, tc.want, emittedLogs(frame))
		})
	}
}

// TestCallTracerWithLogIndexPerBundle pins that each bundle of
// debug_traceCallMany numbers its logs from zero: a bundle is a block of its
// own, so the count does not carry over from the bundle before it.
func TestCallTracerWithLogIndexPerBundle(t *testing.T) {
	m, _ := createLogIndexTestModule(t)
	api := newDebugApiForTest(m)

	gas := hexutil.Uint64(200000)
	call := ethapi.CallArgs{From: &m.Address, To: &emitTwoAddr, Gas: &gas}
	bundles := []Bundle{{Transactions: []ethapi.CallArgs{call}}, {Transactions: []ethapi.CallArgs{call}}}

	txIndex := -1
	stateCtx := StateContext{BlockNumber: rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber), TransactionIndex: &txIndex}

	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	require.NoError(t, api.TraceCallMany(m.Ctx, bundles, stateCtx, withLogCallTracer(), stream))
	require.NoError(t, stream.Flush())

	var traces [][]traceFrame
	require.NoError(t, json.Unmarshal(buf.Bytes(), &traces))
	require.Len(t, traces, len(bundles))

	for i, bundle := range traces {
		require.Len(t, bundle, 1)
		require.Equal(t, wantByTxn[0].want, emittedLogs(bundle[0]), "bundle %d", i)
	}
}
