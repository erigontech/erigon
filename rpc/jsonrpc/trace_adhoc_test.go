// Copyright 2024 The Erigon Authors
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
	"crypto/ecdsa"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/execution/vm/runtime"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func TestEmptyQuery(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	// Call GetTransactionReceipt for transaction which is not in the database
	latest := rpc.LatestBlockNumber
	results, err := api.CallMany(context.Background(), json.RawMessage("[]"), &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)
	if err != nil {
		t.Errorf("calling CallMany: %v", err)
	}
	if results == nil {
		t.Errorf("expected empty array, got nil")
	}
	if len(results) > 0 {
		t.Errorf("expected empty array, got %d elements", len(results))
	}
}

func TestCoinbaseBalance(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	// Call GetTransactionReceipt for transaction which is not in the database
	latest := rpc.LatestBlockNumber
	results, err := api.CallMany(context.Background(), json.RawMessage(`
[
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e","gas":"0x15f90","gasPrice":"0x4a817c800","value":"0x1"},["trace", "stateDiff"]],
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e","gas":"0x15f90","gasPrice":"0x4a817c800","value":"0x1"},["trace", "stateDiff"]]
]
`), &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)
	if err != nil {
		t.Errorf("calling CallMany: %v", err)
	}
	if results == nil {
		t.Errorf("expected empty array, got nil")
	}
	if len(results) != 2 {
		t.Errorf("expected array with 2 elements, got %d elements", len(results))
	}
	// Expect balance increase of the coinbase (zero address)
	if _, ok := results[1].StateDiff[accounts.ZeroAddress]; !ok {
		t.Errorf("expected balance increase for coinbase (zero address)")
	}
}

func internedAddress(addr string) accounts.Address {
	return accounts.InternAddress(common.HexToAddress(addr))
}

func TestSwapBalance(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	// Call GetTransactionReceipt for transaction which is not in the database
	latest := rpc.LatestBlockNumber
	/*
		Let's assume A - 0x71562b71999873db5b286df957af199ec94617f7 B - 0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b
		A has big balance.
		1. Sending 2 wei from rich existing account to empty account. Gp: 0 wei. Spent: 2 wei
		2. Return 1 wei to initial sender. Gp: 0 wei. Spent: 1 wei.
		Balance new: 1 wei
		Balance old diff is 1 wei.
	*/
	results, err := api.CallMany(context.Background(), json.RawMessage(`
[
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","gas":"0x5208","gasPrice":"0x0","value":"0x2"},["trace", "stateDiff"]],
	[{"from":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","to":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["trace", "stateDiff"]]
]
`), &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)
	if err != nil {
		t.Errorf("calling CallMany: %v", err)
	}
	if results == nil {
		t.Errorf("expected empty array, got nil")
	}

	if len(results) != 2 {
		t.Errorf("expected array with 2 elements, got %d elements", len(results))
	}

	// Checking state diff
	if res, ok := results[0].StateDiff[internedAddress("0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b")]; !ok {
		t.Errorf("don't found B in first tx")
	} else {
		b, okConv := res.Balance.(map[string]*hexutil.U256)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			require.Equal(t, uint64(2), b[i].Uint64())
		}
	}

	if res, ok := results[0].StateDiff[internedAddress("0x71562b71999873db5b286df957af199ec94617f7")]; !ok {
		t.Errorf("don't found A in first tx")
	} else {
		b, okConv := res.Balance.(map[string]*StateDiffBalance)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			require.Equal(t, uint64(2), b[i].From.Uint64()-b[i].To.Uint64())
		}
	}

	if res, ok := results[1].StateDiff[internedAddress("0x71562b71999873db5b286df957af199ec94617f7")]; !ok {
		t.Errorf("don't found A in second tx")
	} else {
		b, okConv := res.Balance.(map[string]*StateDiffBalance)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			require.Equal(t, uint64(1), b[i].To.Uint64()-b[i].From.Uint64())
		}
	}

	if res, ok := results[1].StateDiff[internedAddress("0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b")]; !ok {
		t.Errorf("don't found B in second tx")
	} else {
		b, okConv := res.Balance.(map[string]*hexutil.U256)
		if !okConv {
			b := res.Balance.(map[string]*StateDiffBalance)
			for i := range b {
				require.Equal(t, uint64(1), b[i].From.Uint64()-b[i].To.Uint64())
			}
		} else {
			for i := range b {
				require.Equal(t, uint64(1), b[i].Uint64())
			}
		}
	}
}

// Same swap as TestSwapBalance, but call 1 requests only "trace": call 2 spends
// the wei received in call 1, so its stateDiff must match the control run where
// both calls request "stateDiff" — trace-type selection must not change the
// sequential state seen by later calls.
func TestCallManyMixedTraceTypesKeepSequentialState(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	latest := rpc.LatestBlockNumber
	parent := &rpc.BlockNumberOrHash{BlockNumber: &latest}

	const swap = `[
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","gas":"0x5208","gasPrice":"0x0","value":"0x2"},[%s]],
	[{"from":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","to":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["stateDiff"]]
]`
	control, err := api.CallMany(context.Background(), json.RawMessage(fmt.Sprintf(swap, `"trace", "stateDiff"`)), parent, nil)
	require.NoError(t, err)
	require.Len(t, control, 2)

	mixed, err := api.CallMany(context.Background(), json.RawMessage(fmt.Sprintf(swap, `"trace"`)), parent, nil)
	require.NoError(t, err)
	require.Len(t, mixed, 2)

	require.Equal(t, control[1].StateDiff, mixed[1].StateDiff)
}

// Pins the trace-only path: without stateDiff, ibs is never reset, so each call
// still executes on top of the previous calls' state (whole-block replay
// semantics). Call 1 deploys a contract, call 2 invokes it and must see its
// runtime code.
func TestCallManyTraceOnlyKeepsSequentialState(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	latest := rpc.LatestBlockNumber
	parent := &rpc.BlockNumberOrHash{BlockNumber: &latest}

	// Init code deploys runtime code that returns 42.
	const deploy = `[{"from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x30000","gasPrice":"0x0","data":"0x600a600c600039600a6000f3602a60005260206000f3"},["trace"]]`
	deployRes, err := api.CallMany(context.Background(), json.RawMessage("["+deploy+"]"), parent, nil)
	require.NoError(t, err)
	require.Len(t, deployRes, 1)
	created, ok := deployRes[0].Trace[0].Result.(*CreateTraceResult)
	require.True(t, ok)
	require.NotNil(t, created.Address)

	pair := fmt.Sprintf(`[
	%s,
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"%s","gas":"0x30000","gasPrice":"0x0"},["trace"]]
]`, deploy, created.Address)
	results, err := api.CallMany(context.Background(), json.RawMessage(pair), parent, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)
	invoked, ok := results[1].Trace[0].Result.(*TraceResult)
	require.True(t, ok)
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000000000000002a", invoked.Output.String())
}

func TestCorrectStateDiff(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	// Call GetTransactionReceipt for transaction which is not in the database
	latest := rpc.LatestBlockNumber
	/*
		C->D 1 wei
		A->B 2 wei
		B->A 1 wei
	*/
	results, err := api.CallMany(context.Background(), json.RawMessage(`
[
	[{"from":"0x0D3ab14BBaD3D99F4203bd7a11aCB94882050E7e","to":"0x703c4b2bD70c169f5717101CaeE543299Fc946C7","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["trace", "stateDiff"]],
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","gas":"0x5208","gasPrice":"0x0","value":"0x2"},["trace", "stateDiff"]],
	[{"from":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","to":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["trace", "stateDiff"]]
]
`), &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)
	if err != nil {
		t.Errorf("calling CallMany: %v", err)
	}
	if results == nil {
		t.Errorf("expected empty array, got nil")
	}

	if len(results) != 3 {
		t.Errorf("expected array with 3 elements, got %d elements", len(results))
	}

	// Checking state diff
	if _, ok := results[0].StateDiff[internedAddress("0x71562b71999873db5b286df957af199ec94617f7")]; ok {
		t.Errorf("A shouldn't be in first sd")
	}
	if _, ok := results[0].StateDiff[internedAddress("0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b")]; ok {
		t.Errorf("B shouldn't be in first sd")
	}

	if res, ok := results[0].StateDiff[internedAddress("0x703c4b2bD70c169f5717101CaeE543299Fc946C7")]; !ok {
		t.Errorf("don't found C in first tx")
	} else {
		b, okConv := res.Balance.(map[string]*hexutil.U256)
		if !okConv {
			b := res.Balance.(map[string]*StateDiffBalance)
			for i := range b {
				require.Equal(t, uint64(1), b[i].To.Uint64()-b[i].From.Uint64())
			}
		} else {
			for i := range b {
				require.Equal(t, uint64(1), b[i].Uint64())
			}
		}
	}

	if res, ok := results[0].StateDiff[internedAddress("0x0D3ab14BBaD3D99F4203bd7a11aCB94882050E7e")]; !ok {
		t.Errorf("don't found C in first tx")
	} else {
		b, okConv := res.Balance.(map[string]*StateDiffBalance)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			require.Equal(t, uint64(1), b[i].From.Uint64()-b[i].To.Uint64())
		}
	}

	if _, ok := results[1].StateDiff[internedAddress("0x0D3ab14BBaD3D99F4203bd7a11aCB94882050E7e")]; ok {
		t.Errorf("C shouldn't be in second sd")
	}
	if _, ok := results[1].StateDiff[internedAddress("0x703c4b2bD70c169f5717101CaeE543299Fc946C7")]; ok {
		t.Errorf("D shouldn't be in second sd")
	}

	if res, ok := results[1].StateDiff[internedAddress("0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b")]; !ok {
		t.Errorf("don't found B in first tx")
	} else {
		b, okConv := res.Balance.(map[string]*hexutil.U256)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			require.Equal(t, uint64(2), b[i].Uint64())
		}
	}

	if res, ok := results[1].StateDiff[internedAddress("0x71562b71999873db5b286df957af199ec94617f7")]; !ok {
		t.Errorf("don't found A in first tx")
	} else {
		b, okConv := res.Balance.(map[string]*StateDiffBalance)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			require.Equal(t, uint64(2), b[i].From.Uint64()-b[i].To.Uint64())
		}
	}

	if _, ok := results[2].StateDiff[internedAddress("0x0D3ab14BBaD3D99F4203bd7a11aCB94882050E7e")]; ok {
		t.Errorf("C shouldn't be in third sd")
	}
	if _, ok := results[2].StateDiff[internedAddress("0x703c4b2bD70c169f5717101CaeE543299Fc946C7")]; ok {
		t.Errorf("D shouldn't be in third sd")
	}

	if res, ok := results[2].StateDiff[internedAddress("0x71562b71999873db5b286df957af199ec94617f7")]; !ok {
		t.Errorf("don't found A in second tx")
	} else {
		b, okConv := res.Balance.(map[string]*StateDiffBalance)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			require.Equal(t, uint64(1), b[i].To.Uint64()-b[i].From.Uint64())
		}
	}

	if res, ok := results[2].StateDiff[internedAddress("0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b")]; !ok {
		t.Errorf("don't found B in second tx")
	} else {
		b, okConv := res.Balance.(map[string]*hexutil.U256)
		if !okConv {
			b := res.Balance.(map[string]*StateDiffBalance)
			for i := range b {
				require.Equal(t, uint64(1), b[i].From.Uint64()-b[i].To.Uint64())
			}
		} else {
			for i := range b {
				require.Equal(t, uint64(1), b[i].Uint64())
			}
		}
	}
}

func TestReplayTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	var txnHash common.Hash
	if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
		b, err := m.BlockReader.BlockByNumber(m.Ctx, tx, 6)
		if err != nil {
			return err
		}
		txnHash = b.Transactions()[5].Hash()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Call GetTransactionReceipt for transaction which is not in the database
	results, err := api.ReplayTransaction(context.Background(), txnHash, []string{"stateDiff"}, new(bool), nil)
	if err != nil {
		t.Errorf("calling ReplayTransaction: %v", err)
	}
	require.NotNil(t, results)
	require.NotNil(t, results.StateDiff)
	addrDiff := results.StateDiff[internedAddress("0x0000000000000006000000000000000000000000")]
	v := addrDiff.Balance.(map[string]*hexutil.U256)["+"].ToInt().Uint64()
	require.Equal(t, uint64(1_000_000_000_000_000), v)
}

func TestReplayBlockTransactions(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)

	// Call GetTransactionReceipt for transaction which is not in the database
	n := rpc.BlockNumber(6)
	results, err := api.ReplayBlockTransactions(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, []string{"stateDiff"}, new(bool), nil)
	if err != nil {
		t.Errorf("calling ReplayBlockTransactions: %v", err)
	}
	require.NotNil(t, results)
	require.NotNil(t, results[0].StateDiff)
	addrDiff := results[0].StateDiff[internedAddress("0x0000000000000001000000000000000000000000")]
	v := addrDiff.Balance.(map[string]*hexutil.U256)["+"].ToInt().Uint64()
	require.Equal(t, uint64(1_000_000_000_000_000), v)
}

func TestOeTracer(t *testing.T) {
	type callContext struct {
		Number              math.HexOrDecimal64 `json:"number"`
		Hash                common.Hash         `json:"hash"`
		Difficulty          *uint256.Int        `json:"difficulty"`
		Time                math.HexOrDecimal64 `json:"timestamp"`
		GasLimit            math.HexOrDecimal64 `json:"gasLimit"`
		BaseFee             *uint256.Int        `json:"baseFeePerGas"`
		Miner               common.Address      `json:"miner"`
		TransactionHash     common.Hash         `json:"transactionHash"`
		TransactionPosition uint64              `json:"transactionPosition"`
	}

	type testcase struct {
		Genesis      *types.Genesis  `json:"genesis"`
		Context      *callContext    `json:"context"`
		Input        string          `json:"input"`
		TracerConfig json.RawMessage `json:"tracerConfig"`
		Result       []*ParityTrace  `json:"result"`
	}

	dirPath := "oetracer"
	files, err := dir.ReadDir(filepath.Join("testdata", dirPath))
	require.NoError(t, err)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		file := file // capture range variable
		t.Run(strings.TrimSuffix(file.Name(), ".json"), func(t *testing.T) {
			t.Parallel()

			test := new(testcase)
			blob, err := os.ReadFile(filepath.Join("testdata", dirPath, file.Name()))
			require.NoError(t, err)
			err = json.Unmarshal(blob, test)
			require.NoError(t, err)
			tx, err := types.UnmarshalTransactionFromBinary(common.FromHex(test.Input), false /* blobTxnsAreWrappedWithBlobs */)
			require.NoError(t, err)

			// Configure a blockchain with the given prestate
			signer := types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number), uint64(test.Context.Time))
			context := evmtypes.BlockContext{
				CanTransfer: protocol.CanTransfer,
				Transfer:    misc.Transfer,
				Coinbase:    accounts.InternAddress(test.Context.Miner),
				BlockNumber: uint64(test.Context.Number),
				Time:        uint64(test.Context.Time),
				GasLimit:    uint64(test.Context.GasLimit),
			}
			if test.Context.Difficulty != nil {
				context.Difficulty = *test.Context.Difficulty
			}
			if test.Context.BaseFee != nil {
				baseFee := test.Context.BaseFee
				context.BaseFee = *baseFee
			}
			rules := context.Rules(test.Genesis.Config)

			m := execmoduletester.New(t)
			dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()

			statedb, _ := testutil.MakePreState(rules, m.DB, dbTx, test.Genesis.Alloc, context.BlockNumber)
			msg, err := tx.AsMessage(*signer, test.Context.BaseFee, rules)
			require.NoError(t, err)
			txContext := protocol.NewEVMTxContext(msg)

			traceResult := &TraceCallResult{Trace: []*ParityTrace{}}
			tracer := OeTracer{}
			tracer.r = traceResult
			tracer.config, err = parseOeTracerConfig(&config.TraceConfig{TracerConfig: &test.TracerConfig})
			require.NoError(t, err)
			evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Tracer: tracer.Tracer().Hooks})

			st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
			_, err = st.Execute(true /* refunds */, false /* gasBailout */)
			require.NoError(t, err)

			for _, trace := range traceResult.Trace {
				blockNum := uint64(test.Context.Number)
				txnPos := test.Context.TransactionPosition
				trace.BlockHash = &test.Context.Hash
				trace.BlockNumber = &blockNum
				trace.TransactionHash = &test.Context.TransactionHash
				trace.TransactionPosition = &txnPos
			}

			// normalize result by marshalling and unmarshalling again
			// to be able to do equality comparison with expected output
			// (this exists just to ensure ordering of json attributes is the same)
			tracesJsonBytes, err := json.Marshal(traceResult.Trace)
			require.NoError(t, err)
			var normalizedResult []*ParityTrace
			err = json.Unmarshal(tracesJsonBytes, &normalizedResult)
			require.NoError(t, err)

			want, err := json.Marshal(test.Result)
			require.NoError(t, err)
			have, err := json.Marshal(normalizedResult)
			require.NoError(t, err)
			require.Equal(t, string(want), string(have))
		})
	}
}

// signedTransferAtLatest signs a 1-wei transfer from the test chain's funded
// account at its latest nonce, so trace_rawTransaction accepts it.
func signedTransferAtLatest(t *testing.T, m *execmoduletester.ExecModuleTester) (encoded []byte, from, to accounts.Address) {
	t.Helper()
	sender := crypto.PubkeyToAddress(m.Key.PublicKey)
	recipient := common.HexToAddress("0x1234")
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)
	nonce, err := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil).GetTransactionCount(context.Background(), sender, &latest)
	require.NoError(t, err)
	txn, err := types.SignTx(types.NewTransaction(uint64(*nonce), recipient, uint256.NewInt(1), params.TxGas, new(uint256.Int), nil),
		*types.LatestSignerForChainID(m.ChainConfig.ChainID), m.Key)
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, txn.MarshalBinary(&buf))
	return buf.Bytes(), accounts.InternAddress(sender), accounts.InternAddress(recipient)
}

func TestRawTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)

	encoded, _, _ := signedTransferAtLatest(t, m)
	result, err := api.RawTransaction(context.Background(), encoded, []string{"trace"})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Trace)
}

func TestRawTransactionStateDiff(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)

	encoded, from, to := signedTransferAtLatest(t, m)

	result, err := api.RawTransaction(context.Background(), encoded, []string{"stateDiff"})
	require.NoError(t, err)
	require.NotNil(t, result)

	require.Empty(t, result.Trace)
	require.Nil(t, result.VmTrace)

	require.NotNil(t, result.StateDiff, "StateDiff must be populated")
	require.NotEmpty(t, result.StateDiff, "StateDiff must contain at least one entry")

	senderDiff, senderInDiff := result.StateDiff[from]
	require.True(t, senderInDiff, "sender must appear in StateDiff")

	receiverDiff, receiverInDiff := result.StateDiff[to]
	require.True(t, receiverInDiff, "receiver must appear in StateDiff")

	// Sender nonce must increment by exactly 1.
	nonceDiff, ok := senderDiff.Nonce.(map[string]*StateDiffNonce)
	require.True(t, ok, "sender nonce must be a change map")
	n := nonceDiff["*"]
	require.NotNil(t, n, "sender nonce change must be present")
	require.Equal(t, uint64(n.From)+1, uint64(n.To), "sender nonce must increment by 1")

	// Sender balance must decrease (is a change, not "=").
	_, balanceEqual := senderDiff.Balance.(string)
	require.False(t, balanceEqual, "sender balance must change")
	balanceDiff, ok := senderDiff.Balance.(map[string]*StateDiffBalance)
	require.True(t, ok, "sender balance must be a change map")
	bd := balanceDiff["*"]
	require.NotNil(t, bd, "sender balance change entry must be present")
	require.Negative(t, bd.To.ToInt().Cmp(bd.From.ToInt()), "sender balance must decrease")

	// Receiver balance must increase: either a new account ("+") or a change ("*" with To > From).
	switch v := receiverDiff.Balance.(type) {
	case map[string]*hexutil.U256:
		val, exists := v["+"]
		require.True(t, exists, "new receiver account balance must use '+' key")
		require.Positive(t, val.ToInt().Sign(), "receiver initial balance must be positive")
	case map[string]*StateDiffBalance:
		bd2 := v["*"]
		require.NotNil(t, bd2, "receiver balance change entry must be present")
		require.Positive(t, bd2.To.ToInt().Cmp(bd2.From.ToInt()), "receiver balance must increase")
	default:
		t.Fatalf("unexpected receiver balance diff type: %T", receiverDiff.Balance)
	}
}

func TestRawTransactionVmTrace(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)

	encoded, _, _ := signedTransferAtLatest(t, m)

	result, err := api.RawTransaction(context.Background(), encoded, []string{"vmTrace"})
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotNil(t, result.VmTrace, "VmTrace must be initialised when requested")
	require.Empty(t, result.Trace, "Trace must be empty when not requested")
	require.Nil(t, result.StateDiff, "StateDiff must be nil when not requested")
}

func TestRawTransactionAllTraceTypes(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)

	encoded, _, _ := signedTransferAtLatest(t, m)

	result, err := api.RawTransaction(context.Background(), encoded, []string{"trace", "stateDiff", "vmTrace"})
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotEmpty(t, result.Trace, "Trace must be populated")
	require.NotNil(t, result.StateDiff, "StateDiff must be populated")
	require.NotNil(t, result.VmTrace, "VmTrace must be initialised")
}

// stateDiffBalanceDelta returns an account's balance change (to - from) as
// reported in a trace stateDiff.
func stateDiffBalanceDelta(t *testing.T, diff map[accounts.Address]*StateDiffAccount, addr common.Address) *big.Int {
	t.Helper()
	acc, ok := diff[accounts.InternAddress(addr)]
	require.True(t, ok, "%x must appear in stateDiff", addr)
	switch v := acc.Balance.(type) {
	case string:
		require.Equal(t, "=", v)
		return new(big.Int)
	case map[string]*StateDiffBalance:
		return new(big.Int).Sub(v["*"].To.ToInt(), v["*"].From.ToInt())
	case map[string]*hexutil.U256:
		if born, ok := v["+"]; ok {
			return born.ToInt()
		}
		return new(big.Int).Neg(v["-"].ToInt())
	default:
		t.Fatalf("unexpected balance diff type %T", acc.Balance)
		return nil
	}
}

// TestRawTransactionStateDiffChargesFees checks that a signed transaction's
// stateDiff is the transaction's actual transition: the sender pays value plus
// gasUsed times the effective gas price, the fee recipient gets the tip, the
// base fee is burned, and no other ether appears or disappears. A sender that
// cannot pay for its gas is rejected.
func TestRawTransactionStateDiffChargesFees(t *testing.T) {
	c := newBaseFeeTestChain(t, chain.TestChainOsakaConfig)
	coinbase := common.HexToAddress("0xc0ffee")
	c.mineBlock(t, func(block *blockgen.BlockGen) { block.SetCoinbase(coinbase) })
	baseFee := c.head.BaseFee()
	require.Positive(t, baseFee.Sign())

	recipient := common.HexToAddress("0x1234")
	tipCap := uint256.NewInt(2_000_000_000)
	rawTransfer := func(value *uint256.Int) []byte {
		txn, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{
				Nonce:    0,
				To:       &recipient,
				Value:    *value,
				GasLimit: 50_000, // above the 21000 used, so unused gas must not be charged
			},
			ChainID: *c.signer.ChainID(),
			TipCap:  *tipCap,
			FeeCap:  *uint256.NewInt(100_000_000_000),
		}, *c.signer, c.bankKey)
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, txn.MarshalBinary(&buf))
		return buf.Bytes()
	}

	t.Run("stateDiff is the real transition", func(t *testing.T) {
		value := uint256.NewInt(1)
		result, err := c.traceAPI().RawTransaction(context.Background(), rawTransfer(value), []string{TraceTypeStateDiff})
		require.NoError(t, err)

		const gasUsed = 21_000
		tip := new(big.Int).Mul(big.NewInt(gasUsed), tipCap.ToBig())
		burn := new(big.Int).Mul(big.NewInt(gasUsed), baseFee.ToBig())
		senderPays := new(big.Int).Add(value.ToBig(), tip)
		senderPays.Add(senderPays, burn)

		sender := stateDiffBalanceDelta(t, result.StateDiff, c.bankAddress)
		require.Equal(t, new(big.Int).Neg(senderPays).String(), sender.String(), "sender pays value + gasUsed * effective gas price")
		require.Equal(t, tip.String(), stateDiffBalanceDelta(t, result.StateDiff, coinbase).String(), "fee recipient gets the tip")
		require.Equal(t, value.ToBig().String(), stateDiffBalanceDelta(t, result.StateDiff, recipient).String())

		total := new(big.Int)
		for addr := range result.StateDiff {
			total.Add(total, stateDiffBalanceDelta(t, result.StateDiff, addr.Value()))
		}
		require.Equal(t, new(big.Int).Neg(burn).String(), total.String(), "only the base fee leaves circulation")
	})

	t.Run("sender that cannot pay gas limit * fee cap is rejected", func(t *testing.T) {
		// 100 ether in the bank, minus 0.001 ether: enough for value + 50000 gas at the
		// effective price (under 3 gwei), not enough for 50000 gas at the 100 gwei fee cap.
		value, overflow := uint256.FromBig(new(big.Int).Sub(new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil), big.NewInt(1e15)))
		require.False(t, overflow)
		require.Less(t, new(uint256.Int).Add(baseFee, tipCap).Uint64(), uint64(3_000_000_000))
		result, err := c.traceAPI().RawTransaction(context.Background(), rawTransfer(value), []string{TraceTypeTrace})
		require.ErrorIs(t, err, protocol.ErrInsufficientFunds)
		require.Nil(t, result)
	})
}

func TestParseOeTracerConfigRejectsCustomTracer(t *testing.T) {
	tracer := "callTracer"
	_, err := parseOeTracerConfig(&config.TraceConfig{Tracer: &tracer})
	require.Error(t, err)
	require.Contains(t, err.Error(), "trace_*")
	require.Contains(t, err.Error(), "debug_*")
}

func TestParseOeTracerConfigToleratesEmptyTracer(t *testing.T) {
	empty := ""
	_, err := parseOeTracerConfig(&config.TraceConfig{Tracer: &empty})
	require.NoError(t, err)
}

func TestTraceCallRejectsCustomTracer(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)

	tracer := "callTracer"
	latest := rpc.LatestBlockNumber
	_, err := api.Call(context.Background(), TraceCallParam{}, []string{TraceTypeTrace}, &rpc.BlockNumberOrHash{BlockNumber: &latest}, &config.TraceConfig{Tracer: &tracer})
	require.Error(t, err)
	require.Contains(t, err.Error(), "trace_*")
	require.Contains(t, err.Error(), "debug_*")
}

func TestRawTransactionInvalidType(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)

	encoded, _, _ := signedTransferAtLatest(t, m)

	_, err := api.RawTransaction(context.Background(), encoded, []string{"unknown"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unrecognized trace type")
}

func TestTraceCallBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, bankAddr, contractAddr, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := newTraceApiForTest(m)

	// EVM bytecode: GASPRICE (0x3a), PUSH1 0x00, MSTORE, PUSH1 0x20, PUSH1 0x00, RETURN
	gasPriceCode := hexutil.Bytes{0x3a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	result, err := api.Call(context.Background(), TraceCallParam{
		From:                 &bankAddr,
		To:                   &contractAddr,
		MaxFeePerGas:         (*hexutil.U256)(uint256.NewInt(100)),
		MaxPriorityFeePerGas: (*hexutil.U256)(uint256.NewInt(2)),
	}, []string{TraceTypeTrace}, nil, &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(contractAddr): {Code: &gasPriceCode},
		},
		BlockOverrides: &ethapi.BlockOverrides{
			BaseFeePerGas: (*hexutil.U256)(uint256.NewInt(10)),
		},
	})
	require.NoError(t, err)
	// effective gas price = BaseFeePerGas(10) + MaxPriorityFeePerGas(2) = 12 = 0xc
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000000000000000c", result.Output.String())
}

// State overrides are synthetic pre-state, so the stateDiff baseline must be read
// from the overridden state and the override itself must not surface as a change.
func TestTraceCallStateDiffBaselineIncludesStateOverrides(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.AllProtocolChanges)
	api := newTraceApiForTest(m)

	overriddenBalance := (*hexutil.U256)(uint256.MustFromDecimal("7000000000000000000000"))
	recipient := common.HexToAddress("0x00000000000000000000000000000000deadbeef")

	result, err := api.Call(context.Background(), TraceCallParam{
		From:  &bankAddr,
		To:    &recipient,
		Value: (*hexutil.U256)(uint256.NewInt(1)),
	}, []string{TraceTypeStateDiff}, nil, &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(bankAddr): {Balance: &overriddenBalance},
		},
	})
	require.NoError(t, err)

	sender := result.StateDiff[accounts.InternAddress(bankAddr)]
	require.NotNil(t, sender)
	balance, ok := sender.Balance.(map[string]*StateDiffBalance)
	require.True(t, ok, "sender balance must be reported as changed, got %v", sender.Balance)
	require.Equal(t, *overriddenBalance, *balance["*"].From)
}

func TestTraceCallStateDiffIgnoresOverriddenCode(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.AllProtocolChanges)
	api := newTraceApiForTest(m)

	recipient := common.HexToAddress("0x00000000000000000000000000000000deadbeef")
	overriddenCode := hexutil.Bytes{0x60, 0x00, 0x60, 0x00, 0xf3}

	result, err := api.Call(context.Background(), TraceCallParam{
		From:  &bankAddr,
		To:    &recipient,
		Value: (*hexutil.U256)(uint256.NewInt(1)),
	}, []string{TraceTypeStateDiff}, nil, &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(recipient): {Code: &overriddenCode},
		},
	})
	require.NoError(t, err)

	recipientDiff := result.StateDiff[accounts.InternAddress(recipient)]
	require.NotNil(t, recipientDiff)
	require.Equal(t, "=", recipientDiff.Code)
}

func TestTraceCallStateDiffStorageBaselineIncludesStateOverrides(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.AllProtocolChanges)
	api := newTraceApiForTest(m)

	target := common.HexToAddress("0x00000000000000000000000000000000cafe0001")
	slot := common.HexToHash("0x01")
	overriddenSlot := common.HexToHash("0x05")
	// PUSH1 0x09, PUSH1 0x01, SSTORE, STOP
	sstoreCode := hexutil.Bytes{0x60, 0x09, 0x60, 0x01, 0x55, 0x00}

	result, err := api.Call(context.Background(), TraceCallParam{
		From: &bankAddr,
		To:   &target,
	}, []string{TraceTypeStateDiff}, nil, &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(target): {
				Code:      &sstoreCode,
				StateDiff: &map[common.Hash]common.Hash{slot: overriddenSlot},
			},
		},
	})
	require.NoError(t, err)

	targetDiff := result.StateDiff[accounts.InternAddress(target)]
	require.NotNil(t, targetDiff)
	storage, ok := targetDiff.Storage[slot]["*"].(*StateDiffStorage)
	require.True(t, ok, "slot must be reported as changed, got %v", targetDiff.Storage[slot])
	require.Equal(t, overriddenSlot, storage.From)
	require.Equal(t, common.HexToHash("0x09"), storage.To)
}

// An account that is only overridden and never touched by the call stays out of the
// diff: the override is pre-state, not an effect of the traced transaction.
func TestTraceCallStateDiffOmitsUntouchedOverriddenAccount(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.AllProtocolChanges)
	api := newTraceApiForTest(m)

	recipient := common.HexToAddress("0x00000000000000000000000000000000deadbeef")
	untouched := common.HexToAddress("0x00000000000000000000000000000000cafe0002")
	untouchedBalance := (*hexutil.U256)(uint256.NewInt(123))

	result, err := api.Call(context.Background(), TraceCallParam{
		From:  &bankAddr,
		To:    &recipient,
		Value: (*hexutil.U256)(uint256.NewInt(1)),
	}, []string{TraceTypeStateDiff}, nil, &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(untouched): {Balance: &untouchedBalance},
		},
	})
	require.NoError(t, err)
	require.NotContains(t, result.StateDiff, accounts.InternAddress(untouched))
}

// vmTrace attaches a sub only to call and create ops that run a child frame.
func TestTraceCallVmTraceSubs(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.TestChainOsakaConfig)
	api := newTraceApiForTest(m)
	target := common.HexToAddress("0x00000000000000000000000000000000cafe0004")

	for _, tc := range []struct {
		name   string
		code   []byte
		nonce  hexutil.Uint64
		extra  ethapi.StateOverrides
		pc     int
		frame  string
		hasSub bool
	}{
		{
			name:   "call without code",
			code:   []byte{byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.GAS), byte(vm.CALL), byte(vm.STOP)},
			pc:     7,
			frame:  CALL,
			hasSub: true,
		},
		{
			name:  "call with insufficient balance",
			code:  []byte{byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH1), 1, byte(vm.PUSH0), byte(vm.GAS), byte(vm.CALL), byte(vm.STOP)},
			pc:    8,
			frame: CALL,
		},
		{
			name:  "create with insufficient balance",
			code:  []byte{byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH1), 1, byte(vm.CREATE), byte(vm.STOP)},
			pc:    4,
			frame: CREATE,
		},
		{
			name:  "create with nonce overflow",
			code:  []byte{byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.CREATE), byte(vm.STOP)},
			nonce: math.MaxUint64,
			pc:    3,
			frame: CREATE,
		},
		{
			name: "create with address collision",
			code: []byte{byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.CREATE), byte(vm.STOP)},
			extra: ethapi.StateOverrides{
				accounts.InternAddress(types.CreateAddress(target, 0)): {Nonce: new(hexutil.Uint64(1))},
			},
			pc:     3,
			frame:  CREATE,
			hasSub: true,
		},
		{
			name:  "selfdestruct",
			code:  []byte{byte(vm.PUSH0), byte(vm.SELFDESTRUCT)},
			pc:    1,
			frame: SUICIDE,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			code := hexutil.Bytes(tc.code)
			overrides := ethapi.StateOverrides{accounts.InternAddress(target): {Code: &code, Nonce: &tc.nonce}}
			maps.Copy(overrides, tc.extra)
			result, err := api.Call(context.Background(), TraceCallParam{From: &bankAddr, To: &target},
				[]string{TraceTypeTrace, TraceTypeVmTrace}, nil, &config.TraceConfig{StateOverrides: &overrides})
			require.NoError(t, err)
			require.Len(t, result.Trace, 2)
			require.Equal(t, tc.frame, result.Trace[1].Type)

			var op *VmTraceOp
			for _, o := range result.VmTrace.Ops {
				if o.Pc == tc.pc {
					op = o
				}
			}
			require.NotNil(t, op)
			if tc.hasSub {
				require.NotNil(t, op.Sub)
			} else {
				require.Nil(t, op.Sub)
			}
		})
	}
}

// Call data takes the same data/input precedence as eth_call: input wins when both are set.
func TestTraceCallInputField(t *testing.T) {
	m, _, bankAddr := fundedBankGenesis(t, chain.AllProtocolChanges)
	api := newTraceApiForTest(m)

	echo := common.HexToAddress("0x00000000000000000000000000000000cafe0003")
	// CALLDATASIZE, PUSH1 0x00, PUSH1 0x00, CALLDATACOPY, CALLDATASIZE, PUSH1 0x00, RETURN
	echoCode := hexutil.Bytes{0x36, 0x60, 0x00, 0x60, 0x00, 0x37, 0x36, 0x60, 0x00, 0xf3}
	traceConfig := &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(echo): {Code: &echoCode},
		},
	}

	for _, tc := range []struct {
		name   string
		fields string
		output string
	}{
		{name: "data", fields: `"data":"0xaa"`, output: "0xaa"},
		{name: "input", fields: `"input":"0xbb"`, output: "0xbb"},
		{name: "equal", fields: `"data":"0xcc","input":"0xcc"`, output: "0xcc"},
		{name: "input wins", fields: `"data":"0xaa","input":"0xbb"`, output: "0xbb"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var args TraceCallParam
			call := fmt.Sprintf(`{"from":%q,"to":%q,%s}`, bankAddr.Hex(), echo.Hex(), tc.fields)
			require.NoError(t, json.Unmarshal([]byte(call), &args))

			result, err := api.Call(context.Background(), args, []string{TraceTypeTrace}, nil, traceConfig)
			require.NoError(t, err)
			require.Equal(t, tc.output, result.Output.String())
		})
	}
}

func TestCallManyInputField(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	latest := rpc.LatestBlockNumber

	// Init code deploys runtime code that returns 42.
	const deploy = `[[{"from":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x30000","gasPrice":"0x0","input":"0x600a600c600039600a6000f3602a60005260206000f3"},["trace"]]]`
	results, err := api.CallMany(context.Background(), json.RawMessage(deploy), &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)
	require.NoError(t, err)
	require.Len(t, results, 1)
	created, ok := results[0].Trace[0].Result.(*CreateTraceResult)
	require.True(t, ok)
	require.Equal(t, "0x602a60005260206000f3", created.Code.String())
}

// runtimeReturningOpcode returns the given zero-argument opcode's value as a
// 32-byte word: <opcode>, PUSH1 0x00, MSTORE, PUSH1 0x20, PUSH1 0x00, RETURN.
func runtimeReturningOpcode(opcode byte) []byte {
	return []byte{opcode, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
}

// deployCodeReturningOpcode returns CREATE init code that deploys a contract
// whose runtime is runtimeReturningOpcode.
func deployCodeReturningOpcode(opcode byte) []byte {
	runtime := runtimeReturningOpcode(opcode)
	initHeader := []byte{
		0x60, byte(len(runtime)), // PUSH1 length
		0x60, 0x0c, // PUSH1 12 (runtime offset in initcode)
		0x60, 0x00, // PUSH1 0 (memory destination)
		0x39,                     // CODECOPY
		0x60, byte(len(runtime)), // PUSH1 length
		0x60, 0x00, // PUSH1 0 (memory offset)
		0xf3, // RETURN
	}
	return append(initHeader, runtime...)
}

const (
	opCoinbase    = byte(vm.COINBASE)
	opTimestamp   = byte(vm.TIMESTAMP)
	opNumber      = byte(vm.NUMBER)
	opDifficulty  = byte(vm.DIFFICULTY) // PREVRANDAO post-merge
	opGaslimit    = byte(vm.GASLIMIT)
	opGasprice    = byte(vm.GASPRICE)
	opBasefee     = byte(vm.BASEFEE)
	opBlobbasefee = byte(vm.BLOBBASEFEE)
)

// baseFeeTestChain is a funded single-account chain used to test BlockOverrides
// handling: bankKey funds bankAddress at genesis under the given chain config.
// head tracks the current chain tip so successive blocks can be mined on top
// of each other.
type baseFeeTestChain struct {
	m           *execmoduletester.ExecModuleTester
	bankKey     *ecdsa.PrivateKey
	bankAddress common.Address
	signer      *types.Signer
	head        *types.Block
}

func newBaseFeeTestChain(t *testing.T, cfg *chain.Config) *baseFeeTestChain {
	t.Helper()

	m, bankKey, bankAddress := fundedBankGenesis(t, cfg)
	return &baseFeeTestChain{
		m:           m,
		bankKey:     bankKey,
		bankAddress: bankAddress,
		signer:      types.LatestSignerForChainID(m.ChainConfig.ChainID),
		head:        m.Genesis,
	}
}

func (c *baseFeeTestChain) mineBlock(t *testing.T, gen func(*blockgen.BlockGen)) *blockgen.ChainPack {
	t.Helper()

	chainB, err := c.m.GenerateChainFrom(c.head, 1, func(_ int, block *blockgen.BlockGen) {
		gen(block)
	})
	require.NoError(t, err)
	require.NoError(t, c.m.InsertChain(chainB))
	c.head = chainB.TopBlock

	return chainB
}

// deployOpcodeContract mines a block deploying a contract whose runtime
// returns the given opcode's value, and returns its address.
func (c *baseFeeTestChain) deployOpcodeContract(t *testing.T, opcode byte) common.Address {
	t.Helper()

	var contractAddr common.Address
	c.mineBlock(t, func(block *blockgen.BlockGen) {
		nonce := block.TxNonce(c.bankAddress)
		tx, err := types.SignTx(&types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    nonce,
				GasLimit: 500_000,
				Data:     deployCodeReturningOpcode(opcode),
			},
			GasPrice: *uint256.NewInt(1_000_000_000),
		}, *c.signer, c.bankKey)
		require.NoError(t, err)
		block.AddTx(tx)
		contractAddr = types.CreateAddress(c.bankAddress, nonce)
	})

	return contractAddr
}

// callWithDynamicFee mines a block with n EIP-1559 calls to contractAddr and
// returns the call transactions' hashes, the block number they landed in,
// and that block's real (non-overridden) BaseFee.
func (c *baseFeeTestChain) callWithDynamicFee(t *testing.T, contractAddr common.Address, tipCap uint64, n int) (callTxHashes []common.Hash, blockNumber uint64, realBaseFee *uint256.Int) {
	t.Helper()

	chainB := c.mineBlock(t, func(block *blockgen.BlockGen) {
		for range n {
			nonce := block.TxNonce(c.bankAddress)
			tx, err := types.SignTx(&types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{
					Nonce:    nonce,
					To:       &contractAddr,
					GasLimit: 100_000,
				},
				ChainID: *c.signer.ChainID(),
				TipCap:  *uint256.NewInt(tipCap),
				FeeCap:  *uint256.NewInt(1_000_000_000_000),
			}, *c.signer, c.bankKey)
			require.NoError(t, err)
			block.AddTx(tx)
			callTxHashes = append(callTxHashes, tx.Hash())
		}
	})

	callBlock := chainB.Headers[len(chainB.Headers)-1]
	return callTxHashes, callBlock.Number.Uint64(), callBlock.BaseFee
}

// mineParallelEligibleBlock mines a block with 2 calls to contractAddr
// (satisfying doCallBlockParallel's len(txs) > 1 requirement), then mines one
// more block on top so the first is no longer latest (satisfying the
// historical-state-reader requirement). Returns the eligible block's number
// and its real (non-overridden) BaseFee.
func (c *baseFeeTestChain) mineParallelEligibleBlock(t *testing.T, contractAddr common.Address, tipCap uint64) (blockNumber uint64, realBaseFee *uint256.Int) {
	t.Helper()

	_, blockNumber, realBaseFee = c.callWithDynamicFee(t, contractAddr, tipCap, 2)
	c.callWithDynamicFee(t, contractAddr, tipCap, 1)
	return blockNumber, realBaseFee
}

func traceConfigWithBaseFeeOverride(baseFee *uint256.Int) *config.TraceConfig {
	return &config.TraceConfig{
		BlockOverrides: &ethapi.BlockOverrides{
			BaseFeePerGas: (*hexutil.U256)(baseFee),
		},
	}
}

func (c *baseFeeTestChain) traceAPI() *TraceAPIImpl {
	return newTraceApiForTest(c.m)
}

// setupBaseFeeOverrideCall deploys an opcode-emitting contract, mines a
// single EIP-1559 call to it, and returns everything a BlockOverrides
// baseFee-override test needs: the contract address, the call's tx hash, the
// block it landed in, and a baseFee override distinct from the block's real one.
func (c *baseFeeTestChain) setupBaseFeeOverrideCall(t *testing.T, opcode byte, tipCap uint64) (contractAddr common.Address, callTxHash common.Hash, blockNumber uint64, overrideBaseFee *uint256.Int) {
	t.Helper()

	contractAddr = c.deployOpcodeContract(t, opcode)
	callTxHashes, blockNumber, realBaseFee := c.callWithDynamicFee(t, contractAddr, tipCap, 1)
	return contractAddr, callTxHashes[0], blockNumber, new(uint256.Int).AddUint64(realBaseFee, 1_000_000)
}

func TestCallManyBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
	contractAddr := c.deployOpcodeContract(t, opGasprice)
	api := c.traceAPI()

	calls := fmt.Sprintf(`[[{"from":%q,"to":%q,"maxFeePerGas":"0x77359400","maxPriorityFeePerGas":"0x2"},["trace"]]]`,
		c.bankAddress.Hex(), contractAddr.Hex())

	results, err := api.CallMany(context.Background(), json.RawMessage(calls), nil, traceConfigWithBaseFeeOverride(uint256.NewInt(10)))
	require.NoError(t, err)
	require.Len(t, results, 1)
	// effective gas price = BaseFeePerGas(10) + MaxPriorityFeePerGas(2) = 12 = 0xc
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000000000000000c", results[0].Output.String())
}

// blockOverrideOpcodeCase pairs one non-baseFee BlockOverrides field with the
// opcode that observes it and the 32-byte word that opcode should return.
type blockOverrideOpcodeCase struct {
	name     string
	opcode   byte
	override *ethapi.BlockOverrides
	expected []byte
}

func blockOverrideOpcodeCases() []blockOverrideOpcodeCase {
	feeRecipient := common.HexToAddress("0x0000000000000000000000000000000000001234")
	prevRandao := common.HexToHash("0xabababababababababababababababababababababababababababababab")

	return []blockOverrideOpcodeCase{
		{
			name:     "number",
			opcode:   opNumber,
			override: &ethapi.BlockOverrides{Number: (*hexutil.U256)(uint256.NewInt(999))},
			expected: uint256.NewInt(999).PaddedBytes(32),
		},
		{
			name:     "timestamp",
			opcode:   opTimestamp,
			override: &ethapi.BlockOverrides{Time: newUint64(12345)},
			expected: uint256.NewInt(12345).PaddedBytes(32),
		},
		{
			name:     "gasLimit",
			opcode:   opGaslimit,
			override: &ethapi.BlockOverrides{GasLimit: newUint64(30_000_000)},
			expected: uint256.NewInt(30_000_000).PaddedBytes(32),
		},
		{
			name:     "feeRecipient",
			opcode:   opCoinbase,
			override: &ethapi.BlockOverrides{FeeRecipient: &feeRecipient},
			expected: common.LeftPadBytes(feeRecipient[:], 32),
		},
		{
			name:     "prevRandao",
			opcode:   opDifficulty,
			override: &ethapi.BlockOverrides{PrevRandao: &prevRandao},
			expected: prevRandao[:],
		},
		{
			name:     "blobBaseFee",
			opcode:   opBlobbasefee,
			override: &ethapi.BlockOverrides{BlobBaseFee: (*hexutil.U256)(uint256.NewInt(777))},
			expected: uint256.NewInt(777).PaddedBytes(32),
		},
	}
}

// TestCallManyBlockOverridesOtherFieldsAffectOpcodes checks BlockOverrides
// fields other than BaseFeePerGas — number, timestamp, gasLimit,
// feeRecipient, prevRandao, blobBaseFee — all reach the EVM via CallMany,
// not just baseFee.
func TestCallManyBlockOverridesOtherFieldsAffectOpcodes(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	for _, tc := range blockOverrideOpcodeCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
			contractAddr := c.deployOpcodeContract(t, tc.opcode)
			api := c.traceAPI()

			calls := fmt.Sprintf(`[[{"from":%q,"to":%q},["trace"]]]`, c.bankAddress.Hex(), contractAddr.Hex())
			results, err := api.CallMany(context.Background(), json.RawMessage(calls), nil, &config.TraceConfig{
				BlockOverrides: tc.override,
			})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, hexutil.Bytes(tc.expected).String(), results[0].Output.String())
		})
	}
}

func TestReplayTransactionBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	const tipCap = 2
	c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
	_, callTxHash, _, overrideBaseFee := c.setupBaseFeeOverrideCall(t, opGasprice, tipCap)
	api := c.traceAPI()

	result, err := api.ReplayTransaction(context.Background(), callTxHash, []string{"trace"}, new(bool), traceConfigWithBaseFeeOverride(overrideBaseFee))
	require.NoError(t, err)
	require.NotNil(t, result)

	expectedGasPrice := new(uint256.Int).AddUint64(overrideBaseFee, tipCap)
	require.Equal(t, hexutil.Bytes(expectedGasPrice.PaddedBytes(32)).String(), result.Output.String())
}

func TestReplayBlockTransactionsBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	const tipCap = 2
	c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
	_, _, blockNumber, overrideBaseFee := c.setupBaseFeeOverrideCall(t, opGasprice, tipCap)
	api := c.traceAPI()

	n := rpc.BlockNumber(blockNumber)
	results, err := api.ReplayBlockTransactions(c.m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, []string{"trace"}, new(bool), traceConfigWithBaseFeeOverride(overrideBaseFee))
	require.NoError(t, err)
	require.Len(t, results, 1)

	expectedGasPrice := new(uint256.Int).AddUint64(overrideBaseFee, tipCap)
	require.Equal(t, hexutil.Bytes(expectedGasPrice.PaddedBytes(32)).String(), results[0].Output.String())
}

// TestReplayBlockTransactionsParallelPathBlockOverridesBaseFee exercises
// doCallBlockParallel (taken for historical, multi-tx blocks when neither
// stateDiff nor vmTrace is requested), which builds its own BlockContext per
// worker and must apply BlockOverrides independently of the sequential path.
func TestReplayBlockTransactionsParallelPathBlockOverridesBaseFee(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	const tipCap = 2
	c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
	contractAddr := c.deployOpcodeContract(t, opBasefee)
	blockNumber, realBaseFee := c.mineParallelEligibleBlock(t, contractAddr, tipCap)
	api := c.traceAPI()

	overrideBaseFee := new(uint256.Int).AddUint64(realBaseFee, 1_000_000)
	n := rpc.BlockNumber(blockNumber)
	results, err := api.ReplayBlockTransactions(c.m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, []string{"trace"}, new(bool), traceConfigWithBaseFeeOverride(overrideBaseFee))
	require.NoError(t, err)
	require.Len(t, results, 2)

	expected := hexutil.Bytes(overrideBaseFee.PaddedBytes(32)).String()
	require.Equal(t, expected, results[0].Output.String())
	require.Equal(t, expected, results[1].Output.String())
}

// TestReplayBlockTransactionsParallelPathBlockOverridesOtherFieldsAffectOpcodes
// checks that doCallBlockParallel's per-worker BlockContext picks up
// BlockOverrides fields other than BaseFeePerGas too.
func TestReplayBlockTransactionsParallelPathBlockOverridesOtherFieldsAffectOpcodes(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	for _, tc := range blockOverrideOpcodeCases() {
		t.Run(tc.name, func(t *testing.T) {
			c := newBaseFeeTestChain(t, chain.AllProtocolChanges)
			contractAddr := c.deployOpcodeContract(t, tc.opcode)
			blockNumber, _ := c.mineParallelEligibleBlock(t, contractAddr, 2)
			api := c.traceAPI()

			n := rpc.BlockNumber(blockNumber)
			results, err := api.ReplayBlockTransactions(c.m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, []string{"trace"}, new(bool), &config.TraceConfig{
				BlockOverrides: tc.override,
			})
			require.NoError(t, err)
			require.Len(t, results, 2)

			expected := hexutil.Bytes(tc.expected).String()
			require.Equal(t, expected, results[0].Output.String())
			require.Equal(t, expected, results[1].Output.String())
		})
	}
}

// delayedSpuriousDragonConfig activates every fork through Tangerine Whistle
// at genesis but delays Spurious Dragon (which makes EIP-155-protected
// transactions mandatory) to block 3, so tests can probe the
// Homestead/Spurious-Dragon signer boundary.
func delayedSpuriousDragonConfig() *chain.Config {
	return &chain.Config{
		ChainID:               uint256.NewInt(1337),
		Rules:                 chain.EtHashRules,
		HomesteadBlock:        common.NewUint64(0),
		TangerineWhistleBlock: common.NewUint64(0),
		SpuriousDragonBlock:   common.NewUint64(3),
		Ethash:                new(chain.EthashConfig),
	}
}

// mineProtectedTxAtBlock3 mines two empty blocks (still pre-Spurious Dragon)
// then a third block, exactly at Spurious Dragon activation, containing a
// single EIP-155-protected bank-to-bank transfer. Blocks 1-2 are left empty
// so real block validation (which correctly uses each block's own number)
// never has to recover a protected transaction under a signer that rejects
// it. Returns the transfer's hash.
func (c *baseFeeTestChain) mineProtectedTxAtBlock3(t *testing.T) common.Hash {
	t.Helper()

	c.mineBlock(t, func(*blockgen.BlockGen) {})
	c.mineBlock(t, func(*blockgen.BlockGen) {})

	var txHash common.Hash
	c.mineBlock(t, func(block *blockgen.BlockGen) {
		nonce := block.TxNonce(c.bankAddress)
		tx, err := types.SignTx(&types.LegacyTx{
			CommonTx: types.CommonTx{
				Nonce:    nonce,
				To:       &c.bankAddress,
				GasLimit: 21_000,
			},
			GasPrice: *uint256.NewInt(1_000_000_000),
		}, *c.signer, c.bankKey)
		require.NoError(t, err)
		block.AddTx(tx)
		txHash = tx.Hash()
	})
	require.EqualValues(t, 3, c.head.NumberU64())
	return txHash
}

// TestReplayTransactionSignerReflectsBlockOverridesNumber reproduces a bug
// where callTransaction derived fork rules from the BlockOverrides-adjusted
// BlockContext but recovered the transaction sender with a signer built from
// the block's real, un-overridden number. Replaying an EIP-155-protected
// transaction while overriding the block number back before Spurious Dragon
// must fail: a protected legacy transaction cannot be validly interpreted
// under a signer that predates EIP-155.
func TestReplayTransactionSignerReflectsBlockOverridesNumber(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	c := newBaseFeeTestChain(t, delayedSpuriousDragonConfig())
	txHash := c.mineProtectedTxAtBlock3(t)

	api := c.traceAPI()
	_, err := api.ReplayTransaction(context.Background(), txHash, []string{"trace"}, new(bool), &config.TraceConfig{
		BlockOverrides: &ethapi.BlockOverrides{Number: (*hexutil.U256)(uint256.NewInt(1))},
	})
	require.ErrorContains(t, err, "protected txn is not supported by signer")
}

func traceCallValueTransfer() TraceCallParam {
	from := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	to := common.HexToAddress("0x0d3ab14bbad3d99f4203bd7a11acb94882050e7e")
	return TraceCallParam{From: &from, To: &to, Value: (*hexutil.U256)(uint256.NewInt(1))}
}

func TestTraceCallKeepsTraceEmptyWhenNotRequested(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	latest := rpc.LatestBlockNumber

	for _, traceTypes := range [][]string{
		{TraceTypeStateDiff},
		{TraceTypeVmTrace},
		{TraceTypeVmTrace, TraceTypeStateDiff},
	} {
		t.Run(strings.Join(traceTypes, "+"), func(t *testing.T) {
			result, err := api.Call(context.Background(), traceCallValueTransfer(), traceTypes, &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Empty(t, result.Trace)

			// An unrequested trace must serialize as [], never as null.
			encoded, err := json.Marshal(result)
			require.NoError(t, err)
			require.Contains(t, string(encoded), `"trace":[]`)
		})
	}
}

func TestTraceCallPopulatesTraceWhenRequested(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	latest := rpc.LatestBlockNumber

	result, err := api.Call(context.Background(), traceCallValueTransfer(), []string{TraceTypeTrace}, &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Trace)
	require.Nil(t, result.VmTrace)
	require.Nil(t, result.StateDiff)
}

func TestTraceCallWithoutTraceTypes(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	latest := rpc.LatestBlockNumber

	result, err := api.Call(context.Background(), traceCallValueTransfer(), []string{}, &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result.Trace)
	require.Nil(t, result.VmTrace)
	require.Nil(t, result.StateDiff)
}

func TestRawTransactionWithoutTraceTypes(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)

	encoded, _, _ := signedTransferAtLatest(t, m)

	result, err := api.RawTransaction(context.Background(), encoded, []string{})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Empty(t, result.Trace)
	require.Nil(t, result.VmTrace)
	require.Nil(t, result.StateDiff)
}

func TestTraceCallRejectsCustomTracerWithoutTraceType(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	latest := rpc.LatestBlockNumber

	tracer := "callTracer"
	_, err := api.Call(context.Background(), traceCallValueTransfer(), []string{TraceTypeStateDiff}, &rpc.BlockNumberOrHash{BlockNumber: &latest}, &config.TraceConfig{Tracer: &tracer})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not support custom tracers")
}

func TestReplayTransactionInvalidType(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	var txnHash common.Hash
	if err := m.DB.View(context.Background(), func(tx kv.Tx) error {
		b, err := m.BlockReader.BlockByNumber(m.Ctx, tx, 6)
		if err != nil {
			return err
		}
		txnHash = b.Transactions()[5].Hash()
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	_, err := api.ReplayTransaction(context.Background(), txnHash, []string{"unknown"}, new(bool), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unrecognized trace type")
}

// theAddr receives 1e15 wei in block 1 and another 1e15 wei in block 2. With
// parent block 1, a bundle call that touches theAddr must see the post-block-1
// balance, never the one block 2's real transaction produced.
func TestCallManyHistoricalParentPinsStateBoundary(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := newTraceApiForTest(m)
	parentNum := rpc.BlockNumber(1)
	parent := &rpc.BlockNumberOrHash{BlockNumber: &parentNum}

	const bundle = `[
	[{"from":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","to":"0x703c4b2bD70c169f5717101CaeE543299Fc946C7","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["trace"]],
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x0100000000000000000000000000000000000000","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["stateDiff"]]
]`

	results, err := api.CallMany(context.Background(), json.RawMessage(bundle), parent, nil)
	require.NoError(t, err)
	require.Len(t, results, 2)

	diff, ok := results[1].StateDiff[internedAddress("0x0100000000000000000000000000000000000000")]
	require.True(t, ok, "theAddr missing from the second call's stateDiff")
	balance, ok := diff.Balance.(map[string]*StateDiffBalance)
	require.True(t, ok, "unexpected balance diff shape %+v", diff.Balance)
	require.Len(t, balance, 1)
	for _, b := range balance {
		require.Equal(t, uint64(1000000000000000), b.From.ToInt().Uint64(),
			"second bundle call read state past the parent boundary")
		require.Equal(t, uint64(1000000000000001), b.To.ToInt().Uint64())
	}
}

func TestOeTracerMcopyMemory(t *testing.T) {
	for _, tc := range []struct {
		name string
		code string
		off  int
		data string
	}{
		{"copy", "60016000526020600060205e00", 32, "0x" + strings.Repeat("00", 31) + "01"},
		{"overlap", "63010203045f526004601c601d5e00", 29, "0x01020304"},
		{"zero_length", "5f5f60205e00", 0, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := &TraceCallResult{VmTrace: &VmTrace{}}
			tracer := &OeTracer{r: result}
			_, _, err := runtime.Execute(common.FromHex(tc.code), nil, &runtime.Config{
				GasLimit:  1000000,
				EVMConfig: vm.Config{Tracer: tracer.Tracer().Hooks},
			}, t.TempDir())
			require.NoError(t, err)
			var found bool
			for _, op := range result.VmTrace.Ops {
				if op.Op == "MCOPY" {
					found = true
					require.NotNil(t, op.Ex)
					if tc.data == "" {
						require.Nil(t, op.Ex.Mem)
					} else {
						require.Equal(t, &VmTraceMem{Off: tc.off, Data: tc.data}, op.Ex.Mem)
					}
				}
			}
			require.True(t, found)
		})
	}
}

func TestOeTracerStateGasUsage(t *testing.T) {
	result := &TraceCallResult{}
	tracer := &OeTracer{r: result}
	hooks := tracer.Tracer().Hooks
	hooks.OnTxStart(&tracing.VMContext{Rules: &chain.Rules{IsAmsterdam: true}},
		types.NewTransaction(0, accounts.ZeroAddress.Value(), nil, 100_000, nil, nil), accounts.ZeroAddress)
	hooks.EmitEnter(0, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
		mdgas.MdGas{Execution: 1000, State: 200}, uint256.Int{}, nil)
	hooks.EmitEnter(1, byte(vm.CALL), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
		mdgas.MdGas{Execution: 800, State: 200}, uint256.Int{}, nil)
	hooks.EmitExit(1, nil, mdgas.MdGasUsage{Execution: 20, State: -30}, nil, false)
	hooks.EmitExit(0, nil, mdgas.MdGasUsage{Execution: 100, State: 50}, nil, false)
	hooks.EmitTxEnd(&types.Receipt{GasUsed: 37_000},
		mdgas.TxnGasUsage{BlockExecutionGasUsed: 30_000, BlockStateGasUsed: 12_000, GasRefund: 5_000}, nil)
	encoded, err := json.Marshal(result.Trace)
	require.NoError(t, err)
	var frames []map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(encoded, &frames))
	require.Len(t, frames, 2)
	require.JSONEq(t, `{"gasUsed":"0x64","stateGasUsed":"0x32","output":"0x"}`, string(frames[0]["result"]))
	require.JSONEq(t, `{"gasUsed":"0x14","stateGasUsed":"-0x1e","output":"0x"}`, string(frames[1]["result"]))
	for _, field := range []string{"regularGasUsed", "stateGasUsed", "gasRefund"} {
		require.NotContains(t, frames[0], field)
		require.NotContains(t, frames[1], field)
	}
}

func TestOeTracerStateGasUsagePresence(t *testing.T) {
	for _, tc := range []struct {
		name      string
		amsterdam bool
		frameErr  error
	}{
		{name: "pre-Amsterdam"},
		{name: "zero usage", amsterdam: true},
		{name: "reverted creation", amsterdam: true, frameErr: vm.ErrExecutionReverted},
		{name: "failed creation", amsterdam: true, frameErr: vm.ErrOutOfGas},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := &TraceCallResult{}
			tracer := &OeTracer{r: result}
			hooks := tracer.Tracer().Hooks
			hooks.OnTxStart(&tracing.VMContext{Rules: &chain.Rules{IsAmsterdam: tc.amsterdam}},
				types.NewTransaction(0, accounts.ZeroAddress.Value(), nil, 100_000, nil, nil), accounts.ZeroAddress)
			hooks.EmitEnter(0, byte(vm.CREATE), accounts.ZeroAddress, accounts.ZeroAddress, false, nil,
				mdgas.MdGas{Execution: 1000, State: 200}, uint256.Int{}, nil)
			hooks.EmitExit(0, nil, mdgas.MdGasUsage{Execution: 100}, tc.frameErr, tc.frameErr != nil)
			encoded, err := json.Marshal(result.Trace[0])
			require.NoError(t, err)
			var frame map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(encoded, &frame))
			for _, field := range []string{"regularGasUsed", "stateGasUsed", "gasRefund"} {
				require.NotContains(t, frame, field)
			}
			var action map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(frame["action"], &action))
			if tc.amsterdam {
				require.JSONEq(t, `"0xc8"`, string(action["stateGasReservoir"]))
			} else {
				require.NotContains(t, action, "stateGasReservoir")
			}
			if errors.Is(tc.frameErr, vm.ErrOutOfGas) {
				require.JSONEq(t, `null`, string(frame["result"]))
				return
			}
			var resultFields map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(frame["result"], &resultFields))
			if tc.amsterdam {
				require.JSONEq(t, `"0x0"`, string(resultFields["stateGasUsed"]))
			} else {
				require.NotContains(t, resultFields, "stateGasUsed")
			}
		})
	}
}

func TestOeTracerStateGasAfterCall(t *testing.T) {
	ibs := state.New(state.NewNoopReader())
	t.Cleanup(ibs.Close)
	parent := accounts.InternAddress(common.HexToAddress("0x2000"))
	child := accounts.InternAddress(common.HexToAddress("0x1000"))
	require.NoError(t, ibs.SetCode(parent, []byte{
		byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.PUSH0),
		byte(vm.PUSH2), 0x10, 0x00, byte(vm.GAS), byte(vm.CALL), byte(vm.POP), byte(vm.STOP),
	}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(child, []byte{
		byte(vm.PUSH1), 1, byte(vm.PUSH0), byte(vm.SSTORE), byte(vm.STOP),
	}, tracing.CodeChangeUnspecified))
	result := &TraceCallResult{VmTrace: &VmTrace{}}
	tracer := &OeTracer{r: result}
	hooks := tracer.Tracer().Hooks
	env := vm.NewEVM(evmtypes.BlockContext{Transfer: misc.Transfer}, evmtypes.TxContext{}, ibs,
		chain.AllProtocolChanges, vm.Config{Tracer: hooks})
	hooks.OnTxStart(env.GetVMContext(), nil, accounts.ZeroAddress)
	_, remaining, _, err := env.Call(accounts.ZeroAddress, parent, nil,
		mdgas.MdGas{Execution: 500_000, State: 2 * params.StateGasPerStorageSet}, uint256.Int{}, false)
	require.NoError(t, err)
	require.EqualValues(t, params.StateGasPerStorageSet, remaining.State)
	call := result.VmTrace.Ops[len(result.VmTrace.Ops)-3]
	require.Equal(t, "CALL", call.Op)
	require.Equal(t, remaining.State, call.Ex.StateGasRemaining)
}

func TestOeTracerStateGasRemaining(t *testing.T) {
	for _, tc := range []struct {
		name         string
		executionGas uint64
		refill       bool
		wantErr      error
	}{
		{name: "spill", executionGas: 500_000},
		{name: "refill", executionGas: 500_000, refill: true},
		{name: "failed state charge", executionGas: 13_000, wantErr: vm.ErrOutOfGas},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			t.Cleanup(ibs.Close)
			result := &TraceCallResult{VmTrace: &VmTrace{}}
			tracer := &OeTracer{r: result}
			env := vm.NewEVM(evmtypes.BlockContext{}, evmtypes.TxContext{}, ibs, chain.AllProtocolChanges,
				vm.Config{Tracer: tracer.Tracer().Hooks})
			contract := *vm.NewContract(accounts.ZeroAddress, accounts.ZeroAddress, accounts.ZeroAddress, uint256.Int{})
			contract.Code = []byte{byte(vm.PUSH1), 1, byte(vm.PUSH0), byte(vm.SSTORE)}
			if tc.refill {
				contract.Code = append(contract.Code, byte(vm.PUSH0), byte(vm.PUSH0), byte(vm.SSTORE))
			}
			contract.Code = append(contract.Code, byte(vm.STOP))
			_, remaining, used, err := env.Run(contract,
				mdgas.MdGas{Execution: tc.executionGas, State: params.StateGasPerStorageSet / 2}, nil, false)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			if tc.refill || tc.wantErr != nil {
				require.Zero(t, used.StateSpill)
			} else {
				require.Positive(t, used.StateSpill)
			}
			lastStore := result.VmTrace.Ops[2]
			if tc.refill {
				lastStore = result.VmTrace.Ops[len(result.VmTrace.Ops)-2]
			}
			require.Equal(t, "SSTORE", lastStore.Op)
			require.EqualValues(t, remaining.Execution, lastStore.Ex.GasRemaining)
			encoded, err := json.Marshal(result.VmTrace.Ops[2])
			require.NoError(t, err)
			var step map[string]any
			require.NoError(t, json.Unmarshal(encoded, &step))
			require.EqualValues(t, params.StateGasPerStorageSet, step["stateGasCost"])
			require.NotContains(t, step, "stateGasSpill")
			encoded, err = json.Marshal(lastStore.Ex)
			require.NoError(t, err)
			var ex map[string]any
			require.NoError(t, json.Unmarshal(encoded, &ex))
			if remaining.State == 0 {
				require.NotContains(t, ex, "stateUsed")
			} else {
				require.EqualValues(t, remaining.State, ex["stateUsed"])
			}
		})
	}
}

type vmTraceOpContext struct {
	tracing.OpContext
	stack  []uint256.Int
	memory []byte
}

func (c *vmTraceOpContext) StackData() []uint256.Int { return c.stack }
func (c *vmTraceOpContext) MemoryData() []byte       { return c.memory }
func (c *vmTraceOpContext) Gas() mdgas.MdGas         { return mdgas.MdGas{Execution: 1000} }

// TestOeTracerCoversInstructionSet fails when an opcode of the latest fork
// pushes or writes memory but vmTrace does not report it.
func TestOeTracerCoversInstructionSet(t *testing.T) {
	jt := vm.LookupInstructionSet((&evmtypes.BlockContext{}).Rules(chain.AllProtocolChanges))
	scope := &vmTraceOpContext{stack: make([]uint256.Int, 17), memory: make([]byte, 64)}
	for i := range scope.stack {
		scope.stack[i].SetOne()
	}
	for i := range jt {
		op := vm.OpCode(i)
		switch op {
		case vm.CALL, vm.CALLCODE, vm.DELEGATECALL, vm.STATICCALL, vm.CREATE, vm.CREATE2:
			continue // push and mem are reported when the callee returns
		}
		t.Run(op.String(), func(t *testing.T) {
			tracer := &OeTracer{r: &TraceCallResult{VmTrace: &VmTrace{}}}
			for pc, o := range []vm.OpCode{vm.JUMPDEST, op, vm.JUMPDEST} {
				tracer.OnOpcodeV2(uint64(pc), byte(o), mdgas.MdGas{Execution: 1000}, mdgas.MdGas{}, scope, nil, 0, nil)
			}
			ex := tracer.r.VmTrace.Ops[1].Ex
			require.Len(t, ex.Push, jt[op].NumPush())
			switch op {
			case vm.KECCAK256, vm.LOG0, vm.LOG1, vm.LOG2, vm.LOG3, vm.LOG4, vm.RETURN, vm.REVERT:
				require.Nil(t, ex.Mem)
			default:
				require.Equal(t, jt[op].UsesMemory(), ex.Mem != nil)
			}
		})
	}
}

// traceCallFieldsCall is an EIP-1559-priced call from the bank. Tests add fields to it.
const traceCallFieldsCall = `{"from":%q,"to":%q,"gas":"0x493e0","maxFeePerGas":"0x77359400","maxPriorityFeePerGas":"0x77359400","data":"0x"%s}`

// TestTraceCallParamCallArgsFields checks that TraceCallParam converts nonce, chainId,
// blobVersionedHashes and authorizationList the way eth_call's CallArgs does.
func TestTraceCallParamCallArgsFields(t *testing.T) {
	from, to := common.HexToAddress("0xb0b"), common.HexToAddress("0xca11")
	auth := `{"chainId":"0x539","address":"0x0000000000000000000000000000000000001002","nonce":"0x3","yParity":"0x1","r":"0x1111","s":"0x2222"}`
	for _, tc := range []struct {
		name   string
		fields string
	}{
		{name: "nonce", fields: `,"nonce":"0x7"`},
		{name: "chainId", fields: `,"chainId":"0x539"`},
		{name: "blobVersionedHashes", fields: `,"blobVersionedHashes":["0x0100000000000000000000000000000000000000000000000000000000000001"],"maxFeePerBlobGas":"0x2"`},
		{name: "authorizationList", fields: `,"authorizationList":[` + auth + `]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			call := []byte(fmt.Sprintf(traceCallFieldsCall, from.Hex(), to.Hex(), tc.fields))
			var traceArgs TraceCallParam
			require.NoError(t, json.Unmarshal(call, &traceArgs))
			var callArgs ethapi.CallArgs
			require.NoError(t, json.Unmarshal(call, &callArgs))
			baseFee := uint256.NewInt(params.InitialBaseFee)

			traceMsg, err := traceArgs.ToMessage(0, baseFee)
			require.NoError(t, err)
			callMsg, err := callArgs.ToMessage(0, baseFee)
			require.NoError(t, err)
			require.Equal(t, callMsg.Nonce(), traceMsg.Nonce())
			require.Equal(t, callMsg.BlobHashes(), traceMsg.BlobHashes())
			require.Equal(t, callMsg.Authorizations(), traceMsg.Authorizations())

			traceTxn, err := traceArgs.ToTransaction(0, baseFee)
			require.NoError(t, err)
			callTxn, err := callArgs.ToTransaction(0, baseFee)
			require.NoError(t, err)
			require.Equal(t, callTxn.Type(), traceTxn.Type())
			require.Equal(t, callTxn.GetNonce(), traceTxn.GetNonce())
			require.Equal(t, callTxn.GetChainID(), traceTxn.GetChainID())
			require.Equal(t, callTxn.GetBlobHashes(), traceTxn.GetBlobHashes())
			require.Equal(t, callTxn.GetAuthorizations(), traceTxn.GetAuthorizations())
		})
	}
}

// TestTraceCallFields runs call objects with nonce, chainId, blobVersionedHashes and
// authorizationList through trace_call and trace_callMany.
func TestTraceCallFields(t *testing.T) {
	marker := common.HexToAddress("0x1002")
	blobHashReader := common.HexToAddress("0x1003")
	bankKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	bank := crypto.PubkeyToAddress(bankKey.PublicKey)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: chain.TestChainOsakaConfig.Copy(),
		Alloc: types.GenesisAlloc{
			bank: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(20), nil)},
			// PUSH1 42, PUSH0, MSTORE, PUSH1 32, PUSH0, RETURN
			marker: {Code: []byte{0x60, 0x2a, 0x5f, 0x52, 0x60, 0x20, 0x5f, 0xf3}},
			// PUSH0, BLOBHASH, PUSH0, MSTORE, PUSH1 32, PUSH0, RETURN
			blobHashReader: {Code: []byte{0x5f, 0x49, 0x5f, 0x52, 0x60, 0x20, 0x5f, 0xf3}},
		},
	}), execmoduletester.WithKey(bankKey))
	api := newTraceApiForTest(m)
	word42 := common.BigToHash(big.NewInt(42)).Hex()

	authorityKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	authority := crypto.PubkeyToAddress(authorityKey.PublicKey)
	signed, err := types.SignAuthorization(authorityKey, *chain.TestChainOsakaConfig.ChainID, marker, 0)
	require.NoError(t, err)
	authJSON, err := json.Marshal([]types.JsonAuthorization{types.JsonAuthorization{}.FromAuthorization(signed)})
	require.NoError(t, err)
	authorizationList := `,"authorizationList":` + string(authJSON)

	callObject := func(to common.Address, fields string) string {
		return fmt.Sprintf(traceCallFieldsCall, bank.Hex(), to.Hex(), fields)
	}
	traceCall := func(t *testing.T, call string) *TraceCallResult {
		t.Helper()
		var args TraceCallParam
		require.NoError(t, json.Unmarshal([]byte(call), &args))
		res, err := api.Call(context.Background(), args, []string{TraceTypeTrace}, nil, nil)
		require.NoError(t, err)
		return res
	}
	traceCallMany := func(t *testing.T, calls ...string) []*TraceCallResult {
		t.Helper()
		items := make([]string, len(calls))
		for i, call := range calls {
			items[i] = "[" + call + `,["trace"]]`
		}
		res, err := api.CallMany(context.Background(), json.RawMessage("["+strings.Join(items, ",")+"]"), nil, nil)
		require.NoError(t, err)
		require.Len(t, res, len(calls))
		return res
	}
	rootGas := func(res *TraceCallResult) uint64 {
		return res.Trace[0].Action.(*CallTraceAction).Gas.Uint64()
	}

	t.Run("authorizationList", func(t *testing.T) {
		absent := traceCall(t, callObject(authority, ""))
		require.Equal(t, "0x", absent.Output.String(), "without the list the call reaches an account with no code")

		delegated := traceCall(t, callObject(authority, authorizationList))
		require.Equal(t, word42, delegated.Output.String(), "the authorization delegates the authority to the marker")
		require.Equal(t, rootGas(absent)-params.PerEmptyAccountCost, rootGas(delegated), "intrinsic gas charges the authorization")
	})

	t.Run("authorizationList in trace_callMany", func(t *testing.T) {
		res := traceCallMany(t, callObject(authority, authorizationList), callObject(authority, ""))
		require.Equal(t, word42, res[0].Output.String(), "the authorization delegates the authority to the marker")
		require.Equal(t, word42, res[1].Output.String(), "the delegation persists to the next call in the bundle")

		absent := traceCallMany(t, callObject(authority, ""))
		require.Equal(t, "0x", absent[0].Output.String())
	})

	t.Run("blobVersionedHashes", func(t *testing.T) {
		hash := "0x0100000000000000000000000000000000000000000000000000000000000001"
		withHash := traceCall(t, callObject(blobHashReader, `,"maxFeePerBlobGas":"0x77359400","blobVersionedHashes":["`+hash+`"]`))
		require.Equal(t, hash, withHash.Output.String(), "BLOBHASH reads the supplied versioned hash")

		withoutHash := traceCall(t, callObject(blobHashReader, `,"maxFeePerBlobGas":"0x77359400"`))
		require.Equal(t, common.Hash{}.Hex(), withoutHash.Output.String())
	})

	// As in eth_call, the nonce and chainId are not checked against the state or the chain,
	// so neither changes how the call runs.
	for _, tc := range []struct{ name, fields string }{
		{name: "nonce", fields: `,"nonce":"0x7"`},
		{name: "chainId", fields: `,"chainId":"0x1"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, word42, traceCall(t, callObject(marker, tc.fields)).Output.String())
			require.Equal(t, word42, traceCallMany(t, callObject(marker, tc.fields))[0].Output.String())
		})
	}
}

// TestRawTransactionValidatesAgainstLatestState checks that trace_rawTransaction
// runs a signed transaction only if it is valid at the latest state: the nonce
// must equal the state nonce, the sender must afford value plus gas limit times
// fee cap, and the sender must not have code other than an EIP-7702 delegation.
// A valid transaction that reverts or runs out of gas is still traced.
func TestRawTransactionValidatesAgainstLatestState(t *testing.T) {
	const stateNonce = 10
	cfg := chain.TestChainOsakaConfig.Copy()
	key := func(n int64) *ecdsa.PrivateKey {
		k, err := crypto.HexToECDSA(fmt.Sprintf("%064x", n))
		require.NoError(t, err)
		return k
	}
	eoaKey, codeSenderKey, delegatedKey := key(1), key(2), key(3)
	eoa := crypto.PubkeyToAddress(eoaKey.PublicKey)
	marker := common.HexToAddress("0x1002")
	reverter := common.HexToAddress("0x1003")
	oneEther := big.NewInt(common.Ether)
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: cfg,
		Alloc: types.GenesisAlloc{
			eoa: {Balance: oneEther, Nonce: stateNonce},
			crypto.PubkeyToAddress(codeSenderKey.PublicKey): {Balance: oneEther, Nonce: stateNonce, Code: []byte{0x00}},
			crypto.PubkeyToAddress(delegatedKey.PublicKey):  {Balance: oneEther, Nonce: stateNonce, Code: types.AddressToDelegation(accounts.InternAddress(marker))},
			// PUSH1 42, PUSH0, SSTORE, PUSH1 42, PUSH0, MSTORE, PUSH1 32, PUSH0, RETURN
			marker: {Code: []byte{0x60, 0x2a, 0x5f, 0x55, 0x60, 0x2a, 0x5f, 0x52, 0x60, 0x20, 0x5f, 0xf3}},
			// PUSH1 42, PUSH0, MSTORE, PUSH1 32, PUSH0, REVERT
			reverter: {Code: []byte{0x60, 0x2a, 0x5f, 0x52, 0x60, 0x20, 0x5f, 0xfd}},
		},
	}), execmoduletester.WithKey(eoaKey))
	api := newTraceApiForTest(m)
	signer := types.LatestSignerForChainID(cfg.ChainID)
	word42 := common.BigToHash(big.NewInt(42)).Hex()
	feeCap := uint256.NewInt(10 * common.GWei)

	type tx struct {
		key      *ecdsa.PrivateKey
		nonce    uint64
		to       *common.Address // nil for CREATE
		value    *big.Int
		gasLimit uint64
	}
	rawTx := func(t *testing.T, tx tx) []byte {
		t.Helper()
		var data []byte
		if tx.to == nil {
			data = []byte{0x60, 0x00, 0x60, 0x00, 0xf3} // PUSH1 0, PUSH1 0, RETURN
		}
		value, overflow := uint256.FromBig(tx.value)
		require.False(t, overflow)
		signed, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: tx.nonce, To: tx.to, Value: *value, GasLimit: tx.gasLimit, Data: data},
			ChainID:  *cfg.ChainID,
			TipCap:   *uint256.NewInt(1),
			FeeCap:   *feeCap,
		}, *signer, tx.key)
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, signed.MarshalBinary(&buf))
		return buf.Bytes()
	}
	valid := tx{key: eoaKey, nonce: stateNonce, to: &marker, value: big.NewInt(1), gasLimit: 100_000}

	// Leaves enough for gas limit * effective gas price (base fee + 1 wei tip; the
	// genesis block is latest), but not for gas limit * fee cap.
	latestBaseFee := m.Genesis.BaseFee()
	gasAtFeeCap := new(big.Int).Mul(big.NewInt(int64(valid.gasLimit)), feeCap.ToBig())
	gasAtEffectivePrice := new(big.Int).Mul(big.NewInt(int64(valid.gasLimit)), new(big.Int).Add(latestBaseFee.ToBig(), big.NewInt(1)))
	valueBelowFeeCapCost := new(big.Int).Sub(oneEther, new(big.Int).Div(new(big.Int).Add(gasAtFeeCap, gasAtEffectivePrice), big.NewInt(2)))

	for _, tc := range []struct {
		name    string
		modify  func(*tx)
		wantErr error
	}{
		{name: "nonce below state nonce", modify: func(tx *tx) { tx.nonce-- }, wantErr: protocol.ErrNonceTooLow},
		{name: "nonce above state nonce", modify: func(tx *tx) { tx.nonce++ }, wantErr: protocol.ErrNonceTooHigh},
		{name: "create with nonce below state nonce", modify: func(tx *tx) { tx.to = nil; tx.nonce-- }, wantErr: protocol.ErrNonceTooLow},
		{name: "create with nonce above state nonce", modify: func(tx *tx) { tx.to = nil; tx.nonce++ }, wantErr: protocol.ErrNonceTooHigh},
		{name: "value above balance", modify: func(tx *tx) { tx.value = new(big.Int).Add(oneEther, big.NewInt(1)) }, wantErr: protocol.ErrInsufficientFunds},
		{name: "value affordable but not with upfront gas", modify: func(tx *tx) { tx.value = oneEther }, wantErr: protocol.ErrInsufficientFunds},
		{name: "gas affordable at effective price but not at fee cap", modify: func(tx *tx) { tx.value = valueBelowFeeCapCost }, wantErr: protocol.ErrInsufficientFunds},
		{name: "sender with ordinary code", modify: func(tx *tx) { tx.key = codeSenderKey }, wantErr: protocol.ErrSenderNoEOA},
		{name: "gas limit above the EIP-7825 cap", modify: func(tx *tx) { tx.gasLimit = params.MaxTxnGasLimit + 1 }, wantErr: protocol.ErrGasLimitTooHigh},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := valid
			tc.modify(&tx)
			for _, traceTypes := range [][]string{{TraceTypeTrace, TraceTypeStateDiff, TraceTypeVmTrace}, {}} {
				result, err := api.RawTransaction(context.Background(), rawTx(t, tx), traceTypes)
				require.ErrorIs(t, err, tc.wantErr, "trace types %v", traceTypes)
				require.Nil(t, result)
			}
		})
	}

	traceValid := func(t *testing.T, tx tx) *TraceCallResult {
		t.Helper()
		result, err := api.RawTransaction(context.Background(), rawTx(t, tx), []string{TraceTypeTrace})
		require.NoError(t, err)
		require.Len(t, result.Trace, 1)
		return result
	}

	t.Run("valid EOA sender", func(t *testing.T) {
		result := traceValid(t, valid)
		require.Equal(t, word42, result.Output.String())
		require.Empty(t, result.Trace[0].Error)
	})

	t.Run("valid EIP-7702 delegated sender", func(t *testing.T) {
		tx := valid
		tx.key = delegatedKey
		result := traceValid(t, tx)
		require.Equal(t, word42, result.Output.String())
		require.Empty(t, result.Trace[0].Error)
	})

	t.Run("valid create deploys at the signed nonce's address", func(t *testing.T) {
		tx := valid
		tx.to = nil
		result := traceValid(t, tx)
		require.Empty(t, result.Trace[0].Error)
		created, ok := result.Trace[0].Result.(*CreateTraceResult)
		require.True(t, ok, "unexpected result type %T", result.Trace[0].Result)
		require.Equal(t, types.CreateAddress(eoa, stateNonce), *created.Address)
	})

	t.Run("valid transaction that reverts", func(t *testing.T) {
		tx := valid
		tx.to = &reverter
		result := traceValid(t, tx)
		require.Equal(t, "Reverted", result.Trace[0].Error)
		require.Equal(t, word42, result.Output.String())
	})

	t.Run("valid transaction that runs out of gas during execution", func(t *testing.T) {
		tx := valid
		tx.gasLimit = params.TxGas + 5_000 // enough to start, not enough for the SSTORE
		result := traceValid(t, tx)
		require.Equal(t, vm.ErrOutOfGas.Error(), result.Trace[0].Error)
	})
}
