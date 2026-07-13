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
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing/tracers/config"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

func TestEmptyQuery(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})
	// Call GetTransactionReceipt for transaction which is not in the database
	var latest = rpc.LatestBlockNumber
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
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})
	// Call GetTransactionReceipt for transaction which is not in the database
	var latest = rpc.LatestBlockNumber
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
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})
	// Call GetTransactionReceipt for transaction which is not in the database
	var latest = rpc.LatestBlockNumber
	results, err := api.CallMany(context.Background(), json.RawMessage(`
[
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","gas":"0x5208","gasPrice":"0x0","value":"0x2"},["trace", "stateDiff"]],
	[{"from":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","to":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["trace", "stateDiff"]]
]
`), &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)

	/*
		Let's assume A - 0x71562b71999873db5b286df957af199ec94617f7 B - 0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b
		A has big balance.
		1. Sending 2 wei from rich existing account to empty account. Gp: 0 wei. Spent: 2 wei
		2. Return 1 wei to initial sender. Gp: 0 wei. Spent: 1 wei.
		Balance new: 1 wei
		Balance old diff is 1 wei.
	*/
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
		b, okConv := res.Balance.(map[string]*hexutil.Big)
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
		b, okConv := res.Balance.(map[string]*hexutil.Big)
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

func TestCorrectStateDiff(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})
	// Call GetTransactionReceipt for transaction which is not in the database
	var latest = rpc.LatestBlockNumber
	results, err := api.CallMany(context.Background(), json.RawMessage(`
[
	[{"from":"0x0D3ab14BBaD3D99F4203bd7a11aCB94882050E7e","to":"0x703c4b2bD70c169f5717101CaeE543299Fc946C7","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["trace", "stateDiff"]],
	[{"from":"0x71562b71999873db5b286df957af199ec94617f7","to":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","gas":"0x5208","gasPrice":"0x0","value":"0x2"},["trace", "stateDiff"]],
	[{"from":"0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b","to":"0x71562b71999873db5b286df957af199ec94617f7","gas":"0x5208","gasPrice":"0x0","value":"0x1"},["trace", "stateDiff"]]
]
`), &rpc.BlockNumberOrHash{BlockNumber: &latest}, nil)

	/*
		C->D 1 wei
		A->B 2 wei
		B->A 1 wei
	*/
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
		b, okConv := res.Balance.(map[string]*hexutil.Big)
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
		b, okConv := res.Balance.(map[string]*hexutil.Big)
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
		b, okConv := res.Balance.(map[string]*hexutil.Big)
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
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})
	var txnHash common.Hash
	if err := m.OverlayDB().View(context.Background(), func(tx kv.Tx) error {
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
	v := addrDiff.Balance.(map[string]*hexutil.Big)["+"].ToInt().Uint64()
	require.Equal(t, uint64(1_000_000_000_000_000), v)
}

func TestReplayBlockTransactions(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})

	// Call GetTransactionReceipt for transaction which is not in the database
	n := rpc.BlockNumber(6)
	results, err := api.ReplayBlockTransactions(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, []string{"stateDiff"}, new(bool), nil)
	if err != nil {
		t.Errorf("calling ReplayBlockTransactions: %v", err)
	}
	require.NotNil(t, results)
	require.NotNil(t, results[0].StateDiff)
	addrDiff := results[0].StateDiff[internedAddress("0x0000000000000001000000000000000000000000")]
	v := addrDiff.Balance.(map[string]*hexutil.Big)["+"].ToInt().Uint64()
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

			statedb, _ := testutil.MakePreState(rules, dbTx, test.Genesis.Alloc, context.BlockNumber)
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

// rawTxFromBlock reads the first transaction from the given block number and
// returns its binary encoding together with the sender and recipient addresses.
func rawTxFromBlock(t *testing.T, m *execmoduletester.ExecModuleTester, blockNum uint64) (encoded []byte, from, to accounts.Address) {
	t.Helper()
	if err := m.OverlayDB().View(context.Background(), func(tx kv.Tx) error {
		b, err := m.BlockReader.BlockByNumber(m.Ctx, tx, blockNum)
		if err != nil {
			return err
		}
		txn := b.Transactions()[0]
		var buf bytes.Buffer
		if err = txn.MarshalBinary(&buf); err != nil {
			return err
		}
		encoded = buf.Bytes()
		signer := types.MakeSigner(m.ChainConfig, b.NumberU64(), b.Time())
		from, err = txn.Sender(*signer)
		if err != nil {
			return err
		}
		if toAddr := txn.GetTo(); toAddr != nil {
			to = accounts.InternAddress(*toAddr)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return
}

func TestRawTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})

	encoded, _, _ := rawTxFromBlock(t, m, 6)
	result, err := api.RawTransaction(context.Background(), encoded, []string{"trace"})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.NotEmpty(t, result.Trace)
}

func TestRawTransactionStateDiff(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})

	encoded, from, to := rawTxFromBlock(t, m, 6)

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
	case map[string]*hexutil.Big:
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
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})

	encoded, _, _ := rawTxFromBlock(t, m, 6)

	result, err := api.RawTransaction(context.Background(), encoded, []string{"vmTrace"})
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotNil(t, result.VmTrace, "VmTrace must be initialised when requested")
	require.Empty(t, result.Trace, "Trace must be empty when not requested")
	require.Nil(t, result.StateDiff, "StateDiff must be nil when not requested")
}

func TestRawTransactionAllTraceTypes(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})

	encoded, _, _ := rawTxFromBlock(t, m, 6)

	result, err := api.RawTransaction(context.Background(), encoded, []string{"trace", "stateDiff", "vmTrace"})
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotEmpty(t, result.Trace, "Trace must be populated")
	require.NotNil(t, result.StateDiff, "StateDiff must be populated")
	require.NotNil(t, result.VmTrace, "VmTrace must be initialised")
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
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})

	tracer := "callTracer"
	var latest = rpc.LatestBlockNumber
	_, err := api.Call(context.Background(), TraceCallParam{}, []string{TraceTypeTrace}, &rpc.BlockNumberOrHash{BlockNumber: &latest}, &config.TraceConfig{Tracer: &tracer})
	require.Error(t, err)
	require.Contains(t, err.Error(), "trace_*")
	require.Contains(t, err.Error(), "debug_*")
}

func TestRawTransactionInvalidType(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})

	encoded, _, _ := rawTxFromBlock(t, m, 6)

	_, err := api.RawTransaction(context.Background(), encoded, []string{"unknown"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unrecognized trace type")
}

func TestTraceCallBlockOverridesBaseFeeAffectsGasPrice(t *testing.T) {
	if testing.Short() {
		t.Skip("slow test")
	}

	m, bankAddr, contractAddr, _ := chainWithDeployedContractAndConfig(t, chain.AllProtocolChanges)
	api := NewTraceAPI(newBaseApiForTest(m), m.OverlayDB(), &httpcfg.HttpCfg{})

	// EVM bytecode: GASPRICE (0x3a), PUSH1 0x00, MSTORE, PUSH1 0x20, PUSH1 0x00, RETURN
	gasPriceCode := hexutil.Bytes{0x3a, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
	result, err := api.Call(context.Background(), TraceCallParam{
		From:                 &bankAddr,
		To:                   &contractAddr,
		MaxFeePerGas:         (*hexutil.Big)(big.NewInt(100)),
		MaxPriorityFeePerGas: (*hexutil.Big)(big.NewInt(2)),
	}, []string{TraceTypeTrace}, nil, &config.TraceConfig{
		StateOverrides: &ethapi.StateOverrides{
			accounts.InternAddress(contractAddr): {Code: &gasPriceCode},
		},
		BlockOverrides: &ethapi.BlockOverrides{
			BaseFeePerGas: (*hexutil.Big)(big.NewInt(10)),
		},
	})
	require.NoError(t, err)
	// effective gas price = BaseFeePerGas(10) + MaxPriorityFeePerGas(2) = 12 = 0xc
	require.Equal(t, "0x000000000000000000000000000000000000000000000000000000000000000c", result.Output.String())
}

// deployCodeReturningOpcode returns CREATE init code that deploys a contract
// whose runtime returns the given zero-argument opcode's value as a 32-byte
// word: <opcode>, PUSH1 0x00, MSTORE, PUSH1 0x20, PUSH1 0x00, RETURN.
func deployCodeReturningOpcode(opcode byte) []byte {
	runtime := []byte{opcode, 0x60, 0x00, 0x52, 0x60, 0x20, 0x60, 0x00, 0xf3}
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

	chainB, err := blockgen.GenerateChain(c.m.ChainConfig, c.head, c.m.Engine, c.m.DB, 1, func(_ int, block *blockgen.BlockGen) {
		gen(block)
	}, c.m.PublishedSD())
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
			BaseFeePerGas: (*hexutil.Big)(baseFee.ToBig()),
		},
	}
}

func (c *baseFeeTestChain) traceAPI() *TraceAPIImpl {
	return NewTraceAPI(newBaseApiForTest(c.m), c.m.OverlayDB(), &httpcfg.HttpCfg{})
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

	calls := fmt.Sprintf(`[[{"from":"%s","to":"%s","maxFeePerGas":"0x77359400","maxPriorityFeePerGas":"0x2"},["trace"]]]`,
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
			override: &ethapi.BlockOverrides{Number: (*hexutil.Big)(big.NewInt(999))},
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
			override: &ethapi.BlockOverrides{BlobBaseFee: (*hexutil.Big)(big.NewInt(777))},
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

			calls := fmt.Sprintf(`[[{"from":"%s","to":"%s"},["trace"]]]`, c.bankAddress.Hex(), contractAddr.Hex())
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
		BlockOverrides: &ethapi.BlockOverrides{Number: (*hexutil.Big)(big.NewInt(1))},
	})
	require.ErrorContains(t, err, "protected txn is not supported by signer")
}
