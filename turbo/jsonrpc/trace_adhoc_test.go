package jsonrpc

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/common/hexutil"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/tests"
	"github.com/ledgerwatch/erigon/turbo/stages/mock"
)

func TestEmptyQuery(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})
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
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})
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
	if _, ok := results[1].StateDiff[libcommon.Address{}]; !ok {
		t.Errorf("expected balance increase for coinbase (zero address)")
	}
}

func TestSwapBalance(t *testing.T) {
	t.Log("start swapping balance")
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})
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
		1. Sending 21000 + 2 wei from rich existing to empty account. Gp: 1 wei. Spent: 21000*1+21000+2 wei
		2. Return 1 wei to initial sender. Gp: 1 wei. Spent: 21000*1+1.
		Balance new: 1 wei
		Balance old diff is 21000*2 + 1 wei.
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
	if res, ok := results[0].StateDiff[libcommon.HexToAddress("0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b")]; !ok {
		t.Errorf("don't found B in first tx")
	} else {
		b, okConv := res.Balance.(map[string]*hexutil.Big)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			println("1st 0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b balance", b[i].Uint64())
			require.Equal(t, uint64(2), b[i].Uint64())
		}
	}

	if res, ok := results[0].StateDiff[libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")]; !ok {
		t.Errorf("don't found A in first tx")
	} else {
		b, okConv := res.Balance.(map[string]*StateDiffBalance)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			println("1st 0x71562b71999873db5b286df957af199ec94617f7 diff", b[i].From.Uint64(), b[i].To.Uint64())
			require.Equal(t, uint64(2), b[i].From.Uint64()-b[i].To.Uint64())
		}
	}

	if res, ok := results[1].StateDiff[libcommon.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")]; !ok {
		t.Errorf("don't found A in second tx")
	} else {
		b, okConv := res.Balance.(map[string]*StateDiffBalance)
		if !okConv {
			t.Errorf("bad interface %+v", res.Balance)
		}
		for i := range b {
			println("2nd 0x71562b71999873db5b286df957af199ec94617f7 diff", b[i].From.Uint64(), b[i].To.Uint64())
			require.Equal(t, uint64(1), b[i].To.Uint64()-b[i].From.Uint64())
		}
	}

	if res, ok := results[1].StateDiff[libcommon.HexToAddress("0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b")]; !ok {
		t.Errorf("don't found B in second tx")
	} else {
		b, okConv := res.Balance.(map[string]*hexutil.Big)
		if !okConv {
			b := res.Balance.(map[string]*StateDiffBalance)
			for i := range b {
				println("2nd 0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b diff", b[i].From.Uint64(), b[i].To.Uint64())
				require.Equal(t, uint64(1), b[i].From.Uint64()-b[i].To.Uint64())
			}
		} else {
			for i := range b {
				println("2nd 0x14627ea0e2B27b817DbfF94c3dA383bB73F8C30b balance", b[i].Uint64())
				require.Equal(t, uint64(1), b[i].Uint64())
			}
		}
	}
}

func TestReplayTransaction(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})
	var txnHash libcommon.Hash
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
	addrDiff := results.StateDiff[libcommon.HexToAddress("0x0000000000000006000000000000000000000000")]
	v := addrDiff.Balance.(map[string]*hexutil.Big)["+"].ToInt().Uint64()
	require.Equal(t, uint64(1_000_000_000_000_000), v)
}

func TestReplayBlockTransactions(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestSentry(t)
	api := NewTraceAPI(newBaseApiForTest(m), m.DB, &httpcfg.HttpCfg{})

	// Call GetTransactionReceipt for transaction which is not in the database
	n := rpc.BlockNumber(6)
	results, err := api.ReplayBlockTransactions(m.Ctx, rpc.BlockNumberOrHash{BlockNumber: &n}, []string{"stateDiff"}, new(bool), nil)
	if err != nil {
		t.Errorf("calling ReplayBlockTransactions: %v", err)
	}
	require.NotNil(t, results)
	require.NotNil(t, results[0].StateDiff)
	addrDiff := results[0].StateDiff[libcommon.HexToAddress("0x0000000000000001000000000000000000000000")]
	v := addrDiff.Balance.(map[string]*hexutil.Big)["+"].ToInt().Uint64()
	require.Equal(t, uint64(1_000_000_000_000_000), v)
}

func TestOeTracer(t *testing.T) {
	type callContext struct {
		Number              math.HexOrDecimal64   `json:"number"`
		Hash                libcommon.Hash        `json:"hash"`
		Difficulty          *math.HexOrDecimal256 `json:"difficulty"`
		Time                math.HexOrDecimal64   `json:"timestamp"`
		GasLimit            math.HexOrDecimal64   `json:"gasLimit"`
		BaseFee             *math.HexOrDecimal256 `json:"baseFeePerGas"`
		Miner               libcommon.Address     `json:"miner"`
		TransactionHash     libcommon.Hash        `json:"transactionHash"`
		TransactionPosition uint64                `json:"transactionPosition"`
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
				CanTransfer: core.CanTransfer,
				Transfer:    core.Transfer,
				Coinbase:    test.Context.Miner,
				BlockNumber: uint64(test.Context.Number),
				Time:        uint64(test.Context.Time),
				Difficulty:  (*big.Int)(test.Context.Difficulty),
				GasLimit:    uint64(test.Context.GasLimit),
			}
			if test.Context.BaseFee != nil {
				context.BaseFee, _ = uint256.FromBig((*big.Int)(test.Context.BaseFee))
			}
			rules := test.Genesis.Config.Rules(context.BlockNumber, context.Time)

			m := mock.Mock(t)
			dbTx, err := m.DB.BeginRw(m.Ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()

			statedb, _ := tests.MakePreState(rules, dbTx, test.Genesis.Alloc, context.BlockNumber)
			msg, err := tx.AsMessage(*signer, (*big.Int)(test.Context.BaseFee), rules)
			require.NoError(t, err)
			txContext := core.NewEVMTxContext(msg)

			traceResult := &TraceCallResult{Trace: []*ParityTrace{}}
			tracer := OeTracer{}
			tracer.r = traceResult
			tracer.config, err = parseOeTracerConfig(&tracers.TraceConfig{TracerConfig: &test.TracerConfig})
			require.NoError(t, err)
			evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Debug: true, Tracer: &tracer})

			st := core.NewStateTransition(evm, msg, new(core.GasPool).AddGas(tx.GetGas()).AddBlobGas(tx.GetBlobGas()))
			_, err = st.TransitionDb(true /* refunds */, false /* gasBailout */)
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
