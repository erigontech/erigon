// Copyright 2021 The go-ethereum Authors
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

package tracetest

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dir"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/eth/tracers"
	debugtracer "github.com/erigontech/erigon/eth/tracers/debug"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/stages/mock"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/tests"
)

// prestateTrace is the result of a prestateTrace run.
type prestateTrace = map[common.Address]*account

type account struct {
	Balance string                      `json:"balance"`
	Code    string                      `json:"code"`
	Nonce   uint64                      `json:"nonce"`
	Storage map[common.Hash]common.Hash `json:"storage"`
}

// testcase defines a single test to check the stateDiff tracer against.
type testcase struct {
	Genesis      *types.Genesis  `json:"genesis"`
	Context      *callContext    `json:"context"`
	Input        string          `json:"input"`
	TracerConfig json.RawMessage `json:"tracerConfig"`
	Result       interface{}     `json:"result"`
}

func TestPrestateTracerLegacy(t *testing.T) {
	testPrestateTracer("prestateTracerLegacy", "prestate_tracer_legacy", t)
}

func TestPrestateTracer(t *testing.T) {
	testPrestateTracer("prestateTracer", "prestate_tracer", t)
}

func TestPrestateWithDiffModeTracer(t *testing.T) {
	testPrestateTracer("prestateTracer", "prestate_tracer_with_diff_mode", t)
}

func testPrestateTracer(tracerName string, dirPath string, t *testing.T) {
	files, err := dir.ReadDir(filepath.Join("testdata", dirPath))
	if err != nil {
		t.Fatalf("failed to retrieve tracer test suite: %v", err)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		file := file // capture range variable
		t.Run(camel(strings.TrimSuffix(file.Name(), ".json")), func(t *testing.T) {
			t.Parallel()

			var (
				test = new(testcase)
			)
			// Call tracer test found, read if from disk
			if blob, err := os.ReadFile(filepath.Join("testdata", dirPath, file.Name())); err != nil {
				t.Fatalf("failed to read testcase: %v", err)
			} else if err := json.Unmarshal(blob, test); err != nil {
				t.Fatalf("failed to parse testcase: %v", err)
			}
			tx, err := types.UnmarshalTransactionFromBinary(common.FromHex(test.Input), false /* blobTxnsAreWrappedWithBlobs */)
			if err != nil {
				t.Fatalf("failed to parse testcase input: %v", err)
			}
			// Configure a blockchain with the given prestate
			signer := types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number), uint64(test.Context.Time))
			context := evmtypes.BlockContext{
				CanTransfer: core.CanTransfer,
				Transfer:    consensus.Transfer,
				Coinbase:    test.Context.Miner,
				BlockNumber: uint64(test.Context.Number),
				Time:        uint64(test.Context.Time),
				Difficulty:  (*big.Int)(test.Context.Difficulty),
				GasLimit:    uint64(test.Context.GasLimit),
			}
			if test.Context.BaseFee != nil {
				context.BaseFee, _ = uint256.FromBig((*big.Int)(test.Context.BaseFee))
			}
			rules := context.Rules(test.Genesis.Config)
			m := mock.Mock(t)
			dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()
			statedb, err := tests.MakePreState(rules, dbTx, test.Genesis.Alloc, context.BlockNumber)
			require.NoError(t, err)
			tracer, err := tracers.New(tracerName, new(tracers.Context), test.TracerConfig)
			if err != nil {
				t.Fatalf("failed to create call tracer: %v", err)
			}
			if outputDir, ok := os.LookupEnv("PRESTATE_TRACER_TEST_DEBUG_TRACER_OUTPUT_DIR"); ok {
				recordOptions := debugtracer.RecordOptions{
					DisableOnOpcodeMemoryRecording:    true,
					DisableOnOpcodeStackRecording:     true,
					DisableOnBlockchainInitRecording:  true,
					DisableOnBlockStartRecording:      true,
					DisableOnBlockEndRecording:        true,
					DisableOnGenesisBlockRecording:    true,
					DisableOnSystemCallStartRecording: true,
					DisableOnSystemCallEndRecording:   true,
					DisableOnBalanceChangeRecording:   true,
					DisableOnNonceChangeRecording:     true,
					DisableOnStorageChangeRecording:   true,
					DisableOnLogRecording:             true,
				}
				tracer = debugtracer.New(
					outputDir,
					debugtracer.WithWrappedTracer(tracer),
					debugtracer.WithRecordOptions(recordOptions),
					debugtracer.WithFlushMode(debugtracer.FlushModeTxn),
				)
			}
			statedb.SetHooks(tracer.Hooks)
			msg, err := tx.AsMessage(*signer, (*big.Int)(test.Context.BaseFee), rules)
			if err != nil {
				t.Fatalf("failed to prepare transaction for tracing: %v", err)
			}
			txContext := core.NewEVMTxContext(msg)
			evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Tracer: tracer.Hooks})
			tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
			st := core.NewStateTransition(evm, msg, new(core.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
			vmRet, err := st.TransitionDb(true /* refunds */, false /* gasBailout */)
			if err != nil {
				t.Fatalf("failed to execute transaction: %v", err)
			}
			tracer.OnTxEnd(&types.Receipt{GasUsed: vmRet.GasUsed}, nil)
			// Retrieve the trace result and compare against the expected
			res, err := tracer.GetResult()
			if err != nil {
				t.Fatalf("failed to retrieve trace result: %v", err)
			}
			// The legacy javascript calltracer marshals json in js, which
			// is not deterministic (as opposed to the golang json encoder).
			if strings.HasSuffix(dirPath, "_legacy") {
				// This is a tweak to make it deterministic. Can be removed when
				// we remove the legacy tracer.
				var x prestateTrace
				if err := json.Unmarshal(res, &x); err != nil {
					t.Fatalf("prestateTrace tweak Unmarshal: %v", err)
				}
				if res, err = json.Marshal(x); err != nil {
					t.Fatalf("prestateTrace tweak Marshal: %v", err)
				}
			}
			want, err := json.Marshal(test.Result)
			if err != nil {
				t.Fatalf("failed to marshal test: %v", err)
			}
			if string(want) != string(res) {
				t.Fatalf("trace mismatch\n have: %v\n want: %v\n", string(res), string(want))
			}
		})
	}
}
