// Copyright 2021 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package tracetest

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/stretchr/testify/require"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/eth/tracers"
	"github.com/ledgerwatch/erigon/tests"
)

// prestateTrace is the result of a prestateTrace run.
type prestateTrace = map[libcommon.Address]*account

type account struct {
	Balance string                            `json:"balance"`
	Code    string                            `json:"code"`
	Nonce   uint64                            `json:"nonce"`
	Storage map[libcommon.Hash]libcommon.Hash `json:"storage"`
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
	testPrestateDiffTracer("prestateTracerLegacy", "prestate_tracer_legacy", t)
}

func TestPrestateTracer(t *testing.T) {
	testPrestateDiffTracer("prestateTracer", "prestate_tracer", t)
}

func TestPrestateWithDiffModeTracer(t *testing.T) {
	testPrestateDiffTracer("prestateTracer", "prestate_tracer_with_diff_mode", t)
}

func testPrestateDiffTracer(tracerName string, dirPath string, t *testing.T) {
	files, err := os.ReadDir(filepath.Join("testdata", dirPath))
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
			tx, err := types.UnmarshalTransactionFromBinary(common.FromHex(test.Input))
			if err != nil {
				t.Fatalf("failed to parse testcase input: %v", err)
			}
			// Configure a blockchain with the given prestate
			var (
				signer    = types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number))
				origin, _ = signer.Sender(tx)
				txContext = evmtypes.TxContext{
					Origin:   origin,
					GasPrice: tx.GetFeeCap(),
				}
				context = evmtypes.BlockContext{
					CanTransfer: core.CanTransfer,
					Transfer:    core.Transfer,
					Coinbase:    test.Context.Miner,
					BlockNumber: uint64(test.Context.Number),
					Time:        uint64(test.Context.Time),
					Difficulty:  (*big.Int)(test.Context.Difficulty),
					GasLimit:    uint64(test.Context.GasLimit),
				}
				rules = test.Genesis.Config.Rules(context.BlockNumber, context.Time)
			)
			m := stages.Mock(t)
			dbTx, err := m.DB.BeginRw(m.Ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()
			statedb, _ := tests.MakePreState(rules, dbTx, test.Genesis.Alloc, context.BlockNumber)
			if test.Genesis.BaseFee != nil {
				context.BaseFee, _ = uint256.FromBig(test.Genesis.BaseFee)
			}
			tracer, err := tracers.New(tracerName, new(tracers.Context), test.TracerConfig)
			if err != nil {
				t.Fatalf("failed to create call tracer: %v", err)
			}
			evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Debug: true, Tracer: tracer})
			msg, err := tx.AsMessage(*signer, nil, rules) // BaseFee is set to nil and not to contet.BaseFee, to match the output to go-ethereum tests
			if err != nil {
				t.Fatalf("failed to prepare transaction for tracing: %v", err)
			}
			st := core.NewStateTransition(evm, msg, new(core.GasPool).AddGas(tx.GetGas()))
			if _, err = st.TransitionDb(true /* refunds */, false /* gasBailout */); err != nil {
				t.Fatalf("failed to execute transaction: %v", err)
			}
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
