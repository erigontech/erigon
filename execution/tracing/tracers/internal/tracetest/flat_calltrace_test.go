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

package tracetest

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

type flatCallTrace struct {
	Action       flatCallTraceAction `json:"action"`
	Error        string              `json:"error,omitempty"`
	Result       flatCallTraceResult `json:"result"`
	Subtraces    int                 `json:"subtraces"`
	TraceAddress []int               `json:"traceAddress"`
	Type         string              `json:"type"`
}

type flatCallTraceAction struct {
	Author         common.Address `json:"author,omitempty"`
	RewardType     string         `json:"rewardType,omitempty"`
	SelfDestructed common.Address `json:"address,omitempty"`
	Balance        hexutil.Big    `json:"balance"`
	CallType       string         `json:"callType,omitempty"`
	CreationMethod string         `json:"creationMethod,omitempty"`
	From           common.Address `json:"from,omitempty"`
	Gas            hexutil.Uint64 `json:"gas,omitempty"`
	Init           hexutil.Bytes  `json:"init,omitempty"`
	Input          hexutil.Bytes  `json:"input,omitempty"`
	RefundAddress  common.Address `json:"refundAddress,omitempty"`
	To             common.Address `json:"to,omitempty"`
	Value          hexutil.Big    `json:"value"`
}

type flatCallTraceResult struct {
	Address common.Address `json:"address,omitempty"`
	Code    hexutil.Bytes  `json:"code,omitempty"`
	GasUsed hexutil.Uint64 `json:"gasUsed,omitempty"`
	Output  hexutil.Bytes  `json:"output,omitempty"`
}

func TestFlatCallTracerGethFixtures(t *testing.T) {
	files, err := os.ReadDir(filepath.Join("testdata", "call_tracer_flat"))
	require.NoError(t, err)
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		t.Run(camel(strings.TrimSuffix(file.Name(), ".json")), func(t *testing.T) {
			blob, err := os.ReadFile(filepath.Join("testdata", "call_tracer_flat", file.Name()))
			require.NoError(t, err)
			test := new(testcase)
			require.NoError(t, json.Unmarshal(blob, test))
			tx, err := types.UnmarshalTransactionFromBinary(common.FromHex(test.Input), false)
			require.NoError(t, err)
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
				context.BaseFee = *test.Context.BaseFee
			}
			rules := context.Rules(test.Genesis.Config)
			m := execmoduletester.New(t)
			dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
			require.NoError(t, err)
			defer dbTx.Rollback()
			statedb, err := testutil.MakePreState(rules, m.DB, dbTx, test.Genesis.Alloc, context.BlockNumber)
			require.NoError(t, err)
			cfg := test.TracerConfig
			if cfg == nil {
				cfg = json.RawMessage("{}")
			}
			tracer, err := tracers.New("flatCallTracer", new(tracers.Context), cfg)
			require.NoError(t, err)
			statedb.SetHooks(tracer.Hooks)
			msg, err := tx.AsMessage(*signer, test.Context.BaseFee, rules)
			require.NoError(t, err)
			evm := vm.NewEVM(context, protocol.NewEVMTxContext(msg), statedb, test.Genesis.Config, vm.Config{Tracer: tracer.Hooks})
			tracer.OnTxStart(evm.GetVMContext(), tx, msg.From())
			st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
			vmRet, err := st.Execute(true, false)
			require.NoError(t, err)
			tracer.EmitTxEnd(&types.Receipt{GasUsed: vmRet.ReceiptGasUsed}, vmRet.TxnGasUsage, nil)
			res, err := tracer.GetResult()
			require.NoError(t, err)
			var have, want []flatCallTrace
			require.NoError(t, json.Unmarshal(res, &have))
			wantBlob, _ := json.Marshal(test.Result)
			require.NoError(t, json.Unmarshal(wantBlob, &want))
			h, _ := json.MarshalIndent(have, "", " ")
			w, _ := json.MarshalIndent(want, "", " ")
			if !bytes.Equal(h, w) {
				t.Fatalf("trace mismatch\nhave %s\nwant %s", h, w)
			}
		})
	}
}
