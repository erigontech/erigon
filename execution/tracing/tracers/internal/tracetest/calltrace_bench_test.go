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
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dir"

	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/tests/testutil"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/js"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func BenchmarkTracers(b *testing.B) {
	files, err := dir.ReadDir(filepath.Join("testdata", "call_tracer"))
	if err != nil {
		b.Fatalf("failed to retrieve tracer test suite: %v", err)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".json") {
			continue
		}
		file := file // capture range variable
		b.Run(camel(strings.TrimSuffix(file.Name(), ".json")), func(b *testing.B) {
			blob, err := os.ReadFile(filepath.Join("testdata", "call_tracer", file.Name()))
			if err != nil {
				b.Fatalf("failed to read testcase: %v", err)
			}
			test := new(callTracerTest)
			if err := json.Unmarshal(blob, test); err != nil {
				b.Fatalf("failed to parse testcase: %v", err)
			}
			benchTracer(b, "callTracer", test)
		})
	}
}

func benchTracer(b *testing.B, tracerName string, test *callTracerTest) {
	// Configure a blockchain with the given prestate
	tx, err := types.DecodeTransaction(common.FromHex(test.Input))
	if err != nil {
		b.Fatalf("failed to parse testcase input: %v", err)
	}
	signer := types.MakeSigner(test.Genesis.Config, uint64(test.Context.Number), uint64(test.Context.Time))
	context := evmtypes.BlockContext{
		CanTransfer: protocol.CanTransfer,
		Transfer:    misc.Transfer,
		Coinbase:    accounts.InternAddress(test.Context.Miner),
		BlockNumber: uint64(test.Context.Number),
		Time:        uint64(test.Context.Time),
		Difficulty:  *test.Context.Difficulty,
		GasLimit:    uint64(test.Context.GasLimit),
	}
	rules := context.Rules(test.Genesis.Config)
	msg, err := tx.AsMessage(*signer, nil, rules)
	if err != nil {
		b.Fatalf("failed to prepare transaction for tracing: %v", err)
	}
	origin, _ := signer.Sender(tx)
	baseFee := test.Context.BaseFee
	txContext := evmtypes.TxContext{
		Origin:   origin,
		GasPrice: tx.GetEffectiveGasTip(baseFee),
	}
	m := execmoduletester.New(b)
	dbTx, err := m.DB.BeginTemporalRw(m.Ctx)
	require.NoError(b, err)
	defer dbTx.Rollback()
	statedb, _ := testutil.MakePreState(rules, m.DB, dbTx, test.Genesis.Alloc, uint64(test.Context.Number))

	b.ReportAllocs()
	for b.Loop() {
		tracer, err := tracers.New(tracerName, new(tracers.Context), nil)
		if err != nil {
			b.Fatalf("failed to create call tracer: %v", err)
		}
		evm := vm.NewEVM(context, txContext, statedb, test.Genesis.Config, vm.Config{Tracer: tracer.Hooks})
		snap := statedb.PushSnapshot()
		st := protocol.NewTxnExecutor(evm, msg, new(protocol.GasPool).AddGas(tx.GetGasLimit()).AddBlobGas(tx.GetBlobGas()))
		if _, err = st.Execute(true /* refunds */, false /* gasBailout */); err != nil {
			b.Fatalf("failed to execute transaction: %v", err)
		}
		if _, err = tracer.GetResult(); err != nil {
			b.Fatal(err)
		}
		statedb.RevertToSnapshot(snap, nil)
		statedb.PopSnapshot(snap)
	}
}
