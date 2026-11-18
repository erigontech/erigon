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
	"encoding/json"
	"os"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/misc"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	debugtracer "github.com/erigontech/erigon/execution/tracing/tracers/debug"
	"github.com/erigontech/erigon/execution/tracing/tracers/logger"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func ecrecoverInput(t *testing.T) ([]byte, accounts.Address) {
	t.Helper()
	data, err := os.ReadFile("../../../../vm/testdata/precompiles/ecRecover.json")
	require.NoError(t, err)
	var fixtures []struct{ Input, Expected, Name string }
	require.NoError(t, json.Unmarshal(data, &fixtures))
	for _, fixture := range fixtures {
		if fixture.Name == "ValidKey" {
			return common.FromHex(fixture.Input), accounts.InternAddress(common.BytesToAddress(common.FromHex(fixture.Expected)))
		}
	}
	t.Fatal("missing ecRecover fixture")
	return nil, accounts.NilAddress
}

func callEcrecover(ibs *state.IntraBlockState, hooks *tracing.Hooks, input []byte, gas uint64) ([]byte, error) {
	from := accounts.InternAddress(common.HexToAddress("0x1000"))
	to := accounts.InternAddress(common.HexToAddress("0x01"))
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = new(uint64)
	blockCtx := evmtypes.BlockContext{
		Transfer: misc.Transfer,
		Coinbase: accounts.InternAddress(common.HexToAddress("0x2000")),
	}
	evm := vm.NewEVM(blockCtx, evmtypes.TxContext{}, ibs, config, vm.Config{Tracer: hooks})
	if hooks != nil && hooks.OnTxStart != nil {
		tx := types.NewTransaction(0, to.Value(), new(uint256.Int), 100_000, new(uint256.Int), input)
		hooks.OnTxStart(evm.GetVMContext(), tx, from)
	}
	output, _, _, err := evm.Call(from, to, input, mdgas.MdGas{Execution: gas}, uint256.Int{}, false)
	return output, err
}

func TestEcrecoverPrestate(t *testing.T) {
	input, signer := ecrecoverInput(t)
	for _, wrapper := range []string{"direct", "mux", "debug"} {
		t.Run(wrapper, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, ibs.SetCode(signer, []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))
			name := "prestateTracer"
			var config json.RawMessage
			if wrapper == "mux" {
				name, config = "muxTracer", json.RawMessage(`{"prestateTracer":{}}`)
			}
			tracer, err := tracers.New(name, nil, config)
			require.NoError(t, err)
			if wrapper == "debug" {
				tracer = debugtracer.New(t.TempDir(), debugtracer.WithWrappedTracer(tracer))
			}
			output, err := callEcrecover(ibs, tracer.Hooks, input, 6100)
			require.NoError(t, err)
			require.Empty(t, output)
			result, err := tracer.GetResult()
			require.NoError(t, err)
			if wrapper == "mux" {
				var results map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(result, &results))
				result = results["prestateTracer"]
			}
			var pre prestateTrace
			require.NoError(t, json.Unmarshal(result, &pre))
			require.Contains(t, pre, signer)
			require.Equal(t, "0x00", pre[signer].Code)
		})
	}
}

func TestEcrecoverAccessList(t *testing.T) {
	input, signer := ecrecoverInput(t)
	for _, tc := range []struct {
		name     string
		input    []byte
		gas      uint64
		warm     bool
		wantRead bool
		err      error
	}{
		{name: "cold", input: input, gas: 6100, wantRead: true},
		{name: "cold out of gas", input: input, gas: 6099, err: vm.ErrOutOfGas},
		{name: "warm", input: input, gas: 3200, warm: true, wantRead: true},
		{name: "warm out of gas", input: input, gas: 3199, warm: true, err: vm.ErrOutOfGas},
		{name: "invalid signature", gas: 3000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ibs := state.New(state.NewNoopReader())
			defer ibs.Close()
			require.NoError(t, ibs.SetCode(signer, []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))
			if tc.warm {
				ibs.AddAddressToAccessList(signer)
			}
			tracer := logger.NewAccessListTracer(nil, nil, ibs)
			output, err := callEcrecover(ibs, tracer.Hooks(), tc.input, tc.gas)
			require.ErrorIs(t, err, tc.err)
			require.Empty(t, output)
			list := tracer.AccessList()
			if !tc.wantRead {
				require.Empty(t, list)
				return
			}
			require.Len(t, list, 1)
			require.Equal(t, signer.Value(), list[0].Address)
			require.Empty(t, list[0].StorageKeys)
		})
	}
}

func TestEcrecoverPrestateDelegation(t *testing.T) {
	input, signer := ecrecoverInput(t)
	target := accounts.InternAddress(common.HexToAddress("0x3000"))
	code := types.AddressToDelegation(target)
	ibs := state.New(state.NewNoopReader())
	defer ibs.Close()
	require.NoError(t, ibs.SetCode(target, []byte{byte(vm.STOP)}, tracing.CodeChangeUnspecified))
	require.NoError(t, ibs.SetCode(signer, code, tracing.CodeChangeUnspecified))
	tracer, err := tracers.New("prestateTracer", nil, nil)
	require.NoError(t, err)
	snapshot := ibs.PushSnapshot()
	output, err := callEcrecover(ibs, tracer.Hooks, input, 6100)
	require.NoError(t, err)
	require.Equal(t, signer.Value(), common.BytesToAddress(output))
	ibs.RevertToSnapshot(snapshot, nil)
	ibs.PopSnapshot(snapshot)
	result, err := tracer.GetResult()
	require.NoError(t, err)
	var pre prestateTrace
	require.NoError(t, json.Unmarshal(result, &pre))
	require.Contains(t, pre, signer)
	require.Equal(t, hexutil.Encode(code), pre[signer].Code)
	require.NotContains(t, pre, target)
}
