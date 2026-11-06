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

package rules_test

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// groupEndBalances is init code that logs and deploys the balances of EEZL2
// and the native system address.
func groupEndBalances() []byte {
	var code []byte
	for i, addr := range []common.Address{protocol.RollupXEEZL2Address, protocol.NativeSystemAddress} {
		code = append(append(code, byte(vm.PUSH20)), addr[:]...)
		code = append(code, byte(vm.BALANCE), byte(vm.PUSH1), byte(32*i), byte(vm.MSTORE))
	}
	return append(code,
		byte(vm.PUSH1), 64, byte(vm.PUSH1), 0, byte(vm.LOG0),
		byte(vm.PUSH1), 64, byte(vm.PUSH1), 0, byte(vm.RETURN))
}

// TestGroupEndBurnRPCReplay checks that the RPC methods replaying several
// transactions of a block on one state burn the protocol balances at a group
// end as block execution does, also when the transaction ending the group
// reverts. With EEZL2 as coinbase, a user transaction after a load tips EEZL2
// and ends the group; the contract created next logs and deploys the balances
// of EEZL2 and the funded native system address, which block execution burnt.
func TestGroupEndBurnRPCReplay(t *testing.T) {
	reverter := common.HexToAddress("0x8e7e")
	for _, tc := range []struct {
		name   string
		to     common.Address
		status uint64
	}{
		{"group end", common.HexToAddress("0x7ec1"), types.ReceiptStatusSuccessful},
		{"reverted group end", reverter, types.ReceiptStatusFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			initCode := groupEndBalances()
			c := newGroupEndChain(t, types.GenesisAlloc{
				derivertest.UserAddress():    groupEndRich,
				protocol.NativeSystemAddress: {Balance: big.NewInt(common.GWei)},
				reverter:                     {Code: []byte{byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.REVERT)}},
			})
			creation, err := types.SignTx(&types.DynamicFeeTransaction{
				CommonTx: types.CommonTx{Nonce: 1, GasLimit: 200_000, Data: initCode},
				ChainID:  *c.spec.Config.ChainID,
				TipCap:   *uint256.NewInt(groupEndTipCap),
				FeeCap:   *uint256.NewInt(groupEndFeeCap),
			}, *types.LatestSignerForChainID(c.spec.Config.ChainID), derivertest.UserKey)
			require.NoError(t, err)
			created := types.CreateAddress(derivertest.UserAddress(), 1)

			dbg.Exec3Parallel = false
			m, chain := derivertest.NewL2Of(t, c.spec)
			parent, err := chain.HeaderByNumber(t.Context(), 0)
			require.NoError(t, err)
			built, err := chain.Extend(t.Context(), parent, deriver.HeaderInputs{
				Number:     1,
				ParentHash: parent.Hash(),
				Time:       parent.Time + c.l2().BlockInterval,
				Coinbase:   protocol.RollupXEEZL2Address,
				Extra:      []byte{},
				GasLimit:   c.l2().BlockGasLimit,
			}, []types.Transaction{
				c.nativeTx(protocol.LoadExecutionTableSelector, 0),
				c.userTx(derivertest.UserKey, 0, tc.to, 0, 50_000),
				creation,
			}, common.Hash{}, common.Hash{})
			require.NoError(t, err)
			require.Equal(t, tc.status, built.Receipts[1].Status, "group end")
			require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[2].Status, "creation")
			require.Len(t, built.Receipts[2].Logs, 1)
			want := built.Receipts[2].Logs[0].Data
			require.Equal(t, hexutil.Bytes(make([]byte, 64)), want, "burnt at the group end")

			checkGroupEndRPCReplay(t, m, built.Block, initCode, created, want)
		})
	}
}

// checkGroupEndRPCReplay checks that the RPC methods replaying block 1 see the
// balances the contract created at index 2 logged and deployed.
func checkGroupEndRPCReplay(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block, initCode []byte, created common.Address, want hexutil.Bytes) {
	base := jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	// The versioned call state answers a read with the writes flushed to its version map.
	forEachCallState := func(t *testing.T, run func(t *testing.T)) {
		for _, versioned := range []bool{false, true} {
			t.Run(fmt.Sprintf("versioned=%v", versioned), func(t *testing.T) {
				defer func(prev bool) { dbg.CallNoMaterialize = prev }(dbg.CallNoMaterialize)
				dbg.CallNoMaterialize = versioned
				run(t)
			})
		}
	}

	t.Run("debug_traceBlockByNumber", func(t *testing.T) {
		forEachCallState(t, func(t *testing.T) {
			api := jsonrpc.NewPrivateDebugAPI(base, m.DB, nil, &rpccfg.DebugApiConfig{})
			tracer := "callTracer"
			var buf bytes.Buffer
			stream := jsonstream.New(&buf)
			require.NoError(t, api.TraceBlockByNumber(t.Context(), rpc.BlockNumber(1), &tracersConfig.TraceConfig{Tracer: &tracer}, stream))
			require.NoError(t, stream.Flush())
			var traces []struct {
				Result struct {
					Output hexutil.Bytes `json:"output"`
				} `json:"result"`
			}
			require.NoError(t, json.Unmarshal(buf.Bytes(), &traces), buf.String())
			require.Len(t, traces, 3)
			require.Equal(t, want, traces[2].Result.Output)
		})
	})

	t.Run("trace_replayBlockTransactions", func(t *testing.T) {
		api := jsonrpc.NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})
		for _, traceTypes := range [][]string{{"trace"}, {"trace", "stateDiff"}, {"vmTrace"}} {
			t.Run(fmt.Sprint(traceTypes), func(t *testing.T) {
				results, err := api.ReplayBlockTransactions(t.Context(), rpc.BlockNumberOrHashWithNumber(1), traceTypes, nil, nil)
				require.NoError(t, err)
				require.Len(t, results, 3)
				require.Equal(t, want, results[2].Output)
				if results[1].StateDiff == nil {
					return
				}
				// The burn belongs to the transaction ending the group: it removes
				// the native system address and leaves EEZL2 as it found it.
				raw, err := json.Marshal(results[1].StateDiff)
				require.NoError(t, err)
				var diff map[string]struct {
					Balance json.RawMessage `json:"balance"`
				}
				require.NoError(t, json.Unmarshal(raw, &diff))
				native, ok := diff[strings.ToLower(protocol.NativeSystemAddress.Hex())]
				require.True(t, ok, "%s", raw)
				require.Contains(t, string(native.Balance), `"-"`, "%s", raw)
				require.NotContains(t, diff, strings.ToLower(protocol.RollupXEEZL2Address.Hex()), "%s", raw)
			})
		}
	})

	t.Run("trace_replayTransaction", func(t *testing.T) {
		api := jsonrpc.NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})
		blockResults, err := api.ReplayBlockTransactions(t.Context(), rpc.BlockNumberOrHashWithNumber(1), []string{"stateDiff"}, nil, nil)
		require.NoError(t, err)
		require.Len(t, blockResults, 3)
		for i, txn := range block.Transactions() {
			res, err := api.ReplayTransaction(t.Context(), txn.Hash(), []string{"stateDiff"}, nil, nil)
			require.NoError(t, err)
			want, err := json.Marshal(blockResults[i].StateDiff)
			require.NoError(t, err)
			got, err := json.Marshal(res.StateDiff)
			require.NoError(t, err)
			require.JSONEq(t, string(want), string(got), "transaction %d", i)
		}
	})

	t.Run("eth_callMany", func(t *testing.T) {
		forEachCallState(t, func(t *testing.T) {
			api := jsonrpc.NewEthAPI(base, m.DB, nil, nil, nil, &rpccfg.EthApiConfig{GasCap: 5_000_000, ReturnDataLimit: 100_000}, log.New())
			from, data, index := derivertest.UserAddress(), hexutil.Bytes(initCode), 2
			feeCap, tipCap := hexutil.U256(*uint256.NewInt(groupEndFeeCap)), hexutil.U256{}
			call := ethapi.CallArgs{From: &from, Data: &data, MaxFeePerGas: &feeCap, MaxPriorityFeePerGas: &tipCap}
			res, err := api.CallMany(t.Context(), []jsonrpc.Bundle{{Transactions: []ethapi.CallArgs{call}}},
				jsonrpc.StateContext{BlockNumber: rpc.BlockNumberOrHashWithNumber(1), TransactionIndex: &index}, nil, nil)
			require.NoError(t, err)
			require.Len(t, res, 1)
			require.Len(t, res[0], 1)
			require.Equal(t, hex.EncodeToString(want), res[0][0]["value"], "%v", res[0][0])
		})
	})

	// After the block, the block-end burn has cleared what the creation tipped.
	t.Run("eth_callMany after the block", func(t *testing.T) {
		forEachCallState(t, func(t *testing.T) {
			api := jsonrpc.NewEthAPI(base, m.DB, nil, nil, nil, &rpccfg.EthApiConfig{GasCap: 5_000_000, ReturnDataLimit: 100_000}, log.New())
			from, data := derivertest.UserAddress(), hexutil.Bytes(initCode)
			feeCap, tipCap := hexutil.U256(*uint256.NewInt(groupEndFeeCap)), hexutil.U256{}
			call := ethapi.CallArgs{From: &from, Data: &data, MaxFeePerGas: &feeCap, MaxPriorityFeePerGas: &tipCap}
			after := -1
			for _, index := range []*int{nil, &after} {
				res, err := api.CallMany(t.Context(), []jsonrpc.Bundle{{Transactions: []ethapi.CallArgs{call}}},
					jsonrpc.StateContext{BlockNumber: rpc.BlockNumberOrHashWithNumber(1), TransactionIndex: index}, nil, nil)
				require.NoError(t, err)
				require.Len(t, res, 1)
				require.Len(t, res[0], 1)
				require.Equal(t, hex.EncodeToString(make([]byte, 64)), res[0][0]["value"], "%v", res[0][0])
			}
		})
	})

	overlay := func() *jsonrpc.OverlayAPIImpl {
		return jsonrpc.NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: 5_000_000}, jsonrpc.NewOtterscanAPI(base, m.DB, 25))
	}

	t.Run("overlay_getLogs", func(t *testing.T) {
		hash := block.Hash()
		logs, err := overlay().GetLogs(t.Context(), filters.FilterCriteria{BlockHash: &hash}, nil, nil)
		require.NoError(t, err)
		require.Len(t, logs, 1)
		require.Equal(t, created, logs[0].Address)
		require.Equal(t, want, logs[0].Data)
	})

	t.Run("overlay_callConstructor", func(t *testing.T) {
		code, err := overlay().CallConstructor(t.Context(), created, initCode)
		require.NoError(t, err)
		require.NotNil(t, code)
		require.NotNil(t, code.Code)
		require.Equal(t, want, *code.Code)
	})
}
