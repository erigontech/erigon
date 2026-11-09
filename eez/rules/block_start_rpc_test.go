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
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	eezconfig "github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// TestBlockStartBurnTraceReplay checks that the trace methods replaying one
// transaction see the accounts a rollupX v0.0.1 block reads at its start, the
// EEZL2 and system address it burns and the EIP-2935 contract it writes, as
// the transactions before it left them. Block 1 credits all three; block 2
// burns the first two at its start and credits all three again before a
// contract logs and returns their balances and sends each 1 wei.
func TestBlockStartBurnTraceReplay(t *testing.T) {
	eezl2, sys, history := protocol.RollupXEEZL2Address, protocol.SystemAddress, params.HistoryStorageAddress.Value()
	targets := []common.Address{eezl2, sys, history}
	reader := common.HexToAddress("0xba1a")
	var code []byte
	for i, addr := range targets {
		code = append(append(code, byte(vm.PUSH20)), addr[:]...)
		code = append(code, byte(vm.BALANCE), byte(vm.PUSH1), byte(32*i), byte(vm.MSTORE),
			// CALL(GAS, addr, 1, 0, 0, 0, 0): the EIP-2935 contract reverts it, without input.
			byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH1), 1, byte(vm.PUSH20))
		code = append(append(code, addr[:]...), byte(vm.GAS), byte(vm.CALL), byte(vm.POP))
	}
	size := byte(32 * len(targets))
	code = append(code,
		byte(vm.PUSH1), size, byte(vm.PUSH1), 0, byte(vm.LOG0),
		byte(vm.PUSH1), size, byte(vm.PUSH1), 0, byte(vm.RETURN))

	spec := networks.Chiado699069
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	// STOP: EEZL2 takes value.
	genesis.Alloc[eezl2] = types.GenesisAccount{Code: []byte{byte(vm.STOP)}}
	genesis.Alloc[reader] = types.GenesisAccount{Code: code}
	spec.Genesis = &genesis
	l2 := genesis.Config.L2.(*eezconfig.L2Config)
	require.Equal(t, eezconfig.RollupX, l2.RuleSet())
	m, execChain := derivertest.NewL2Of(t, spec)

	var nonce uint64
	userTx := func(to common.Address, value uint64, data []byte) types.Transaction {
		tx, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: 200_000, To: &to, Value: *uint256.NewInt(value), Data: data},
			ChainID:  *genesis.Config.ChainID,
			FeeCap:   *uint256.NewInt(100 * common.GWei),
		}, *types.LatestSignerForChainID(genesis.Config.ChainID), derivertest.UserKey)
		require.NoError(t, err)
		nonce++
		return tx
	}
	// credit sends each target its value; the EIP-2935 contract takes it with
	// a query for the parent of block number.
	credit := func(number uint64, values ...uint64) []types.Transaction {
		query := common.BigToHash(new(big.Int).SetUint64(number - 1))
		txs := make([]types.Transaction, len(targets))
		for i, addr := range targets {
			var data []byte
			if addr == history {
				data = query[:]
			}
			txs[i] = userTx(addr, values[i], data)
		}
		return txs
	}
	parent, err := execChain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	extend := func(txs []types.Transaction) *types.BlockWithReceipts {
		b, err := execChain.Extend(t.Context(), parent, deriver.HeaderInputs{
			Number:     parent.Number.Uint64() + 1,
			ParentHash: parent.Hash(),
			Time:       parent.Time + l2.BlockInterval,
			Extra:      []byte{},
			GasLimit:   l2.BlockGasLimit,
		}, txs, common.Hash{}, common.Hash{})
		require.NoError(t, err)
		for i, r := range b.Receipts {
			require.Equal(t, types.ReceiptStatusSuccessful, r.Status, "block %d transaction %d", b.Block.NumberU64(), i)
		}
		parent = b.Block.HeaderNoCopy()
		return b
	}
	extend(credit(1, 1000, 2000, 3000))
	built := extend(append(credit(2, 11, 22, 33), userTx(reader, 10, nil)))

	var first uint64
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		first, err = m.BlockReader.TxnumReader().Min(t.Context(), tx, 2)
		return err
	}))
	// first is block 2's opening system calls, which burn.
	require.Equal(t, []uint64{1000, 2000, 3000}, balancesAt(t, m, first, targets...), "end of block 1")
	require.Equal(t, []uint64{0, 0, 3000}, balancesAt(t, m, first+1, targets...), "after block 2's opening")

	read := built.Block.Transactions()[3]
	require.Len(t, built.Receipts[3].Logs, 1)
	want := hexutil.Bytes(built.Receipts[3].Logs[0].Data)
	require.Equal(t, balanceWords(11, 22, 3033), want, "block execution")

	base := jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	api := jsonrpc.NewTraceAPI(base, m.DB, &rpccfg.TraceApiConfig{})
	blockResults, err := api.ReplayBlockTransactions(t.Context(), rpc.BlockNumberOrHashWithNumber(2), []string{"trace", "stateDiff"}, nil, nil)
	require.NoError(t, err)
	require.Len(t, blockResults, 4)
	require.Equal(t, want, blockResults[3].Output, "trace_replayBlockTransactions")
	wantDiff, err := json.Marshal(blockResults[3].StateDiff)
	require.NoError(t, err)

	for _, traceTypes := range [][]string{{"trace"}, {"stateDiff"}, {"trace", "stateDiff"}, {"vmTrace"}} {
		t.Run(fmt.Sprint("trace_replayTransaction ", traceTypes), func(t *testing.T) {
			res, err := api.ReplayTransaction(t.Context(), read.Hash(), traceTypes, nil, nil)
			require.NoError(t, err)
			require.Equal(t, want, res.Output)
			if res.StateDiff != nil {
				got, err := json.Marshal(res.StateDiff)
				require.NoError(t, err)
				require.JSONEq(t, string(wantDiff), string(got))
			}
		})
	}
	t.Run("trace_transaction", func(t *testing.T) {
		traces, err := api.Transaction(t.Context(), read.Hash(), nil, nil)
		require.NoError(t, err)
		require.NotEmpty(t, traces)
		require.Equal(t, want, traceOutput(t, traces[0]))
	})
	t.Run("trace_get", func(t *testing.T) {
		trace, err := api.Get(t.Context(), read.Hash(), []hexutil.Uint64{}, nil, nil)
		require.NoError(t, err)
		require.NotNil(t, trace)
		require.Equal(t, want, traceOutput(t, *trace))
	})
}

// balancesAt returns the balances of addrs history holds before txNum.
func balancesAt(t *testing.T, m *execmoduletester.ExecModuleTester, txNum uint64, addrs ...common.Address) []uint64 {
	t.Helper()
	out := make([]uint64, len(addrs))
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		r := state.NewHistoryReaderV3(tx, txNum)
		for i, addr := range addrs {
			a, err := r.ReadAccountData(accounts.InternAddress(addr))
			if err != nil {
				return err
			}
			if a != nil {
				out[i] = a.Balance.Uint64()
			}
		}
		return nil
	}))
	return out
}

func balanceWords(balances ...uint64) hexutil.Bytes {
	var out []byte
	for _, b := range balances {
		word := common.BigToHash(new(big.Int).SetUint64(b))
		out = append(out, word[:]...)
	}
	return out
}

// traceOutput returns the output of a call trace.
func traceOutput(t *testing.T, trace jsonrpc.ParityTrace) hexutil.Bytes {
	t.Helper()
	raw, err := json.Marshal(trace)
	require.NoError(t, err)
	var decoded struct {
		TraceAddress []int `json:"traceAddress"`
		Result       struct {
			Output hexutil.Bytes `json:"output"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(raw, &decoded), string(raw))
	require.Empty(t, decoded.TraceAddress, string(raw))
	return decoded.Result.Output
}
