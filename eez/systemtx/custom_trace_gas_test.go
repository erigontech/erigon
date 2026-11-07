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

package systemtx_test

import (
	"bytes"
	"encoding/json"
	"maps"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/rawtemporaldb"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	execprotocol "github.com/erigontech/erigon/execution/protocol"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/stagedsync"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// TestCustomTraceGasFromBlock checks that the custom trace stage replays a
// type-0x76 transaction with the gas its block has left when that is below its
// limit, as block execution ran it.
func TestCustomTraceGasFromBlock(t *testing.T) {
	eezl2 := protocol.RollupXEEZL2Address
	spec := networks.Chiado699069
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	// JUMPDEST PUSH1 0 JUMP: EEZL2 burns all the gas it gets.
	genesis.Alloc[eezl2] = types.GenesisAccount{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}}
	spec.Genesis = &genesis
	l2 := genesis.Config.L2.(*config.L2Config)
	require.Equal(t, config.RollupX, l2.RuleSet())
	m, chain := derivertest.NewL2Of(t, spec)

	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	// Two blocks: what the first counts is not the second's.
	built := make([]*types.BlockWithReceipts, 2)
	for i := range built {
		number := uint64(i) + 1
		nonce, err := chain.Nonce(t.Context(), number-1, protocol.SystemAddress)
		require.NoError(t, err)
		sys, err := systemtx.New(protocol.SystemTx{ChainID: genesis.Config.ChainID.Uint64(), Nonce: nonce, To: eezl2})
		require.NoError(t, err)
		left := sys.GetGasLimit() - 1_000_000
		user, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: uint64(i), GasLimit: l2.BlockGasLimit - left, To: &eezl2},
			ChainID:  *genesis.Config.ChainID,
			FeeCap:   *uint256.NewInt(100 * common.GWei),
		}, *types.LatestSignerForChainID(genesis.Config.ChainID), derivertest.UserKey)
		require.NoError(t, err)
		built[i], err = chain.Extend(t.Context(), parent, deriver.HeaderInputs{
			Number:     number,
			ParentHash: parent.Hash(),
			Time:       parent.Time + l2.BlockInterval,
			Extra:      []byte{},
			GasLimit:   l2.BlockGasLimit,
		}, []types.Transaction{user, sys}, common.Hash{}, common.Hash{})
		require.NoError(t, err)
		require.Equal(t, left, built[i].Receipts[1].GasUsed)
		require.Equal(t, l2.BlockGasLimit, built[i].Receipts[1].CumulativeGasUsed)
		parent = built[i].Block.HeaderNoCopy()
	}

	cfg := stagedsync.StageCustomTraceCfg([]string{"receipt"}, m.DB, m.Dirs, m.BlockReader, m.ChainConfig, m.Engine, m.Cfg().Genesis, m.Cfg().Sync)
	require.NoError(t, stagedsync.StageCustomTraceReset(t.Context(), m.DB, cfg.Produce))
	for i, b := range built {
		require.Equal(t, make([]uint64, len(b.Receipts)), domainCumulative(t, m, uint64(i)+1, len(b.Receipts)), "receipt domain reset")
	}
	require.NoError(t, stagedsync.SpawnCustomTrace(cfg, t.Context(), m.Log))
	for i, b := range built {
		want := make([]uint64, len(b.Receipts))
		for j, r := range b.Receipts {
			want[j] = r.CumulativeGasUsed
		}
		require.Equal(t, want, domainCumulative(t, m, uint64(i)+1, len(b.Receipts)), "block %d", i+1)
	}
}

// TestCustomTraceGasFromBlockAmsterdam checks that the custom trace stage counts
// a block's gas as block execution does: a refund, which receipts subtract and
// the block does not from Amsterdam, leaves a later type-0x76 transaction no more gas.
// No rule set runs that type under Amsterdam; here it only exposes the count.
func TestCustomTraceGasFromBlockAmsterdam(t *testing.T) {
	m, l2, built := systemTxAfter(t, refunder, nil)

	amsterdam := *m.ChainConfig
	amsterdam.AmsterdamTime = common.NewUint64(0)
	cfg := stagedsync.StageCustomTraceCfg([]string{"receipt"}, m.DB, m.Dirs, m.BlockReader, &amsterdam, m.Engine, m.Cfg().Genesis, m.Cfg().Sync)
	require.NoError(t, stagedsync.StageCustomTraceReset(t.Context(), m.DB, cfg.Produce))
	require.NoError(t, stagedsync.SpawnCustomTrace(cfg, t.Context(), m.Log))

	got := domainCumulative(t, m, 1, len(built.Receipts))
	blockGas := got[0] + params.SstoreClearsScheduleRefundEIP8038
	require.Equal(t, l2.BlockGasLimit-blockGas, got[1]-got[0])
}

// TestCustomTraceStateGasAmsterdam checks that the custom trace stage and
// debug_traceBlock count a block's gas from Amsterdam as the larger of its
// regular and state gas: the state gas of new slots leaves a later native
// transaction less than it encodes. That limit is above what the block admits;
// here it only exposes the count.
func TestCustomTraceStateGasAmsterdam(t *testing.T) {
	const slots = 150
	r := newAmsterdamReplay(t, slots)
	left := r.built.Block.GasLimit() - slots*params.StateGasPerStorageSet
	require.Less(t, left, r.built.Block.Transactions()[1].GetGasLimit())
	require.Equal(t, left, r.cumulative[1]-r.cumulative[0], "custom trace stage")
	// The header holds the block's Osaka gas, which the trace's assert-mode check compares.
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false
	require.Equal(t, left, traceBlockGasUsed(t, r.m, false)[1], "debug_traceBlock")
}

// refunder clears a slot for the refund and burns gas down to 100000.
var refunder = common.HexToAddress("0x00000000000000000000000000000000000fe1d0")

// systemTxAfter extends a chain with a user transaction and a type-0x76
// transaction burning the gas the block has left, below its limit.
func systemTxAfter(t *testing.T, to common.Address, data []byte) (*execmoduletester.ExecModuleTester, *config.L2Config, *types.BlockWithReceipts) {
	t.Helper()
	eezl2 := protocol.RollupXEEZL2Address
	spec := networks.Chiado699069
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	// JUMPDEST PUSH1 0 JUMP: EEZL2 burns all the gas it gets.
	genesis.Alloc[eezl2] = types.GenesisAccount{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}}
	genesis.Alloc[refunder] = types.GenesisAccount{
		Code: []byte{
			byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.SSTORE),
			byte(vm.JUMPDEST), byte(vm.PUSH3), 0x01, 0x86, 0xa0, byte(vm.GAS), byte(vm.GT), byte(vm.PUSH1), 5, byte(vm.JUMPI),
			byte(vm.STOP),
		},
		Storage: map[common.Hash]common.Hash{{}: common.BigToHash(big.NewInt(1))},
	}
	spec.Genesis = &genesis
	l2 := genesis.Config.L2.(*config.L2Config)
	m, chain := derivertest.NewL2Of(t, spec)

	parent, err := chain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	nonce, err := chain.Nonce(t.Context(), 0, protocol.SystemAddress)
	require.NoError(t, err)
	sys, err := systemtx.New(protocol.SystemTx{ChainID: genesis.Config.ChainID.Uint64(), Nonce: nonce, To: eezl2})
	require.NoError(t, err)
	user, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{GasLimit: l2.BlockGasLimit - sys.GetGasLimit() + 1_000_000, To: &to, Data: data},
		ChainID:  *genesis.Config.ChainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *types.LatestSignerForChainID(genesis.Config.ChainID), derivertest.UserKey)
	require.NoError(t, err)
	built, err := chain.Extend(t.Context(), parent, deriver.HeaderInputs{
		Number:     1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, []types.Transaction{user, sys}, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[0].Status)

	return m, l2, built
}

// TestTraceBlockGasFromBlock checks that debug_traceBlock gives a type-0x76
// transaction the gas block execution gave it, whether or not the trace shows
// the refund of an earlier transaction.
func TestTraceBlockGasFromBlock(t *testing.T) {
	for name, user := range map[string]struct {
		to   common.Address
		data []byte
	}{
		"refund":         {to: refunder},
		"calldata floor": {to: common.Address{0xf1}, data: bytes.Repeat([]byte{1}, 350_000)},
	} {
		t.Run(name, func(t *testing.T) {
			m, _, built := systemTxAfter(t, user.to, user.data)
			receipts := built.Receipts
			for _, noRefunds := range []bool{false, true} {
				gas := traceBlockGasUsed(t, m, noRefunds)
				require.Len(t, gas, len(receipts))
				// The option shows: the transaction before reports other gas than its receipt.
				if noRefunds {
					require.NotEqual(t, receipts[0].GasUsed, gas[0])
				} else {
					require.Equal(t, receipts[0].GasUsed, gas[0])
				}
				require.Equal(t, receipts[1].GasUsed, gas[1], "noRefunds %v", noRefunds)
			}
		})
	}
}

// TestTraceBlockGasFromBlockSpent checks that debug_traceBlock fails a type-0x76
// transaction when state overrides make the transactions before it spend the
// whole block, rather than running it with its limit.
func TestTraceBlockGasFromBlockSpent(t *testing.T) {
	m, l2, built := systemTxAfter(t, refunder, nil)
	execChain := deriver.NewExecChain(m.ExecModule, m.DB, m.BlockReader, m.ReceiptsReader, m.ChainConfig)
	chainID := m.ChainConfig.ChainID
	burner := common.Address{0xb0}
	var txs []types.Transaction
	// Each within the transaction gas cap, together the block's limit.
	for i, gas := range []uint64{l2.BlockGasLimit / 2, l2.BlockGasLimit - l2.BlockGasLimit/2} {
		user, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: uint64(1 + i), GasLimit: gas, To: &burner},
			ChainID:  *chainID,
			FeeCap:   *uint256.NewInt(100 * common.GWei),
		}, *types.LatestSignerForChainID(chainID), derivertest.UserKey)
		require.NoError(t, err)
		txs = append(txs, user)
	}
	nonce, err := execChain.Nonce(t.Context(), 1, protocol.SystemAddress)
	require.NoError(t, err)
	sys, err := systemtx.New(protocol.SystemTx{ChainID: chainID.Uint64(), Nonce: nonce, To: protocol.RollupXEEZL2Address})
	require.NoError(t, err)
	parent := built.Block.HeaderNoCopy()
	second, err := execChain.Extend(t.Context(), parent, deriver.HeaderInputs{
		Number:     2,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, append(txs, sys), common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Len(t, second.Receipts, 3)

	// The trace's gas differs from the header's, which the assert-mode check compares.
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false
	api := jsonrpc.NewPrivateDebugAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, &rpccfg.DebugApiConfig{})
	// JUMPDEST PUSH1 0 JUMP: the user transactions burn their limits.
	loop := hexutil.Bytes{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}
	tracer := "callTracer"
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	require.NoError(t, api.TraceBlockByNumber(t.Context(), rpc.BlockNumber(2), &tracersConfig.TraceConfig{
		Tracer:         &tracer,
		StateOverrides: &ethapi.StateOverrides{accounts.InternAddress(burner): {Code: &loop}},
	}, stream))
	require.NoError(t, stream.Flush())
	var traces []struct {
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
		Result struct {
			GasUsed hexutil.Uint64 `json:"gasUsed"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &traces), buf.String())
	require.Len(t, traces, 3)
	require.Nil(t, traces[0].Error)
	require.Nil(t, traces[1].Error)
	require.Equal(t, l2.BlockGasLimit, uint64(traces[0].Result.GasUsed+traces[1].Result.GasUsed))
	require.NotNil(t, traces[2].Error, buf.String())
	require.Contains(t, traces[2].Error.Message, execprotocol.ErrIntrinsicGas.Error())
}

// traceBlockGasUsed returns the gas debug_traceBlockByNumber reports for each
// transaction of block 1.
func traceBlockGasUsed(t *testing.T, m *execmoduletester.ExecModuleTester, noRefunds bool) []uint64 {
	t.Helper()
	api := jsonrpc.NewPrivateDebugAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, &rpccfg.DebugApiConfig{})
	tracer := "callTracer"
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	require.NoError(t, api.TraceBlockByNumber(t.Context(), rpc.BlockNumber(1), &tracersConfig.TraceConfig{Tracer: &tracer, NoRefunds: &noRefunds}, stream))
	require.NoError(t, stream.Flush())
	var traces []struct {
		Error  json.RawMessage `json:"error"`
		Result struct {
			GasUsed hexutil.Uint64 `json:"gasUsed"`
		} `json:"result"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &traces), buf.String())
	gas := make([]uint64, len(traces))
	for i, trace := range traces {
		require.Empty(t, trace.Error, "transaction %d", i)
		gas[i] = uint64(trace.Result.GasUsed)
	}
	return gas
}

// domainCumulative returns the cumulative gas the receipt domain holds for the n
// transactions of block.
func domainCumulative(t *testing.T, m *execmoduletester.ExecModuleTester, block uint64, n int) []uint64 {
	t.Helper()
	out := make([]uint64, n)
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		first, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, block)
		require.NoError(t, err)
		for i := range out {
			// ReceiptAsOf(n) reads what txNum n-1 wrote; transaction i is at first+1+i.
			out[i], _, _, err = rawtemporaldb.ReceiptAsOf(tx, first+2+uint64(i))
			require.NoError(t, err)
		}
		return nil
	}))
	return out
}
