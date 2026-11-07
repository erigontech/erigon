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
	"time"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/jsonrpc"
	"github.com/erigontech/erigon/rpc/jsonrpc/receipts"
	"github.com/erigontech/erigon/rpc/jsonstream"
	"github.com/erigontech/erigon/rpc/rpccfg"
)

// amsterdamReplay holds a block of a transaction storing new slots and, at
// index 1, a native transaction encoding the cap, with the cumulative gas of
// the receipts the custom trace stage rewrote under Amsterdam.
type amsterdamReplay struct {
	m          *execmoduletester.ExecModuleTester
	config     *chain.Config
	built      *types.BlockWithReceipts
	cumulative []uint64
}

// newAmsterdamReplay builds the block under Osaka, then rewrites the receipt
// domain under Amsterdam, where each new slot costs state gas.
func newAmsterdamReplay(t *testing.T, slots byte) *amsterdamReplay {
	t.Helper()
	eezl2 := protocol.RollupXEEZL2Address
	filler := common.HexToAddress("0x00000000000000000000000000000000000f111e")
	spec := networks.Chiado699169
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	// JUMPDEST PUSH1 0 JUMP: EEZL2 burns all the gas it gets.
	genesis.Alloc[eezl2] = types.GenesisAccount{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}}
	// Stores i in slot i, for i from slots down to 1.
	genesis.Alloc[filler] = types.GenesisAccount{Code: []byte{
		byte(vm.PUSH2), 0, slots,
		byte(vm.JUMPDEST), byte(vm.DUP1), byte(vm.DUP1), byte(vm.SSTORE),
		byte(vm.PUSH1), 1, byte(vm.SWAP1), byte(vm.SUB),
		byte(vm.DUP1), byte(vm.PUSH1), 3, byte(vm.JUMPI),
		byte(vm.STOP),
	}}
	spec.Genesis = &genesis
	l2 := genesis.Config.L2.(*config.L2Config)
	require.Equal(t, config.RollupXV01, l2.RuleSet())
	m, execChain := derivertest.NewL2Of(t, spec)

	parent, err := execChain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	user, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{GasLimit: protocol.NativeTxGasCap, To: &filler},
		ChainID:  *genesis.Config.ChainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *types.LatestSignerForChainID(genesis.Config.ChainID), derivertest.UserKey)
	require.NoError(t, err)
	native, err := systemtx.NewNative(protocol.NativeTx{
		ChainID:  genesis.Config.ChainID.Uint64(),
		GasLimit: protocol.NativeTxGasCap,
		To:       eezl2,
		Input:    protocol.ExecuteIncomingCrossChainCallSelector[:],
	})
	require.NoError(t, err)
	built, err := execChain.Extend(t.Context(), parent, deriver.HeaderInputs{
		Number:     1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, []types.Transaction{user, native}, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[0].Status)
	require.Equal(t, protocol.NativeTxGasCap, built.Block.Transactions()[1].GetGasLimit())

	amsterdam := *m.ChainConfig
	amsterdam.AmsterdamTime = common.NewUint64(0)
	cfg := stagedsync.StageCustomTraceCfg([]string{"receipt"}, m.DB, m.Dirs, m.BlockReader, &amsterdam, m.Engine, m.Cfg().Genesis, m.Cfg().Sync)
	require.NoError(t, stagedsync.StageCustomTraceReset(t.Context(), m.DB, cfg.Produce))
	require.NoError(t, stagedsync.SpawnCustomTrace(cfg, t.Context(), m.Log))
	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteChainConfig(tx, parent.Hash(), &amsterdam)
	}))
	return &amsterdamReplay{m: m, config: &amsterdam, built: built, cumulative: domainCumulative(t, m, 1, len(built.Receipts))}
}

// traceGasUsed returns the gas debug_traceTransaction reports for txn.
func traceGasUsed(t *testing.T, m *execmoduletester.ExecModuleTester, txn types.Transaction) uint64 {
	t.Helper()
	gas, err := traceTransactionGas(t, m, txn)
	require.NoError(t, err)
	return gas
}

func traceTransactionGas(t *testing.T, m *execmoduletester.ExecModuleTester, txn types.Transaction) (uint64, error) {
	t.Helper()
	api := jsonrpc.NewPrivateDebugAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, &rpccfg.DebugApiConfig{})
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	tracer := "callTracer"
	if err := api.TraceTransaction(t.Context(), txn.Hash(), &tracersConfig.TraceConfig{Tracer: &tracer}, stream); err != nil {
		return 0, err
	}
	require.NoError(t, stream.Flush())
	var trace struct {
		GasUsed hexutil.Uint64 `json:"gasUsed"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &trace), buf.String())
	return uint64(trace.GasUsed), nil
}

// generatedGasUsed returns the gas of the receipt GetReceipt generates under cc
// for the transaction at index of block, by re-executing it alone.
func generatedGasUsed(t *testing.T, m *execmoduletester.ExecModuleTester, cc *chain.Config, block *types.Block, index int) uint64 {
	t.Helper()
	gas, err := generatedReceiptGas(t, m, cc, block, index)
	require.NoError(t, err)
	return gas
}

func generatedReceiptGas(t *testing.T, m *execmoduletester.ExecModuleTester, cc *chain.Config, block *types.Block, index int) (uint64, error) {
	t.Helper()
	gen := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, 5*time.Minute)
	var receipt *types.Receipt
	var genErr error
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		first, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, block.NumberU64())
		require.NoError(t, err)
		// Transaction i is at first+1+i.
		txNum := first + 1 + uint64(index)
		_, persisted, err := gen.PersistedReceipt(t.Context(), tx, block.HeaderNoCopy(), index, txNum)
		require.NoError(t, err)
		require.False(t, persisted, "a persisted receipt would not re-execute")
		receipt, genErr = gen.GetReceipt(t.Context(), cc, tx, block.HeaderNoCopy(), block.Transactions()[index], index, txNum, nil)
		return nil
	}))
	if genErr != nil {
		return 0, genErr
	}
	return receipt.GasUsed, nil
}

// TestReplayEncodedGasAmsterdam checks that a replay runs a native transaction
// with the limit it encodes, not with what the receipts before it leave.
func TestReplayEncodedGasAmsterdam(t *testing.T) {
	r := newAmsterdamReplay(t, 128)
	native := r.built.Block.Transactions()[1]
	limit := native.GetGasLimit()
	require.Less(t, r.built.Block.GasLimit()-r.cumulative[0], limit)
	// The block's gas before it, the larger of its regular and state gas, leaves it its limit.
	require.Equal(t, limit, r.cumulative[1]-r.cumulative[0])
	require.Equal(t, limit, traceGasUsed(t, r.m, native), "debug_traceTransaction")
	require.Equal(t, limit, generatedGasUsed(t, r.m, r.config, r.built.Block, 1), "GetReceipt")
	intrinsic, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:   native.GetData(),
		IsEIP2: true, IsEIP2028: true, IsEIP7623: true, IsEIP7976: true, IsEIP7981: true, IsEIP2780: true,
	})
	require.False(t, overflow)
	requireTraceGas(t, r.m, r.built.Block, 1, limit, intrinsic.ExecutionGas)
}

// traceMethods replay a transaction of a block; each returns the gas the
// top-level call of the transaction at index gets. With alone, the method runs
// the transaction without the ones before it.
var traceMethods = []struct {
	name  string
	alone bool
	gas   func(t *testing.T, api *jsonrpc.TraceAPIImpl, block *types.Block, index int) (uint64, error)
}{
	{"trace_replayTransaction", true, func(t *testing.T, api *jsonrpc.TraceAPIImpl, block *types.Block, index int) (uint64, error) {
		res, err := api.ReplayTransaction(t.Context(), block.Transactions()[index].Hash(), []string{"trace"}, nil, nil)
		if err != nil {
			return 0, err
		}
		return rootCallGas(t, res.Trace, index), nil
	}},
	{"trace_transaction", true, func(t *testing.T, api *jsonrpc.TraceAPIImpl, block *types.Block, index int) (uint64, error) {
		traces, err := api.Transaction(t.Context(), block.Transactions()[index].Hash(), nil, nil)
		if err != nil {
			return 0, err
		}
		return rootCallGas(t, traces, index), nil
	}},
	// Without stateDiff or vmTrace the transactions of a block run each alone.
	{"trace_replayBlockTransactions trace", true, replayBlockGas("trace")},
	{"trace_replayBlockTransactions trace,stateDiff", false, replayBlockGas("trace", "stateDiff")},
	{"trace_block", true, func(t *testing.T, api *jsonrpc.TraceAPIImpl, block *types.Block, index int) (uint64, error) {
		traces, err := api.Block(t.Context(), rpc.BlockNumber(block.NumberU64()), nil, nil)
		if err != nil {
			return 0, err
		}
		return rootCallGas(t, traces, index), nil
	}},
	{"trace_filter", true, func(t *testing.T, api *jsonrpc.TraceAPIImpl, block *types.Block, index int) (uint64, error) {
		n := rpc.BlockNumber(block.NumberU64())
		var buf bytes.Buffer
		stream := jsonstream.New(&buf)
		if err := api.Filter(t.Context(), jsonrpc.TraceFilterRequest{FromBlock: &n, ToBlock: &n}, nil, nil, stream); err != nil {
			return 0, err
		}
		require.NoError(t, stream.Flush())
		return rootCallGas(t, json.RawMessage(buf.Bytes()), index), nil
	}},
}

func replayBlockGas(traceTypes ...string) func(t *testing.T, api *jsonrpc.TraceAPIImpl, block *types.Block, index int) (uint64, error) {
	return func(t *testing.T, api *jsonrpc.TraceAPIImpl, block *types.Block, index int) (uint64, error) {
		results, err := api.ReplayBlockTransactions(t.Context(), rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(block.NumberU64())), traceTypes, nil, nil)
		if err != nil {
			return 0, err
		}
		require.Len(t, results, block.Transactions().Len())
		return rootCallGas(t, results[index].Trace, index), nil
	}
}

// rootCallGas returns the gas of the top-level call of the transaction at index
// in traces, the parity traces of it or of its block.
func rootCallGas(t *testing.T, traces any, index int) uint64 {
	t.Helper()
	raw, err := json.Marshal(traces)
	require.NoError(t, err)
	var decoded []struct {
		Action struct {
			Gas hexutil.Uint64 `json:"gas"`
		} `json:"action"`
		TraceAddress        []int   `json:"traceAddress"`
		TransactionPosition *uint64 `json:"transactionPosition"`
		Type                string  `json:"type"`
	}
	require.NoError(t, json.Unmarshal(raw, &decoded), string(raw))
	var gas []uint64
	for _, tr := range decoded {
		if tr.Type == "call" && len(tr.TraceAddress) == 0 && (tr.TransactionPosition == nil || *tr.TransactionPosition == uint64(index)) {
			gas = append(gas, uint64(tr.Action.Gas))
		}
	}
	require.Len(t, gas, 1, string(raw))
	return gas[0]
}

// requireTraceGas checks that each trace method runs the transaction at index of
// block, whose target burns all it gets, with want: its top-level call gets that
// gas less intrinsic.
func requireTraceGas(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block, index int, want, intrinsic uint64) {
	t.Helper()
	api := newTraceAPI(m)
	for _, method := range traceMethods {
		t.Run(method.name, func(t *testing.T) {
			gas, err := method.gas(t, api, block, index)
			require.NoError(t, err)
			require.Equal(t, want, gas+intrinsic)
		})
	}
}

func newTraceAPI(m *execmoduletester.ExecModuleTester) *jsonrpc.TraceAPIImpl {
	return jsonrpc.NewTraceAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, &rpccfg.TraceApiConfig{})
}

// TestReplayGasFromBlock checks that up to Osaka a replay runs a type-0x76
// transaction, which encodes no limit, with what the receipts before it leave.
func TestReplayGasFromBlock(t *testing.T) {
	m, _, built := systemTxAfter(t, refunder, nil)
	want := built.Receipts[1].GasUsed
	require.Less(t, want, built.Block.Transactions()[1].GetGasLimit())
	require.Equal(t, want, traceGasUsed(t, m, built.Block.Transactions()[1]), "debug_traceTransaction")
	require.Equal(t, want, generatedGasUsed(t, m, m.ChainConfig, built.Block, 1), "GetReceipt")
	// It has no input: its intrinsic gas is params.TxGas.
	requireTraceGas(t, m, built.Block, 1, want, params.TxGas)
}

// TestReplayGasFromBlockFirstInBlock checks that a replay does not take the receipts
// of the block before for the ones before a type-0x76 transaction first in its block.
func TestReplayGasFromBlockFirstInBlock(t *testing.T) {
	m, l2, built := systemTxAfter(t, refunder, nil)
	execChain := deriver.NewExecChain(m.ExecModule, m.DB, m.BlockReader, m.ReceiptsReader, m.ChainConfig)
	chainID := m.ChainConfig.ChainID
	// Block 2 ends with less than the type-0x76 limit left.
	gas := built.Block.Transactions()[0].GetGasLimit()
	user, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: 1, GasLimit: gas, To: &refunder},
		ChainID:  *chainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *types.LatestSignerForChainID(chainID), derivertest.UserKey)
	require.NoError(t, err)
	extend := func(parent *types.Header, txs ...types.Transaction) *types.BlockWithReceipts {
		b, err := execChain.Extend(t.Context(), parent, deriver.HeaderInputs{
			Number:     parent.Number.Uint64() + 1,
			ParentHash: parent.Hash(),
			Time:       parent.Time + l2.BlockInterval,
			Extra:      []byte{},
			GasLimit:   l2.BlockGasLimit,
		}, txs, common.Hash{}, common.Hash{})
		require.NoError(t, err)
		return b
	}
	second := extend(built.Block.HeaderNoCopy(), user)
	nonce, err := execChain.Nonce(t.Context(), 2, protocol.SystemAddress)
	require.NoError(t, err)
	sys, err := systemtx.New(protocol.SystemTx{ChainID: chainID.Uint64(), Nonce: nonce, To: protocol.RollupXEEZL2Address})
	require.NoError(t, err)
	// A transaction after it, so that a block trace runs the transactions each alone.
	transfer, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: 2, GasLimit: params.TxGas, To: &common.Address{0xf1}},
		ChainID:  *chainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *types.LatestSignerForChainID(chainID), derivertest.UserKey)
	require.NoError(t, err)
	third := extend(second.Block.HeaderNoCopy(), sys, transfer)
	require.Less(t, l2.BlockGasLimit-second.Receipts[0].CumulativeGasUsed, sys.GetGasLimit())
	require.Equal(t, sys.GetGasLimit(), third.Receipts[0].GasUsed)
	require.Equal(t, types.ReceiptStatusSuccessful, third.Receipts[1].Status)
	require.Equal(t, third.Receipts[0].GasUsed, traceGasUsed(t, m, third.Block.Transactions()[0]), "debug_traceTransaction")
	require.Equal(t, third.Receipts[0].GasUsed, generatedGasUsed(t, m, m.ChainConfig, third.Block, 0), "GetReceipt")
	requireTraceGas(t, m, third.Block, 0, third.Receipts[0].GasUsed, params.TxGas)
}

// TestReplayGasFromBlockLaggingReceipts checks that a replay running a type-0x76
// transaction without the ones before it fails, rather than takes an earlier
// block's receipt for theirs, where the receipt domain ends before its block,
// and that neither a replay of a transaction with its own limit nor one running
// them needs their receipts.
func TestReplayGasFromBlockLaggingReceipts(t *testing.T) {
	m, l2, first := systemTxAfter(t, refunder, nil)
	execChain := deriver.NewExecChain(m.ExecModule, m.DB, m.BlockReader, m.ReceiptsReader, m.ChainConfig)
	chainID := m.ChainConfig.ChainID
	sign := func(nonce, gas uint64, to common.Address) types.Transaction {
		txn, err := types.SignTx(&types.DynamicFeeTransaction{
			CommonTx: types.CommonTx{Nonce: nonce, GasLimit: gas, To: &to},
			ChainID:  *chainID,
			FeeCap:   *uint256.NewInt(100 * common.GWei),
		}, *types.LatestSignerForChainID(chainID), derivertest.UserKey)
		require.NoError(t, err)
		return txn
	}
	// Block 2 repeats block 1 with a transfer before the type-0x76 transaction,
	// which gets less than its limit.
	user := sign(1, first.Block.Transactions()[0].GetGasLimit(), refunder)
	plain := sign(2, params.TxGas, common.Address{0xee})
	nonce, err := execChain.Nonce(t.Context(), 1, protocol.SystemAddress)
	require.NoError(t, err)
	sys, err := systemtx.New(protocol.SystemTx{ChainID: chainID.Uint64(), Nonce: nonce, To: protocol.RollupXEEZL2Address})
	require.NoError(t, err)
	parent := first.Block.HeaderNoCopy()
	second, err := execChain.Extend(t.Context(), parent, deriver.HeaderInputs{
		Number:     2,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, []types.Transaction{user, plain, sys}, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	want := second.Receipts[2].GasUsed
	require.Less(t, want, sys.GetGasLimit())

	// The custom trace stage produces the receipt domain again up to block 1 only.
	setExecuted := func(block uint64) {
		require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
			return stages.SaveStageProgress(tx, stages.Execution, block)
		}))
	}
	cfg := stagedsync.StageCustomTraceCfg([]string{"receipt"}, m.DB, m.Dirs, m.BlockReader, m.ChainConfig, m.Engine, m.Cfg().Genesis, m.Cfg().Sync)
	require.NoError(t, stagedsync.StageCustomTraceReset(t.Context(), m.DB, cfg.Produce))
	setExecuted(1)
	require.NoError(t, stagedsync.SpawnCustomTrace(cfg, t.Context(), m.Log))
	setExecuted(2)
	require.Equal(t, []uint64{first.Receipts[0].CumulativeGasUsed, first.Receipts[1].CumulativeGasUsed}, domainCumulative(t, m, 1, 2))
	// Read for the transaction before the type-0x76 one, the domain's latest record leaves it its limit.
	require.Greater(t, l2.BlockGasLimit-domainCumulative(t, m, 2, 2)[1], sys.GetGasLimit())

	gas, err := traceTransactionGas(t, m, plain)
	require.NoError(t, err, "debug_traceTransaction of the transfer")
	require.Equal(t, second.Receipts[1].GasUsed, gas)
	gas, err = generatedReceiptGas(t, m, m.ChainConfig, second.Block, 1)
	require.NoError(t, err, "GetReceipt of the transfer")
	require.Equal(t, second.Receipts[1].GasUsed, gas)

	missing := "block 2: no receipt for the gas used before tx 2"
	_, err = traceTransactionGas(t, m, sys)
	require.ErrorContains(t, err, missing, "debug_traceTransaction")
	_, err = generatedReceiptGas(t, m, m.ChainConfig, second.Block, 2)
	require.ErrorContains(t, err, missing, "GetReceipt")
	api := newTraceAPI(m)
	// A block trace fails on the type-0x76 transaction, so only this one replays the transfer.
	_, err = api.ReplayTransaction(t.Context(), plain.Hash(), []string{"trace"}, nil, nil)
	require.NoError(t, err, "trace_replayTransaction of the transfer")
	for _, method := range traceMethods {
		t.Run(method.name, func(t *testing.T) {
			gas, err := method.gas(t, api, second.Block, 2)
			if method.alone {
				require.ErrorContains(t, err, missing)
				return
			}
			require.NoError(t, err)
			require.Equal(t, want, gas+params.TxGas)
		})
	}
}

// TestExecutionWitnessGasFromBlock checks that the witness of a block proves
// it, to its own stateless check and to the stateless executor, when a
// type-0x76 transaction gets less than its limit from the block and its
// target reads another account with its limit.
func TestExecutionWitnessGasFromBlock(t *testing.T) {
	// The witness reads the parent's commitment from history.
	schema := statecfg.Schema
	statecfg.EnableHistoricalCommitment()
	t.Cleanup(func() { statecfg.Schema = schema })

	eezl2 := protocol.RollupXEEZL2Address
	burner := common.HexToAddress("0x00000000000000000000000000000000000b0123")
	low, high := common.HexToAddress("0x8eb1"), common.HexToAddress("0x8eb2")
	threshold := uint64(16_000_000)
	storeBalance := func(code []byte, addr common.Address) []byte {
		code = append(append(code, byte(vm.PUSH20)), addr[:]...)
		return append(code, byte(vm.BALANCE), byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP))
	}
	// Stores in slot 0 the balance of high when GAS exceeds threshold, else of low.
	code := []byte{byte(vm.GAS), byte(vm.PUSH3), byte(threshold >> 16), byte(threshold >> 8), byte(threshold), byte(vm.LT), byte(vm.PUSH1), 0, byte(vm.JUMPI)}
	code = storeBalance(code, low)
	code[7] = byte(len(code))
	code = storeBalance(append(code, byte(vm.JUMPDEST)), high)

	spec := networks.Chiado699069
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	genesis.Alloc[eezl2] = types.GenesisAccount{Code: code}
	// JUMPDEST PUSH1 0 JUMP: burns all the gas it gets.
	genesis.Alloc[burner] = types.GenesisAccount{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}}
	genesis.Alloc[low] = types.GenesisAccount{Balance: big.NewInt(1)}
	genesis.Alloc[high] = types.GenesisAccount{Balance: big.NewInt(2)}
	spec.Genesis = &genesis
	l2 := genesis.Config.L2.(*config.L2Config)
	require.Equal(t, config.RollupX, l2.RuleSet())
	m, execChain := derivertest.NewL2Of(t, spec)

	parent, err := execChain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	nonce, err := execChain.Nonce(t.Context(), 0, protocol.SystemAddress)
	require.NoError(t, err)
	sys, err := systemtx.New(protocol.SystemTx{ChainID: genesis.Config.ChainID.Uint64(), Nonce: nonce, To: eezl2})
	require.NoError(t, err)
	left := sys.GetGasLimit() - 1_000_000
	require.Less(t, left, threshold)
	require.Greater(t, sys.GetGasLimit()-params.TxGas, threshold)
	user, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{GasLimit: l2.BlockGasLimit - left, To: &burner},
		ChainID:  *genesis.Config.ChainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *types.LatestSignerForChainID(genesis.Config.ChainID), derivertest.UserKey)
	require.NoError(t, err)
	built, err := execChain.Extend(t.Context(), parent, deriver.HeaderInputs{
		Number:     1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, []types.Transaction{user, sys}, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, l2.BlockGasLimit-left, built.Receipts[0].GasUsed)
	require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[1].Status)
	stored, err := execChain.Storage(t.Context(), 1, eezl2, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, common.BigToHash(big.NewInt(1)), stored, "block execution reads low")

	require.NoError(t, m.DB.Update(t.Context(), func(tx kv.RwTx) error {
		return rawdb.WriteDBCommitmentHistoryEnabled(tx, true)
	}))
	api := jsonrpc.NewPrivateDebugAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, &rpccfg.DebugApiConfig{})
	number := rpc.BlockNumber(1)
	// The witness checks itself statelessly under assertions only.
	res, err := func() (*jsonrpc.ExecutionWitnessResult, error) {
		defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
		dbg.AssertEnabled = true
		return api.ExecutionWitness(t.Context(), rpc.BlockNumberOrHash{BlockNumber: &number}, nil)
	}()
	require.NoError(t, err)
	raw := func(bs []hexutil.Bytes) [][]byte {
		out := make([][]byte, len(bs))
		for i, b := range bs {
			out[i] = b
		}
		return out
	}
	w := &stateless.Witness{State: raw(res.State), Codes: raw(res.Codes), Headers: raw(res.Headers)}
	_, err = stateless.Verify(m.ChainConfig, m.Engine, built.Block, w)
	require.NoError(t, err)
}

// TestOverlayCallConstructorGasFromBlock checks that overlay_callConstructor
// replays a type-0x76 transaction before the creation with the gas its block
// left, below its limit, and the replaced constructor with the gas cap, above
// what the block left: EEZL2 stores one of two values by the gas it gets, and
// the constructor deploys what EEZL2 returns of it.
func TestOverlayCallConstructorGasFromBlock(t *testing.T) {
	eezl2 := protocol.RollupXEEZL2Address
	burner := common.HexToAddress("0x00000000000000000000000000000000000b0123")
	threshold := uint64(16_000_000)
	// Without input, stores in slot 0 2 when GAS exceeds threshold, else 1;
	// with input, returns slot 0.
	code := []byte{
		byte(vm.CALLDATASIZE), byte(vm.PUSH1), 0, byte(vm.JUMPI),
		byte(vm.GAS), byte(vm.PUSH3), byte(threshold >> 16), byte(threshold >> 8), byte(threshold), byte(vm.LT), byte(vm.PUSH1), 0, byte(vm.JUMPI),
		byte(vm.PUSH1), 1, byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP),
	}
	code[11] = byte(len(code))
	code = append(code, byte(vm.JUMPDEST), byte(vm.PUSH1), 2, byte(vm.PUSH1), 0, byte(vm.SSTORE), byte(vm.STOP))
	code[2] = byte(len(code))
	code = append(code, byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.SLOAD), byte(vm.PUSH1), 0, byte(vm.MSTORE),
		byte(vm.PUSH1), 32, byte(vm.PUSH1), 0, byte(vm.RETURN))
	// Calls EEZL2 with one byte of input and deploys the word it returns.
	initCode := []byte{byte(vm.PUSH1), 32, byte(vm.PUSH1), 0, byte(vm.PUSH1), 1, byte(vm.PUSH1), 0, byte(vm.PUSH1), 0, byte(vm.PUSH20)}
	initCode = append(append(initCode, eezl2[:]...), byte(vm.GAS), byte(vm.CALL), byte(vm.POP),
		byte(vm.PUSH1), 32, byte(vm.PUSH1), 0, byte(vm.RETURN))

	spec := networks.Chiado699069
	genesis := *spec.Genesis
	genesis.Alloc = maps.Clone(genesis.Alloc)
	genesis.Alloc[derivertest.UserAddress()] = types.GenesisAccount{Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(21), nil)}
	genesis.Alloc[eezl2] = types.GenesisAccount{Code: code}
	// JUMPDEST PUSH1 0 JUMP: burns all the gas it gets.
	genesis.Alloc[burner] = types.GenesisAccount{Code: []byte{byte(vm.JUMPDEST), byte(vm.PUSH1), 0, byte(vm.JUMP)}}
	spec.Genesis = &genesis
	l2 := genesis.Config.L2.(*config.L2Config)
	require.Equal(t, config.RollupX, l2.RuleSet())
	m, execChain := derivertest.NewL2Of(t, spec)

	parent, err := execChain.HeaderByNumber(t.Context(), 0)
	require.NoError(t, err)
	nonce, err := execChain.Nonce(t.Context(), 0, protocol.SystemAddress)
	require.NoError(t, err)
	sys, err := systemtx.New(protocol.SystemTx{ChainID: genesis.Config.ChainID.Uint64(), Nonce: nonce, To: eezl2})
	require.NoError(t, err)
	left := sys.GetGasLimit() - 1_000_000
	require.Less(t, left, threshold)
	require.Greater(t, sys.GetGasLimit()-params.TxGas, threshold)
	signer := types.LatestSignerForChainID(genesis.Config.ChainID)
	user, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{GasLimit: l2.BlockGasLimit - left, To: &burner},
		ChainID:  *genesis.Config.ChainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *signer, derivertest.UserKey)
	require.NoError(t, err)
	creation, err := types.SignTx(&types.DynamicFeeTransaction{
		CommonTx: types.CommonTx{Nonce: 1, GasLimit: 200_000, Data: initCode},
		ChainID:  *genesis.Config.ChainID,
		FeeCap:   *uint256.NewInt(100 * common.GWei),
	}, *signer, derivertest.UserKey)
	require.NoError(t, err)
	built, err := execChain.Extend(t.Context(), parent, deriver.HeaderInputs{
		Number:     1,
		ParentHash: parent.Hash(),
		Time:       parent.Time + l2.BlockInterval,
		Extra:      []byte{},
		GasLimit:   l2.BlockGasLimit,
	}, []types.Transaction{user, sys, creation}, common.Hash{}, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, l2.BlockGasLimit-left, built.Receipts[0].GasUsed)
	require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[1].Status)
	require.Equal(t, types.ReceiptStatusSuccessful, built.Receipts[2].Status)
	want := common.BigToHash(big.NewInt(1))
	stored, err := execChain.Storage(t.Context(), 1, eezl2, common.Hash{})
	require.NoError(t, err)
	require.Equal(t, want, stored, "block execution stores 1")

	base := jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs})
	require.Greater(t, left, l2.BlockGasLimit-built.Receipts[1].CumulativeGasUsed)
	api := jsonrpc.NewOverlayAPI(base, m.DB, &rpccfg.OverlayApiConfig{GasCap: left}, jsonrpc.NewOtterscanAPI(base, m.DB, 25))
	got, err := api.CallConstructor(t.Context(), types.CreateAddress(derivertest.UserAddress(), 1), initCode)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, got.Code)
	require.Equal(t, hexutil.Bytes(want[:]), *got.Code)
}
