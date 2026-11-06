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
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/deriver"
	"github.com/erigontech/erigon/eez/deriver/derivertest"
	"github.com/erigontech/erigon/eez/networks"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/stagedsync"
	tracersConfig "github.com/erigontech/erigon/execution/tracing/tracers/config"
	_ "github.com/erigontech/erigon/execution/tracing/tracers/native"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
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
	api := jsonrpc.NewPrivateDebugAPI(
		jsonrpc.NewBaseApi(nil, m.StateCache, m.BlockReader, m.Engine, &rpccfg.BaseApiConfig{Dirs: m.Dirs}),
		m.DB, nil, &rpccfg.DebugApiConfig{})
	var buf bytes.Buffer
	stream := jsonstream.New(&buf)
	tracer := "callTracer"
	require.NoError(t, api.TraceTransaction(t.Context(), txn.Hash(), &tracersConfig.TraceConfig{Tracer: &tracer}, stream))
	require.NoError(t, stream.Flush())
	var trace struct {
		GasUsed hexutil.Uint64 `json:"gasUsed"`
	}
	require.NoError(t, json.Unmarshal(buf.Bytes(), &trace), buf.String())
	return uint64(trace.GasUsed)
}

// generatedGasUsed returns the gas of the receipt GetReceipt generates under cc
// for the transaction at index of block, by re-executing it alone.
func generatedGasUsed(t *testing.T, m *execmoduletester.ExecModuleTester, cc *chain.Config, block *types.Block, index int) uint64 {
	t.Helper()
	gen := receipts.NewGenerator(m.Dirs, m.BlockReader, m.Engine, nil, 5*time.Minute)
	var gasUsed uint64
	require.NoError(t, m.DB.ViewTemporal(t.Context(), func(tx kv.TemporalTx) error {
		first, err := m.BlockReader.TxnumReader().Min(t.Context(), tx, block.NumberU64())
		require.NoError(t, err)
		// Transaction i is at first+1+i.
		txNum := first + 1 + uint64(index)
		_, persisted, err := gen.PersistedReceipt(t.Context(), tx, block.HeaderNoCopy(), index, txNum)
		require.NoError(t, err)
		require.False(t, persisted, "a persisted receipt would not re-execute")
		receipt, err := gen.GetReceipt(t.Context(), cc, tx, block.HeaderNoCopy(), block.Transactions()[index], index, txNum, nil)
		require.NoError(t, err)
		gasUsed = receipt.GasUsed
		return nil
	}))
	return gasUsed
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
}

// TestReplayGasFromBlock checks that up to Osaka a replay runs a type-0x76
// transaction, which encodes no limit, with what the receipts before it leave.
func TestReplayGasFromBlock(t *testing.T) {
	m, _, built := systemTxAfter(t, refunder, nil)
	want := built.Receipts[1].GasUsed
	require.Equal(t, want, traceGasUsed(t, m, built.Block.Transactions()[1]), "debug_traceTransaction")
	require.Equal(t, want, generatedGasUsed(t, m, m.ChainConfig, built.Block, 1), "GetReceipt")
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
	third := extend(second.Block.HeaderNoCopy(), sys)
	require.Less(t, l2.BlockGasLimit-second.Receipts[0].CumulativeGasUsed, sys.GetGasLimit())
	require.Equal(t, sys.GetGasLimit(), third.Receipts[0].GasUsed)
	require.Equal(t, third.Receipts[0].GasUsed, traceGasUsed(t, m, third.Block.Transactions()[0]), "debug_traceTransaction")
	require.Equal(t, third.Receipts[0].GasUsed, generatedGasUsed(t, m, m.ChainConfig, third.Block, 0), "GetReceipt")
}
