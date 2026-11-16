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

package composer

import (
	"bytes"
	"encoding/json"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/eez/config"
	"github.com/erigontech/erigon/eez/internal/stateless"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	eezrules "github.com/erigontech/erigon/eez/rules"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
)

// syncWindow is a recorded window ending at a Sync block with cross-chain
// transactions.
type syncWindow struct {
	config *chain.Config
	engine rules.Engine
	sync   *types.Block
	pre    PreState
	exec   *stateless.Result
}

func loadSyncWindow(t *testing.T) *syncWindow {
	t.Helper()
	systemtx.Register()
	fw, err := testfixtures.LoadWindow("captured-devnet-window-468")
	require.NoError(t, err)
	w := &syncWindow{config: new(chain.Config)}
	require.NoError(t, json.Unmarshal(fw.ChainConfig, w.config))
	last := fw.Blocks[len(fw.Blocks)-1]
	w.sync = new(types.Block)
	require.NoError(t, rlp.DecodeBytes(last.RLP, w.sync))
	w.config.L2 = &config.L2Config{RollupID: 1, L1ChainID: 7331, Registry: common.HexToAddress("0x01"), BlockInterval: 2, L1BlockTime: 12, BlockGasLimit: w.sync.GasLimit()}
	w.engine, err = eezrules.New(w.config)
	require.NoError(t, err)
	plain := func(in []hexutil.Bytes) [][]byte {
		out := make([][]byte, len(in))
		for i, b := range in {
			out[i] = b
		}
		return out
	}
	witness := &stateless.Witness{State: plain(last.Witness.State), Codes: plain(last.Witness.Codes), Headers: plain(last.Witness.Headers)}
	reader, headers, blockHash, err := stateless.PreState(w.config, w.sync, witness)
	require.NoError(t, err)
	w.pre = PreState{Reader: reader, Headers: headers, BlockHash: blockHash}
	w.exec, err = stateless.Execute(w.config, w.engine, w.sync, witness)
	require.NoError(t, err)
	return w
}

func (w *syncWindow) ctx() protocol.SyncTxContext {
	return protocol.SyncTxContext{ChainID: w.config.ChainID.Uint64(), RollupID: 1, EEZL2: protocol.EEZL2Address}
}

func requireReceipt(t *testing.T, want, got *types.Receipt, i int) {
	t.Helper()
	require.Equal(t, want.Status, got.Status, "transaction %d", i)
	require.Equal(t, want.GasUsed, got.GasUsed, "transaction %d", i)
	require.Equal(t, want.CumulativeGasUsed, got.CumulativeGasUsed, "transaction %d", i)
	require.Len(t, got.Logs, len(want.Logs), "transaction %d", i)
	for j := range want.Logs {
		require.Equal(t, want.Logs[j].Address, got.Logs[j].Address, "transaction %d log %d", i, j)
		require.Equal(t, want.Logs[j].Topics, got.Logs[j].Topics, "transaction %d log %d", i, j)
		require.Equal(t, want.Logs[j].Data, got.Logs[j].Data, "transaction %d log %d", i, j)
	}
}

// TestDraftReplaysASyncBlock: a draft of a recorded Sync block's
// transactions gets the receipts the block got, and so does a fork of it.
func TestDraftReplaysASyncBlock(t *testing.T) {
	w := loadSyncWindow(t)
	d, err := NewDraft(w.config, w.engine, w.sync.Header(), w.pre)
	require.NoError(t, err)
	txs := w.sync.Transactions()
	require.Len(t, txs, 5)
	var fork *Draft
	for i, tx := range txs {
		if i == 2 {
			fork, err = d.Fork()
			require.NoError(t, err)
		}
		r, err := d.Apply(tx, nil)
		require.NoError(t, err)
		requireReceipt(t, w.exec.Receipts[i], r, i)
	}
	for i, tx := range txs[2:] {
		r, err := fork.Apply(tx, nil)
		require.NoError(t, err)
		requireReceipt(t, w.exec.Receipts[i+2], r, i+2)
	}
	require.Equal(t, w.sync.GasUsed(), d.GasUsed())
}

// recordedDelivery is the inbound call the window's Sync block delivers in
// its last transaction.
func recordedDelivery(t *testing.T, w *syncWindow) ([]byte, *Call, []byte) {
	t.Helper()
	txs := w.sync.Transactions()
	var buf bytes.Buffer
	require.NoError(t, txs[4].MarshalBinary(&buf))
	sys, err := protocol.DecodeSystemTx(buf.Bytes())
	require.NoError(t, err)
	table, err := protocol.DecodeExecuteIncomingCrossChainCall(sys.Input)
	require.NoError(t, err)
	require.Len(t, table.Entries, 1)
	e := table.Entries[0]
	require.Len(t, e.IncomingCalls, 1)
	in := e.IncomingCalls[0]
	c := &Call{Target: in.TargetAddress, RollupID: 1, Source: in.SourceAddress, SourceRollup: in.SourceRollupID, Data: in.Data}
	require.False(t, c.Value.SetFromBig(in.Value))
	return buf.Bytes(), c, e.ReturnData
}

// TestDeliveriesProbeTheTarget: on the recorded Sync block before its
// delivery, probing the delivered call recovers the target's recorded output,
// and the delivery made with it is the recorded one.
func TestDeliveriesProbeTheTarget(t *testing.T) {
	w := loadSyncWindow(t)
	raw, call, output := recordedDelivery(t, w)
	d, err := NewDraft(w.config, w.engine, w.sync.Header(), w.pre)
	require.NoError(t, err)
	for _, tx := range w.sync.Transactions()[:4] {
		_, err := d.Apply(tx, nil)
		require.NoError(t, err)
	}
	ds, err := NewDeliveries(w.ctx(), d)
	require.NoError(t, err)
	out, err := ds.Dispatch(call)
	require.NoError(t, err)
	require.Equal(t, output, out)
	require.Len(t, ds.Delivered, 1)
	var got bytes.Buffer
	require.NoError(t, ds.Delivered[0].Tx.MarshalBinary(&got))
	require.Equal(t, raw, got.Bytes())
	require.Len(t, d.Txs(), 4, "the draft is the caller's to extend")
}

func TestDeliveriesRefuse(t *testing.T) {
	w := loadSyncWindow(t)
	_, call, _ := recordedDelivery(t, w)
	d, err := NewDraft(w.config, w.engine, w.sync.Header(), w.pre)
	require.NoError(t, err)
	for _, tx := range w.sync.Transactions()[:4] {
		_, err := d.Apply(tx, nil)
		require.NoError(t, err)
	}
	for _, c := range []struct {
		name  string
		shape func(c *Call)
		want  string
	}{
		{"another rollup", func(c *Call) { c.RollupID = 2 }, "call to rollup 2"},
		{"EEZL2", func(c *Call) { c.Target = protocol.EEZL2Address }, "EEZL2"},
		{"a reverting target", func(c *Call) { c.Data = []byte{0xde, 0xad, 0xbe, 0xef}; c.Value = uint256.Int{} }, "reverted"},
	} {
		t.Run(c.name, func(t *testing.T) {
			ds, err := NewDeliveries(w.ctx(), d)
			require.NoError(t, err)
			bad := *call
			c.shape(&bad)
			_, err = ds.Dispatch(&bad)
			require.Error(t, err)
			require.True(t, IsPoison(err), err)
			require.ErrorContains(t, err, c.want)
			require.Empty(t, ds.Delivered)
		})
	}
}

func TestProbeTracer(t *testing.T) {
	eezl2, other := accounts.InternAddress(protocol.EEZL2Address), accounts.InternAddress(common.HexToAddress("0x5"))
	deliver := append(slices.Clone(executeOnBehalf), 1, 2)
	p := &probeTracer{eezl2: eezl2}
	h := p.hooks()
	enter := func(op vm.OpCode, from accounts.Address, input []byte) {
		h.OnEnter(0, byte(op), from, other, false, input, 0, uint256.Int{}, nil)
	}
	enter(vm.CALL, other, deliver) // not EEZL2
	enter(vm.CALL, eezl2, []byte{1, 2, 3, 4})
	enter(vm.STATICCALL, eezl2, deliver)
	enter(vm.CALL, eezl2, deliver)
	h.OnExit(0, []byte{9}, 0, nil, false)
	h.OnExit(0, nil, 0, nil, false)
	h.OnExit(0, nil, 0, nil, false)
	h.OnExit(0, nil, 0, nil, false)
	require.Equal(t, []capture{{success: true, output: []byte{9}}}, p.captures)
	enter(vm.CALL, eezl2, deliver)
	h.OnExit(0, []byte{7}, 0, vm.ErrExecutionReverted, true)
	require.Equal(t, capture{output: []byte{7}}, p.captures[1])
}

func TestProbed(t *testing.T) {
	out, err := probed([]capture{{success: true, output: []byte{1}}})
	require.NoError(t, err)
	require.Equal(t, []byte{1}, out)
	for name, c := range map[string][]capture{
		"no call":     nil,
		"nested call": {{success: true}, {success: true}},
		"reverted":    {{output: []byte{1}}},
	} {
		_, err := probed(c)
		require.Error(t, err, name)
	}
}
