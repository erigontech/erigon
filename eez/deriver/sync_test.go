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
package deriver

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// appliedAll is the settlement of every entry of b, roles assigned as the
// attribution assigns them.
func appliedAll(b *protocol.ProofSystemBatch) *protocol.Settlement {
	s := &protocol.Settlement{}
	var outbound, inbound int
	for i := range b.Entries {
		e := &b.Entries[i]
		a := protocol.AppliedEntry{Index: i}
		switch {
		case e.ProxyEntryHash != (common.Hash{}):
			a.Role, a.Ordinal = protocol.RoleInbound, inbound
			inbound++
		case len(e.L2ToL1Calls) == 0:
			a.Role = protocol.RoleAnchor
		default:
			a.Role, a.Ordinal = protocol.RoleOutbound, outbound
			outbound++
		}
		s.Applied = append(s.Applied, a)
	}
	return s
}

type window468 struct {
	batch   *protocol.ProofSystemBatch
	payload *Payload
	block   *types.Block
	txs     [][]byte
	nonce   uint64
}

func loadWindow468(t *testing.T) *window468 {
	t.Helper()
	systemtx.Register()
	w, err := testfixtures.LoadWindow("captured-devnet-window-468")
	require.NoError(t, err)
	r := &window468{block: new(types.Block)}
	r.batch, err = protocol.DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	r.payload, err = DecodePayload(r.batch, 1)
	require.NoError(t, err)
	require.NoError(t, rlp.DecodeBytes(w.Blocks[len(w.Blocks)-1].RLP, r.block))
	for _, tx := range r.block.Transactions() {
		var buf bytes.Buffer
		require.NoError(t, tx.MarshalBinary(&buf))
		r.txs = append(r.txs, buf.Bytes())
	}
	// The system address nonce at the parent; executing the rebuilt block
	// checks it.
	r.nonce = r.block.Transactions()[0].GetNonce()
	return r
}

var ctx468 = protocol.SyncTxContext{ChainID: 6290, RollupID: 1, EEZL2: protocol.EEZL2Address}

// TestSyncBlockWindow468 rebuilds the only recorded Sync block with system
// transactions byte for byte: two outbound pairs and a delivery.
func TestSyncBlockWindow468(t *testing.T) {
	w := loadWindow468(t)
	sb, err := BuildSyncBlock(w.payload, appliedAll(w.batch), ctx468, w.nonce)
	require.NoError(t, err)
	require.Equal(t, w.txs, sb.Txs)
	require.Len(t, sb.Outbound, 2)
}

func TestSyncBlockPartialAndResumed(t *testing.T) {
	w := loadWindow468(t)
	all := appliedAll(w.batch).Applied
	load0, user0, user1 := w.txs[0], w.txs[1], w.txs[3]
	nonced := func(tx []byte, nonce uint64) []byte {
		decoded, err := types.UnmarshalTransactionFromBinary(tx, false)
		require.NoError(t, err)
		body := decoded.(*systemtx.Transaction).Body()
		body.Nonce = nonce
		renonced, err := systemtx.New(body)
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, renonced.MarshalBinary(&buf))
		return buf.Bytes()
	}

	for _, tc := range []struct {
		name    string
		applied []protocol.AppliedEntry
		want    [][]byte
	}{
		{"anchor only", all[:1], [][]byte{}},
		{"anchor and first outbound", all[:2], [][]byte{load0, user0}},
		{"second outbound only", all[2:3], [][]byte{nonced(w.txs[2], w.nonce+1), user1}},
		{"delivery only", all[3:], [][]byte{nonced(w.txs[4], w.nonce+2)}},
		{"first outbound and delivery", []protocol.AppliedEntry{all[1], all[3]}, [][]byte{load0, user0, nonced(w.txs[4], w.nonce+1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sb, err := BuildSyncBlock(w.payload, &protocol.Settlement{Applied: tc.applied}, ctx468, w.nonce)
			require.NoError(t, err)
			require.Equal(t, tc.want, sb.Txs)
		})
	}

	beyond := protocol.AppliedEntry{Index: 9, Role: protocol.RoleOutbound, Ordinal: 2}
	_, err := BuildSyncBlock(w.payload, &protocol.Settlement{Applied: []protocol.AppliedEntry{beyond}}, ctx468, w.nonce)
	require.ErrorIs(t, err, ErrSyncBlock)
	beyond.Role = protocol.RoleInbound
	_, err = BuildSyncBlock(w.payload, &protocol.Settlement{Applied: []protocol.AppliedEntry{beyond}}, ctx468, w.nonce)
	require.ErrorIs(t, err, ErrSyncBlock)

	short := *w.payload
	spanCopy := *w.payload.Span
	spanCopy.TxCounts = append([]uint32(nil), spanCopy.TxCounts...)
	spanCopy.TxCounts[len(spanCopy.TxCounts)-1] = 1
	spanCopy.Transactions = spanCopy.Transactions[:1]
	short.Span = &spanCopy
	_, err = BuildSyncBlock(&short, appliedAll(w.batch), ctx468, w.nonce)
	require.ErrorIs(t, err, ErrSyncBlock)
}

func TestResumeTxs(t *testing.T) {
	a, b, c := []byte{1}, []byte{2}, []byte{3}
	settled, other := common.Hash{1}, common.Hash{2}

	_, replay := ResumeTxs(settled, settled, [][]byte{a}, [][]byte{b})
	require.False(t, replay, "local is the settled block")

	txs, replay := ResumeTxs(other, settled, [][]byte{a}, [][]byte{b, c})
	require.True(t, replay)
	require.Equal(t, [][]byte{a, b, c}, txs)

	txs, _ = ResumeTxs(other, settled, [][]byte{a, b, c}, [][]byte{b, c})
	require.Equal(t, [][]byte{a, b, c}, txs, "content the block ends with is not appended twice")

	txs, replay = ResumeTxs(other, settled, [][]byte{a}, nil)
	require.True(t, replay, "a differing block is rebuilt even without new content")
	require.Equal(t, [][]byte{a}, txs)
}

func TestResumeNativeTxs(t *testing.T) {
	native := func(gas uint64) []byte {
		enc, err := (&protocol.NativeTx{ChainID: 699169, SourceHash: common.Hash{7}, GasLimit: gas, To: protocol.RollupXEEZL2Address, Input: []byte{1}}).MarshalBinary()
		require.NoError(t, err)
		return enc
	}
	user := []byte{2}
	entry, final, other := common.Hash{1}, common.Hash{2}, common.Hash{3}
	s := &protocol.Settlement{EntryState: entry, FinalState: final}
	suffix := [][]byte{native(protocol.NativeTxGasCap), user}

	_, replay, err := ResumeNativeTxs(final, s, [][]byte{user, native(1_000), user}, suffix)
	require.NoError(t, err)
	require.False(t, replay, "the settled block, its native gas limit bound to its position")

	txs, replay, err := ResumeNativeTxs(entry, s, [][]byte{user}, suffix)
	require.NoError(t, err)
	require.True(t, replay)
	require.Equal(t, append([][]byte{user}, suffix...), txs)

	_, _, err = ResumeNativeTxs(final, s, [][]byte{user, user, user}, suffix)
	require.ErrorIs(t, err, ErrResume, "the settled block ends with other transactions")
	_, _, err = ResumeNativeTxs(final, s, [][]byte{user}, suffix)
	require.ErrorIs(t, err, ErrResume, "the settled block is shorter than its suffix")
	_, _, err = ResumeNativeTxs(other, s, [][]byte{user}, suffix)
	require.ErrorIs(t, err, ErrResume)
}

func TestBindNativeGas(t *testing.T) {
	systemtx.Register()
	native := func(gas uint64) types.Transaction {
		tx, err := systemtx.NewNative(protocol.NativeTx{ChainID: 699169, GasLimit: gas, To: protocol.RollupXEEZL2Address, Input: []byte{1}})
		require.NoError(t, err)
		return tx
	}
	user := &types.DynamicFeeTransaction{}
	receipts := func(cumulative ...uint64) types.Receipts {
		rs := make(types.Receipts, len(cumulative))
		for i, c := range cumulative {
			rs[i] = &types.Receipt{CumulativeGasUsed: c}
		}
		return rs
	}

	txs := []types.Transaction{native(protocol.NativeTxGasCap), user}
	got, bound, err := bindNativeGas(txs, receipts(100_000, 200_000), 30_000_000)
	require.NoError(t, err)
	require.True(t, bound, "the cap is what a block with room leaves")
	require.Equal(t, txs, got)

	txs = []types.Transaction{user, native(protocol.NativeTxGasCap), native(protocol.NativeTxGasCap)}
	got, bound, err = bindNativeGas(txs, receipts(29_000_000, 29_400_000, 29_500_000), 30_000_000)
	require.NoError(t, err)
	require.False(t, bound)
	require.Equal(t, uint64(1_000_000), got[1].GetGasLimit())
	require.Equal(t, uint64(600_000), got[2].GetGasLimit())
	require.Equal(t, protocol.NativeTxGasCap, txs[1].GetGasLimit(), "the input is not modified")

	_, bound, err = bindNativeGas(got, receipts(29_000_000, 29_400_000, 29_500_000), 30_000_000)
	require.NoError(t, err)
	require.True(t, bound)

	_, _, err = bindNativeGas(txs, receipts(1), 30_000_000)
	require.Error(t, err)
}
