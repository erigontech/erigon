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
	"math/big"
	"slices"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/protocol/da"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// window returns the recorded blocks of a window and the batch L1 mined
// for them.
func window(t *testing.T, name string) ([]*types.Block, *protocol.ProofSystemBatch, []byte) {
	t.Helper()
	systemtx.Register()
	w, err := testfixtures.LoadWindow(name)
	require.NoError(t, err)
	blocks := make([]*types.Block, len(w.Blocks))
	for i, wb := range w.Blocks {
		blocks[i] = new(types.Block)
		require.NoError(t, rlp.DecodeBytes(wb.RLP, blocks[i]))
	}
	mined, err := protocol.DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	return blocks, mined, w.PostBatch
}

func devnetBatcher(proofSystem common.Address) *Batcher {
	return &Batcher{RollupID: 1, ProofSystem: proofSystem, Signer: types.LatestSignerForChainID(uint256.NewInt(6290))}
}

// TestAnchorBatchOfRecordedWindows is T-CMP-4 for anchor-only batches: from
// the blocks of a window the reference composer settled, the batch is the
// one L1 mined, byte for byte once it carries the mined proof.
func TestAnchorBatchOfRecordedWindows(t *testing.T) {
	for _, name := range []string{"captured-devnet-window-84", "captured-devnet-anchor-402"} {
		t.Run(name, func(t *testing.T) {
			blocks, mined, input := window(t, name)
			b, err := devnetBatcher(mined.ProofSystems[0]).AnchorBatch(blocks[0].ParentHash(), blocks)
			require.NoError(t, err)
			require.Empty(t, b.Proofs)
			b.Proofs = mined.Proofs
			enc, err := protocol.EncodePostAndVerifyBatch(b)
			require.NoError(t, err)
			require.Equal(t, input, enc)
		})
	}
}

// TestAnchorBatchRefuses: a batch that would not derive to the blocks is
// not built: a terminal with transactions, a system transaction by type or
// by sender, a chain broken or not on the cursor.
func TestAnchorBatchRefuses(t *testing.T) {
	blocks, mined, _ := window(t, "captured-devnet-anchor-402")
	bt := devnetBatcher(mined.ProofSystems[0])
	cursor := blocks[0].ParentHash()
	require.NotEmpty(t, blocks[0].Transactions(), "the window's first block carries a transaction")
	ordinary := blocks[0].Transactions()[0]

	// with returns blocks with block i rebuilt to carry txs, its children
	// relinked.
	with := func(i int, txs ...types.Transaction) []*types.Block {
		out := append([]*types.Block(nil), blocks...)
		for j := i; j < len(out); j++ {
			h := types.CopyHeader(out[j].Header())
			body := out[j].Transactions()
			if j == i {
				body = txs
			} else {
				h.ParentHash = out[j-1].Hash()
			}
			out[j] = types.NewBlock(h, body, nil, nil, nil, nil)
		}
		return out
	}
	last := len(blocks) - 1

	_, err := bt.AnchorBatch(cursor, with(last, ordinary))
	require.ErrorContains(t, err, "terminal block")

	// By type, whatever the sender.
	system, err := systemtx.New(protocol.SystemTx{ChainID: 6290, To: protocol.EEZL2Address})
	require.NoError(t, err)
	native, err := systemtx.NewNative(protocol.NativeTx{ChainID: 6290, To: protocol.RollupXEEZL2Address, GasLimit: 21_000})
	require.NoError(t, err)
	for _, tx := range []types.Transaction{system, native} {
		tx.SetSender(accounts.InternAddress(common.HexToAddress("0xa11ce")))
		_, err = bt.AnchorBatch(cursor, with(1, tx))
		require.ErrorContains(t, err, "system transaction of type", "type %#x", tx.Type())
	}

	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signed := func(t *testing.T) types.Transaction {
		to := common.HexToAddress("0xb0b")
		tx, err := types.SignTx(&types.DynamicFeeTransaction{CommonTx: types.CommonTx{GasLimit: 21_000, To: &to}, ChainID: *uint256.NewInt(6290)}, *bt.Signer, key)
		require.NoError(t, err)
		return tx
	}
	_, err = bt.AnchorBatch(cursor, with(1, signed(t)))
	require.NoError(t, err, "an ordinary sender")
	sysKeyed := withSender(t, signed(t), protocol.SystemAddress)
	_, err = bt.AnchorBatch(cursor, with(1, sysKeyed))
	require.ErrorContains(t, err, "system transaction by its sender")

	_, err = bt.AnchorBatch(common.Hash{1}, blocks)
	require.ErrorContains(t, err, "cursor")
	broken := append([]*types.Block{blocks[0]}, blocks[2:]...)
	_, err = bt.AnchorBatch(cursor, broken)
	require.ErrorContains(t, err, "parent")
	_, err = bt.AnchorBatch(cursor, nil)
	require.Error(t, err)
}

// withSender returns tx with its sender recorded as sender, as a block read
// from the database carries it.
func withSender(t *testing.T, tx types.Transaction, sender common.Address) types.Transaction {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))
	out, err := types.DecodeTransaction(buf.Bytes())
	require.NoError(t, err)
	out.SetSender(accounts.InternAddress(sender))
	return out
}

// TestAnchorBatchCarriesEachBlock: the DA carries each block's beneficiary,
// extra data and transactions, in order.
func TestAnchorBatchCarriesEachBlock(t *testing.T) {
	blocks, mined, _ := window(t, "captured-devnet-anchor-402")
	for i := range blocks {
		h := types.CopyHeader(blocks[i].Header())
		h.Coinbase = common.Address{byte(i + 1)}
		h.Extra = []byte{byte(0xe0 + i)}
		if i > 0 {
			h.ParentHash = blocks[i-1].Hash()
		}
		blocks[i] = types.NewBlock(h, blocks[i].Transactions(), nil, nil, nil, nil)
	}
	b, err := devnetBatcher(mined.ProofSystems[0]).AnchorBatch(blocks[0].ParentHash(), blocks)
	require.NoError(t, err)
	msgs, err := da.Decode(nil, b.CallData)
	require.NoError(t, err)
	payload, err := da.ParsePayload(msgs)
	require.NoError(t, err)
	require.Equal(t, uint64(1), payload.RollupID)
	require.Empty(t, payload.Actions)
	span, err := da.DecodeSpan(payload.Operations)
	require.NoError(t, err)
	var txs [][]byte
	for i, block := range blocks {
		require.Equal(t, block.Coinbase(), span.Beneficiaries[i], "block %d", i)
		require.Equal(t, block.Extra(), span.ExtraData[i], "block %d", i)
		require.Equal(t, uint32(len(block.Transactions())), span.TxCounts[i], "block %d", i)
		for _, tx := range block.Transactions() {
			var buf bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&buf))
			txs = append(txs, buf.Bytes())
		}
	}
	require.Equal(t, txs, span.Transactions)
	require.Equal(t, blocks[len(blocks)-1].Hash(), b.Entries[0].RollupUpdates[0].NewRoot)
}

// effectsOf returns the effects a batch's DA publishes.
func effectsOf(t *testing.T, b *protocol.ProofSystemBatch) Effects {
	t.Helper()
	msgs, err := da.Decode(nil, b.CallData)
	require.NoError(t, err)
	p, err := da.ParsePayload(msgs)
	require.NoError(t, err)
	var fx Effects
	for i := range p.Actions {
		e, err := protocol.EntryFromAction(&p.Actions[i], 1)
		require.NoError(t, err)
		if p.Actions[i].SourceRollupID == 1 {
			fx.Outbound = append(fx.Outbound, *e)
		} else {
			fx.Inbound = append(fx.Inbound, *e)
		}
	}
	return fx
}

func settled(b *protocol.ProofSystemBatch) []common.Hash {
	var out []common.Hash
	for i := range b.Entries {
		out = append(out, b.Entries[i].RollupUpdates[0].NewRoot)
	}
	return out
}

// TestBatchOfWindow468 is T-CMP-4 for a batch with effects: from window
// 468's blocks, the effects its DA publishes and the blocks its entries
// settle, the batch is the mined one, less its proof.
func TestBatchOfWindow468(t *testing.T) {
	blocks, mined, input := window(t, "captured-devnet-window-468")
	fx := effectsOf(t, mined)
	require.Len(t, fx.Outbound, 2)
	require.Len(t, fx.Inbound, 1)
	b, err := devnetBatcher(mined.ProofSystems[0]).Batch(blocks[0].ParentHash(), blocks, fx, settled(mined), false)
	require.NoError(t, err)
	require.Empty(t, b.Proofs)
	b.Proofs = mined.Proofs
	enc, err := protocol.EncodePostAndVerifyBatch(b)
	require.NoError(t, err)
	require.Equal(t, input, enc)

	live, err := devnetBatcher(mined.ProofSystems[0]).Batch(blocks[0].ParentHash(), blocks, fx, settled(mined), true)
	require.NoError(t, err)
	require.Equal(t, uint64(protocol.LiveBlockNumber), live.BlockNumber)
}

// TestBatchMovesEther: an outbound effect takes its call's value out of
// the rollup, an inbound one brings it in.
func TestBatchMovesEther(t *testing.T) {
	blocks, mined, _ := window(t, "captured-devnet-window-468")
	fx := effectsOf(t, mined)
	fx.Outbound[0].L2ToL1Calls[0].Value = big.NewInt(3)
	fx.Inbound[0].L2ToL1Calls[0].Value = big.NewInt(5)
	b, err := devnetBatcher(mined.ProofSystems[0]).Batch(blocks[0].ParentHash(), blocks, fx, settled(mined), false)
	require.NoError(t, err)
	var deltas []int64
	for _, e := range b.Entries {
		deltas = append(deltas, e.RollupUpdates[0].EtherDelta.Int64())
	}
	require.Equal(t, []int64{0, -3, 0, 5}, deltas)
}

// TestBatchRefuses: a batch whose effects do not match its terminal block
// or the blocks it settles is not built.
func twoCalls(e protocol.ExecutionEntry) protocol.ExecutionEntry {
	e.L2ToL1Calls = []protocol.CrossChainCall{e.L2ToL1Calls[0], e.L2ToL1Calls[0]}
	return e
}

func proxied(e protocol.ExecutionEntry) protocol.ExecutionEntry {
	e.ProxyEntryHash = common.Hash{1}
	return e
}

func noCall(e protocol.ExecutionEntry) protocol.ExecutionEntry {
	e.L2ToL1Calls = nil
	return e
}

func TestBatchRefuses(t *testing.T) {
	blocks, mined, _ := window(t, "captured-devnet-window-468")
	bt := devnetBatcher(mined.ProofSystems[0])
	cursor := blocks[0].ParentHash()
	fx := effectsOf(t, mined)
	candidates := settled(mined)
	for _, c := range []struct {
		name       string
		fx         Effects
		candidates []common.Hash
		want       string
	}{
		{"a settled block missing", fx, candidates[:3], "candidates"},
		{"another terminal", fx, append(slices.Clone(candidates[:3]), common.Hash{1}), "terminal"},
		{"an outbound effect missing", Effects{Outbound: fx.Outbound[:1], Inbound: fx.Inbound}, append(slices.Clone(candidates[:2]), candidates[3]), "user transactions"},
		{"no effects", Effects{}, candidates[3:], "user transactions"},
		{"an outbound effect with a proxy entry", Effects{Outbound: []protocol.ExecutionEntry{proxied(fx.Outbound[0]), fx.Outbound[1]}, Inbound: fx.Inbound}, candidates, "outbound effect 0"},
		{"an outbound effect of two calls", Effects{Outbound: []protocol.ExecutionEntry{twoCalls(fx.Outbound[0]), fx.Outbound[1]}, Inbound: fx.Inbound}, candidates, "outbound effect 0"},
		{"an inbound effect of no call", Effects{Outbound: fx.Outbound, Inbound: []protocol.ExecutionEntry{noCall(fx.Inbound[0])}}, candidates, "inbound effect 0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := bt.Batch(cursor, blocks, c.fx, c.candidates, false)
			require.ErrorContains(t, err, c.want)
		})
	}
}
