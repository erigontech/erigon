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
