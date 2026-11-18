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

package seqsig_test

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/eez/seqsig"
	"github.com/erigontech/erigon/eez/systemtx"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

var (
	to      = common.HexToAddress("0x0101")
	chainID = *uint256.NewInt(1337)
)

func common1() types.CommonTx {
	return types.CommonTx{Nonce: 1, GasLimit: 21_000, To: &to, Data: []byte{1}}
}

func dynamicFee() types.DynamicFeeTransaction {
	return types.DynamicFeeTransaction{CommonTx: common1(), ChainID: chainID, TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2)}
}

// txsOfEveryType returns one transaction of each type an EEZ block may carry.
func txsOfEveryType(t *testing.T) []types.Transaction {
	t.Helper()
	systemtx.Register()
	sys, err := systemtx.New(protocol.SystemTx{ChainID: 1337, To: protocol.EEZL2Address, Input: []byte{1}})
	require.NoError(t, err)
	return []types.Transaction{
		&types.LegacyTx{CommonTx: common1(), GasPrice: *uint256.NewInt(1)},
		&types.AccessListTx{LegacyTx: types.LegacyTx{CommonTx: common1(), GasPrice: *uint256.NewInt(1)}, ChainID: chainID},
		&types.DynamicFeeTransaction{CommonTx: common1(), ChainID: chainID, TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2)},
		&types.BlobTx{DynamicFeeTransaction: dynamicFee(), MaxFeePerBlobGas: *uint256.NewInt(1), BlobVersionedHashes: []common.Hash{{1}}},
		&types.SetCodeTransaction{DynamicFeeTransaction: dynamicFee(), Authorizations: []types.Authorization{{ChainID: chainID, Address: to}}},
		sys,
	}
}

// blockWith returns block 7 on parent carrying txs.
func blockWith(txs ...types.Transaction) *types.Block {
	return types.NewBlock(&types.Header{ParentHash: parent, Root: empty.RootHash, Number: *uint256.NewInt(7)}, txs, nil, nil, nil, nil)
}

// prefixed returns payload behind the canonical RLP header of base, 0x80 for
// a string or 0xc0 for a list.
func prefixed(base byte, payload []byte) []byte {
	n := len(payload)
	if n < 56 {
		return append([]byte{base + byte(n)}, payload...)
	}
	var size []byte
	for ; n > 0; n >>= 8 {
		size = append([]byte{byte(n)}, size...)
	}
	return append(append([]byte{base + 55 + byte(len(size))}, size...), payload...)
}

// assemble returns the block RLP of header with items as its transaction
// list, written as given, and no ommers.
func assemble(t *testing.T, header *types.Header, items ...[]byte) []byte {
	t.Helper()
	h, err := rlp.EncodeToBytes(header)
	require.NoError(t, err)
	txs := prefixed(0xc0, bytes.Join(items, nil))
	return prefixed(0xc0, bytes.Join([][]byte{h, txs, {0xc0}}, nil))
}

func typed(t *testing.T, tx types.Transaction) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, tx.MarshalBinary(&buf))
	return buf.Bytes()
}

// TestDecodeBlockEveryType: a block carrying one transaction of each type an
// EEZ block may carry decodes, and its encoding is the one the sequencer
// sends.
func TestDecodeBlockEveryType(t *testing.T) {
	b := blockWith(txsOfEveryType(t)...)
	enc := encodeBlock(t, b)
	var items [][]byte
	for _, tx := range b.Transactions() {
		if tx.Type() == types.LegacyTxType {
			legacy, err := rlp.EncodeToBytes(tx)
			require.NoError(t, err)
			items = append(items, legacy)
			continue
		}
		items = append(items, prefixed(0x80, typed(t, tx)))
	}
	require.Equal(t, enc, assemble(t, b.HeaderNoCopy(), items...), "typed transactions are strings, legacy ones lists")

	got, err := seqsig.DecodeBlock(enc)
	require.NoError(t, err)
	require.Equal(t, b.Hash(), got.Hash())
	require.Len(t, got.Transactions(), 6)

	v, err := signed(t, headOf(b), enc).Verify(domain, common.HexToAddress(signerHex))
	require.NoError(t, err)
	require.Equal(t, b.Hash(), v.Block.Hash())
}

// TestDecodeBlockRejects: a block is refused unless it is in its canonical
// form, its header has the Ethereum shape, and every transaction is of a type
// an EEZ block may carry. The decoder itself refuses the first three
// non-canonical forms; an AuRa step and empty seal in place of the zero mix
// hash and nonce decode, with the canonical block's hash, and only the
// re-encoding refuses them.
func TestDecodeBlockRejects(t *testing.T) {
	systemtx.Register()
	df := &types.DynamicFeeTransaction{CommonTx: common1(), ChainID: chainID, TipCap: *uint256.NewInt(1), FeeCap: *uint256.NewInt(2)}
	dfEnc := typed(t, df)
	require.Less(t, len(dfEnc), 56, "short enough for a one-byte string header")
	dfHeader := blockWith(df).Header()

	legacy := &types.LegacyTx{CommonTx: common1(), GasPrice: *uint256.NewInt(1)}
	legacyEnc, err := rlp.EncodeToBytes(legacy)
	require.NoError(t, err)

	native, err := systemtx.NewNative(protocol.NativeTx{ChainID: 1337, GasLimit: 1, To: protocol.RollupXEEZL2Address})
	require.NoError(t, err)
	otherTarget, err := systemtx.New(protocol.SystemTx{ChainID: 1337, To: protocol.RollupXEEZL2Address, Input: []byte{1}})
	require.NoError(t, err)
	history, err := systemtx.NewHistory(protocol.HistoryTx{ChainID: 1337, Blocks: []protocol.HistoryBlock{{Number: 1, Hash: common.Hash{1}}}})
	require.NoError(t, err)

	wide := new(uint256.Int).Lsh(uint256.NewInt(1), 64)
	shaped := func(h *types.Header) []byte {
		h.ParentHash, h.Root = parent, empty.RootHash
		if h.Number.IsZero() {
			h.Number = *uint256.NewInt(7)
		}
		return encodeBlock(t, types.NewBlock(h, nil, nil, nil, nil, nil))
	}

	h7, err := rlp.EncodeToBytes(testBlock(7).HeaderNoCopy())
	require.NoError(t, err)
	withWithdrawals := prefixed(0xc0, append(bytes.Clone(h7), 0xc0, 0xc0, 0xc0))

	// The zero mix hash and nonce as two empty strings: an AuRa step and an
	// empty seal, which hash as the zero mix hash and nonce do.
	zeroMixNonce := append(append([]byte{0xa0}, make([]byte, 32)...), append([]byte{0x88}, make([]byte, 8)...)...)
	require.Equal(t, 1, bytes.Count(h7, zeroMixNonce))
	_, payload, _, err := rlp.Split(h7)
	require.NoError(t, err)
	aura := prefixed(0xc0, append(prefixed(0xc0, bytes.Replace(payload, zeroMixNonce, []byte{0x80, 0x80}, 1)), 0xc0, 0xc0))
	var decoded types.Block
	require.NoError(t, rlp.DecodeBytes(aura, &decoded))
	require.Equal(t, testBlock(7).Hash(), decoded.Hash(), "the decoder takes it, with the canonical block's hash")

	for _, c := range []struct {
		name string
		enc  []byte
		want error
	}{
		{"typed transaction without its string", assemble(t, dfHeader, dfEnc), seqsig.ErrBlockDecode},
		{"typed transaction with a long-form length", assemble(t, dfHeader, append([]byte{0xb8, byte(len(dfEnc))}, dfEnc...)), seqsig.ErrBlockDecode},
		{"legacy transaction as a string", assemble(t, blockWith(legacy).Header(), prefixed(0x80, legacyEnc)), seqsig.ErrBlockDecode},
		{"type 0x45", encodeBlock(t, blockWith(native)), seqsig.ErrBlockDecode},
		{"type 0x76 to another EEZL2", encodeBlock(t, blockWith(otherTarget)), seqsig.ErrBlockDecode},
		{"type 0x46", encodeBlock(t, blockWith(history)), seqsig.ErrBlockDecode},
		{"number above 64 bits", shaped(&types.Header{Number: *new(uint256.Int).AddUint64(wide, 7)}), seqsig.ErrBlockDecode},
		{"base fee above 64 bits", shaped(&types.Header{BaseFee: wide}), seqsig.ErrBlockDecode},
		{"AuRa step and seal", shaped(&types.Header{AuRaStep: 5, AuRaSeal: []byte{0xaa, 0xbb}}), seqsig.ErrBlockDecode},
		{"withdrawals the header does not commit to", withWithdrawals, seqsig.ErrBodyMismatch},
		{"header with an empty AuRa seal", aura, seqsig.ErrBlockDecode},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := seqsig.DecodeBlock(c.enc)
			require.ErrorIs(t, err, c.want)
		})
	}
	require.NoError(t, func() error { _, err := seqsig.DecodeBlock(assemble(t, dfHeader, prefixed(0x80, dfEnc))); return err }())
}
