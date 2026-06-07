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

package types

import (
	"bytes"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/execution/rlp"
)

func richHeader() *Header {
	baseFee := uint256.NewInt(7)
	wHash := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000077")
	pbbr := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000088")
	reqHash := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000099")
	balHash := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000aa")
	return &Header{
		ParentHash:            common.HexToHash("0x01"),
		UncleHash:             empty.UncleHash,
		Coinbase:              common.HexToAddress("0x02"),
		Root:                  common.HexToHash("0x03"),
		TxHash:                empty.RootHash,
		ReceiptHash:           empty.RootHash,
		Difficulty:            *uint256.NewInt(5),
		Number:                *uint256.NewInt(99),
		GasLimit:              30_000_000,
		GasUsed:               21_000,
		Time:                  1234,
		Extra:                 []byte{0xaa},
		MixDigest:             common.HexToHash("0x04"),
		Nonce:                 EncodeNonce(0x1234),
		BaseFee:               baseFee,
		WithdrawalsHash:       &wHash,
		ParentBeaconBlockRoot: &pbbr,
		RequestsHash:          &reqHash,
		BlockAccessListHash:   &balHash,
	}
}

func TestBlockNonce(t *testing.T) {
	t.Parallel()
	n := EncodeNonce(0x1234)
	require.Equal(t, uint64(0x1234), n.Uint64())

	text, err := n.MarshalText()
	require.NoError(t, err)
	var got BlockNonce
	require.NoError(t, got.UnmarshalText(text))
	require.Equal(t, n, got)
}

func TestHeader_HashCalcSizeSanity(t *testing.T) {
	t.Parallel()
	h := richHeader()
	require.Equal(t, h.Hash(), h.CalcHash())
	require.Positive(t, uint64(h.Size()))
	require.NoError(t, h.SanityCheck())

	// Over-sized extradata is rejected.
	bad := richHeader()
	bad.Extra = make([]byte, 100*1024+1)
	require.ErrorContains(t, bad.SanityCheck(), "extradata")
}

func TestNewEmptyHeaderForAssembling(t *testing.T) {
	t.Parallel()
	h := NewEmptyHeaderForAssembling()
	require.NotNil(t, h)
	// A mutable header recomputes its hash each call rather than caching nil.
	require.Equal(t, h.Hash(), h.CalcHash())
}

func TestHeader_RLPRoundTrip(t *testing.T) {
	t.Parallel()
	// A consistent pre-fork header (decode is strict about EIP field ordering).
	h := &Header{
		ParentHash:  common.HexToHash("0x01"),
		UncleHash:   empty.UncleHash,
		Coinbase:    common.HexToAddress("0x02"),
		Root:        common.HexToHash("0x03"),
		TxHash:      empty.RootHash,
		ReceiptHash: empty.RootHash,
		Difficulty:  *uint256.NewInt(5),
		Number:      *uint256.NewInt(99),
		GasLimit:    30_000_000,
		GasUsed:     21_000,
		Time:        1234,
		Extra:       []byte{0xaa},
	}
	var buf bytes.Buffer
	require.NoError(t, h.EncodeRLP(&buf))

	var got Header
	require.NoError(t, got.DecodeRLP(rlp.NewStream(&buf, 0)))
	require.Equal(t, h.Hash(), got.Hash())
}

func TestBlock_Accessors(t *testing.T) {
	t.Parallel()
	h := richHeader()
	blk := NewBlockFromNetwork(h, &Body{})

	require.Equal(t, uint64(99), blk.NumberU64())
	require.Equal(t, *uint256.NewInt(99), blk.Number())
	require.Equal(t, uint64(0x1234), blk.NonceU64())
	require.Equal(t, EncodeNonce(0x1234), blk.Nonce())
	require.Equal(t, common.HexToHash("0x01"), blk.ParentHash())
	require.Equal(t, empty.RootHash, blk.TxHash())
	require.Equal(t, empty.RootHash, blk.ReceiptHash())
	require.Equal(t, empty.UncleHash, blk.UncleHash())
	require.Equal(t, []byte{0xaa}, blk.Extra())
	require.Equal(t, common.HexToAddress("0x02"), blk.Coinbase())
	require.Equal(t, common.HexToHash("0x03"), blk.Root())
	require.Equal(t, common.HexToHash("0x04"), blk.MixDigest())
	require.Equal(t, uint64(30_000_000), blk.GasLimit())
	require.Equal(t, uint64(21_000), blk.GasUsed())
	require.Equal(t, uint64(1234), blk.Time())
	require.Equal(t, *uint256.NewInt(5), blk.Difficulty())
	require.Equal(t, uint64(7), blk.BaseFee().Uint64())
	require.Equal(t, h.Bloom, blk.Bloom())

	require.Equal(t, *h.WithdrawalsHash, *blk.WithdrawalsHash())
	require.Equal(t, *h.ParentBeaconBlockRoot, *blk.ParentBeaconBlockRoot())
	require.Equal(t, *h.RequestsHash, *blk.RequestsHash())
	require.Equal(t, *h.BlockAccessListHash, *blk.BlockAccessListHash())

	require.Same(t, h, blk.HeaderNoCopy())
	require.NotSame(t, h, blk.Header()) // Header() returns a copy
	require.Empty(t, blk.Transactions())
	require.Empty(t, blk.Uncles())
	require.NotNil(t, blk.Body())
	require.Equal(t, h.Hash(), blk.Hash())
	require.Positive(t, uint64(blk.Size()))
	require.NoError(t, blk.SanityCheck())
}

func TestBlock_CopyAndWithSeal(t *testing.T) {
	t.Parallel()
	blk := NewBlockWithHeader(richHeader())

	cp := blk.Copy()
	require.Equal(t, blk.Hash(), cp.Hash())
	require.NotSame(t, blk.HeaderNoCopy(), cp.HeaderNoCopy())

	sealed := blk.WithSeal(richHeader())
	require.Equal(t, blk.Hash(), sealed.Hash())
}

func TestBlock_HashCheck(t *testing.T) {
	t.Parallel()
	valid := &Header{TxHash: empty.RootHash, UncleHash: empty.UncleHash, ReceiptHash: empty.RootHash, Number: *uint256.NewInt(1)}
	require.NoError(t, NewBlockWithHeader(valid).HashCheck(false))

	// No transactions but a non-empty receipt hash is inconsistent.
	bad := &Header{TxHash: empty.RootHash, UncleHash: empty.UncleHash, ReceiptHash: common.HexToHash("0xdead")}
	require.Error(t, NewBlockWithHeader(bad).HashCheck(false))
}
