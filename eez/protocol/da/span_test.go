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
package da

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/eez/internal/testfixtures"
	"github.com/erigontech/erigon/eez/protocol"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types"
)

// spanBlock is one block's span-carried inputs.
type spanBlock struct {
	beneficiary common.Address
	extraData   []byte
	txs         [][]byte
}

// encodeSpan is a test encoder for native_block_span_v0 with shortest
// varints and maximal runs, so every valid span is its own encoding.
func encodeSpan(blocks []spanBlock) []byte {
	out := []byte{SpanVersionV0}
	out = binary.AppendUvarint(out, uint64(len(blocks)))
	for i := range blocks {
		out = binary.AppendUvarint(out, uint64(len(blocks[i].txs)))
	}
	for i := 0; i < len(blocks); {
		j := i + 1
		for j < len(blocks) && blocks[j].beneficiary == blocks[i].beneficiary {
			j++
		}
		out = binary.AppendUvarint(out, uint64(j-i))
		out = append(out, blocks[i].beneficiary[:]...)
		i = j
	}
	for i := 0; i < len(blocks); {
		j := i + 1
		for j < len(blocks) && bytes.Equal(blocks[j].extraData, blocks[i].extraData) {
			j++
		}
		out = binary.AppendUvarint(out, uint64(j-i))
		out = append(out, byte(len(blocks[i].extraData)))
		out = append(out, blocks[i].extraData...)
		i = j
	}
	for i := range blocks {
		for _, tx := range blocks[i].txs {
			out = binary.AppendUvarint(out, uint64(len(tx)))
		}
	}
	for i := range blocks {
		for _, tx := range blocks[i].txs {
			out = append(out, tx...)
		}
	}
	return out
}

// spanBlocks regroups a decoded span by block.
func spanBlocks(s *Span) []spanBlock {
	blocks := make([]spanBlock, len(s.TxCounts))
	txs := s.Transactions
	for i, n := range s.TxCounts {
		blocks[i] = spanBlock{beneficiary: s.Beneficiaries[i], extraData: s.ExtraData[i], txs: txs[:n:n]}
		txs = txs[n:]
	}
	return blocks
}

// Rollup0 Appendix D.4.4 vectors, eez-association/specs-draft 2c6d346
// docs/rollup0-spec/D-wire-formats.md.
const (
	spanVector1 = "0x00060000000000000600000000000000000000000000000000000000000600"
	spanVector2 = "0x00060100000000000211111111111111111111111111111111111111110422222222222222222222222222222222222222220101aa0202bbcc03006f02f86c820539808405f5e100843b9aca0082520894000000000000000000000000000000000000dead8080c001a04849ec4d7eed2e9eb1da330cdb0a22cdb4c1e32d682556dcc2b491ea5d747dfda05e38e68ce47b2ae70e6b36de8f1e1413df4dbc73a30610675f0a8e8c1505e7a7"
	spanVector7 = "0x0006000000000000060000000000000000000000000000000000000000060000"
)

// TestSpanVectors is T-VEC-4 (span part): Appendix D.4.4 vectors 1 to 7.
func TestSpanVectors(t *testing.T) {
	s, err := DecodeSpan(common.FromHex(spanVector1))
	require.NoError(t, err)
	require.Equal(t, []uint32{0, 0, 0, 0, 0, 0}, s.TxCounts)
	require.Equal(t, make([]common.Address, 6), s.Beneficiaries)
	require.Equal(t, [][]byte{{}, {}, {}, {}, {}, {}}, s.ExtraData)
	require.Empty(t, s.Transactions)

	s, err = DecodeSpan(common.FromHex(spanVector2))
	require.NoError(t, err)
	require.Len(t, common.FromHex(spanVector2), 171)
	require.Equal(t, []uint32{1, 0, 0, 0, 0, 0}, s.TxCounts)
	a1, a2 := common.HexToAddress("0x1111111111111111111111111111111111111111"), common.HexToAddress("0x2222222222222222222222222222222222222222")
	require.Equal(t, []common.Address{a1, a1, a2, a2, a2, a2}, s.Beneficiaries)
	require.Equal(t, [][]byte{{0xaa}, {0xbb, 0xcc}, {0xbb, 0xcc}, {}, {}, {}}, s.ExtraData)
	require.Len(t, s.Transactions, 1)
	require.Len(t, s.Transactions[0], 111)
	tx, err := types.DecodeTransaction(s.Transactions[0])
	require.NoError(t, err)
	require.Equal(t, uint8(types.DynamicFeeTxType), tx.Type())
	require.Equal(t, common.HexToHash("0xfdf9c966ac23a592fda032328421cabc27414887b3496d1c6faec3b10e1f9b59"), tx.Hash())
	sender, err := types.LatestSignerForChainID(uint256.NewInt(1337)).Sender(tx)
	require.NoError(t, err)
	require.Equal(t, common.HexToAddress("0x7e5f4552091a69125d5dfcb7b8c2659029395bdf"), sender.Value())

	for _, c := range []struct {
		name, operations string
		err              error
	}{
		{"3: empty payload", "0x", ErrSpanEmpty},
		{"4: unknown version", "0x01", ErrSpanVersion},
		{"5: non-shortest block count", "0x008600", ErrSpanVarint},
		{"6: impossible block count", "0x0006", ErrSpanImplausible},
		{"7: trailing byte", spanVector7, ErrSpanTrailingByte},
	} {
		_, err := DecodeSpan(common.FromHex(c.operations))
		require.ErrorIs(t, err, c.err, c.name)
	}
}

// TestWindow84CallData rebuilds the devnet batch's callData from the blocks
// it settles: their beneficiaries, extraData and transactions encode to the
// span its RollupOperation carries.
func TestWindow84CallData(t *testing.T) {
	w, err := testfixtures.LoadWindow("captured-devnet-window-84")
	require.NoError(t, err)
	var blocks []spanBlock
	for _, wb := range w.Blocks {
		var b types.Block
		require.NoError(t, rlp.DecodeBytes(wb.RLP, &b))
		sb := spanBlock{beneficiary: b.Coinbase(), extraData: b.Extra(), txs: [][]byte{}}
		for _, tx := range b.Transactions() {
			var buf bytes.Buffer
			require.NoError(t, tx.MarshalBinary(&buf))
			sb.txs = append(sb.txs, buf.Bytes())
		}
		blocks = append(blocks, sb)
	}
	batch, err := protocol.DecodePostAndVerifyBatch(w.PostBatch)
	require.NoError(t, err)
	callData := encode(Message{Type: RollupOperation, RollupID: 1, Data: encodeSpan(blocks)})
	require.Equal(t, []byte(batch.CallData), callData)

	msgs, err := Decode(nil, batch.CallData)
	require.NoError(t, err)
	p, err := ParsePayload(msgs)
	require.NoError(t, err)
	s, err := DecodeSpan(p.Operations)
	require.NoError(t, err)
	require.Len(t, s.TxCounts, len(w.Blocks))
}

// TestChiadoSpanIsNotV0 records that the live Chiado composer does not emit
// the Appendix D.4.3 layout: its span declares 150 empty blocks and encodes
// their transaction counts in three bytes (ff 96 01) where V0 needs 150.
func TestChiadoSpanIsNotV0(t *testing.T) {
	blob, err := testfixtures.LoadChiadoBlob()
	require.NoError(t, err)
	msgs, err := Decode([][]byte{blob}, nil)
	require.NoError(t, err)
	p, err := ParsePayload(msgs)
	require.NoError(t, err)
	_, err = DecodeSpan(p.Operations)
	require.ErrorIs(t, err, ErrSpanImplausible)
}

func TestSpanRejects(t *testing.T) {
	addr := bytes.Repeat([]byte{0x11}, 20)
	other := bytes.Repeat([]byte{0x22}, 20)
	join := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	header := []byte{SpanVersionV0, 2, 0, 0} // two blocks, no transactions
	extra := []byte{2, 0}                    // one run of empty extraData
	for _, c := range []struct {
		name       string
		operations []byte
		err        error
	}{
		{"zero block count", []byte{0, 0}, ErrSpanNoBlocks},
		{"truncated counts", []byte{0, 2, 0}, ErrSpanImplausible},
		{"fifth varint byte above 0x0f", []byte{0, 0xff, 0xff, 0xff, 0xff, 0x10}, ErrSpanVarint},
		{"six-byte varint", []byte{0, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00}, ErrSpanVarint},
		{"zero run", join(header, []byte{0}, addr, []byte{2}, addr, extra), ErrSpanRunCoverage},
		{"run overshoot", join(header, []byte{3}, addr, extra), ErrSpanRunCoverage},
		{"non-maximal run", join(header, []byte{1}, addr, []byte{1}, addr, extra), ErrSpanNonMaximal},
		{"truncated beneficiary", join(header, []byte{2}, addr[:19]), ErrSpanTruncated},
		{"extraData too long", join(header, []byte{2}, addr, []byte{2, 33}, make([]byte, 33)), ErrSpanExtraData},
		{"zero-length transaction", join([]byte{SpanVersionV0, 1, 1, 1}, addr, []byte{1, 0, 0, 0xaa}), ErrSpanEmptyTx},
		{"too many transactions", join([]byte{SpanVersionV0, 1, 5, 1}, addr, []byte{1, 0}, []byte{1, 1, 1}), ErrSpanImplausible},
		{"short transaction bytes", join([]byte{SpanVersionV0, 1, 1, 1}, addr, []byte{1, 0, 2, 0xaa}), ErrSpanTruncated},
		{"valid with two runs", join(header, []byte{1}, addr, []byte{1}, other, extra), nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := DecodeSpan(c.operations)
			if c.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, c.err)
		})
	}
}

func TestSpanRoundTrip(t *testing.T) {
	a, b := common.HexToAddress("0xa"), common.HexToAddress("0xb")
	blocks := []spanBlock{
		{beneficiary: a, extraData: []byte{}, txs: [][]byte{{1}, bytes.Repeat([]byte{2}, 300)}},
		{beneficiary: a, extraData: []byte{}, txs: [][]byte{}},
		{beneficiary: b, extraData: bytes.Repeat([]byte{3}, MaxExtraData), txs: [][]byte{{4}}},
		{beneficiary: a, extraData: []byte{5}, txs: [][]byte{}},
	}
	enc := encodeSpan(blocks)
	s, err := DecodeSpan(enc)
	require.NoError(t, err)
	require.Equal(t, blocks, spanBlocks(s))
}

// FuzzDecodeSpan checks that every span DecodeSpan accepts is the unique
// encoding of what it decodes to.
func FuzzDecodeSpan(f *testing.F) {
	for _, v := range []string{spanVector1, spanVector2, spanVector7} {
		f.Add(common.FromHex(v))
	}
	f.Fuzz(func(t *testing.T, operations []byte) {
		s, err := DecodeSpan(operations)
		if err != nil {
			return
		}
		require.Equal(t, operations, encodeSpan(spanBlocks(s)))
	})
}
