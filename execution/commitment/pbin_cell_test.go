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

package commitment

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

func pbinTestEmptyCell() pbinCell {
	var c pbinCell
	c.reset()
	return c
}

func pbinTestBranchCell(pattern byte, bitLen int16) pbinCell {
	c := pbinTestEmptyCell()
	c.kind = pbinNodeBranch
	c.prefix = pbinPathFromBits(bytes.Repeat([]byte{pattern}, 66), bitLen)
	for i := range c.hash {
		c.hash[i] = pattern ^ byte(i)
	}
	c.hashLen = length.Hash
	return c
}

// pbinTestLeafCell carries a storage plain key — the widest a cell holds.
func pbinTestLeafCell(pattern byte, bitLen int16) pbinCell {
	c := pbinTestBranchCell(pattern, bitLen)
	c.kind = pbinNodeLeaf
	for i := range c.storageAddr {
		c.storageAddr[i] = pattern + byte(i)
	}
	c.storageAddrLen = length.Addr + length.Hash
	return c
}

// pbinTestChunkLeafCell is the one leaf shape carrying its value in the record
// instead of a plain key: a code chunk.
func pbinTestChunkLeafCell(pattern byte, bitLen int16) pbinCell {
	c := pbinTestBranchCell(pattern, bitLen)
	c.kind = pbinNodeLeaf
	for i := range c.Storage {
		c.Storage[i] = pattern ^ byte(i+1)
	}
	c.Flags, c.StorageLen = StorageUpdate, pbinValueLength
	return c
}

// The 66-byte storage path does not fit the shared codec's fields, so every
// admissible bit length is checked: a silent truncation commits a wrong root.
func TestPBinBranchCodecRoundTripPrefixBitLengths(t *testing.T) {
	t.Parallel()

	var enc pbinBranchEncoder
	for bitLen := int16(0); bitLen <= pbinMaxPathBits; bitLen++ {
		cells := [2]pbinCell{
			pbinTestBranchCell(0xA5, bitLen),
			pbinTestLeafCell(0x5A, pbinMaxPathBits-bitLen),
		}

		rec, err := enc.encode(0b11, 0b11, &cells)
		require.NoErrorf(t, err, "bitLen %d", bitLen)

		var got [2]pbinCell
		afterMap, err := pbinDecodeBranch(bytes.Clone(rec), &got, 0, nil)
		require.NoErrorf(t, err, "bitLen %d", bitLen)
		require.Equal(t, uint16(0b11), afterMap)
		require.Equalf(t, cells, got, "bitLen %d", bitLen)
	}
}

func TestPBinBranchCodecOmitsRecordHeader(t *testing.T) {
	t.Parallel()

	cells := [2]pbinCell{pbinTestBranchCell(0xA5, 3), pbinTestBranchCell(0x5A, 7)}
	var enc pbinBranchEncoder
	rec, err := enc.encode(0b11, 0b11, &cells)
	require.NoError(t, err)
	require.Equal(t, byte(pbinFieldBranch|pbinFieldHash), rec[0])
}

func TestPBinBranchDecodeAcceptsDescentDepthAndDigestCache(t *testing.T) {
	t.Parallel()

	want := [2]pbinCell{pbinTestBranchCell(0xA5, 17), pbinTestLeafCell(0x5A, 31)}
	var enc pbinBranchEncoder
	record, err := enc.encode(0b11, 0b11, &want)
	require.NoError(t, err)

	keys := pbinDigestCache{sum: pbinBlake3Hash}
	var got [2]pbinCell
	afterMap, err := pbinDecodeBranch(record, &got, 17, &keys)
	require.NoError(t, err)
	require.Equal(t, uint16(0b11), afterMap)
	require.Equal(t, want, got)
}

func TestPBinCellCodecFixedFieldCosts(t *testing.T) {
	t.Parallel()

	account := pbinTestEmptyCell()
	account.kind = pbinNodeLeaf
	account.accountAddrLen = length.Addr
	account.hashLen = 0

	accountWithHash := account
	accountWithHash.hashLen = length.Hash

	storage := pbinTestEmptyCell()
	storage.kind = pbinNodeLeaf
	storage.storageAddrLen = length.Addr + length.Hash

	storageWithHash := storage
	storageWithHash.hashLen = length.Hash

	value := pbinTestChunkLeafCell(0x31, 0)
	value.hashLen = 0

	valueWithHash := value
	valueWithHash.hashLen = length.Hash

	branch := pbinTestEmptyCell()
	branch.kind = pbinNodeBranch

	for _, tc := range []struct {
		name      string
		cell      pbinCell
		fixedSize int
	}{
		{"branch", branch, 0},
		{"branch and hash", pbinTestBranchCell(0x01, 0), length.Hash},
		{"account address", account, length.Addr},
		{"account address and hash", accountWithHash, length.Addr + length.Hash},
		{"storage address", storage, length.Addr + length.Hash},
		{"storage address and hash", storageWithHash, length.Addr + 2*length.Hash},
		{"record value", value, pbinValueLength},
		{"record value and hash", valueWithHash, pbinValueLength + length.Hash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := pbinAppendCell(nil, &tc.cell)
			require.NoError(t, err)
			require.Len(t, got, 2+tc.fixedSize)
		})
	}
}

func TestPBinBranchCodecRoundTripCellShapes(t *testing.T) {
	t.Parallel()

	accountLeaf := pbinTestEmptyCell()
	accountLeaf.kind = pbinNodeLeaf
	accountLeaf.prefix = pbinPathFromBits(bytes.Repeat([]byte{0x11}, 66), 17)
	copy(accountLeaf.accountAddr[:], bytes.Repeat([]byte{0x42}, length.Addr))
	accountLeaf.accountAddrLen = length.Addr

	for _, tc := range []struct {
		name     string
		touchMap uint16
		afterMap uint16
		cells    [2]pbinCell
	}{
		{"both branches", 0b11, 0b11, [2]pbinCell{pbinTestBranchCell(0x01, 3), pbinTestBranchCell(0x02, 528)}},
		{"leaf and branch", 0b11, 0b11, [2]pbinCell{pbinTestLeafCell(0x03, 271), pbinTestBranchCell(0x04, 5)}},
		{"hashless account leaf", 0b11, 0b11, [2]pbinCell{accountLeaf, pbinTestLeafCell(0x05, 64)}},
		{"maps do not control payload", 0b10, 0b10, [2]pbinCell{pbinTestBranchCell(0x07, 9), pbinTestBranchCell(0x08, 9)}},
		{"record-resident chunk leaf", 0b11, 0b11, [2]pbinCell{pbinTestChunkLeafCell(0x09, 12), pbinTestBranchCell(0x0A, 21)}},
		{"two chunk leaves", 0b11, 0b11, [2]pbinCell{pbinTestChunkLeafCell(0x0B, 0), pbinTestChunkLeafCell(0x0C, 528)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var enc pbinBranchEncoder
			rec, err := enc.encode(tc.touchMap, tc.afterMap, &tc.cells)
			require.NoError(t, err)

			var got [2]pbinCell
			afterMap, err := pbinDecodeBranch(rec, &got, 0, nil)
			require.NoError(t, err)
			require.Equal(t, uint16(0b11), afterMap)
			require.Equal(t, tc.cells, got)
		})
	}
}

// The record is self-contained by construction: re-encoding what was decoded
// must reproduce the bytes, so no merge-with-previous path can be needed.
func TestPBinBranchCodecIsCanonical(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		cells [2]pbinCell
	}{
		{"plain-key leaf and branch", [2]pbinCell{pbinTestLeafCell(0x7C, 33), pbinTestBranchCell(0x3E, 528)}},
		{"chunk leaf and branch", [2]pbinCell{pbinTestChunkLeafCell(0x6D, 33), pbinTestBranchCell(0x3E, 528)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var enc pbinBranchEncoder
			rec, err := enc.encode(0b11, 0b11, &tc.cells)
			require.NoError(t, err)
			want := bytes.Clone(rec)

			var got [2]pbinCell
			_, err = pbinDecodeBranch(want, &got, 0, nil)
			require.NoError(t, err)

			again, err := enc.encode(0b11, 0b11, &got)
			require.NoError(t, err)
			require.Equal(t, want, again)
		})
	}
}

// pbinTestRecord assembles a record by hand so decode can be probed with bytes
// the encoder would never emit.
func pbinTestRecord(bodies ...[]byte) []byte {
	rec := make([]byte, 0)
	for _, b := range bodies {
		rec = append(rec, b...)
	}
	return rec
}

// pbinTestCellBody takes the prefix bytes raw, deliberately not derived from the
// bit count, so a test can make the two disagree.
func pbinTestCellBody(fields pbinCellFields, prefixBitLen uint64, prefix []byte, tail ...byte) []byte {
	body := []byte{byte(fields)}
	body = binary.AppendUvarint(body, prefixBitLen)
	body = append(body, prefix...)
	return append(body, tail...)
}

func pbinTestFixedVal(val []byte) []byte {
	return append([]byte(nil), val...)
}

func TestPBinDecodeRejectsTruncatedFixedFields(t *testing.T) {
	t.Parallel()

	account := pbinTestEmptyCell()
	account.kind = pbinNodeLeaf
	account.accountAddrLen = length.Addr
	storage := pbinTestEmptyCell()
	storage.kind = pbinNodeLeaf
	storage.storageAddrLen = length.Addr + length.Hash
	value := pbinTestChunkLeafCell(0x41, 0)
	branch := pbinTestBranchCell(0x52, 0)

	for _, tc := range []struct {
		name string
		cell pbinCell
	}{
		{"account address", account},
		{"storage address", storage},
		{"record value", value},
		{"hash", branch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			record, err := pbinAppendCell(nil, &tc.cell)
			require.NoError(t, err)
			_, err = pbinDecodeCell(record[:len(record)-1], 0, new(pbinCell), 0, nil)
			require.Error(t, err)
			require.ErrorContains(t, err, "fixed value")
		})
	}
}

// A declared bit count that disagrees with the bytes behind it must be rejected,
// not read as a shorter or longer prefix: the prefix is inside the branch hash,
// so spurious pad bits silently change the root.
func TestPBinBranchDecodeRejects(t *testing.T) {
	t.Parallel()

	body := pbinTestCellBody(pbinFieldBranch, 8, []byte{0xFF})

	for _, tc := range []struct {
		name string
		rec  []byte
	}{
		{"missing first cell body", nil},
		{"missing second cell body", pbinTestRecord(body)},
		{"unknown field bit", pbinTestRecord(pbinTestCellBody(0x80, 0, nil), body)},
		{"no node kind", pbinTestRecord(pbinTestCellBody(0, 0, nil), body)},
		{"both node kinds", pbinTestRecord(pbinTestCellBody(pbinFieldBranch|pbinFieldLeaf, 0, nil), body)},
		{"prefix shorter than its bit count", pbinTestRecord(pbinTestCellBody(pbinFieldBranch, 16, []byte{0xFF}), body)},
		{"prefix longer than its bit count", pbinTestRecord(pbinTestCellBody(pbinFieldBranch, 8, []byte{0xFF, 0xFF}), body)},
		{"non-zero pad bits", pbinTestRecord(pbinTestCellBody(pbinFieldBranch, 3, []byte{0xFF}), body)},
		{"bit count beyond the longest path", pbinTestRecord(pbinTestCellBody(pbinFieldBranch, pbinMaxPathBits+1, bytes.Repeat([]byte{0xFF}, 67)), body)},
		{"truncated uvarint", pbinTestRecord([]byte{byte(pbinFieldBranch), 0x80}, body)},
		{"hash with an extra byte", pbinTestRecord(pbinTestCellBody(pbinFieldBranch|pbinFieldHash, 0, nil, pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, 33))...), body)},
		{"truncated hash", pbinTestRecord(pbinTestCellBody(pbinFieldBranch|pbinFieldHash, 0, nil, pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, 31))...), body)},
		{"account address with an extra byte", pbinTestRecord(pbinTestCellBody(pbinFieldLeaf|pbinFieldAccountAddr, 0, nil, pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, 21))...), body)},
		{"storage address with an extra byte", pbinTestRecord(pbinTestCellBody(pbinFieldLeaf|pbinFieldStorageAddr, 0, nil, pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, 51))...), body)},
		{"trailing bytes", pbinTestRecord(body, body, []byte{0x00})},
		// A leaf resolves its value through its plain key, so one without a plain
		// key would hash a zero-valued state instead of failing.
		{"leaf without a plain key", pbinTestRecord(pbinTestCellBody(pbinFieldLeaf, 0, nil), body)},
		{"leaf naming both plain keys", pbinTestRecord(pbinTestCellBody(pbinFieldLeaf|pbinFieldAccountAddr|pbinFieldStorageAddr, 0, nil,
			append(pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, length.Addr)), pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, length.Addr+length.Hash))...)...))},
		// A record-resident value and a plain key are two answers to the same
		// question; a branch has no value at all.
		{"leaf naming a plain key and a record value", pbinTestRecord(pbinTestCellBody(pbinFieldLeaf|pbinFieldAccountAddr|pbinFieldLeafValue, 0, nil,
			append(pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, length.Addr)), pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, pbinValueLength))...)...))},
		{"branch carrying a record value", pbinTestRecord(pbinTestCellBody(pbinFieldBranch|pbinFieldLeafValue, 0, nil,
			pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, pbinValueLength))...))},
		{"record value shorter than a leaf value", pbinTestRecord(pbinTestCellBody(pbinFieldLeaf|pbinFieldLeafValue, 0, nil,
			pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, pbinValueLength-1))...))},
		{"truncated record value", pbinTestRecord(pbinTestCellBody(pbinFieldLeaf|pbinFieldLeafValue, 0, nil,
			pbinTestFixedVal(bytes.Repeat([]byte{0xEE}, pbinValueLength-1))...), body)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cells [2]pbinCell
			_, err := pbinDecodeBranch(tc.rec, &cells, 0, nil)
			require.Error(t, err)
		})
	}
}

func TestPBinBranchEncodeRejects(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		touchMap uint16
		afterMap uint16
		cells    [2]pbinCell
	}{
		{"cell bit outside the arity", 0b100, 0b100, [2]pbinCell{}},
		{"touched bit outside the arity", 0b1011, 0b11, [2]pbinCell{pbinTestBranchCell(1, 1), pbinTestBranchCell(2, 1)}},
		{"present cell with no node kind", 0b01, 0b01, [2]pbinCell{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var enc pbinBranchEncoder
			_, err := enc.encode(tc.touchMap, tc.afterMap, &tc.cells)
			require.Error(t, err)
		})
	}
}

// A record carries keys and hashes, never state, so a decoded cell must come
// back unloaded no matter what the encoder was handed.
func TestPBinBranchCodecDropsLoadedState(t *testing.T) {
	t.Parallel()

	cells := [2]pbinCell{pbinTestLeafCell(0x2B, 40), pbinTestBranchCell(0x4D, 8)}
	cells[0].loaded = cellLoadStorage
	cells[0].Nonce = 9
	cells[0].Flags = NonceUpdate

	var enc pbinBranchEncoder
	rec, err := enc.encode(0b11, 0b11, &cells)
	require.NoError(t, err)

	var got [2]pbinCell
	_, err = pbinDecodeBranch(bytes.Clone(rec), &got, 0, nil)
	require.NoError(t, err)
	require.Equal(t, cellLoadNone, got[0].loaded)
	require.Zero(t, got[0].Nonce)
	require.Zero(t, got[0].Flags)
}

// Decoding into a reused cell must not leave any bits of the previous prefix
// behind — a stale bitLen would extend the new prefix with foreign bits.
func TestPBinBranchDecodeClearsReusedCells(t *testing.T) {
	t.Parallel()

	cells := [2]pbinCell{pbinTestLeafCell(0xFF, 528), pbinTestLeafCell(0xFF, 528)}
	want := [2]pbinCell{pbinTestBranchCell(0x0F, 3), pbinTestBranchCell(0xF0, 0)}

	var enc pbinBranchEncoder
	rec, err := enc.encode(0b11, 0b11, &want)
	require.NoError(t, err)

	_, err = pbinDecodeBranch(bytes.Clone(rec), &cells, 0, nil)
	require.NoError(t, err)
	require.Equal(t, want, cells)
}

func TestPBinCellReset(t *testing.T) {
	t.Parallel()

	c := pbinTestLeafCell(0xC3, 271)
	c.Nonce = 7
	c.Balance.SetUint64(11)
	c.Flags = BalanceUpdate | NonceUpdate

	c.reset()
	require.Equal(t, int16(0), c.prefix.bitLen)
	require.Zero(t, c.prefix.w)
	require.Equal(t, empty.CodeHash, c.CodeHash)
	require.Equal(t, pbinTestEmptyCell(), c)
}

func pbinTestFillGrid(g *pbinGrid, rows int) {
	g.activeRows = rows
	g.root = pbinTestBranchCell(0x99, 5)
	for row := range rows {
		g.rows[row][0] = pbinTestLeafCell(byte(row), 271)
		g.rows[row][1] = pbinTestBranchCell(byte(row), 33)
		g.depths[row] = int16(row * 7)
		g.branchBefore[row] = true
		g.touchMap[row] = 0b11
		g.afterMap[row] = 0b10
	}
}

func pbinTestRequireRowEmpty(t *testing.T, g *pbinGrid, row int) {
	t.Helper()
	require.Equal(t, pbinTestEmptyCell(), g.rows[row][0])
	require.Equal(t, pbinTestEmptyCell(), g.rows[row][1])
	require.Zero(t, g.depths[row])
	require.False(t, g.branchBefore[row])
	require.Zero(t, g.touchMap[row])
	require.Zero(t, g.afterMap[row])
}

// resetForReuse only has to clear what the finished run left live; rows above
// activeRows are initialized by unfold before anything reads them.
func TestPBinGridResetForReuse(t *testing.T) {
	t.Parallel()

	g := new(pbinGrid)
	pbinTestFillGrid(g, 3)
	stale := g.rows[2][0]
	g.activeRows = 2
	g.resetForReuse()

	require.Zero(t, g.activeRows)
	require.Equal(t, pbinTestEmptyCell(), g.root)
	pbinTestRequireRowEmpty(t, g, 0)
	pbinTestRequireRowEmpty(t, g, 1)
	require.Equal(t, stale, g.rows[2][0])
}

// A row consumes at least the bit it splits on, so 528 rows cover the deepest
// path.
func TestPBinGridBounds(t *testing.T) {
	t.Parallel()

	g := new(pbinGrid)
	require.Equal(t, pbinMaxPathBits, len(g.rows))
	require.Equal(t, pbinGridRows, len(g.depths))
	require.Equal(t, 2, len(g.rows[0]))
}
