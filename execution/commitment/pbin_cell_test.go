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

// pbinTestBranchCell builds a branch-pointing cell with a prefix of the given
// bit length and a distinguishable hash.
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

// pbinTestLeafCell builds a leaf cell carrying a storage plain key, the widest
// plain key a cell holds.
func pbinTestLeafCell(pattern byte, bitLen int16) pbinCell {
	c := pbinTestBranchCell(pattern, bitLen)
	c.kind = pbinNodeLeaf
	for i := range c.storageAddr {
		c.storageAddr[i] = pattern + byte(i)
	}
	c.storageAddrLen = length.Addr + length.Hash
	return c
}

// A prefix of any admissible bit length must survive a record round-trip: the
// 66-byte storage path does not fit the shared codec's fields, and a silent
// truncation would commit a wrong root (guards H4).
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
		touchMap, afterMap, err := pbinDecodeBranch(bytes.Clone(rec), &got)
		require.NoErrorf(t, err, "bitLen %d", bitLen)
		require.Equal(t, uint16(0b11), touchMap)
		require.Equal(t, uint16(0b11), afterMap)
		require.Equalf(t, cells, got, "bitLen %d", bitLen)
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
		{"only the right cell present", 0b10, 0b10, [2]pbinCell{pbinTestEmptyCell(), pbinTestBranchCell(0x07, 9)}},
		{"deleted left cell", 0b11, 0b10, [2]pbinCell{pbinTestEmptyCell(), pbinTestBranchCell(0x08, 9)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var enc pbinBranchEncoder
			rec, err := enc.encode(tc.touchMap, tc.afterMap, &tc.cells)
			require.NoError(t, err)

			var got [2]pbinCell
			touchMap, afterMap, err := pbinDecodeBranch(rec, &got)
			require.NoError(t, err)
			require.Equal(t, tc.touchMap, touchMap)
			require.Equal(t, tc.afterMap, afterMap)
			require.Equal(t, tc.cells, got)
		})
	}
}

// The record is self-contained by construction: re-encoding what was decoded
// must reproduce the bytes, so no merge-with-previous path can be needed.
func TestPBinBranchCodecIsCanonical(t *testing.T) {
	t.Parallel()

	cells := [2]pbinCell{pbinTestLeafCell(0x7C, 33), pbinTestBranchCell(0x3E, 528)}

	var enc pbinBranchEncoder
	rec, err := enc.encode(0b11, 0b11, &cells)
	require.NoError(t, err)
	want := bytes.Clone(rec)

	var got [2]pbinCell
	_, _, err = pbinDecodeBranch(want, &got)
	require.NoError(t, err)

	again, err := enc.encode(0b11, 0b11, &got)
	require.NoError(t, err)
	require.Equal(t, want, again)
}

// pbinTestRecord assembles a record by hand so decode can be probed with bytes
// the encoder would never produce.
func pbinTestRecord(touchMap, afterMap uint16, bodies ...[]byte) []byte {
	rec := make([]byte, 4)
	binary.BigEndian.PutUint16(rec, touchMap)
	binary.BigEndian.PutUint16(rec[2:], afterMap)
	for _, b := range bodies {
		rec = append(rec, b...)
	}
	return rec
}

// pbinTestCellBody spells one cell body: fields, uvarint bit count, then the
// caller's raw prefix bytes — deliberately not derived from the bit count.
func pbinTestCellBody(fields pbinCellFields, prefixBitLen uint64, prefix []byte, tail ...byte) []byte {
	body := []byte{byte(fields)}
	body = binary.AppendUvarint(body, prefixBitLen)
	body = append(body, prefix...)
	return append(body, tail...)
}

func pbinTestLenAndVal(val []byte) []byte {
	return append(binary.AppendUvarint(nil, uint64(len(val))), val...)
}

// A declared bit count that disagrees with the bytes behind it must be
// rejected rather than read as a shorter or longer prefix: the prefix is inside
// the branch hash, so spurious pad bits silently change the root (guards H3).
func TestPBinBranchDecodeRejects(t *testing.T) {
	t.Parallel()

	body := pbinTestCellBody(pbinFieldBranch, 8, []byte{0xFF})

	for _, tc := range []struct {
		name string
		rec  []byte
	}{
		{"truncated header", []byte{0x00, 0x03, 0x00}},
		{"cell bit outside the arity", pbinTestRecord(0b100, 0b100, body)},
		{"touched bit outside the arity", pbinTestRecord(0b1011, 0b11, body, body)},
		{"missing cell body", pbinTestRecord(0b11, 0b11, body)},
		{"unknown field bit", pbinTestRecord(0b01, 0b01, pbinTestCellBody(0x80, 0, nil))},
		{"no node kind", pbinTestRecord(0b01, 0b01, pbinTestCellBody(0, 0, nil))},
		{"both node kinds", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldBranch|pbinFieldLeaf, 0, nil))},
		{"prefix shorter than its bit count", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldBranch, 16, []byte{0xFF}))},
		{"prefix longer than its bit count", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldBranch, 8, []byte{0xFF, 0xFF}))},
		{"non-zero pad bits", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldBranch, 3, []byte{0xFF}))},
		{"bit count beyond the longest path", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldBranch, pbinMaxPathBits+1, bytes.Repeat([]byte{0xFF}, 67)))},
		{"truncated uvarint", pbinTestRecord(0b01, 0b01, []byte{byte(pbinFieldBranch), 0x80})},
		{"hash longer than a digest", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldBranch|pbinFieldHash, 0, nil, pbinTestLenAndVal(bytes.Repeat([]byte{0xEE}, 33))...))},
		{"truncated hash", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldBranch|pbinFieldHash, 0, nil, 32, 0xEE))},
		{"account address of the wrong length", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldLeaf|pbinFieldAccountAddr, 0, nil, pbinTestLenAndVal(bytes.Repeat([]byte{0xEE}, 21))...))},
		{"storage address of the wrong length", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldLeaf|pbinFieldStorageAddr, 0, nil, pbinTestLenAndVal(bytes.Repeat([]byte{0xEE}, 51))...))},
		{"trailing bytes", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldBranch, 0, nil), []byte{0x00})},
		// A leaf resolves its value through its plain key, so one without a plain
		// key would hash a zero-valued state instead of failing.
		{"leaf without a plain key", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldLeaf, 0, nil))},
		{"leaf naming both plain keys", pbinTestRecord(0b01, 0b01, pbinTestCellBody(pbinFieldLeaf|pbinFieldAccountAddr|pbinFieldStorageAddr, 0, nil,
			append(pbinTestLenAndVal(bytes.Repeat([]byte{0xEE}, length.Addr)), pbinTestLenAndVal(bytes.Repeat([]byte{0xEE}, length.Addr+length.Hash))...)...))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cells [2]pbinCell
			_, _, err := pbinDecodeBranch(tc.rec, &cells)
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
	_, _, err = pbinDecodeBranch(bytes.Clone(rec), &got)
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

	_, _, err = pbinDecodeBranch(bytes.Clone(rec), &cells)
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
