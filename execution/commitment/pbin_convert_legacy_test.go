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
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// pbinTestLegacyAppendCell spells a cell the way the pre-version format did: a
// uvarint length ahead of every field, and a prefix on every cell including a
// storage leaf. It exists so the converter can be tested against real legacy
// bytes rather than against its own reader.
func pbinTestLegacyAppendCell(dst []byte, c *pbinCell) []byte {
	var fields pbinCellFields
	switch c.kind {
	case pbinNodeLeaf:
		fields = pbinFieldLeaf
	case pbinNodeBranch:
		fields = pbinFieldBranch
	}
	if c.accountAddrLen > 0 {
		fields |= pbinFieldAccountAddr
	}
	if c.storageAddrLen > 0 {
		fields |= pbinFieldStorageAddr
	}
	if c.kind == pbinNodeLeaf && fields&pbinFieldValue == 0 {
		fields |= pbinFieldLeafValue
	}
	if c.hashLen > 0 {
		fields |= pbinFieldHash
	}

	lenAndVal := func(dst, v []byte) []byte {
		return append(binary.AppendUvarint(dst, uint64(len(v))), v...)
	}
	dst = append(dst, byte(fields))
	dst = binary.AppendUvarint(dst, uint64(c.prefix.bitLen))
	dst = c.prefix.appendPackedBits(dst)
	if fields&pbinFieldAccountAddr != 0 {
		dst = lenAndVal(dst, c.accountAddr[:c.accountAddrLen])
	}
	if fields&pbinFieldStorageAddr != 0 {
		dst = lenAndVal(dst, c.storageAddr[:c.storageAddrLen])
	}
	if fields&pbinFieldLeafValue != 0 {
		dst = lenAndVal(dst, c.Storage[:pbinValueLength])
	}
	if fields&pbinFieldHash != 0 {
		dst = lenAndVal(dst, c.hash[:c.hashLen])
	}
	return dst
}

func pbinTestLegacyRecord(touchMap, afterMap uint16, cells *[2]pbinCell) []byte {
	out := binary.BigEndian.AppendUint16(nil, touchMap)
	out = binary.BigEndian.AppendUint16(out, afterMap)
	for bit := range cells {
		if afterMap&(uint16(1)<<uint(bit)) != 0 {
			out = pbinTestLegacyAppendCell(out, &cells[bit])
		}
	}
	return out
}

func TestPBinEncodeLegacyRecordRoundTrips(t *testing.T) {
	t.Parallel()

	path := pbinTestKeyPrefix(pbinTestBaseStorageKey(), 8)
	key := pbinEncodeBitPath(&path)
	cells := [2]pbinCell{
		pbinTestBranchCell(0xA5, 3),
		pbinTestChunkLeafCell(0x5A, 7),
	}
	var enc pbinBranchEncoder
	current, err := enc.encode(pbinCellBits, pbinCellBits, &cells)
	require.NoError(t, err)

	legacy, err := PBinEncodeLegacyRecord(key, current)
	require.NoError(t, err)

	got, err := NewPBinRecordConverter().ConvertBranch(key, legacy)
	require.NoError(t, err)
	require.Equal(t, current, got)
}

func TestPBinEncodeLegacyRecordRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	path := pbinTestKeyPrefix(pbinTestBaseStorageKey(), 8)
	_, err := PBinEncodeLegacyRecord(pbinEncodeBitPath(&path), []byte{byte(pbinFieldBranch)})
	require.ErrorIs(t, err, errPBinMalformedBranch)
}

func TestPBinEncodeLegacyRecordRejectsAlreadyLegacyInput(t *testing.T) {
	t.Parallel()

	path := pbinTestKeyPrefix(pbinTestBaseStorageKey(), 8)
	key := pbinEncodeBitPath(&path)
	cells := [2]pbinCell{pbinTestBranchCell(0xA5, 3), pbinTestBranchCell(0x5A, 7)}
	legacy := pbinTestLegacyRecord(pbinCellBits, pbinCellBits, &cells)

	_, err := PBinEncodeLegacyRecord(key, legacy)
	require.ErrorContains(t, err, "already a legacy record")
}

func TestPBinConvertBranchMatchesTheCurrentEncoder(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	for _, divergence := range []int16{8, 63, 64, 271, 527} {
		t.Run(fmt.Sprintf("bit %d", divergence), func(t *testing.T) {
			t.Parallel()

			a := pbinTestStorageLeaf(base, 0x11)
			b := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, divergence), 0x22)
			left, right := pbinTestBranchOrder(t, a, b, divergence)
			cells := [2]pbinCell{left.recordCell(t, divergence+1), right.recordCell(t, divergence+1)}

			prefix := pbinTestKeyPrefix(a.treeKey, divergence)
			key := pbinEncodeBitPath(&prefix)
			legacy := pbinTestLegacyRecord(pbinCellBits, pbinCellBits, &cells)

			got, err := NewPBinRecordConverter().ConvertBranch(key, legacy)
			require.NoError(t, err)

			var enc pbinBranchEncoder
			want, err := enc.encode(pbinCellBits, pbinCellBits, &cells)
			require.NoError(t, err)
			require.Equal(t, want, got, "conversion must land on what the current encoder writes")
			require.Less(t, len(got), len(legacy), "the current format is the smaller one")
		})
	}
}

func TestPBinConvertBranchPanicsOnASingleCell(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	a := pbinTestStorageLeaf(base, 0x11)
	cells := [2]pbinCell{a.recordCell(t, 9), pbinTestEmptyCell()}

	prefix := pbinTestKeyPrefix(a.treeKey, 8)
	key := pbinEncodeBitPath(&prefix)
	legacy := pbinTestLegacyRecord(0b01, 0b01, &cells)

	require.PanicsWithValue(t,
		fmt.Sprintf("pbin convert: record at %x names 1 cells (afterMap 0001); "+
			"a one-cell node is collapsed by foldPropagate and never stored", key),
		func() { _, _ = NewPBinRecordConverter().ConvertBranch(key, legacy) })
}

func TestPBinConvertStateMatchesTheCurrentBlob(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	a := pbinTestStorageLeaf(base, 0x33)
	b := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, 64), 0x44)
	left, right := pbinTestBranchOrder(t, a, b, 64)

	ms := NewMockState(t)
	pbinTestPutState(t, ms, a, b)
	pph := NewPBinPatriciaHashed(ms)
	cells := [2]pbinCell{left.recordCell(t, 65), right.recordCell(t, 65)}
	pbinTestSeedRow(pph, pbinTestKeyPrefix(a.treeKey, 64), 65, cells, pbinCellBits, pbinCellBits)
	require.NoError(t, pph.fold())

	want, err := pph.EncodeCurrentState(nil)
	require.NoError(t, err)

	legacy, err := PBinEncodeLegacyState(want)
	require.NoError(t, err)
	require.Error(t, ValidatePBinStateFormat(legacy))

	got, err := NewPBinRecordConverter().ConvertState(legacy)
	require.NoError(t, err)
	require.Equal(t, want, got)

	// children is a fold-time cache and is not serialized, so a restored root
	// carries only what the blob spells.
	fresh := NewPBinPatriciaHashed(ms)
	require.NoError(t, fresh.SetState(got))
	require.Equal(t, pph.grid.root.kind, fresh.grid.root.kind)
	require.Equal(t, pph.grid.root.prefix, fresh.grid.root.prefix)
	require.Equal(t, pph.grid.root.hash, fresh.grid.root.hash)
}

func TestPBinEncodeLegacyStateRejectsMalformedInput(t *testing.T) {
	t.Parallel()

	for name, blob := range map[string][]byte{
		"empty":          nil,
		"marker only":    {pbinStateMarker},
		"current header": {pbinStateMarker, pbinRecordFormat},
		"truncated root": {pbinStateMarker, pbinRecordFormat, 0, 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := PBinEncodeLegacyState(blob)
			require.Error(t, err)
		})
	}
}
