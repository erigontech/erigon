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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/length"
)

func pbinTestEngine(t *testing.T) (*PBinPatriciaHashed, *MockState) {
	t.Helper()
	ms := NewMockState(t)
	return NewPBinPatriciaHashed(ms), ms
}

// pbinTestSpecCell builds a cell whose prefix is spelled out bit by bit, so a
// test can name a divergence point instead of deriving one.
func pbinTestSpecCell(t *testing.T, kind pbinNodeKind, spec string) pbinCell {
	t.Helper()
	c := pbinTestEmptyCell()
	c.kind = kind
	c.prefix = pbinTestPathFromBits(t, pbinTestBitSpec(t, spec))
	switch kind {
	case pbinNodeLeaf:
		// A stored leaf always names a plain key; a record without one is rejected.
		c.storageAddrLen = length.Addr + length.Hash
		c.storageAddr[0], c.storageAddr[1] = 0xB1, byte(len(spec))
	case pbinNodeBranch:
		c.hash = common.Hash{0xB1, byte(len(spec))}
		c.hashLen = length.Hash
	}
	return c
}

func pbinTestPutRecord(t *testing.T, ms *MockState, path pbinBitpath, cells [2]pbinCell) {
	t.Helper()
	var enc pbinBranchEncoder
	rec, err := enc.encode(0b11, 0b11, &cells)
	require.NoError(t, err)
	require.NoError(t, ms.PutBranch(pbinEncodeBitPath(&path), bytes.Clone(rec), nil))
}

func pbinTestPutRootCell(t *testing.T, ms *MockState, c pbinCell) {
	t.Helper()
	rec, err := pbinAppendCell(nil, &c)
	require.NoError(t, err)
	require.NoError(t, ms.PutBranch(pbinRootKey, rec, nil))
}

// pbinTestPutTopRecord seeds a node record at the empty path together with the
// root cell that names it — the pair a stored tree always writes.
func pbinTestPutTopRecord(t *testing.T, ms *MockState, cells [2]pbinCell) {
	t.Helper()
	pbinTestPutRecord(t, ms, pbinBitpath{}, cells)
	pbinTestPutRootCell(t, ms, pbinTestSpecCell(t, pbinNodeBranch, ""))
}

// pbinTestUnfoldStep opens one more row, loading the root cell first when the
// grid is still empty.
func pbinTestUnfoldStep(t *testing.T, pph *PBinPatriciaHashed, probe *pbinBitpath) {
	t.Helper()
	u := pph.needUnfolding(probe)
	if u.action == pbinUnfoldRoot {
		require.NoError(t, pph.unfold(probe, u))
		u = pph.needUnfolding(probe)
	}
	require.NoError(t, pph.unfold(probe, u))
}

// TestPBinNeedUnfolding guards H9: the hex engine's cpl+1 hides a terminator
// nibble, so the binary engine states each outcome instead. What matters is that
// "the probe agrees with the whole prefix" and "the probe leaves the prefix
// partway" are different answers — only the second shortens a stored prefix,
// which is inside that node's hash.
func TestPBinNeedUnfolding(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name        string
		root        pbinCell
		rootChecked bool
		probe       string
		want        pbinUnfolding
	}{
		{
			name:  "an unchecked empty root reads the root cell record",
			root:  pbinTestEmptyCell(),
			probe: "1010",
			want:  pbinUnfolding{action: pbinUnfoldRoot},
		},
		{
			name:        "a checked empty root needs nothing",
			root:        pbinTestEmptyCell(),
			rootChecked: true,
			probe:       "1010",
			want:        pbinUnfolding{},
		},
		{
			name:  "a branch with an empty prefix is a record read",
			root:  pbinTestSpecCell(t, pbinNodeBranch, ""),
			probe: "1010",
			want:  pbinUnfolding{action: pbinUnfoldRecord},
		},
		{
			name:  "cpl == 0 splits at the first bit",
			root:  pbinTestSpecCell(t, pbinNodeBranch, "1011"),
			probe: "0011",
			want:  pbinUnfolding{action: pbinUnfoldSplit, matched: 0},
		},
		{
			name:  "cpl < len(prefix) splits inside it",
			root:  pbinTestSpecCell(t, pbinNodeBranch, "1011"),
			probe: "1001",
			want:  pbinUnfolding{action: pbinUnfoldSplit, matched: 2},
		},
		{
			name:  "cpl == len(prefix) descends through it",
			root:  pbinTestSpecCell(t, pbinNodeBranch, "1011"),
			probe: "10110",
			want:  pbinUnfolding{action: pbinUnfoldDescend, matched: 4},
		},
		{
			name:  "a leaf the probe fully matches is already the target",
			root:  pbinTestSpecCell(t, pbinNodeLeaf, "1011"),
			probe: "1011",
			want:  pbinUnfolding{},
		},
		{
			name:  "a leaf the probe leaves splits",
			root:  pbinTestSpecCell(t, pbinNodeLeaf, "1011"),
			probe: "1000",
			want:  pbinUnfolding{action: pbinUnfoldSplit, matched: 2},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			pph, _ := pbinTestEngine(t)
			pph.grid.root = tc.root
			pph.rootChecked = tc.rootChecked

			probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, tc.probe))
			require.Equal(t, tc.want, pph.needUnfolding(&probe))
		})
	}
}

// TestPBinNeedUnfoldingSelectsCellByBranchBit checks the row case picks the cell
// with the bit the row branches on, the arity-2 stand-in for the hex engine's
// nibble.
func TestPBinNeedUnfoldingSelectsCellByBranchBit(t *testing.T) {
	t.Parallel()

	pph, ms := pbinTestEngine(t)
	pbinTestPutTopRecord(t, ms, [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "000"),
		pbinTestSpecCell(t, pbinNodeBranch, "111"),
	})

	probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, "0000"))
	pbinTestUnfoldStep(t, pph, &probe)
	require.Equal(t, 1, pph.grid.activeRows)
	require.Equal(t, int16(1), pph.grid.depths[0])

	for _, tc := range []struct {
		name  string
		probe string
		want  pbinUnfolding
	}{
		{"left cell, leaf fully matched", "0000", pbinUnfolding{}},
		{"left cell, leaf left at its last bit", "0001", pbinUnfolding{action: pbinUnfoldSplit, matched: 2}},
		{"right cell, branch fully matched", "1111", pbinUnfolding{action: pbinUnfoldDescend, matched: 3}},
		{"right cell, branch left inside its prefix", "1101", pbinUnfolding{action: pbinUnfoldSplit, matched: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, tc.probe))
			require.Equal(t, tc.want, pph.needUnfolding(&probe))
		})
	}
}

// TestPBinUnfoldEmptyPrefixBranchRecord guards H7: EIP-8297 admits a branch node
// with no prefix, so a zero-length prefix cannot double as "this cell is not a
// stored branch". The engine must read the record below and descend into it.
func TestPBinUnfoldEmptyPrefixBranchRecord(t *testing.T) {
	t.Parallel()

	childCells := [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "0101"),
		pbinTestSpecCell(t, pbinNodeLeaf, "1100"),
	}
	rootCells := [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "010"),
		pbinTestSpecCell(t, pbinNodeBranch, ""),
	}
	childPath := pbinTestPathFromBits(t, pbinTestBitSpec(t, "1"))

	pph, ms := pbinTestEngine(t)
	pbinTestPutTopRecord(t, ms, rootCells)
	pbinTestPutRecord(t, ms, childPath, childCells)

	probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, "1101"))
	pbinTestUnfoldStep(t, pph, &probe)
	require.Equal(t, 1, pph.grid.activeRows)

	u := pph.needUnfolding(&probe)
	require.Equal(t, pbinUnfolding{action: pbinUnfoldRecord}, u,
		"a branch cell with a zero-length prefix is still a stored node")

	require.NoError(t, pph.unfold(&probe, u))
	require.Equal(t, 2, pph.grid.activeRows)
	require.Equal(t, int16(2), pph.grid.depths[1])
	require.Equal(t, childPath, pph.currentKey)
	require.True(t, pph.grid.branchBefore[1])
	require.Equal(t, childCells, pph.grid.rows[1])
	require.Equal(t, uint16(0b11), pph.grid.afterMap[1])
	require.Equal(t, uint16(0), pph.grid.touchMap[1])
}

// A missing record below such a cell is an inconsistency, not an empty subtree —
// the other half of H7's failure mode.
func TestPBinUnfoldEmptyPrefixBranchRecordMissing(t *testing.T) {
	t.Parallel()

	pph, ms := pbinTestEngine(t)
	pbinTestPutTopRecord(t, ms, [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "010"),
		pbinTestSpecCell(t, pbinNodeBranch, ""),
	})

	probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, "1101"))
	pbinTestUnfoldStep(t, pph, &probe)
	require.ErrorIs(t, pph.unfold(&probe, pph.needUnfolding(&probe)), errPBinMissingBranch)
}

func TestPBinUnfoldEmptyRoot(t *testing.T) {
	t.Parallel()

	pph, _ := pbinTestEngine(t)
	probe := pbinPathFromBytes(pbinTreeKeyAccount(pbinOracleAddr(1), pbinBasicDataLeafKey))

	u := pph.needUnfolding(&probe)
	require.Equal(t, pbinUnfolding{action: pbinUnfoldRoot}, u)
	require.NoError(t, pph.unfold(&probe, u))

	require.Equal(t, 0, pph.grid.activeRows)
	require.True(t, pph.rootChecked)
	require.Equal(t, pbinUnfolding{}, pph.needUnfolding(&probe), "a checked empty root does not unfold again")
}

// TestPBinUnfoldSplitsInsidePrefix walks the divergence bit across both word
// boundaries of the [9]uint64 path and both zone lengths. A split moves the node
// below one level down and re-cuts its prefix, dropping the bit the new row
// branches on (eip:174-176).
func TestPBinUnfoldSplitsInsidePrefix(t *testing.T) {
	t.Parallel()

	full := pbinTestPathFromBits(t, pbinTestBitPattern(pbinMaxPathBits))

	for _, divergence := range []int16{0, 63, 64, 65, 271, 527} {
		t.Run(fmt.Sprintf("bit %d", divergence), func(t *testing.T) {
			t.Parallel()

			pph, _ := pbinTestEngine(t)
			pph.grid.root = pbinTestEmptyCell()
			pph.grid.root.kind = pbinNodeLeaf
			pph.grid.root.prefix = full
			pph.rootPresent = true

			probe := full
			probe.setBitAt(divergence, full.bit(divergence)^1)

			u := pph.needUnfolding(&probe)
			require.Equal(t, pbinUnfolding{action: pbinUnfoldSplit, matched: divergence}, u)
			require.NoError(t, pph.unfold(&probe, u))

			require.Equal(t, 1, pph.grid.activeRows)
			require.Equal(t, divergence+1, pph.grid.depths[0])
			require.Equal(t, full.slice(0, divergence), pph.currentKey)

			survivorBit := full.bit(divergence)
			survivor := &pph.grid.rows[0][survivorBit]
			require.Equal(t, pbinNodeLeaf, survivor.kind)
			require.Equal(t, full.slice(divergence+1, full.bitLen), survivor.prefix,
				"the survivor drops the bit the new row branches on")
			require.Equal(t, pbinNodeEmpty, pph.grid.rows[0][1-survivorBit].kind,
				"the probe's own side is left for updateCell to fill")

			require.Equal(t, uint16(0), pph.grid.touchMap[0])
			require.Equal(t, uint16(1)<<survivorBit, pph.grid.afterMap[0])
		})
	}
}

// TestPBinUnfoldDescendsThroughPrefix pins the two-step descent: consuming a
// branch cell's prefix leaves a row whose cell has none, and only then is the
// record read — at a key the parent's stored prefix is what reconstructs.
func TestPBinUnfoldDescendsThroughPrefix(t *testing.T) {
	t.Parallel()

	full := pbinTestPathFromBits(t, pbinTestBitPattern(pbinMaxPathBits))

	for _, prefixBits := range []int16{1, 63, 64, 65, 272} {
		t.Run(fmt.Sprintf("%d-bit prefix", prefixBits), func(t *testing.T) {
			t.Parallel()

			childPath := full.slice(0, prefixBits)
			childCells := [2]pbinCell{
				pbinTestSpecCell(t, pbinNodeLeaf, "0101"),
				pbinTestSpecCell(t, pbinNodeLeaf, "1100"),
			}

			pph, ms := pbinTestEngine(t)
			pbinTestPutRecord(t, ms, childPath, childCells)
			pph.grid.root = pbinTestEmptyCell()
			pph.grid.root.kind = pbinNodeBranch
			pph.grid.root.prefix = childPath
			pph.grid.root.hash = common.Hash{0xC0}
			pph.grid.root.hashLen = length.Hash
			pph.rootPresent = true

			u := pph.needUnfolding(&full)
			require.Equal(t, pbinUnfolding{action: pbinUnfoldDescend, matched: prefixBits}, u)
			require.NoError(t, pph.unfold(&full, u))

			require.Equal(t, 1, pph.grid.activeRows)
			require.Equal(t, prefixBits, pph.grid.depths[0])
			require.Equal(t, full.slice(0, prefixBits-1), pph.currentKey)
			require.False(t, pph.grid.branchBefore[0], "materializing a prefix reads nothing")

			branchBit := full.bit(prefixBits - 1)
			cell := &pph.grid.rows[0][branchBit]
			require.Equal(t, pbinNodeBranch, cell.kind)
			require.Equal(t, int16(0), cell.prefix.bitLen, "the whole prefix moved into the descent key")
			require.Equal(t, int16(0), cell.hashLen,
				"the prefix is inside the branch hash, so moving it out of the cell invalidates it")

			u = pph.needUnfolding(&full)
			require.Equal(t, pbinUnfolding{action: pbinUnfoldRecord}, u)
			require.NoError(t, pph.unfold(&full, u))

			require.Equal(t, 2, pph.grid.activeRows)
			require.Equal(t, prefixBits+1, pph.grid.depths[1])
			require.Equal(t, childPath, pph.currentKey, "the record key is the parent path plus the stored prefix")
			require.Equal(t, childCells, pph.grid.rows[1])
		})
	}
}

// A parent cell marked touched but absent means everything below it goes: the
// loaded row is reported as touched and empty afterwards.
func TestPBinUnfoldDeletedSubtree(t *testing.T) {
	t.Parallel()

	childCells := [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "0101"),
		pbinTestSpecCell(t, pbinNodeLeaf, "1100"),
	}
	childPath := pbinTestPathFromBits(t, pbinTestBitSpec(t, "1"))

	pph, ms := pbinTestEngine(t)
	pbinTestPutTopRecord(t, ms, [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "010"),
		pbinTestSpecCell(t, pbinNodeBranch, ""),
	})
	pbinTestPutRecord(t, ms, childPath, childCells)

	probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, "1101"))
	pbinTestUnfoldStep(t, pph, &probe)

	pph.grid.touchMap[0], pph.grid.afterMap[0] = 0b10, 0
	require.NoError(t, pph.unfold(&probe, pph.needUnfolding(&probe)))

	require.Equal(t, uint16(0b11), pph.grid.touchMap[1])
	require.Equal(t, uint16(0), pph.grid.afterMap[1])
}
