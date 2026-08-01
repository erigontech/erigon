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
	"github.com/erigontech/erigon/db/kv"
)

type pbinTestCountingCtx struct {
	PatriciaContext
	branchReads int
}

func (c *pbinTestCountingCtx) Branch(prefix []byte) ([]byte, kv.Step, error) {
	c.branchReads++
	return c.PatriciaContext.Branch(prefix)
}

// pbinTestLeaf is one storage entry in the three forms a fold needs: the plain
// key state is read by, the tree key the path is cut from, and the encoded value
// the engine and the oracle hash.
type pbinTestLeaf struct {
	plainKey []byte
	treeKey  []byte
	storage  []byte
	value    [pbinValueLength]byte
}

func pbinTestStorageLeaf(treeKey []byte, seed byte) pbinTestLeaf {
	storage := []byte{seed, seed ^ 0xFF}
	return pbinTestLeaf{
		plainKey: bytes.Repeat([]byte{seed}, length.Addr+length.Hash),
		treeKey:  treeKey,
		storage:  storage,
		value:    pbinEncodeStorageValue(storage),
	}
}

func (l pbinTestLeaf) update() Update {
	u := Update{Flags: StorageUpdate, StorageLen: int8(len(l.storage))}
	copy(u.Storage[:], l.storage)
	return u
}

// cell cuts the leaf's tree key at depth, the way a row at that depth holds it.
func (l pbinTestLeaf) cell(t *testing.T, depth int16) pbinCell {
	t.Helper()
	full := pbinPathFromBytes(l.treeKey)
	c := pbinTestEmptyCell()
	c.kind = pbinNodeLeaf
	c.prefix = full.slice(depth, full.bitLen)
	copy(c.storageAddr[:], l.plainKey)
	c.storageAddrLen = length.Addr + length.Hash
	c.Update = l.update()
	c.loaded = cellLoadStorage
	return c
}

func (l pbinTestLeaf) entry() pbinOracleEntry {
	return pbinOracleEntry{key: l.treeKey, value: l.value[:]}
}

func pbinTestPutState(t *testing.T, ms *MockState, leaves ...pbinTestLeaf) {
	t.Helper()
	keys := make([][]byte, 0, len(leaves))
	updates := make([]Update, 0, len(leaves))
	for _, l := range leaves {
		keys = append(keys, l.plainKey)
		updates = append(updates, l.update())
	}
	require.NoError(t, ms.applyPlainUpdates(keys, updates))
}

// The zone byte is off limits: it selects the value encoding.
func pbinTestTreeKeyFlipped(t *testing.T, key []byte, d int16) []byte {
	t.Helper()
	require.GreaterOrEqual(t, d, int16(8), "bit %d is inside the zone byte", d)
	require.Less(t, int(d), len(key)*8)
	out := bytes.Clone(key)
	out[d/8] ^= 1 << (7 - uint(d%8))
	return out
}

func pbinTestBaseStorageKey() []byte {
	return pbinTreeKeyStorage(pbinOracleAddr(7), pbinOracleSlot(1000))
}

func pbinTestKeyPrefix(key []byte, bitLen int16) pbinBitpath {
	full := pbinPathFromBytes(key)
	return full.slice(0, bitLen)
}

func pbinTestSeedRow(pph *PBinPatriciaHashed, currentKey pbinBitpath, depth int16, cells [2]pbinCell, touchMap, afterMap uint16) {
	pph.currentKey = currentKey
	pph.grid.rows[0] = cells
	pph.grid.depths[0] = depth
	pph.grid.touchMap[0], pph.grid.afterMap[0] = touchMap, afterMap
	pph.grid.activeRows = 1
}

// pbinTestFillCell fills a row cell the way updateCell does: touched and present.
func pbinTestFillCell(pph *PBinPatriciaHashed, row int, bit uint64, c pbinCell) {
	pph.grid.rows[row][bit] = c
	pph.grid.touchMap[row] |= uint16(1) << bit
	pph.grid.afterMap[row] |= uint16(1) << bit
}

func pbinTestBranchOrder(t *testing.T, a, b pbinTestLeaf, divergence int16) (left, right pbinTestLeaf) {
	t.Helper()
	path := pbinPathFromBytes(a.treeKey)
	if path.bit(divergence) == 1 {
		return b, a
	}
	return a, b
}

// Divergence points span both word boundaries of the path. Nothing merges the
// record a branch fold writes with a predecessor, so it must also survive a
// decode and re-encode unchanged.
func TestPBinFoldBranchMatchesOracle(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	for _, divergence := range []int16{8, 63, 64, 65, 271, 527} {
		t.Run(fmt.Sprintf("bit %d", divergence), func(t *testing.T) {
			t.Parallel()

			a := pbinTestStorageLeaf(base, 0x11)
			b := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, divergence), 0x22)
			left, right := pbinTestBranchOrder(t, a, b, divergence)

			ms := NewMockState(t)
			ctx := &pbinTestCountingCtx{PatriciaContext: ms}
			pph := NewPBinPatriciaHashed(ctx)

			currentKey := pbinTestKeyPrefix(a.treeKey, divergence)
			cells := [2]pbinCell{left.cell(t, divergence+1), right.cell(t, divergence+1)}
			pbinTestSeedRow(pph, currentKey, divergence+1, cells, 0b11, 0b11)

			require.NoError(t, pph.fold())
			require.Equal(t, 0, pph.grid.activeRows)
			require.Equal(t, int16(0), pph.currentKey.bitLen)
			require.True(t, pph.rootTouched)
			require.True(t, pph.rootPresent)
			require.Zero(t, ctx.branchReads, "a fold of loaded cells reads nothing")

			require.Equal(t, pbinNodeBranch, pph.grid.root.kind)
			require.Equal(t, currentKey, pph.grid.root.prefix)
			want := pbinOracleRoot([]pbinOracleEntry{a.entry(), b.entry()})
			require.Equal(t, common.Hash(want), pph.grid.root.hash)

			data, _, err := ms.Branch(pbinEncodeBitPath(&currentKey))
			require.NoError(t, err)
			require.NotEmpty(t, data, "a branch fold stores its row")

			var stored [2]pbinCell
			touchMap, afterMap, err := pbinDecodeBranch(data, &stored)
			require.NoError(t, err)
			require.Equal(t, uint16(0b11), touchMap)
			require.Equal(t, uint16(0b11), afterMap)
			require.Equal(t, cells[0].prefix, stored[0].prefix)
			require.Equal(t, cells[1].prefix, stored[1].prefix)

			var enc pbinBranchEncoder
			again, err := enc.encode(touchMap, afterMap, &stored)
			require.NoError(t, err)
			require.Equal(t, data, []byte(again))
		})
	}
}

// Folding a row as a branch with anything but two children is a lost or
// duplicated sibling, which at arity 2 is half the subtree.
func TestPBinFoldBranchRejectsWrongArity(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	a := pbinTestStorageLeaf(base, 0x11)

	pph, _ := pbinTestEngine(t)
	cells := [2]pbinCell{a.cell(t, 9), pbinTestEmptyCell()}
	pbinTestSeedRow(pph, pbinTestKeyPrefix(a.treeKey, 8), 9, cells, 0b01, 0b01)

	require.Error(t, pph.foldBranch(0, 0, 0, 9, &pph.grid.root))
}

func TestPBinFoldRejectsInconsistentGrid(t *testing.T) {
	t.Parallel()

	t.Run("no active rows", func(t *testing.T) {
		t.Parallel()
		pph, _ := pbinTestEngine(t)
		require.Error(t, pph.fold())
	})
	t.Run("cell bit outside the arity", func(t *testing.T) {
		t.Parallel()
		pph, _ := pbinTestEngine(t)
		pbinTestSeedRow(pph, pbinBitpath{}, 1, [2]pbinCell{}, 0b100, 0b100)
		require.ErrorIs(t, pph.fold(), errPBinCellMaps)
	})
	t.Run("key shorter than the row depth", func(t *testing.T) {
		t.Parallel()
		pph, _ := pbinTestEngine(t)
		pbinTestSeedRow(pph, pbinBitpath{}, 5, [2]pbinCell{}, 0, 0b11)
		require.Error(t, pph.fold())
	})
}

// Unfold consumes a shared prefix into the descent key, so the branch fold below
// sees none of it. The propagate that follows has to hand the node back its full
// prefix, which is inside its hash.
func TestPBinFoldPropagateRestoresDescendedNode(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	for _, divergence := range []int16{8, 63, 64, 65, 271, 527} {
		t.Run(fmt.Sprintf("prefix of %d bits", divergence), func(t *testing.T) {
			t.Parallel()

			a := pbinTestStorageLeaf(base, 0x33)
			b := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, divergence), 0x44)
			left, right := pbinTestBranchOrder(t, a, b, divergence)
			prefix := pbinTestKeyPrefix(a.treeKey, divergence)

			ms := NewMockState(t)
			pbinTestPutState(t, ms, a, b)

			// Build the node once, then meet it again through a cell that knows only
			// its prefix and hash, the way a reload would.
			builder := NewPBinPatriciaHashed(ms)
			cells := [2]pbinCell{left.cell(t, divergence+1), right.cell(t, divergence+1)}
			pbinTestSeedRow(builder, prefix, divergence+1, cells, 0b11, 0b11)
			require.NoError(t, builder.fold())
			nodeHash := builder.grid.root.hash

			ctx := &pbinTestCountingCtx{PatriciaContext: ms}
			pph := NewPBinPatriciaHashed(ctx)
			pph.grid.root = pbinTestEmptyCell()
			pph.grid.root.kind = pbinNodeBranch
			pph.grid.root.prefix = prefix
			pph.grid.root.hash = nodeHash
			pph.grid.root.hashLen = length.Hash
			pph.rootPresent = true

			probe := pbinPathFromBytes(a.treeKey)
			u := pph.needUnfolding(&probe)
			require.Equal(t, pbinUnfolding{action: pbinUnfoldDescend, matched: divergence}, u)
			require.NoError(t, pph.unfold(&probe, u))
			require.Equal(t, int16(0), pph.grid.rows[0][probe.bit(divergence-1)].hashLen,
				"re-cutting a prefix invalidates the hash it is inside")

			u = pph.needUnfolding(&probe)
			require.Equal(t, pbinUnfolding{action: pbinUnfoldRecord}, u)
			require.NoError(t, pph.unfold(&probe, u))
			require.Equal(t, 2, pph.grid.activeRows)

			require.NoError(t, pph.fold())
			require.NoError(t, pph.fold())

			require.Equal(t, 0, pph.grid.activeRows)
			require.Equal(t, pbinNodeBranch, pph.grid.root.kind)
			require.Equal(t, prefix, pph.grid.root.prefix, "the propagate hands back every consumed bit")
			require.Equal(t, nodeHash, pph.grid.root.hash)

			want := pbinOracleRoot([]pbinOracleEntry{a.entry(), b.entry()})
			require.Equal(t, common.Hash(want), pph.grid.root.hash)
			require.Equal(t, 1, ctx.branchReads, "the descent reads the node once")
			require.Zero(t, pph.counters.materializeReads, "a descended node keeps its children")
		})
	}
}

// A leaf commits its complete key, so shortening the prefix it sits behind
// invalidates nothing and no record has to be read to rebuild it.
func TestPBinFoldSplitLeafSurvivorReadsNoBranch(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	for _, divergence := range []int16{8, 63, 64, 65, 271, 527} {
		t.Run(fmt.Sprintf("bit %d", divergence), func(t *testing.T) {
			t.Parallel()

			a := pbinTestStorageLeaf(base, 0x55)
			c := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, divergence), 0x66)

			ms := NewMockState(t)
			pbinTestPutState(t, ms, a, c)
			ctx := &pbinTestCountingCtx{PatriciaContext: ms}
			pph := NewPBinPatriciaHashed(ctx)
			pph.grid.root = a.cell(t, 0)
			pph.rootPresent = true

			probe := pbinPathFromBytes(c.treeKey)
			u := pph.needUnfolding(&probe)
			require.Equal(t, pbinUnfolding{action: pbinUnfoldSplit, matched: divergence}, u)
			require.NoError(t, pph.unfold(&probe, u))
			require.Equal(t, uint64(1), pph.counters.splitsInsidePrefix)

			pbinTestFillCell(pph, 0, probe.bit(divergence), c.cell(t, divergence+1))
			require.NoError(t, pph.fold())

			want := pbinOracleRoot([]pbinOracleEntry{a.entry(), c.entry()})
			require.Equal(t, common.Hash(want), pph.grid.root.hash)
			require.Zero(t, ctx.branchReads, "a leaf survivor needs no record")
			require.Zero(t, pph.counters.materializeReads)
		})
	}
}

// The survivor of a split keeps prefix[matched+1:], and the prefix is inside its
// hash, so the cached hash is stale. The engine has to rebuild it from the
// survivor's own children before the fold above can use it.
func TestPBinFoldSplitInsidePrefixMatchesOracle(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	const nodePrefixBits = 527

	for _, divergence := range []int16{8, 63, 64, 65, 271, 526} {
		t.Run(fmt.Sprintf("bit %d of %d", divergence, nodePrefixBits), func(t *testing.T) {
			t.Parallel()

			a := pbinTestStorageLeaf(base, 0x77)
			b := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, nodePrefixBits), 0x88)
			c := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, divergence), 0x99)
			left, right := pbinTestBranchOrder(t, a, b, nodePrefixBits)
			nodePrefix := pbinTestKeyPrefix(a.treeKey, nodePrefixBits)

			ms := NewMockState(t)
			pbinTestPutState(t, ms, a, b, c)

			builder := NewPBinPatriciaHashed(ms)
			cells := [2]pbinCell{left.cell(t, nodePrefixBits+1), right.cell(t, nodePrefixBits+1)}
			pbinTestSeedRow(builder, nodePrefix, nodePrefixBits+1, cells, 0b11, 0b11)
			require.NoError(t, builder.fold())

			ctx := &pbinTestCountingCtx{PatriciaContext: ms}
			pph := NewPBinPatriciaHashed(ctx)
			pph.grid.root = pbinTestEmptyCell()
			pph.grid.root.kind = pbinNodeBranch
			pph.grid.root.prefix = nodePrefix
			pph.grid.root.hash = builder.grid.root.hash
			pph.grid.root.hashLen = length.Hash
			pph.rootPresent = true

			probe := pbinPathFromBytes(c.treeKey)
			u := pph.needUnfolding(&probe)
			require.Equal(t, pbinUnfolding{action: pbinUnfoldSplit, matched: divergence}, u)
			require.NoError(t, pph.unfold(&probe, u))
			require.Equal(t, uint64(1), pph.counters.splitsInsidePrefix)

			survivorBit := 1 - probe.bit(divergence)
			survivor := &pph.grid.rows[0][survivorBit]
			require.Equal(t, pbinNodeBranch, survivor.kind)
			require.Equal(t, nodePrefix.slice(divergence+1, nodePrefixBits), survivor.prefix)
			require.Equal(t, int16(0), survivor.hashLen, "a shortened prefix voids the cached hash")

			pbinTestFillCell(pph, 0, probe.bit(divergence), c.cell(t, divergence+1))
			require.NoError(t, pph.fold())

			want := pbinOracleRoot([]pbinOracleEntry{a.entry(), b.entry(), c.entry()})
			require.Equal(t, common.Hash(want), pph.grid.root.hash)
			require.Equal(t, nodePrefix.slice(0, divergence), pph.grid.root.prefix)
			require.Equal(t, uint64(1), pph.counters.materializeReads, "the survivor is rebuilt from one record")
		})
	}
}

// A cell whose subtree is stored but missing cannot be rebuilt, and passing the
// stale hash off as current would commit a wrong root.
func TestPBinFoldSplitInsidePrefixMissingRecord(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	const nodePrefixBits = 271
	const divergence = 64

	a := pbinTestStorageLeaf(base, 0xA1)
	c := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, divergence), 0xA2)
	nodePrefix := pbinTestKeyPrefix(a.treeKey, nodePrefixBits)

	ms := NewMockState(t)
	pbinTestPutState(t, ms, a, c)
	pph := NewPBinPatriciaHashed(ms)
	pph.grid.root = pbinTestEmptyCell()
	pph.grid.root.kind = pbinNodeBranch
	pph.grid.root.prefix = nodePrefix
	pph.grid.root.hash = common.Hash{0xDE, 0xAD}
	pph.grid.root.hashLen = length.Hash
	pph.rootPresent = true

	probe := pbinPathFromBytes(c.treeKey)
	require.NoError(t, pph.unfold(&probe, pph.needUnfolding(&probe)))
	pbinTestFillCell(pph, 0, probe.bit(divergence), c.cell(t, divergence+1))

	require.ErrorIs(t, pph.fold(), errPBinMissingBranch)
}

// A record carries plain keys, not values, so a sibling that nothing in this run
// touched has to be read back from state before it can be hashed.
func TestPBinFoldLoadsSiblingState(t *testing.T) {
	t.Parallel()

	base := pbinTestBaseStorageKey()
	const divergence = 271

	a := pbinTestStorageLeaf(base, 0xB1)
	b := pbinTestStorageLeaf(pbinTestTreeKeyFlipped(t, base, divergence), 0xB2)
	left, right := pbinTestBranchOrder(t, a, b, divergence)
	prefix := pbinTestKeyPrefix(a.treeKey, divergence)

	ms := NewMockState(t)
	pbinTestPutState(t, ms, a, b)
	pph := NewPBinPatriciaHashed(ms)

	stateless := [2]pbinCell{left.cell(t, divergence+1), right.cell(t, divergence+1)}
	for i := range stateless {
		stateless[i].Update.Reset()
		stateless[i].loaded = cellLoadNone
	}
	pbinTestSeedRow(pph, prefix, divergence+1, stateless, 0b11, 0b11)
	require.NoError(t, pph.fold())

	want := pbinOracleRoot([]pbinOracleEntry{a.entry(), b.entry()})
	require.Equal(t, common.Hash(want), pph.grid.root.hash)
}

// A row that keeps nothing takes its stored record with it and reports the
// absence upwards.
func TestPBinFoldDeleteDropsRecord(t *testing.T) {
	t.Parallel()

	pph, ms := pbinTestEngine(t)
	key := pbinBitpath{}
	pbinTestPutTopRecord(t, ms, [2]pbinCell{
		pbinTestSpecCell(t, pbinNodeLeaf, "010"),
		pbinTestSpecCell(t, pbinNodeLeaf, "110"),
	})

	probe := pbinTestPathFromBits(t, pbinTestBitSpec(t, "0101"))
	pbinTestUnfoldStep(t, pph, &probe)
	require.True(t, pph.grid.branchBefore[0])
	pph.grid.touchMap[0], pph.grid.afterMap[0] = 0b11, 0

	require.NoError(t, pph.fold())
	require.Equal(t, pbinTestEmptyCell(), pph.grid.root)
	require.True(t, pph.rootTouched)
	require.False(t, pph.rootPresent)

	data, _, err := ms.Branch(pbinEncodeBitPath(&key))
	require.NoError(t, err)
	require.Empty(t, data)
}
