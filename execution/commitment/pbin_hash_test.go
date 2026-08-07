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
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	keccak "github.com/erigontech/fastkeccak"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
)

// leafHash is H(0x00 || key || value) over the complete tree key. The engine
// builds the same preimage from a cell; taking the key and value directly is
// what lets a test state the expected hash.
func (h *pbinHasher) leafHash(key, value []byte) common.Hash {
	if len(key) != pbinAccountKeyLength && len(key) != pbinStorageKeyLength {
		panic(fmt.Sprintf("pbin: leaf key of %d bytes is neither zone length", len(key)))
	}
	if len(value) != pbinValueLength {
		panic(fmt.Sprintf("pbin: leaf value of %d bytes, want %d", len(value), pbinValueLength))
	}
	buf := append(h.buf[:0], pbinLeafTag)
	buf = append(buf, key...)
	buf = append(buf, value...)
	return keccak.Sum256(buf)
}

func pbinTestPathFromBits(t *testing.T, bits []byte) pbinBitpath {
	t.Helper()
	require.LessOrEqual(t, len(bits), pbinMaxPathBits)
	var p pbinBitpath
	for i, b := range bits {
		p.setBitAt(int16(i), uint64(b))
	}
	p.bitLen = int16(len(bits))
	return p
}

// pbinTestBitSpec reads a "1011" literal into the oracle's one-bit-per-byte form.
func pbinTestBitSpec(t *testing.T, spec string) []byte {
	t.Helper()
	bits := make([]byte, 0, len(spec))
	for _, r := range spec {
		switch r {
		case '0':
			bits = append(bits, 0)
		case '1':
			bits = append(bits, 1)
		default:
			t.Fatalf("bit spec %q holds %q", spec, r)
		}
	}
	return bits
}

func pbinTestBitPattern(n int) []byte {
	bits := make([]byte, n)
	for i := range bits {
		bits[i] = byte((i*7 + i/3) & 1)
	}
	return bits
}

func pbinTestOracleLeaf(addr, slot uint64) *pbinOracleLeaf {
	return &pbinOracleLeaf{
		key:   pbinTreeKeyStorage(pbinOracleAddr(addr), pbinOracleSlot(slot)),
		value: pbinOracleValue(addr*1000 + slot),
	}
}

// EIP-8297's empty subtree is 32 zero bytes (eip:208), not the empty-MPT root
// the rest of erigon reaches for.
func TestPBinEmptyTreeHash(t *testing.T) {
	t.Parallel()

	require.Equal(t, make([]byte, 32), pbinEmptyTreeHash[:])
	require.NotEqual(t, empty.RootHash, pbinEmptyTreeHash)

	var h pbinHasher
	var c pbinCell
	var path pbinBitpath
	got, err := h.cellHash(&c, &path)
	require.NoError(t, err)
	require.Equal(t, pbinEmptyTreeHash, got)
	require.NotEqual(t, empty.RootHash, got)
}

// The lengths below are the ones where bit-prefix padding can go wrong.
func TestPBinAppendBitPrefixMatchesOracle(t *testing.T) {
	t.Parallel()

	for _, n := range []int{0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 255, 256, 271, 272, 527, pbinMaxPathBits} {
		bits := pbinTestBitPattern(n)
		path := pbinTestPathFromBits(t, bits)
		require.Equal(t, pbinOracleEncodeBitPrefix(bits), pbinAppendBitPrefix(nil, &path), "%d bits", n)
	}
}

func TestPBinLeafHashMatchesOracle(t *testing.T) {
	t.Parallel()

	var h pbinHasher
	for _, tc := range []struct {
		name string
		leaf *pbinOracleLeaf
	}{
		{
			name: "account key",
			leaf: &pbinOracleLeaf{
				key:   pbinTreeKeyAccount(pbinOracleAddr(1), pbinBasicDataLeafKey),
				value: pbinOracleValue(1),
			},
		},
		{
			name: "storage key",
			leaf: pbinTestOracleLeaf(2, 1000),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := pbinOracleMerkelize(tc.leaf)
			require.Equal(t, common.Hash(want), h.leafHash(tc.leaf.key, tc.leaf.value))
		})
	}
}

func TestPBinBranchHashMatchesOracle(t *testing.T) {
	t.Parallel()

	var h pbinHasher
	left, right := pbinTestOracleLeaf(1, 0), pbinTestOracleLeaf(2, 0)
	leftHash := h.leafHash(left.key, left.value)
	rightHash := h.leafHash(right.key, right.value)

	for _, tc := range []struct {
		name string
		bits []byte
	}{
		{name: "empty prefix", bits: nil},
		{name: "one bit", bits: pbinTestBitSpec(t, "1")},
		{name: "seven bits", bits: pbinTestBitSpec(t, "1011010")},
		{name: "eight bits", bits: pbinTestBitSpec(t, "10110101")},
		{name: "nine bits", bits: pbinTestBitSpec(t, "101101011")},
		{name: "one word", bits: pbinTestBitPattern(64)},
		{name: "past one word", bits: pbinTestBitPattern(65)},
		{name: "deepest branch a 528-bit key admits", bits: pbinTestBitPattern(pbinMaxPathBits - 1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			want := pbinOracleMerkelize(&pbinOracleBranch{prefix: tc.bits, left: left, right: right})
			path := pbinTestPathFromBits(t, tc.bits)
			require.Equal(t, common.Hash(want), h.branchHash(&path, &leftHash, &rightHash))
		})
	}
}

// Covers a branch hash feeding another branch, not just a branch over leaves.
func TestPBinNestedBranchHashMatchesOracle(t *testing.T) {
	t.Parallel()

	var h pbinHasher
	a, b, c := pbinTestOracleLeaf(1, 0), pbinTestOracleLeaf(2, 0), pbinTestOracleLeaf(3, 0)
	innerBits := pbinTestBitSpec(t, "10110")
	outerBits := pbinTestBitSpec(t, "011")

	inner := &pbinOracleBranch{prefix: innerBits, left: a, right: b}
	outer := &pbinOracleBranch{prefix: outerBits, left: inner, right: c}
	want := pbinOracleMerkelize(outer)

	aHash := h.leafHash(a.key, a.value)
	bHash := h.leafHash(b.key, b.value)
	cHash := h.leafHash(c.key, c.value)
	innerPath := pbinTestPathFromBits(t, innerBits)
	innerHash := h.branchHash(&innerPath, &aHash, &bHash)
	outerPath := pbinTestPathFromBits(t, outerBits)

	require.Equal(t, common.Hash(want), h.branchHash(&outerPath, &innerHash, &cHash))
}

// An absent child contributes the empty-subtree constant rather than being
// skipped.
func TestPBinBranchHashEmptyChild(t *testing.T) {
	t.Parallel()

	var h pbinHasher
	leaf := pbinTestOracleLeaf(4, 7)
	leafHash := h.leafHash(leaf.key, leaf.value)
	bits := pbinTestBitSpec(t, "0101")

	want := pbinOracleMerkelize(&pbinOracleBranch{prefix: bits, left: leaf, right: nil})
	path := pbinTestPathFromBits(t, bits)
	require.Equal(t, common.Hash(want), h.branchHash(&path, &leafHash, &pbinEmptyTreeHash))
}

func TestPBinCellHashBranch(t *testing.T) {
	t.Parallel()

	var h pbinHasher
	var path pbinBitpath

	t.Run("returns the stored hash", func(t *testing.T) {
		c := pbinCell{kind: pbinNodeBranch, hash: common.Hash{0xAB}, hashLen: 32}
		got, err := h.cellHash(&c, &path)
		require.NoError(t, err)
		require.Equal(t, c.hash, got)
	})
	t.Run("rejects a branch cell with no hash", func(t *testing.T) {
		c := pbinCell{kind: pbinNodeBranch}
		_, err := h.cellHash(&c, &path)
		require.ErrorIs(t, err, errPBinCellHash)
	})
}

func TestPBinCellHashLeaf(t *testing.T) {
	t.Parallel()

	addr := pbinOracleAddr(9)
	storageKey := pbinTreeKeyStorage(addr, pbinOracleSlot(1000))
	headerSlotKey := pbinTreeKeyStorage(addr, pbinOracleSlot(5))
	codeHash := common.Hash{0xC0, 0xDE}

	balance := new(uint256.Int).SetUint64(0xDEADBEEF)

	basicData, err := pbinEncodeBasicData(7, balance, 0)
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		key   []byte
		cell  pbinCell
		value [pbinValueLength]byte
	}{
		{
			name:  "BASIC_DATA",
			key:   pbinTreeKeyAccount(addr, pbinBasicDataLeafKey),
			cell:  pbinCell{Update: Update{Nonce: 7, Balance: *balance}},
			value: basicData,
		},
		{
			name:  "CODE_HASH",
			key:   pbinTreeKeyAccount(addr, pbinCodeHashLeafKey),
			cell:  pbinCell{Update: Update{CodeHash: codeHash}},
			value: pbinCodeHashValue(codeHash),
		},
		{
			name:  "header-zone storage slot",
			key:   headerSlotKey,
			cell:  pbinCell{Update: Update{Storage: common.Hash{0x11, 0x22}, StorageLen: 2}},
			value: pbinEncodeStorageValue([]byte{0x11, 0x22}),
		},
		{
			name:  "storage-zone slot",
			key:   storageKey,
			cell:  pbinCell{Update: Update{Storage: common.Hash{0x33}, StorageLen: 1}},
			value: pbinEncodeStorageValue([]byte{0x33}),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var h pbinHasher
			full := pbinPathFromBytes(tc.key)
			// Split the key so that both the descent path and the cell prefix carry
			// real bits: the complete key is their concatenation, not either alone.
			const split = 100
			path := full.slice(0, split)
			cell := tc.cell
			cell.kind = pbinNodeLeaf
			cell.prefix = full.slice(split, full.bitLen)

			got, err := h.cellHash(&cell, &path)
			require.NoError(t, err)

			want := pbinOracleMerkelize(&pbinOracleLeaf{key: tc.key, value: tc.value[:]})
			require.Equal(t, common.Hash(want), got)
		})
	}
}

func TestPBinCellHashRejectsMalformedLeaf(t *testing.T) {
	t.Parallel()

	var h pbinHasher
	key := pbinTreeKeyAccount(pbinOracleAddr(3), pbinBasicDataLeafKey)
	full := pbinPathFromBytes(key)

	t.Run("key of neither zone length", func(t *testing.T) {
		path := full.slice(0, 100)
		c := pbinCell{kind: pbinNodeLeaf, prefix: full.slice(100, full.bitLen-1)}
		_, err := h.cellHash(&c, &path)
		require.ErrorIs(t, err, errPBinCellHash)
	})
	t.Run("account-zone sub-index naming no leaf", func(t *testing.T) {
		bad := pbinPathFromBytes(pbinTreeKey(pbinAccountZone, make([]byte, 32), pbinHeaderStorageOffset+pbinHeaderStorageSlots))
		path := bad.slice(0, 100)
		c := pbinCell{kind: pbinNodeLeaf, prefix: bad.slice(100, bad.bitLen)}
		_, err := h.cellHash(&c, &path)
		require.ErrorIs(t, err, errPBinCellHash)
	})
}

// Folds each two-key corpus by hand through the cell hasher, checking the
// primitives compose into the root the reference tree produces.
func TestPBinCellHashBuildsCorpusRoots(t *testing.T) {
	t.Parallel()

	for _, corpus := range []pbinOracleCorpus{
		pbinOracleCorpusSplitAtBit0(),
		pbinOracleCorpusSplitAtLastBit(),
	} {
		t.Run(corpus.name, func(t *testing.T) {
			require.Len(t, corpus.entries, 2)
			var h pbinHasher
			a, b := corpus.entries[0], corpus.entries[1]

			aPath, bPath := pbinPathFromBytes(a.key), pbinPathFromBytes(b.key)
			shared := pbinCommonPrefixBitsAt(&aPath, 0, &bPath)
			prefix := aPath.slice(0, shared)

			left, right := a, b
			if aPath.bit(shared) == 1 {
				left, right = b, a
			}
			leftHash := h.leafHash(left.key, left.value)
			rightHash := h.leafHash(right.key, right.value)

			require.Equal(t, common.Hash(pbinOracleRoot(corpus.entries)), h.branchHash(&prefix, &leftHash, &rightHash))
		})
	}
}
