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

package eip8297

import (
	"testing"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
)

func pathFromBits(bits []byte) Bitpath {
	var path Bitpath
	for i, bit := range bits {
		path.SetBitAt(int16(i), uint64(bit))
	}
	path.BitLen = int16(len(bits))
	return path
}

func bitPattern(length int) []byte {
	bits := make([]byte, length)
	for i := range bits {
		bits[i] = byte((i*7 + i/3) & 1)
	}
	return bits
}

func bitSpec(spec string) []byte {
	bits := make([]byte, 0, len(spec))
	for _, bit := range spec {
		bits = append(bits, byte(bit-'0'))
	}
	return bits
}

func TestAppendBitPrefixMatchesEncoding(t *testing.T) {
	for _, length := range []int{0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65, 255, 256, 271, 272, 527, MaxPathBits} {
		bits := bitPattern(length)
		path := pathFromBits(bits)
		require.Equal(t, EncodeBitPrefix(bits), AppendBitPrefix(nil, &path), length)
	}
}

func TestLeafPreimageHash(t *testing.T) {
	for _, entry := range []Entry{
		{Key: TreeKeyAccount(referenceAddress(1), BasicDataLeafKey), Value: referenceValue(1)},
		{Key: TreeKeyStorage(referenceAddress(2), referenceSlot(1000)), Value: referenceValue(2000)},
	} {
		require.Equal(t, common.Hash(keccak.Sum256(LeafPreimage(nil, entry.Key, entry.Value))), MerkelizeWith(&Leaf{Key: entry.Key, Value: entry.Value}, nil))
	}
}

func TestBranchPreimageHash(t *testing.T) {
	leftLeaf := &Leaf{Key: TreeKeyStorage(referenceAddress(1), referenceSlot(0)), Value: referenceValue(1000)}
	rightLeaf := &Leaf{Key: TreeKeyStorage(referenceAddress(2), referenceSlot(0)), Value: referenceValue(2000)}
	left := MerkelizeWith(leftLeaf, nil)
	right := MerkelizeWith(rightLeaf, nil)
	for _, bits := range [][]byte{
		nil,
		bitSpec("1"),
		bitSpec("1011010"),
		bitSpec("10110101"),
		bitSpec("101101011"),
		bitPattern(64),
		bitPattern(65),
		bitPattern(MaxPathBits - 1),
	} {
		path := pathFromBits(bits)
		want := common.Hash(keccak.Sum256(BranchPreimage(nil, &path, &left, &right)))
		got := MerkelizeWith(&Branch{Prefix: bits, Left: leftLeaf, Right: rightLeaf}, nil)
		require.Equal(t, want, got, len(bits))
	}
}

func TestNestedBranchPreimageHash(t *testing.T) {
	left := &Leaf{Key: TreeKeyStorage(referenceAddress(1), referenceSlot(0)), Value: referenceValue(1000)}
	middle := &Leaf{Key: TreeKeyStorage(referenceAddress(2), referenceSlot(0)), Value: referenceValue(2000)}
	right := &Leaf{Key: TreeKeyStorage(referenceAddress(3), referenceSlot(0)), Value: referenceValue(3000)}
	innerBits, outerBits := bitSpec("10110"), bitSpec("011")
	innerPath, outerPath := pathFromBits(innerBits), pathFromBits(outerBits)
	leftHash := MerkelizeWith(left, nil)
	middleHash := MerkelizeWith(middle, nil)
	rightHash := MerkelizeWith(right, nil)
	inner := &Branch{Prefix: innerBits, Left: left, Right: middle}
	innerHash := common.Hash(keccak.Sum256(BranchPreimage(nil, &innerPath, &leftHash, &middleHash)))
	outer := &Branch{Prefix: outerBits, Left: inner, Right: right}
	want := common.Hash(keccak.Sum256(BranchPreimage(nil, &outerPath, &innerHash, &rightHash)))
	got := MerkelizeWith(outer, nil)
	require.Equal(t, want, got)
}

func TestBranchPreimageEmptyChild(t *testing.T) {
	leaf := &Leaf{Key: TreeKeyStorage(referenceAddress(4), referenceSlot(7)), Value: referenceValue(4007)}
	leafHash := MerkelizeWith(leaf, nil)
	path := pathFromBits(bitSpec("0101"))
	want := common.Hash(keccak.Sum256(BranchPreimage(nil, &path, &leafHash, &EmptyTreeHash)))
	require.Equal(t, want, MerkelizeWith(&Branch{Prefix: bitSpec("0101"), Left: leaf, Right: nil}, nil))
}
