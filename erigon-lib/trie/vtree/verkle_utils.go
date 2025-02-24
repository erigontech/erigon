// Copyright 2024 The Erigon Authors
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

package vtree

import (
	"github.com/crate-crypto/go-ipa/bandersnatch/fr"
	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
)

const (
	VersionLeafKey    = 0
	BalanceLeafKey    = 1
	NonceLeafKey      = 2
	CodeKeccakLeafKey = 3
	CodeSizeLeafKey   = 4
)

var (
	zero                = uint256.NewInt(0)
	HeaderStorageOffset = uint256.NewInt(64)
	CodeOffset          = uint256.NewInt(128)
	MainStorageOffset   = new(uint256.Int).Lsh(uint256.NewInt(256), 31)
	VerkleNodeWidth     = uint256.NewInt(256)
	codeStorageDelta    = uint256.NewInt(0).Sub(CodeOffset, HeaderStorageOffset)

	getTreePolyIndex0Point *verkle.Point
)

func init() {
	getTreePolyIndex0Point = new(verkle.Point)
	err := getTreePolyIndex0Point.SetBytes([]byte{34, 25, 109, 242, 193, 5, 144, 224, 76, 52, 189, 92, 197, 126, 9, 145, 27, 152, 199, 130, 165, 3, 210, 27, 193, 131, 142, 28, 110, 26, 16, 191})
	if err != nil {
		panic(err)
	}
}

// GetTreeKey performs both the work of the spec's get_tree_key function, and that
// of pedersen_hash: it builds the polynomial in pedersen_hash without having to
// create a mostly zero-filled buffer and "type cast" it to a 128-long 16-byte
// array. Since at most the first 5 coefficients of the polynomial will be non-zero,
// these 5 coefficients are created directly.
func GetTreeKey(address []byte, treeIndex *uint256.Int, subIndex byte) []byte {
	if len(address) < 32 {
		var aligned [32]byte
		address = append(aligned[:32-len(address)], address...)
	}
	var poly [5]fr.Element

	poly[0].SetZero()

	// 32-byte address, interpreted as two little endian
	// 16-byte numbers.
	verkle.FromLEBytes(&poly[1], address[:16])
	verkle.FromLEBytes(&poly[2], address[16:])

	// little-endian, 32-byte aligned treeIndex
	var index [32]byte
	for i, b := range treeIndex.Bytes() {
		index[len(treeIndex.Bytes())-1-i] = b
	}
	verkle.FromLEBytes(&poly[3], index[:16])
	verkle.FromLEBytes(&poly[4], index[16:])

	cfg := verkle.GetConfig()
	ret := cfg.CommitToPoly(poly[:], 0)

	// add a constant point
	ret.Add(ret, getTreePolyIndex0Point)

	return PointToHash(ret, subIndex)

}

func GetTreeKeyAccountLeaf(address []byte, leaf byte) []byte {
	return GetTreeKey(address, zero, leaf)
}

func GetTreeKeyVersion(address []byte) []byte {
	return GetTreeKey(address, zero, VersionLeafKey)
}

func GetTreeKeyBalance(address []byte) []byte {
	return GetTreeKey(address, zero, BalanceLeafKey)
}

func GetTreeKeyNonce(address []byte) []byte {
	return GetTreeKey(address, zero, NonceLeafKey)
}

func GetTreeKeyCodeKeccak(address []byte) []byte {
	return GetTreeKey(address, zero, CodeKeccakLeafKey)
}

func GetTreeKeyCodeSize(address []byte) []byte {
	return GetTreeKey(address, zero, CodeSizeLeafKey)
}

func GetTreeKeyCodeChunk(address []byte, chunk *uint256.Int) []byte {
	chunkOffset := new(uint256.Int).Add(CodeOffset, chunk)
	treeIndex := new(uint256.Int).Div(chunkOffset, VerkleNodeWidth)
	subIndexMod := new(uint256.Int).Mod(chunkOffset, VerkleNodeWidth).Bytes()
	var subIndex byte
	if len(subIndexMod) != 0 {
		subIndex = subIndexMod[0]
	}
	return GetTreeKey(address, treeIndex, subIndex)
}

func GetTreeKeyStorageSlot(address []byte, storageKey *uint256.Int) []byte {
	pos := storageKey.Clone()
	if storageKey.Cmp(codeStorageDelta) < 0 {
		pos.Add(HeaderStorageOffset, storageKey)
	} else {
		pos.Add(MainStorageOffset, storageKey)
	}
	treeIndex := new(uint256.Int).Div(pos, VerkleNodeWidth)

	// calculate the sub_index, i.e. the index in the stem tree.
	// Because the modulus is 256, it's the last byte of treeIndex
	subIndexMod := new(uint256.Int).Mod(pos, VerkleNodeWidth).Bytes()
	var subIndex byte
	if len(subIndexMod) != 0 {
		// uint256 is broken into 4 little-endian quads,
		// each with native endianness. Extract the least
		// significant byte.
		subIndex = subIndexMod[0] & 0xFF
	}
	return GetTreeKey(address, treeIndex, subIndex)
}

func PointToHash(evaluated *verkle.Point, suffix byte) []byte {
	// The output of Byte() is big engian for banderwagon. This
	// introduces an imbalance in the tree, because hashes are
	// elements of a 253-bit field. This means more than half the
	// tree would be empty. To avoid this problem, use a little
	// endian commitment and chop the MSB.
	retb := evaluated.Bytes()
	for i := 0; i < 16; i++ {
		retb[31-i], retb[i] = retb[i], retb[31-i]
	}
	retb[31] = suffix
	return retb[:]
}

const (
	PUSH1  = byte(0x60)
	PUSH3  = byte(0x62)
	PUSH4  = byte(0x63)
	PUSH7  = byte(0x66)
	PUSH21 = byte(0x74)
	PUSH30 = byte(0x7d)
	PUSH32 = byte(0x7f)
)

// ChunkifyCode generates the chunked version of an array representing EVM bytecode
func ChunkifyCode(code []byte) []byte {
	var (
		chunkOffset = 0 // offset in the chunk
		chunkCount  = len(code) / 31
		codeOffset  = 0 // offset in the code
	)
	if len(code)%31 != 0 {
		chunkCount++
	}
	chunks := make([]byte, chunkCount*32)
	for i := 0; i < chunkCount; i++ {
		// number of bytes to copy, 31 unless
		// the end of the code has been reached.
		end := 31 * (i + 1)
		if len(code) < end {
			end = len(code)
		}

		// Copy the code itself
		copy(chunks[i*32+1:], code[31*i:end])

		// chunk offset = taken from the
		// last chunk.
		if chunkOffset > 31 {
			// skip offset calculation if push
			// data covers the whole chunk
			chunks[i*32] = 31
			chunkOffset = 1
			continue
		}
		chunks[32*i] = byte(chunkOffset)
		chunkOffset = 0

		// Check each instruction and update the offset
		// it should be 0 unless a PUSHn overflows.
		for ; codeOffset < end; codeOffset++ {
			if code[codeOffset] >= PUSH1 && code[codeOffset] <= PUSH32 {
				codeOffset += int(code[codeOffset] - PUSH1 + 1)
				if codeOffset+1 >= 31*(i+1) {
					codeOffset++
					chunkOffset = codeOffset - 31*(i+1)
					break
				}
			}
		}
	}

	return chunks
}
