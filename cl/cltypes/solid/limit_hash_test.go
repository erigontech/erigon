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

package solid_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/merkle_tree"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/hexutil"
)

// The expected roots below are taken from the ethereum/ssz-specs conformance
// vectors. Every limit spans a chunk count that is not a power of two.

func byteRange(from, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(from + i)
	}
	return out
}

func uint64Repdigits(n int) []uint64 {
	out := make([]uint64, n)
	for i := range out {
		out[i] = uint64(i+1) * 0x1111111111111111
	}
	return out
}

func emptyListRoot(chunkDepth int) common.Hash {
	lengthRoot := merkle_tree.Uint64Root(0)
	return crypto.Sha256(merkle_tree.ZeroHashes[chunkDepth][:], lengthRoot[:])
}

func TestHashSSZNonPowerOfTwoChunkLimit(t *testing.T) {
	byteList96Root := common.HexToHash("0xb5045df293fec01fe688a936c0ab48da1b447a475b6f993005db9f5e3c4284d6")
	uint64List12Root := common.HexToHash("0x69211564323726154ae7c7117d81ac979e10f99003c5f841d3c8c8a34590cccf")

	byteList := solid.NewByteListSSZ(96)
	require.NoError(t, byteList.SetBytes(byteRange(0xa0, 96)))

	bitVector := solid.NewBitVector(513)
	require.NoError(t, bitVector.DecodeSSZ(hexutil.MustDecode("0x4992244992244992244992244992244992244992244992244992244992244992244992244992244992244992244992244992244992244992244992244992244900"), 0))

	bitList := solid.BitlistFromBytes(hexutil.MustDecode("0x499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499224499201"), 640)

	cases := []struct {
		name string
		obj  interface{ HashSSZ() ([32]byte, error) }
		want common.Hash
	}{
		{"ByteList[96]", byteList, byteList96Root},
		{"ParticipationBitList[96]", solid.ParticipationBitListFromBytes(byteRange(0xa0, 96), 96), byteList96Root},
		{"empty ParticipationBitList[96]", solid.NewParticipationBitList(0, 96), emptyListRoot(2)},
		{"Uint64ListSSZ[12]", solid.NewUint64ListSSZFromSlice(12, uint64Repdigits(12)), uint64List12Root},
		{"RawUint64List[12]", solid.NewRawUint64List(12, uint64Repdigits(12)), uint64List12Root},
		{"empty RawUint64List[12]", solid.NewRawUint64List(12, nil), emptyListRoot(2)},
		{"BitList[640]", bitList, common.HexToHash("0x436bf50336b8571c4999431b1855ad9e0cccb68b8aa6222a0d18bf50b0d4126a")},
		{"BitVector[513]", bitVector, common.HexToHash("0x7659afbf9cde88d7ea67851713acb4d1ddb1cc607dd48fb29894bdbb59585f48")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root, err := c.obj.HashSSZ()
			require.NoError(t, err)
			require.Equal(t, c.want, common.Hash(root))
		})
	}
}

func TestElementProofNonPowerOfTwoLimit(t *testing.T) {
	for _, limit := range []int{3, 5} {
		list := solid.NewStaticListSSZ[*solid.Checkpoint](limit, 40)
		for i := range 3 {
			list.Append(&solid.Checkpoint{Epoch: uint64(i + 1)})
		}
		listRoot, err := list.HashSSZ()
		require.NoError(t, err)
		for index := range list.Len() {
			leaf, err := list.Get(index).HashSSZ()
			require.NoError(t, err)
			branch := list.ElementProof(index)
			node := leaf
			for depth, sibling := range branch {
				if depth == len(branch)-1 || index>>depth&1 == 0 {
					node = crypto.Sha256(node[:], sibling[:])
				} else {
					node = crypto.Sha256(sibling[:], node[:])
				}
			}
			require.Equalf(t, common.Hash(listRoot), common.Hash(node), "limit %d index %d", limit, index)
		}
	}
}
