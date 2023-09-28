/*
   Copyright 2021 Erigon contributors
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
       http://www.apache.org/licenses/LICENSE-2.0
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package gointerfaces

import (
	"encoding/binary"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/types"
)

func ConvertH2048ToBloom(h2048 *types.H2048) [256]byte {
	var bloom [256]byte
	copy(bloom[:], ConvertH512ToBytes(h2048.Hi.Hi))
	copy(bloom[64:], ConvertH512ToBytes(h2048.Hi.Lo))
	copy(bloom[128:], ConvertH512ToBytes(h2048.Lo.Hi))
	copy(bloom[192:], ConvertH512ToBytes(h2048.Lo.Lo))
	return bloom
}

func ConvertBytesToH2048(data []byte) *types.H2048 {
	return &types.H2048{
		Hi: &types.H1024{
			Hi: ConvertBytesToH512(data),
			Lo: ConvertBytesToH512(data[64:]),
		},
		Lo: &types.H1024{
			Hi: ConvertBytesToH512(data[128:]),
			Lo: ConvertBytesToH512(data[192:]),
		},
	}
}

func ConvertH256ToHash(h256 *types.H256) [32]byte {
	var hash [32]byte
	binary.BigEndian.PutUint64(hash[0:], h256.Hi.Hi)
	binary.BigEndian.PutUint64(hash[8:], h256.Hi.Lo)
	binary.BigEndian.PutUint64(hash[16:], h256.Lo.Hi)
	binary.BigEndian.PutUint64(hash[24:], h256.Lo.Lo)
	return hash
}

func ConvertH512ToHash(h512 *types.H512) [64]byte {
	var b [64]byte
	binary.BigEndian.PutUint64(b[0:], h512.Hi.Hi.Hi)
	binary.BigEndian.PutUint64(b[8:], h512.Hi.Hi.Lo)
	binary.BigEndian.PutUint64(b[16:], h512.Hi.Lo.Hi)
	binary.BigEndian.PutUint64(b[24:], h512.Hi.Lo.Lo)
	binary.BigEndian.PutUint64(b[32:], h512.Lo.Hi.Hi)
	binary.BigEndian.PutUint64(b[40:], h512.Lo.Hi.Lo)
	binary.BigEndian.PutUint64(b[48:], h512.Lo.Lo.Hi)
	binary.BigEndian.PutUint64(b[56:], h512.Lo.Lo.Lo)
	return b
}

func ConvertHashesToH256(hashes [][32]byte) []*types.H256 {
	res := make([]*types.H256, len(hashes))
	for i := range hashes {
		res[i] = ConvertHashToH256(hashes[i])
	}
	return res
}

func ConvertHashToH256(hash [32]byte) *types.H256 {
	return &types.H256{
		Lo: &types.H128{Lo: binary.BigEndian.Uint64(hash[24:]), Hi: binary.BigEndian.Uint64(hash[16:])},
		Hi: &types.H128{Lo: binary.BigEndian.Uint64(hash[8:]), Hi: binary.BigEndian.Uint64(hash[0:])},
	}
}

func ConvertHashToH512(hash [64]byte) *types.H512 {
	return ConvertBytesToH512(hash[:])
}

func ConvertH160toAddress(h160 *types.H160) [20]byte {
	var addr [20]byte
	binary.BigEndian.PutUint64(addr[0:], h160.Hi.Hi)
	binary.BigEndian.PutUint64(addr[8:], h160.Hi.Lo)
	binary.BigEndian.PutUint32(addr[16:], h160.Lo)
	return addr
}

func ConvertAddressToH160(addr [20]byte) *types.H160 {
	return &types.H160{
		Lo: binary.BigEndian.Uint32(addr[16:]),
		Hi: &types.H128{Lo: binary.BigEndian.Uint64(addr[8:]), Hi: binary.BigEndian.Uint64(addr[0:])},
	}
}

func ConvertH256ToUint256Int(h256 *types.H256) *uint256.Int {
	// Note: uint256.Int is an array of 4 uint64 in little-endian order, i.e. most significant word is [3]
	var i uint256.Int
	i[3] = h256.Hi.Hi
	i[2] = h256.Hi.Lo
	i[1] = h256.Lo.Hi
	i[0] = h256.Lo.Lo
	return &i
}

func ConvertUint256IntToH256(i *uint256.Int) *types.H256 {
	// Note: uint256.Int is an array of 4 uint64 in little-endian order, i.e. most significant word is [3]
	return &types.H256{
		Lo: &types.H128{Lo: i[0], Hi: i[1]},
		Hi: &types.H128{Lo: i[2], Hi: i[3]},
	}
}

func ConvertH512ToBytes(h512 *types.H512) []byte {
	b := ConvertH512ToHash(h512)
	return b[:]
}

func ConvertBytesToH512(b []byte) *types.H512 {
	if len(b) < 64 {
		var b1 [64]byte
		copy(b1[:], b)
		b = b1[:]
	}
	return &types.H512{
		Lo: &types.H256{
			Lo: &types.H128{Lo: binary.BigEndian.Uint64(b[56:]), Hi: binary.BigEndian.Uint64(b[48:])},
			Hi: &types.H128{Lo: binary.BigEndian.Uint64(b[40:]), Hi: binary.BigEndian.Uint64(b[32:])},
		},
		Hi: &types.H256{
			Lo: &types.H128{Lo: binary.BigEndian.Uint64(b[24:]), Hi: binary.BigEndian.Uint64(b[16:])},
			Hi: &types.H128{Lo: binary.BigEndian.Uint64(b[8:]), Hi: binary.BigEndian.Uint64(b[0:])},
		},
	}
}
