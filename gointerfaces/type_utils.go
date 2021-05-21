package gointerfaces

import (
	"encoding/binary"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/gointerfaces/types"
)

func ConvertH256ToHash(h256 *types.H256) common.Hash {
	var hash common.Hash
	binary.BigEndian.PutUint64(hash[0:], h256.Hi.Hi)
	binary.BigEndian.PutUint64(hash[8:], h256.Hi.Lo)
	binary.BigEndian.PutUint64(hash[16:], h256.Lo.Hi)
	binary.BigEndian.PutUint64(hash[24:], h256.Lo.Lo)
	return hash
}

func ConvertHashesToH256(hashes common.Hashes) []*types.H256 {
	res := make([]*types.H256, len(hashes))
	for i := range hashes {
		res[i] = ConvertHashToH256(hashes[i])
	}
	return res
}

func ConvertHashToH256(hash common.Hash) *types.H256 {
	return &types.H256{
		Lo: &types.H128{Lo: binary.BigEndian.Uint64(hash[24:]), Hi: binary.BigEndian.Uint64(hash[16:])},
		Hi: &types.H128{Lo: binary.BigEndian.Uint64(hash[8:]), Hi: binary.BigEndian.Uint64(hash[0:])},
	}
}

func ConvertH160toAddress(h160 *types.H160) common.Address {
	var addr common.Address
	binary.BigEndian.PutUint64(addr[0:], h160.Hi.Hi)
	binary.BigEndian.PutUint64(addr[8:], h160.Hi.Lo)
	binary.BigEndian.PutUint32(addr[16:], h160.Lo)
	return addr
}

func ConvertAddressToH160(addr common.Address) *types.H160 {
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
	var b [64]byte
	binary.BigEndian.PutUint64(b[0:], h512.Hi.Hi.Hi)
	binary.BigEndian.PutUint64(b[8:], h512.Hi.Hi.Lo)
	binary.BigEndian.PutUint64(b[16:], h512.Hi.Lo.Hi)
	binary.BigEndian.PutUint64(b[24:], h512.Hi.Lo.Lo)
	binary.BigEndian.PutUint64(b[32:], h512.Lo.Hi.Hi)
	binary.BigEndian.PutUint64(b[40:], h512.Lo.Hi.Lo)
	binary.BigEndian.PutUint64(b[48:], h512.Lo.Lo.Hi)
	binary.BigEndian.PutUint64(b[56:], h512.Lo.Lo.Lo)
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
