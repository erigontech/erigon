// Copyright 2021 The Erigon Authors
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

package gointerfaces

import (
	"encoding/binary"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/node/gointerfaces/typesproto"
)

func ConvertH2048ToBloom(h2048 *typesproto.H2048) [256]byte {
	var bloom [256]byte
	copy(bloom[:], ConvertH512ToBytes(h2048.Hi.Hi))
	copy(bloom[64:], ConvertH512ToBytes(h2048.Hi.Lo))
	copy(bloom[128:], ConvertH512ToBytes(h2048.Lo.Hi))
	copy(bloom[192:], ConvertH512ToBytes(h2048.Lo.Lo))
	return bloom
}

// h2048Arena holds an H2048 and all nested messages in one allocation.
type h2048Arena struct {
	h  typesproto.H2048
	hi typesproto.H1024
	lo typesproto.H1024
	s0 h512Arena
	s1 h512Arena
	s2 h512Arena
	s3 h512Arena
}

func ConvertBytesToH2048(data []byte) *typesproto.H2048 {
	if len(data) < 256 {
		var d [256]byte
		copy(d[:], data)
		data = d[:]
	}
	a := &h2048Arena{}
	fillH512Arena(&a.s0, data[0:])
	fillH512Arena(&a.s1, data[64:])
	fillH512Arena(&a.s2, data[128:])
	fillH512Arena(&a.s3, data[192:])
	a.hi = typesproto.H1024{Hi: &a.s0.h, Lo: &a.s1.h}
	a.lo = typesproto.H1024{Hi: &a.s2.h, Lo: &a.s3.h}
	a.h = typesproto.H2048{Hi: &a.hi, Lo: &a.lo}
	return &a.h
}

func ConvertH256ToHash(h256 *typesproto.H256) [32]byte {
	var hash [32]byte
	binary.BigEndian.PutUint64(hash[0:], h256.Hi.Hi)
	binary.BigEndian.PutUint64(hash[8:], h256.Hi.Lo)
	binary.BigEndian.PutUint64(hash[16:], h256.Lo.Hi)
	binary.BigEndian.PutUint64(hash[24:], h256.Lo.Lo)
	return hash
}

func ConvertH512ToHash(h512 *typesproto.H512) [64]byte {
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

// h256Arena holds an H256 and all nested messages in one allocation.
type h256Arena struct {
	h  typesproto.H256
	hi typesproto.H128
	lo typesproto.H128
}

func ConvertHashToH256(hash [32]byte) *typesproto.H256 {
	a := &h256Arena{}
	a.hi = h128(hash[0:])
	a.lo = h128(hash[16:])
	a.h = typesproto.H256{Hi: &a.hi, Lo: &a.lo}
	return &a.h
}

func ConvertHashToH512(hash [64]byte) *typesproto.H512 {
	return ConvertBytesToH512(hash[:])
}

func ConvertH160toAddress(h160 *typesproto.H160) [20]byte {
	var addr [20]byte
	binary.BigEndian.PutUint64(addr[0:], h160.Hi.Hi)
	binary.BigEndian.PutUint64(addr[8:], h160.Hi.Lo)
	binary.BigEndian.PutUint32(addr[16:], h160.Lo)
	return addr
}

func ConvertAddressToH160(addr [20]byte) *typesproto.H160 {
	return &typesproto.H160{
		Lo: binary.BigEndian.Uint32(addr[16:]),
		Hi: &typesproto.H128{Lo: binary.BigEndian.Uint64(addr[8:]), Hi: binary.BigEndian.Uint64(addr[0:])},
	}
}

func ConvertH256ToUint256Int(h256 *typesproto.H256) *uint256.Int {
	// Note: uint256.Int is an array of 4 uint64 in little-endian order, i.e. most significant word is [3]
	var i uint256.Int
	i[3] = h256.Hi.Hi
	i[2] = h256.Hi.Lo
	i[1] = h256.Lo.Hi
	i[0] = h256.Lo.Lo
	return &i
}

func ConvertUint256IntToH256(i *uint256.Int) *typesproto.H256 {
	if i == nil {
		return nil
	}
	// Note: uint256.Int is an array of 4 uint64 in little-endian order, i.e. most significant word is [3]
	a := &h256Arena{}
	a.lo = typesproto.H128{Lo: i[0], Hi: i[1]}
	a.hi = typesproto.H128{Lo: i[2], Hi: i[3]}
	a.h = typesproto.H256{Lo: &a.lo, Hi: &a.hi}
	return &a.h
}

func ConvertH512ToBytes(h512 *typesproto.H512) []byte {
	b := ConvertH512ToHash(h512)
	return b[:]
}

// h512Arena holds an H512 and all nested messages in one allocation.
type h512Arena struct {
	h    typesproto.H512
	hi   typesproto.H256
	lo   typesproto.H256
	hihi typesproto.H128
	hilo typesproto.H128
	lohi typesproto.H128
	lolo typesproto.H128
}

// h128 reads 16 bytes from b and returns an H128.
func h128(b []byte) typesproto.H128 {
	return typesproto.H128{Hi: binary.BigEndian.Uint64(b), Lo: binary.BigEndian.Uint64(b[8:])}
}

func fillH512Arena(a *h512Arena, b []byte) {
	a.hihi = h128(b[0:])
	a.hilo = h128(b[16:])
	a.lohi = h128(b[32:])
	a.lolo = h128(b[48:])
	a.hi = typesproto.H256{Hi: &a.hihi, Lo: &a.hilo}
	a.lo = typesproto.H256{Hi: &a.lohi, Lo: &a.lolo}
	a.h = typesproto.H512{Hi: &a.hi, Lo: &a.lo}
}

func ConvertBytesToH512(b []byte) *typesproto.H512 {
	if len(b) < 64 {
		var b1 [64]byte
		copy(b1[:], b)
		b = b1[:]
	}
	a := &h512Arena{}
	fillH512Arena(a, b)
	return &a.h
}
