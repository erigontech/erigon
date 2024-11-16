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

package types

import (
	"fmt"
	"math/bits"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/length"
)

// TODO move to erigon/core/types/accounts once erigon-lib is further unpacked into erigon/
func DecodeAccountBytesV3(enc []byte) (nonce uint64, balance *uint256.Int, hash []byte) {
	if len(enc) == 0 {
		return
	}
	pos := 0
	nonceBytes := int(enc[pos])
	balance = &uint256.Int{}
	pos++
	if nonceBytes > 0 {
		nonce = common.BytesToUint64(enc[pos : pos+nonceBytes])
		pos += nonceBytes
	}
	balanceBytes := int(enc[pos])
	pos++
	if balanceBytes > 0 {
		balance.SetBytes(enc[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	codeHashBytes := int(enc[pos])
	pos++
	if codeHashBytes == length.Hash {
		hash = make([]byte, codeHashBytes)
		copy(hash, enc[pos:pos+codeHashBytes])
		pos += codeHashBytes
	}
	if pos >= len(enc) {
		panic(fmt.Errorf("deserialse2: %d >= %d ", pos, len(enc)))
	}
	return
}

// TODO move to erigon/core/types/accounts once erigon-lib is further unpacked into erigon/
func EncodeAccountBytesV3(nonce uint64, balance *uint256.Int, hash []byte, incarnation uint64) []byte {
	l := 1
	if nonce > 0 {
		l += common.BitLenToByteLen(bits.Len64(nonce))
	}
	l++
	if !balance.IsZero() {
		l += balance.ByteLen()
	}
	l++
	if len(hash) == length.Hash {
		l += 32
	}
	l++
	if incarnation > 0 {
		l += common.BitLenToByteLen(bits.Len64(incarnation))
	}
	value := make([]byte, l)
	pos := 0

	if nonce == 0 {
		value[pos] = 0
		pos++
	} else {
		nonceBytes := common.BitLenToByteLen(bits.Len64(nonce))
		value[pos] = byte(nonceBytes)
		var nonce = nonce
		for i := nonceBytes; i > 0; i-- {
			value[pos+i] = byte(nonce)
			nonce >>= 8
		}
		pos += nonceBytes + 1
	}
	if balance.IsZero() {
		value[pos] = 0
		pos++
	} else {
		balanceBytes := balance.ByteLen()
		value[pos] = byte(balanceBytes)
		pos++
		balance.WriteToSlice(value[pos : pos+balanceBytes])
		pos += balanceBytes
	}
	if len(hash) == 0 {
		value[pos] = 0
		pos++
	} else {
		value[pos] = 32
		pos++
		copy(value[pos:pos+32], hash)
		pos += 32
	}
	if incarnation == 0 {
		value[pos] = 0
	} else {
		incBytes := common.BitLenToByteLen(bits.Len64(incarnation))
		value[pos] = byte(incBytes)
		var inc = incarnation
		for i := incBytes; i > 0; i-- {
			value[pos+i] = byte(inc)
			inc >>= 8
		}
	}
	return value
}
