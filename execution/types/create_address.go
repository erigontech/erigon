// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon/execution/rlp"
)

// CreateAddress creates an ethereum address given the bytes and the nonce
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func CreateAddress(a common.Address, nonce uint64) common.Address {
	listLen := 21 + rlp.U64Len(nonce)
	data := make([]byte, listLen+1)
	pos := rlp.EncodeListPrefix(listLen, data)
	pos += rlp.EncodeAddress(a[:], data[pos:])
	rlp.EncodeU64(nonce, data[pos:])
	return common.BytesToAddress(crypto.Keccak256(data)[12:])
}

// CreateAddress2 creates an ethereum address given the address bytes, initial
// contract code hash and a salt.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func CreateAddress2(b common.Address, salt [32]byte, inithash []byte) common.Address {
	return common.BytesToAddress(crypto.Keccak256([]byte{0xff}, b.Bytes(), salt[:], inithash)[12:])
}
