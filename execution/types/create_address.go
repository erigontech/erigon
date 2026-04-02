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
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/length"
	"github.com/erigontech/erigon/execution/rlp"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// CreateAddress creates an ethereum address given the bytes and the nonce
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func CreateAddress(a common.Address, nonce uint64) common.Address {
	listLen := 21 + rlp.U64Len(nonce)
	data := make([]byte, listLen+1)
	pos := rlp.EncodeListPrefix(listLen, data)
	av := a
	pos += rlp.EncodeAddress(av[:], data[pos:])
	rlp.EncodeU64(nonce, data[pos:])
	h := crypto.HashData(data)
	return common.Address(h[12:])
}

// CreateAddress2 creates an ethereum address given the address bytes, initial
// contract code hash and a salt.
// DESCRIBED: docs/programmers_guide/guide.md#address---identifier-of-an-account
func CreateAddress2(b common.Address, salt [32]byte, inithash accounts.CodeHash) common.Address {
	// 0xff | address (20) | salt (32) | inithash (32) = 85 bytes, fixed size → stack array
	var buf [1 + length.Addr + length.Hash + length.Hash]byte
	buf[0] = 0xff
	copy(buf[1:], b[:])
	copy(buf[1+length.Addr:], salt[:])
	initHashValue := inithash.Value()
	copy(buf[1+length.Addr+length.Hash:], initHashValue[:])
	h := crypto.HashData(buf[:])
	return common.Address(h[12:])
}
