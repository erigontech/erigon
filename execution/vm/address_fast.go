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

package vm

import (
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/accounts"
)

var (
	// Frequently used precompile/zero addresses in the form 0x..00XX.
	oneByteInternedAddresses [256]accounts.Address
	// P256 verify precompile address 0x..0100.
	internedAddress0100 accounts.Address
)

func init() {
	for i := range oneByteInternedAddresses {
		var addr common.Address
		addr[19] = byte(i)
		oneByteInternedAddresses[i] = accounts.InternAddress(addr)
	}

	var p256VerifyAddr common.Address
	p256VerifyAddr[18] = 0x01
	p256VerifyAddr[19] = 0x00
	internedAddress0100 = accounts.InternAddress(p256VerifyAddr)
}

func internAddressFromWord(word *uint256.Int) accounts.Address {
	// Most hot CALL paths target precompiles with very small addresses.
	if word[1]|word[2]|word[3] == 0 {
		low := word[0]
		if low <= 0xff {
			return oneByteInternedAddresses[byte(low)]
		}
		if low == 0x0100 {
			return internedAddress0100
		}
	}
	return accounts.InternAddress(word.Bytes20())
}
