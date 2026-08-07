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

package commitment

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// pbinValueLength is the one leaf value size EIP-8297 admits (eip:132).
const pbinValueLength = 32

// BASIC_DATA field offsets within the leaf value (eip:332-339). Byte 0 (version)
// and the reserved bytes 1..3 stay zero.
const (
	pbinBasicDataCodeSizeOffset = 4
	pbinBasicDataNonceOffset    = 8
	pbinBasicDataBalanceOffset  = 16
)

var (
	errPBinBalanceOverflow  = errors.New("pbin: balance does not fit the 16-byte BASIC_DATA field")
	errPBinCodeSizeOverflow = errors.New("pbin: code size does not fit the 4-byte BASIC_DATA field")
)

// pbinEncodeBasicData packs code_size, nonce and balance big-endian into the
// BASIC_DATA leaf value. A value the field cannot hold is an error rather than a
// silent truncation, which would commit a wrong root.
func pbinEncodeBasicData(nonce uint64, balance *uint256.Int, codeSize uint64) ([pbinValueLength]byte, error) {
	var v [pbinValueLength]byte
	if balance.BitLen() > 128 {
		return v, fmt.Errorf("%w: %s", errPBinBalanceOverflow, balance)
	}
	if codeSize > 1<<32-1 {
		return v, fmt.Errorf("%w: %d", errPBinCodeSizeOverflow, codeSize)
	}
	binary.BigEndian.PutUint32(v[pbinBasicDataCodeSizeOffset:], uint32(codeSize))
	binary.BigEndian.PutUint64(v[pbinBasicDataNonceOffset:], nonce)
	b32 := balance.Bytes32()
	copy(v[pbinBasicDataBalanceOffset:], b32[16:])
	return v, nil
}

// pbinCodeHashValue returns the CODE_HASH leaf value, mapping an unset hash to
// the empty-bytecode hash as the spec requires for a codeless account
// (eip:345-347).
func pbinCodeHashValue(codeHash common.Hash) [pbinValueLength]byte {
	if codeHash == (common.Hash{}) {
		return empty.CodeHash
	}
	return codeHash
}

// EIP-7702 delegation indicators (eip:"Delegation"). Classification reads the
// code bytes, never the hash — a code hash may begin with the marker too.
var pbinDelegationMarker = [3]byte{0xEF, 0x01, 0x00}

const pbinDelegationCodeLength = 23

func pbinIsDelegation(code []byte) bool {
	return len(code) == pbinDelegationCodeLength && [3]byte(code) == pbinDelegationMarker
}

// pbinEncodeDelegation right-pads the indicator into the DELEGATION leaf value.
// This is not the chunk encoding: an indicator never executes, so byte 0 holds
// code rather than a PUSHDATA count.
func pbinEncodeDelegation(code []byte) [pbinValueLength]byte {
	if len(code) != pbinDelegationCodeLength {
		panic(fmt.Sprintf("pbin: delegation indicator of %d bytes, want %d", len(code), pbinDelegationCodeLength))
	}
	var v [pbinValueLength]byte
	copy(v[:], code)
	return v
}

func pbinEncodeStorageValue(value []byte) [pbinValueLength]byte {
	if len(value) > length.Hash {
		panic(fmt.Sprintf("pbin: storage value of %d bytes exceeds %d", len(value), length.Hash))
	}
	var v [pbinValueLength]byte
	copy(v[pbinValueLength-len(value):], value)
	return v
}
