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

package eip8297

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/empty"
	"github.com/erigontech/erigon/common/length"
)

// ValueLength is the one leaf value size EIP-8297 admits (eip:"Tree structure").
const ValueLength = 32

// BASIC_DATA field offsets within the leaf value (eip:"Header values"). Byte 0 (version)
// and the reserved bytes 1..3 stay zero.
const (
	BasicDataCodeSizeOffset = 4
	BasicDataNonceOffset    = 8
	BasicDataBalanceOffset  = 16
)

var (
	ErrBalanceOverflow  = errors.New("pbin: balance does not fit the 16-byte BASIC_DATA field")
	ErrCodeSizeOverflow = errors.New("pbin: code size does not fit the 4-byte BASIC_DATA field")
	ErrLeafValue        = errors.New("pbin: invalid leaf value")
)

// EncodeBasicData packs code_size, nonce and balance big-endian into the
// BASIC_DATA leaf value. A value the field cannot hold is an error rather than a
// silent truncation, which would commit a wrong root.
func EncodeBasicData(nonce uint64, balance *uint256.Int, codeSize uint64) ([ValueLength]byte, error) {
	var v [ValueLength]byte
	if balance.BitLen() > 128 {
		return v, fmt.Errorf("%w: %s", ErrBalanceOverflow, balance)
	}
	if codeSize > 1<<32-1 {
		return v, fmt.Errorf("%w: %d", ErrCodeSizeOverflow, codeSize)
	}
	binary.BigEndian.PutUint32(v[BasicDataCodeSizeOffset:], uint32(codeSize))
	binary.BigEndian.PutUint64(v[BasicDataNonceOffset:], nonce)
	b32 := balance.Bytes32()
	copy(v[BasicDataBalanceOffset:], b32[16:])
	return v, nil
}

// CodeHashValue returns the CODE_HASH leaf value, mapping an unset hash to
// the empty-bytecode hash as the spec requires for a codeless account
// (eip:"Header values").
func CodeHashValue(codeHash common.Hash) [ValueLength]byte {
	if codeHash == (common.Hash{}) {
		return empty.CodeHash
	}
	return codeHash
}

// IsEmptyCodeHash reads both spellings of a codeless account: the
// empty-bytecode hash, and the unset hash a state read leaves behind.
func IsEmptyCodeHash(codeHash common.Hash) bool {
	return codeHash == (common.Hash{}) || codeHash == empty.CodeHash
}

// EIP-7702 delegation indicators (eip:"Delegation"). Classification reads the
// code bytes, never the hash — a code hash may begin with the marker too.
var DelegationMarker = [3]byte{0xEF, 0x01, 0x00}

const DelegationCodeLength = 23

func IsDelegation(code []byte) bool {
	return len(code) == DelegationCodeLength && [3]byte(code) == DelegationMarker
}

// EncodeDelegation right-pads the indicator into the DELEGATION leaf value.
// This is not the chunk encoding: an indicator never executes, so byte 0 holds
// code rather than a PUSHDATA count.
func EncodeDelegation(code []byte) [ValueLength]byte {
	if len(code) != DelegationCodeLength {
		panic(fmt.Sprintf("pbin: delegation indicator of %d bytes, want %d", len(code), DelegationCodeLength))
	}
	var v [ValueLength]byte
	copy(v[:], code)
	return v
}

func EncodeStorageValue(value []byte) [ValueLength]byte {
	if len(value) > length.Hash {
		panic(fmt.Sprintf("pbin: storage value of %d bytes exceeds %d", len(value), length.Hash))
	}
	var v [ValueLength]byte
	copy(v[ValueLength-len(value):], value)
	return v
}

func EncodeLeafValue(treeKey []byte, val *[ValueLength]byte) ([]byte, error) {
	if len(treeKey) == 0 {
		return nil, fmt.Errorf("%w: empty tree key", ErrLeafValue)
	}
	zone := treeKey[0]
	want, known := ZoneKeyLength(zone)
	if !known || len(treeKey) != want {
		return nil, fmt.Errorf("%w: tree key %#x has invalid zone or length", ErrLeafValue, treeKey)
	}

	switch zone {
	case StorageZone:
		first := 0
		for first < ValueLength && val[first] == 0 {
			first++
		}
		if first == ValueLength {
			return nil, fmt.Errorf("%w: zero storage value", ErrLeafValue)
		}
		return append([]byte(nil), val[first:]...), nil
	case CodeZone:
		last := ValueLength
		for last > 0 && val[last-1] == 0 {
			last--
		}
		if last == 0 {
			return nil, fmt.Errorf("%w: zero code chunk", ErrLeafValue)
		}
		return append([]byte(nil), val[:last]...), nil
	case AccountZone:
	default:
		return nil, fmt.Errorf("%w: zone %#x names no leaf", ErrLeafValue, zone)
	}

	switch subIndex := treeKey[len(treeKey)-1]; {
	case subIndex == BasicDataLeafKey:
		if val[0] != 0 || val[1] != 0 || val[2] != 0 || val[3] != 0 {
			return nil, fmt.Errorf("%w: BASIC_DATA version or reserved bytes are non-zero", ErrLeafValue)
		}
		codeSizeLen := 4
		for codeSizeLen > 0 && val[BasicDataCodeSizeOffset+4-codeSizeLen] == 0 {
			codeSizeLen--
		}
		nonceLen := 8
		for nonceLen > 0 && val[BasicDataNonceOffset+8-nonceLen] == 0 {
			nonceLen--
		}
		balanceLen := 16
		for balanceLen > 0 && val[BasicDataBalanceOffset+16-balanceLen] == 0 {
			balanceLen--
		}
		widths := uint16(codeSizeLen)<<9 | uint16(nonceLen)<<5 | uint16(balanceLen)
		enc := make([]byte, 0, 2+codeSizeLen+nonceLen+balanceLen)
		enc = binary.BigEndian.AppendUint16(enc, widths)
		enc = append(enc, val[BasicDataCodeSizeOffset+4-codeSizeLen:BasicDataCodeSizeOffset+4]...)
		enc = append(enc, val[BasicDataNonceOffset+8-nonceLen:BasicDataNonceOffset+8]...)
		enc = append(enc, val[BasicDataBalanceOffset+16-balanceLen:BasicDataBalanceOffset+16]...)
		return enc, nil
	case subIndex == CodeHashLeafKey:
		if *val == [ValueLength]byte(empty.CodeHash) {
			return nil, nil
		}
		return append([]byte(nil), val[:]...), nil
	case subIndex == DelegationLeafKey:
		if val[0] != DelegationMarker[0] || val[1] != DelegationMarker[1] || val[2] != DelegationMarker[2] {
			return nil, fmt.Errorf("%w: DELEGATION marker is invalid", ErrLeafValue)
		}
		for _, b := range val[23:] {
			if b != 0 {
				return nil, fmt.Errorf("%w: DELEGATION trailing bytes are non-zero", ErrLeafValue)
			}
		}
		return append([]byte(nil), val[3:23]...), nil
	case subIndex >= HeaderStorageOffset && subIndex < HeaderStorageOffset+HeaderStorageSlots:
		first := 0
		for first < ValueLength && val[first] == 0 {
			first++
		}
		if first == ValueLength {
			return nil, fmt.Errorf("%w: zero header storage value", ErrLeafValue)
		}
		return append([]byte(nil), val[first:]...), nil
	default:
		return append([]byte(nil), val[:]...), nil
	}
}

func DecodeLeafValue(treeKey []byte, enc []byte) ([ValueLength]byte, error) {
	var val [ValueLength]byte
	if len(treeKey) == 0 {
		return val, fmt.Errorf("%w: empty tree key", ErrLeafValue)
	}
	zone := treeKey[0]
	want, known := ZoneKeyLength(zone)
	if !known || len(treeKey) != want {
		return val, fmt.Errorf("%w: tree key %#x has invalid zone or length", ErrLeafValue, treeKey)
	}

	switch zone {
	case StorageZone:
		if len(enc) == 0 || len(enc) > ValueLength || enc[0] == 0 {
			return val, fmt.Errorf("%w: storage value has invalid length", ErrLeafValue)
		}
		copy(val[ValueLength-len(enc):], enc)
		return val, nil
	case CodeZone:
		if len(enc) == 0 || len(enc) > ValueLength || enc[len(enc)-1] == 0 {
			return val, fmt.Errorf("%w: code chunk has invalid length", ErrLeafValue)
		}
		copy(val[:], enc)
		return val, nil
	case AccountZone:
	default:
		return val, fmt.Errorf("%w: zone %#x names no leaf", ErrLeafValue, zone)
	}

	switch subIndex := treeKey[len(treeKey)-1]; {
	case subIndex == BasicDataLeafKey:
		if len(enc) < 2 {
			return val, fmt.Errorf("%w: BASIC_DATA value is shorter than widths", ErrLeafValue)
		}
		widths := binary.BigEndian.Uint16(enc[:2])
		if widths>>12 != 0 {
			return val, fmt.Errorf("%w: BASIC_DATA widths have reserved bits", ErrLeafValue)
		}
		codeSizeLen := int((widths >> 9) & 0x7)
		nonceLen := int((widths >> 5) & 0xf)
		balanceLen := int(widths & 0x1f)
		if codeSizeLen > 4 || nonceLen > 8 || balanceLen > 16 {
			return val, fmt.Errorf("%w: BASIC_DATA field width exceeds its field", ErrLeafValue)
		}
		if len(enc) != 2+codeSizeLen+nonceLen+balanceLen {
			return val, fmt.Errorf("%w: BASIC_DATA length does not match widths", ErrLeafValue)
		}
		pos := 2
		if codeSizeLen > 0 {
			if enc[pos] == 0 {
				return val, fmt.Errorf("%w: BASIC_DATA code size is not minimal", ErrLeafValue)
			}
			copy(val[BasicDataCodeSizeOffset+4-codeSizeLen:BasicDataCodeSizeOffset+4], enc[pos:pos+codeSizeLen])
			pos += codeSizeLen
		}
		if nonceLen > 0 {
			if enc[pos] == 0 {
				return val, fmt.Errorf("%w: BASIC_DATA nonce is not minimal", ErrLeafValue)
			}
			copy(val[BasicDataNonceOffset+8-nonceLen:BasicDataNonceOffset+8], enc[pos:pos+nonceLen])
			pos += nonceLen
		}
		if balanceLen > 0 {
			if enc[pos] == 0 {
				return val, fmt.Errorf("%w: BASIC_DATA balance is not minimal", ErrLeafValue)
			}
			copy(val[BasicDataBalanceOffset+16-balanceLen:BasicDataBalanceOffset+16], enc[pos:pos+balanceLen])
		}
		return val, nil
	case subIndex == CodeHashLeafKey:
		switch len(enc) {
		case 0:
			return [ValueLength]byte(empty.CodeHash), nil
		case ValueLength:
			copy(val[:], enc)
			return val, nil
		default:
			return val, fmt.Errorf("%w: CODE_HASH value has length %d", ErrLeafValue, len(enc))
		}
	case subIndex == DelegationLeafKey:
		if len(enc) != 20 {
			return val, fmt.Errorf("%w: DELEGATION target has length %d", ErrLeafValue, len(enc))
		}
		copy(val[:3], DelegationMarker[:])
		copy(val[3:23], enc)
		return val, nil
	case subIndex >= HeaderStorageOffset && subIndex < HeaderStorageOffset+HeaderStorageSlots:
		if len(enc) == 0 || len(enc) > ValueLength || enc[0] == 0 {
			return val, fmt.Errorf("%w: header storage value has invalid length", ErrLeafValue)
		}
		copy(val[ValueLength-len(enc):], enc)
		return val, nil
	default:
		if len(enc) != ValueLength {
			return val, fmt.Errorf("%w: reserved account value has length %d", ErrLeafValue, len(enc))
		}
		copy(val[:], enc)
		return val, nil
	}
}
