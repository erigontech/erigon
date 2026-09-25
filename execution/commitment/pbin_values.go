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

// pbinValueLength is the one leaf value size EIP-8297 admits (eip:"Tree structure").
const pbinValueLength = 32

// BASIC_DATA field offsets within the leaf value (eip:"Header values"). Byte 0 (version)
// and the reserved bytes 1..3 stay zero.
const (
	pbinBasicDataCodeSizeOffset = 4
	pbinBasicDataNonceOffset    = 8
	pbinBasicDataBalanceOffset  = 16
)

var (
	errPBinBalanceOverflow  = errors.New("pbin: balance does not fit the 16-byte BASIC_DATA field")
	errPBinCodeSizeOverflow = errors.New("pbin: code size does not fit the 4-byte BASIC_DATA field")
	errPBinLeafValue        = errors.New("pbin: invalid leaf value")
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
// (eip:"Header values").
func pbinCodeHashValue(codeHash common.Hash) [pbinValueLength]byte {
	if codeHash == (common.Hash{}) {
		return empty.CodeHash
	}
	return codeHash
}

// pbinIsEmptyCodeHash reads both spellings of a codeless account: the
// empty-bytecode hash, and the unset hash a state read leaves behind.
func pbinIsEmptyCodeHash(codeHash common.Hash) bool {
	return codeHash == (common.Hash{}) || codeHash == empty.CodeHash
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

func pbinEncodeLeafValue(treeKey []byte, val *[pbinValueLength]byte) ([]byte, error) {
	if len(treeKey) == 0 {
		return nil, fmt.Errorf("%w: empty tree key", errPBinLeafValue)
	}
	zone := treeKey[0]
	want, known := pbinZoneKeyLength(zone)
	if !known || len(treeKey) != want {
		return nil, fmt.Errorf("%w: tree key %#x has invalid zone or length", errPBinLeafValue, treeKey)
	}

	switch zone {
	case pbinStorageZone:
		first := 0
		for first < pbinValueLength && val[first] == 0 {
			first++
		}
		if first == pbinValueLength {
			return nil, fmt.Errorf("%w: zero storage value", errPBinLeafValue)
		}
		return append([]byte(nil), val[first:]...), nil
	case pbinCodeZone:
		last := pbinValueLength
		for last > 0 && val[last-1] == 0 {
			last--
		}
		if last == 0 {
			return nil, fmt.Errorf("%w: zero code chunk", errPBinLeafValue)
		}
		return append([]byte(nil), val[:last]...), nil
	case pbinAccountZone:
	default:
		return nil, fmt.Errorf("%w: zone %#x names no leaf", errPBinLeafValue, zone)
	}

	switch subIndex := treeKey[len(treeKey)-1]; {
	case subIndex == pbinBasicDataLeafKey:
		if val[0] != 0 || val[1] != 0 || val[2] != 0 || val[3] != 0 {
			return nil, fmt.Errorf("%w: BASIC_DATA version or reserved bytes are non-zero", errPBinLeafValue)
		}
		codeSizeLen := 4
		for codeSizeLen > 0 && val[pbinBasicDataCodeSizeOffset+4-codeSizeLen] == 0 {
			codeSizeLen--
		}
		nonceLen := 8
		for nonceLen > 0 && val[pbinBasicDataNonceOffset+8-nonceLen] == 0 {
			nonceLen--
		}
		balanceLen := 16
		for balanceLen > 0 && val[pbinBasicDataBalanceOffset+16-balanceLen] == 0 {
			balanceLen--
		}
		widths := uint16(codeSizeLen)<<9 | uint16(nonceLen)<<5 | uint16(balanceLen)
		enc := make([]byte, 0, 2+codeSizeLen+nonceLen+balanceLen)
		enc = binary.BigEndian.AppendUint16(enc, widths)
		enc = append(enc, val[pbinBasicDataCodeSizeOffset+4-codeSizeLen:pbinBasicDataCodeSizeOffset+4]...)
		enc = append(enc, val[pbinBasicDataNonceOffset+8-nonceLen:pbinBasicDataNonceOffset+8]...)
		enc = append(enc, val[pbinBasicDataBalanceOffset+16-balanceLen:pbinBasicDataBalanceOffset+16]...)
		return enc, nil
	case subIndex == pbinCodeHashLeafKey:
		if *val == [pbinValueLength]byte(empty.CodeHash) {
			return nil, nil
		}
		return append([]byte(nil), val[:]...), nil
	case subIndex == pbinDelegationLeafKey:
		if val[0] != pbinDelegationMarker[0] || val[1] != pbinDelegationMarker[1] || val[2] != pbinDelegationMarker[2] {
			return nil, fmt.Errorf("%w: DELEGATION marker is invalid", errPBinLeafValue)
		}
		for _, b := range val[23:] {
			if b != 0 {
				return nil, fmt.Errorf("%w: DELEGATION trailing bytes are non-zero", errPBinLeafValue)
			}
		}
		return append([]byte(nil), val[3:23]...), nil
	case subIndex >= pbinHeaderStorageOffset && subIndex < pbinHeaderStorageOffset+pbinHeaderStorageSlots:
		first := 0
		for first < pbinValueLength && val[first] == 0 {
			first++
		}
		if first == pbinValueLength {
			return nil, fmt.Errorf("%w: zero header storage value", errPBinLeafValue)
		}
		return append([]byte(nil), val[first:]...), nil
	default:
		return append([]byte(nil), val[:]...), nil
	}
}

func pbinDecodeLeafValue(treeKey []byte, enc []byte) ([pbinValueLength]byte, error) {
	var val [pbinValueLength]byte
	if len(treeKey) == 0 {
		return val, fmt.Errorf("%w: empty tree key", errPBinLeafValue)
	}
	zone := treeKey[0]
	want, known := pbinZoneKeyLength(zone)
	if !known || len(treeKey) != want {
		return val, fmt.Errorf("%w: tree key %#x has invalid zone or length", errPBinLeafValue, treeKey)
	}

	switch zone {
	case pbinStorageZone:
		if len(enc) == 0 || len(enc) > pbinValueLength || enc[0] == 0 {
			return val, fmt.Errorf("%w: storage value has invalid length", errPBinLeafValue)
		}
		copy(val[pbinValueLength-len(enc):], enc)
		return val, nil
	case pbinCodeZone:
		if len(enc) == 0 || len(enc) > pbinValueLength || enc[len(enc)-1] == 0 {
			return val, fmt.Errorf("%w: code chunk has invalid length", errPBinLeafValue)
		}
		copy(val[:], enc)
		return val, nil
	case pbinAccountZone:
	default:
		return val, fmt.Errorf("%w: zone %#x names no leaf", errPBinLeafValue, zone)
	}

	switch subIndex := treeKey[len(treeKey)-1]; {
	case subIndex == pbinBasicDataLeafKey:
		if len(enc) < 2 {
			return val, fmt.Errorf("%w: BASIC_DATA value is shorter than widths", errPBinLeafValue)
		}
		widths := binary.BigEndian.Uint16(enc[:2])
		if widths>>12 != 0 {
			return val, fmt.Errorf("%w: BASIC_DATA widths have reserved bits", errPBinLeafValue)
		}
		codeSizeLen := int((widths >> 9) & 0x7)
		nonceLen := int((widths >> 5) & 0xf)
		balanceLen := int(widths & 0x1f)
		if codeSizeLen > 4 || nonceLen > 8 || balanceLen > 16 {
			return val, fmt.Errorf("%w: BASIC_DATA field width exceeds its field", errPBinLeafValue)
		}
		if len(enc) != 2+codeSizeLen+nonceLen+balanceLen {
			return val, fmt.Errorf("%w: BASIC_DATA length does not match widths", errPBinLeafValue)
		}
		pos := 2
		if codeSizeLen > 0 {
			if enc[pos] == 0 {
				return val, fmt.Errorf("%w: BASIC_DATA code size is not minimal", errPBinLeafValue)
			}
			copy(val[pbinBasicDataCodeSizeOffset+4-codeSizeLen:pbinBasicDataCodeSizeOffset+4], enc[pos:pos+codeSizeLen])
			pos += codeSizeLen
		}
		if nonceLen > 0 {
			if enc[pos] == 0 {
				return val, fmt.Errorf("%w: BASIC_DATA nonce is not minimal", errPBinLeafValue)
			}
			copy(val[pbinBasicDataNonceOffset+8-nonceLen:pbinBasicDataNonceOffset+8], enc[pos:pos+nonceLen])
			pos += nonceLen
		}
		if balanceLen > 0 {
			if enc[pos] == 0 {
				return val, fmt.Errorf("%w: BASIC_DATA balance is not minimal", errPBinLeafValue)
			}
			copy(val[pbinBasicDataBalanceOffset+16-balanceLen:pbinBasicDataBalanceOffset+16], enc[pos:pos+balanceLen])
		}
		return val, nil
	case subIndex == pbinCodeHashLeafKey:
		switch len(enc) {
		case 0:
			return [pbinValueLength]byte(empty.CodeHash), nil
		case pbinValueLength:
			copy(val[:], enc)
			return val, nil
		default:
			return val, fmt.Errorf("%w: CODE_HASH value has length %d", errPBinLeafValue, len(enc))
		}
	case subIndex == pbinDelegationLeafKey:
		if len(enc) != 20 {
			return val, fmt.Errorf("%w: DELEGATION target has length %d", errPBinLeafValue, len(enc))
		}
		copy(val[:3], pbinDelegationMarker[:])
		copy(val[3:23], enc)
		return val, nil
	case subIndex >= pbinHeaderStorageOffset && subIndex < pbinHeaderStorageOffset+pbinHeaderStorageSlots:
		if len(enc) == 0 || len(enc) > pbinValueLength || enc[0] == 0 {
			return val, fmt.Errorf("%w: header storage value has invalid length", errPBinLeafValue)
		}
		copy(val[pbinValueLength-len(enc):], enc)
		return val, nil
	default:
		if len(enc) != pbinValueLength {
			return val, fmt.Errorf("%w: reserved account value has length %d", errPBinLeafValue, len(enc))
		}
		copy(val[:], enc)
		return val, nil
	}
}
