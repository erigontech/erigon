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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	keccak "github.com/erigontech/fastkeccak"
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

	errZeroStorageValue         = fmt.Errorf("%w: zero storage value", ErrLeafValue)
	errZeroHeaderStorageValue   = fmt.Errorf("%w: zero header storage value", ErrLeafValue)
	errStorageValueLength       = fmt.Errorf("%w: storage value has invalid length", ErrLeafValue)
	errHeaderStorageValueLength = fmt.Errorf("%w: header storage value has invalid length", ErrLeafValue)
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

func AccountCode(address []byte, codeHash common.Hash, code []byte) ([]byte, error) {
	if IsEmptyCodeHash(codeHash) {
		return nil, nil
	}
	actual := common.Hash(keccak.Sum256(code))
	if actual != codeHash {
		return nil, fmt.Errorf("pbin: code hash mismatch for address %x: account %x, code %x", address, codeHash, actual)
	}
	return code, nil
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

var basicDataFields = [...]struct{ off, width int }{
	{BasicDataCodeSizeOffset, 4},
	{BasicDataNonceOffset, 8},
	{BasicDataBalanceOffset, 16},
}

var basicDataWidthShifts = [...]uint{9, 5, 0}

var basicDataFieldMinimalErrors = [...]error{
	fmt.Errorf("%w: BASIC_DATA code size is not minimal", ErrLeafValue),
	fmt.Errorf("%w: BASIC_DATA nonce is not minimal", ErrLeafValue),
	fmt.Errorf("%w: BASIC_DATA balance is not minimal", ErrLeafValue),
}

func leafValueKey(treeKey []byte) (byte, byte, error) {
	if len(treeKey) == 0 {
		return 0, 0, fmt.Errorf("%w: empty tree key", ErrLeafValue)
	}
	zone := treeKey[0]
	want, known := ZoneKeyLength(zone)
	if !known || len(treeKey) != want {
		return 0, 0, fmt.Errorf("%w: tree key %#x has invalid zone or length", ErrLeafValue, treeKey)
	}
	return zone, treeKey[len(treeKey)-1], nil
}

func encodeTrimmedValue(val *[ValueLength]byte, zeroErr error) ([]byte, error) {
	trimmed := bytes.TrimLeft(val[:], "\x00")
	if len(trimmed) == 0 {
		return nil, zeroErr
	}
	return append([]byte(nil), trimmed...), nil
}

func decodeTrimmedValue(enc []byte, lengthErr error) ([ValueLength]byte, error) {
	var val [ValueLength]byte
	if len(enc) == 0 || len(enc) > ValueLength || enc[0] == 0 {
		return val, lengthErr
	}
	copy(val[ValueLength-len(enc):], enc)
	return val, nil
}

func EncodeLeafValue(treeKey []byte, val *[ValueLength]byte) ([]byte, error) {
	zone, subIndex, err := leafValueKey(treeKey)
	if err != nil {
		return nil, err
	}

	switch zone {
	case StorageZone:
		return encodeTrimmedValue(val, errZeroStorageValue)
	case CodeZone:
		last := len(bytes.TrimRight(val[:], "\x00"))
		if last == 0 {
			return nil, fmt.Errorf("%w: zero code chunk", ErrLeafValue)
		}
		return append([]byte(nil), val[:last]...), nil
	case AccountZone:
	default:
		return nil, fmt.Errorf("%w: zone %#x names no leaf", ErrLeafValue, zone)
	}

	switch {
	case subIndex == BasicDataLeafKey:
		if [4]byte(val[:4]) != [4]byte{} {
			return nil, fmt.Errorf("%w: BASIC_DATA version or reserved bytes are non-zero", ErrLeafValue)
		}
		var fieldLens [len(basicDataFields)]int
		encodedLen := 2
		var widths uint16
		for i, field := range basicDataFields {
			fieldLen := len(bytes.TrimLeft(val[field.off:field.off+field.width], "\x00"))
			fieldLens[i] = fieldLen
			encodedLen += fieldLen
			widths |= uint16(fieldLen) << basicDataWidthShifts[i]
		}
		enc := make([]byte, 0, encodedLen)
		enc = binary.BigEndian.AppendUint16(enc, widths)
		for i, field := range basicDataFields {
			fieldLen := fieldLens[i]
			enc = append(enc, val[field.off+field.width-fieldLen:field.off+field.width]...)
		}
		return enc, nil
	case subIndex == CodeHashLeafKey:
		if *val == [ValueLength]byte(empty.CodeHash) {
			return nil, nil
		}
		return append([]byte(nil), val[:]...), nil
	case subIndex == DelegationLeafKey:
		if [3]byte(val[:3]) != DelegationMarker {
			return nil, fmt.Errorf("%w: DELEGATION marker is invalid", ErrLeafValue)
		}
		if [9]byte(val[23:]) != [9]byte{} {
			return nil, fmt.Errorf("%w: DELEGATION trailing bytes are non-zero", ErrLeafValue)
		}
		return append([]byte(nil), val[3:23]...), nil
	case subIndex >= HeaderStorageOffset && subIndex < HeaderStorageOffset+HeaderStorageSlots:
		return encodeTrimmedValue(val, errZeroHeaderStorageValue)
	default:
		return append([]byte(nil), val[:]...), nil
	}
}

func DecodeLeafValue(treeKey []byte, enc []byte) ([ValueLength]byte, error) {
	var val [ValueLength]byte
	zone, subIndex, err := leafValueKey(treeKey)
	if err != nil {
		return val, err
	}

	switch zone {
	case StorageZone:
		return decodeTrimmedValue(enc, errStorageValueLength)
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

	switch {
	case subIndex == BasicDataLeafKey:
		if len(enc) < 2 {
			return val, fmt.Errorf("%w: BASIC_DATA value is shorter than widths", ErrLeafValue)
		}
		widths := binary.BigEndian.Uint16(enc[:2])
		if widths>>12 != 0 {
			return val, fmt.Errorf("%w: BASIC_DATA widths have reserved bits", ErrLeafValue)
		}
		var fieldLens [len(basicDataFields)]int
		encodedLen := 2
		for i, field := range basicDataFields {
			fieldLen := int((widths >> basicDataWidthShifts[i]) & uint16(field.width*2-1))
			if fieldLen > field.width {
				return val, fmt.Errorf("%w: BASIC_DATA field width exceeds its field", ErrLeafValue)
			}
			fieldLens[i] = fieldLen
			encodedLen += fieldLen
		}
		if len(enc) != encodedLen {
			return val, fmt.Errorf("%w: BASIC_DATA length does not match widths", ErrLeafValue)
		}
		pos := 2
		for i, field := range basicDataFields {
			fieldLen := fieldLens[i]
			if fieldLen > 0 {
				if enc[pos] == 0 {
					return val, basicDataFieldMinimalErrors[i]
				}
				copy(val[field.off+field.width-fieldLen:field.off+field.width], enc[pos:pos+fieldLen])
				pos += fieldLen
			}
		}
		return val, nil
	case subIndex == CodeHashLeafKey:
		switch len(enc) {
		case 0:
			return [ValueLength]byte(empty.CodeHash), nil
		case ValueLength:
			copy(val[:], enc)
			if val == [ValueLength]byte(empty.CodeHash) {
				return [ValueLength]byte{}, fmt.Errorf("%w: compact value is not canonical", ErrLeafValue)
			}
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
		return decodeTrimmedValue(enc, errHeaderStorageValueLength)
	default:
		if len(enc) != ValueLength {
			return val, fmt.Errorf("%w: reserved account value has length %d", ErrLeafValue, len(enc))
		}
		copy(val[:], enc)
		return val, nil
	}
}
