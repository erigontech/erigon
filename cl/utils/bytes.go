// Copyright 2022 The Erigon Authors
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

package utils

import (
	"encoding/binary"
	"errors"
	"math/bits"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/erigontech/erigon-lib/types/ssz"

	"github.com/golang/snappy"
)

var IsSysLittleEndian bool

const maxDecodeLenAllowed = 15 * datasize.MB

func init() {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		IsSysLittleEndian = true
	case [2]byte{0xAB, 0xCD}:
		IsSysLittleEndian = false
	default:
		panic("Could not determine native endianness.")
	}
}

func Uint32ToBytes4(n uint32) (ret [4]byte) {
	binary.BigEndian.PutUint32(ret[:], n)
	return
}

func Bytes4ToUint32(bytes4 [4]byte) uint32 {
	return binary.BigEndian.Uint32(bytes4[:])
}

func BytesToBytes4(b []byte) (ret [4]byte) {
	copy(ret[:], b)
	return
}

func Uint64ToLE(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func DecompressSnappy(data []byte, lengthCheck bool) ([]byte, error) {
	// Decode the snappy
	lenDecoded, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, err
	}
	if lengthCheck && lenDecoded > int(maxDecodeLenAllowed) {
		return nil, errors.New("snappy: decoded length is too large")
	}
	decodedData := make([]byte, lenDecoded)

	return snappy.Decode(decodedData, data)
}

func CompressSnappy(data []byte) []byte {
	return snappy.Encode(nil, data)
}

func EncodeSSZSnappy(data ssz.Marshaler) ([]byte, error) {
	var (
		enc = make([]byte, 0, data.EncodingSizeSSZ())
		err error
	)
	enc, err = data.EncodeSSZ(enc)
	if err != nil {
		return nil, err
	}

	return snappy.Encode(nil, enc), nil
}

func DecodeSSZSnappy(dst ssz.Unmarshaler, src []byte, version int) error {
	dec, err := snappy.Decode(nil, src)
	if err != nil {
		return err
	}

	err = dst.DecodeSSZ(dec, version)
	if err != nil {
		return err
	}

	return nil
}

// getBitlistLength return the amount of bits in given bitlist.
func GetBitlistLength(b []byte) int {
	if len(b) == 0 {
		return 0
	}
	// The most significant bit is present in the last byte in the array.
	last := b[len(b)-1]

	// Determine the position of the most significant bit.
	msb := bits.Len8(last)
	if msb == 0 {
		return 0
	}

	// The absolute position of the most significant bit will be the number of
	// bits in the preceding bytes plus the position of the most significant
	// bit. Subtract this value by 1 to determine the length of the bitlist.
	return 8*(len(b)-1) + msb - 1
}

func ReverseOfByteSlice(b []byte) (out []byte) {
	out = make([]byte, len(b))
	for i := range b {
		out[i] = b[len(b)-1-i]
	}
	return
}

func FlipBitOn(b []byte, i int) {
	b[i/8] |= 1 << (i % 8)
}

func IsBitOn(b []byte, idx int) bool {
	i := uint8(1 << (idx % 8))
	return b[idx/8]&i == i
}

// IsNonStrictSupersetBitlist checks if bitlist 'a' is a non-strict superset of bitlist 'b'
func IsNonStrictSupersetBitlist(a, b []byte) bool {
	// Ensure 'a' is at least as long as 'b'
	if len(a) < len(b) {
		return false
	}

	// Check each bit in 'b' to ensure it is also set in 'a'
	for i := 0; i < len(b); i++ {
		if (a[i] & b[i]) != b[i] {
			return false
		}
	}

	// If all bits required by 'b' are present in 'a', return true
	return true
}

// IsOverlappingSSZBitlist checks if bitlist 'a' and bitlist 'b' have any overlapping bits
// However, it ignores the last bits in the last byte.
func IsOverlappingSSZBitlist(a, b []byte) bool {
	length := min(len(a), len(b))
	for i := range length {

		if a[i]&b[i] != 0 {
			if i != length-1 {
				return true
			}
			var foundOverlap bool
			// check the overlap bit by bit
			for j := 0; j < 8; j++ {
				if (a[i]>>j)&(b[i]>>j)&1 == 1 {
					if foundOverlap {
						return true
					}
					foundOverlap = true
				}
			}
		}
	}
	return false

}

// func IsOverlappingBitlist(a, b []byte) bool {
// 	length := min(len(a), len(b))
// 	for i := range length {
// 		if a[i]&b[i] != 0 {
// 			return true
// 		}
// 	}
// 	return false
// }

func BitsOnCount(b []byte) int {
	count := 0
	for _, v := range b {
		count += bits.OnesCount8(v)
	}
	return count
}

func MergeBitlists(a, b []byte) {
	for i := range b {
		a[i] |= b[i]
	}
}

func ExtractSlotFromSerializedBeaconState(beaconState []byte) (uint64, error) {
	if len(beaconState) < 48 {
		return 0, errors.New("checkpoint sync read failed, too short")
	}
	return binary.LittleEndian.Uint64(beaconState[40:48]), nil
}
