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
	"math/bits"

	"github.com/erigontech/erigon-lib/types/ssz"

	"github.com/golang/snappy"
)

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

func DecompressSnappy(data []byte) ([]byte, error) {
	// Decode the snappy
	lenDecoded, err := snappy.DecodedLen(data)
	if err != nil {
		return nil, err
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
