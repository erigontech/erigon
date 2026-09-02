// Copyright 2017 The go-ethereum Authors
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

package math

import (
	"testing"
)

func BenchmarkPaddedBigBytesLargePadding(b *testing.B) {
	bigint := MustParseBig256("123456789123456789123456789123456789")
	for b.Loop() {
		PaddedBigBytes(bigint, 200)
	}
}

func BenchmarkPaddedBigBytesSmallPadding(b *testing.B) {
	bigint := MustParseBig256("0x18F8F8F1000111000110011100222004330052300000000000000000FEFCF3CC")
	for b.Loop() {
		PaddedBigBytes(bigint, 5)
	}
}

func BenchmarkPaddedBigBytesSmallOnePadding(b *testing.B) {
	bigint := MustParseBig256("0x18F8F8F1000111000110011100222004330052300000000000000000FEFCF3CC")
	for b.Loop() {
		PaddedBigBytes(bigint, 32)
	}
}

func BenchmarkByteAtBrandNew(b *testing.B) {
	bigint := MustParseBig256("0x18F8F8F1000111000110011100222004330052300000000000000000FEFCF3CC")
	for b.Loop() {
		bigEndianByteAt(bigint, 15)
	}
}

func BenchmarkByteAt(b *testing.B) {
	bigint := MustParseBig256("0x18F8F8F1000111000110011100222004330052300000000000000000FEFCF3CC")
	for b.Loop() {
		bigEndianByteAt(bigint, 15)
	}
}

func BenchmarkByteAtOld(b *testing.B) {

	bigint := MustParseBig256("0x18F8F8F1000111000110011100222004330052300000000000000000FEFCF3CC")
	for b.Loop() {
		PaddedBigBytes(bigint, 32)
	}
}
