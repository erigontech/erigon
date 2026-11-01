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

package hexutil

import (
	"encoding/binary"
	"math/bits"

	"fmt"
	"github.com/holiman/uint256"
	"math"
	"math/big"
	"strconv"
	"strings"
	"testing"
)

func BenchmarkEncodeBig(b *testing.B) {
	inputs := make([]*big.Int, len(encodeBigTests))
	for i, test := range encodeBigTests {
		inputs[i] = test.input.(*big.Int)
	}
	b.ReportAllocs()
	for b.Loop() {
		for _, bigint := range inputs {
			EncodeBig(bigint)
		}
	}
}

func BenchmarkAppendQuoted(b *testing.B) {
	for _, n := range []int{20, 32, 64, 256, 1024} {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte(i * 7)
		}
		dst := make([]byte, 0, QuotedLen(n))
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				dst = AppendQuoted(dst[:0], src)
			}
		})
	}
}

func BenchmarkDecodeHex(b *testing.B) {
	for _, n := range []int{20, 32, 64, 256, 1024, 110820} {
		src := []byte(strings.Repeat("ab", n))
		dst := make([]byte, n)
		b.Run(strconv.Itoa(n)+"B", func(b *testing.B) {
			b.SetBytes(int64(len(src)))
			for i := 0; i < b.N; i++ {
				if _, err := decodeHex(dst, src); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkAppendUint64(b *testing.B) {
	values := []uint64{0, 3, 157, 0x1918bce, 1 << 40, math.MaxUint64}
	buf := make([]byte, 0, 256)
	b.Run("strconv", func(b *testing.B) {
		for b.Loop() {
			buf = buf[:0]
			for _, v := range values {
				buf = strconv.AppendUint(append(buf, "0x"...), v, 16)
			}
		}
	})
	b.Run("AppendUint64", func(b *testing.B) {
		for b.Loop() {
			buf = buf[:0]
			for _, v := range values {
				buf = AppendUint64(buf, v)
			}
		}
	})
}

func hexqViaEncodeHex(dst []byte, v uint64) []byte {
	nibbles := max((bits.Len64(v)+3)/4, 1)
	nbytes := (nibbles + 1) / 2
	var be [8]byte
	binary.BigEndian.PutUint64(be[:], v)
	var digits [16]byte
	encodeHex(digits[:2*nbytes], be[8-nbytes:])
	return append(append(dst, '0', 'x'), digits[2*nbytes-nibbles:2*nbytes]...)
}

func hexqSpread(x uint32) uint64 {
	y := uint64(x)
	y = (y | y<<16) & 0x0000ffff0000ffff
	y = (y | y<<8) & 0x00ff00ff00ff00ff
	y = (y | y<<4) & 0x0f0f0f0f0f0f0f0f
	letters := ((y + 0x0606060606060606) >> 4) & 0x0101010101010101
	return y + 0x3030303030303030 + letters*39
}

func hexqSWAR(dst []byte, v uint64) []byte {
	nibbles := max((bits.Len64(v)+3)/4, 1)
	var digits [16]byte
	binary.BigEndian.PutUint64(digits[:8], hexqSpread(uint32(v>>32)))
	binary.BigEndian.PutUint64(digits[8:], hexqSpread(uint32(v)))
	return append(append(dst, '0', 'x'), digits[16-nibbles:]...)
}

func hexqU256Generic(dst []byte, v uint64) []byte {
	out, _ := U256(*uint256.NewInt(v)).AppendText(dst)
	return out
}

func u256EncodeHex(dst []byte, b U256) []byte {
	z := (*uint256.Int)(&b)
	nibbles := max((z.BitLen()+3)/4, 1)
	nbytes := (nibbles + 1) / 2
	be := z.Bytes32()
	var digits [64]byte
	encodeHex(digits[:2*nbytes], be[32-nbytes:])
	return append(append(dst, '0', 'x'), digits[2*nbytes-nibbles:2*nbytes]...)
}

var hexqSets = map[string][]uint64{
	"pr6":    {0, 3, 157, 0x1918bce, 1 << 40, math.MaxUint64},
	"rpcmix": {0, 1, 2, 0x2c, 0x95, 0x1a3, 0x18db848, 0x5208, 0x1c9c380, 0x6720a1b3, 0x3b9aca00, 0x7a6b2},
}

var hexqArms = []struct {
	name string
	fn   func([]byte, uint64) []byte
}{
	{"strconv", func(dst []byte, v uint64) []byte { return strconv.AppendUint(append(dst, "0x"...), v, 16) }},
	{"loop", AppendUint64},
	{"encodeHex", hexqViaEncodeHex},
	{"swar", hexqSWAR},
	{"u256generic", hexqU256Generic},
}

func BenchmarkHexQuantity(b *testing.B) {
	for _, set := range []string{"pr6", "rpcmix"} {
		values := hexqSets[set]
		for _, arm := range hexqArms {
			for v := uint64(1); v != 0; v <<= 1 {
				for _, x := range []uint64{0, v - 1, v, v + 1, math.MaxUint64} {
					if got, want := string(arm.fn(nil, x)), "0x"+strconv.FormatUint(x, 16); got != want {
						b.Fatalf("%s(%d) = %s, want %s", arm.name, x, got, want)
					}
				}
			}
			buf := make([]byte, 0, 512)
			b.Run(set+"/"+arm.name, func(b *testing.B) {
				for b.Loop() {
					buf = buf[:0]
					for _, v := range values {
						buf = arm.fn(buf, v)
					}
				}
			})
		}
	}
}

func BenchmarkU256Quantity(b *testing.B) {
	var bal, max256 uint256.Int
	bal.SetFromHex("0x1bc16d674ec80000a1b2")
	max256.SetAllOne()
	for _, c := range []struct {
		name string
		v    U256
	}{{"balance10B", U256(bal)}, {"full32B", U256(max256)}} {
		want, _ := c.v.AppendText(nil)
		if got := u256EncodeHex(nil, c.v); string(got) != string(want) {
			b.Fatalf("%s: %s != %s", c.name, got, want)
		}
		buf := make([]byte, 0, 128)
		b.Run(c.name+"/hexEncode", func(b *testing.B) {
			for b.Loop() {
				buf, _ = c.v.AppendText(buf[:0])
			}
		})
		b.Run(c.name+"/encodeHex", func(b *testing.B) {
			for b.Loop() {
				buf = u256EncodeHex(buf[:0], c.v)
			}
		})
	}
}
