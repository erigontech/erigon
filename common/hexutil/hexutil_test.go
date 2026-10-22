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
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type marshalTest struct {
	input any
	want  string
}

type unmarshalTest struct {
	input        string
	want         any
	wantErr      error // if set, decoding must fail on any platform
	wantErr32bit error // if set, decoding must fail on 32bit platforms (used for Uint tests)
}

var (
	encodeBytesTests = []marshalTest{
		{[]byte{}, "0x"},
		{[]byte{0}, "0x00"},
		{[]byte{0, 0, 1, 2}, "0x00000102"},
	}
	encodeBigTests = []marshalTest{
		{bigFromString("0"), "0x0"},
		{bigFromString("1"), "0x1"},
		{bigFromString("ff"), "0xff"},
		{bigFromString("112233445566778899aabbccddeeff"), "0x112233445566778899aabbccddeeff"},
		{bigFromString("80a7f2c1bcc396c00"), "0x80a7f2c1bcc396c00"},
		{bigFromString("-80a7f2c1bcc396c00"), "-0x80a7f2c1bcc396c00"},
	}

	encodeUint64Tests = []marshalTest{
		{uint64(0), "0x0"},
		{uint64(1), "0x1"},
		{uint64(0xff), "0xff"},
		{uint64(0x1122334455667788), "0x1122334455667788"},
	}

	encodeUint16Tests = []marshalTest{
		{uint16(0), "0x00"},
		{uint16(1), "0x01"},
		{uint16(0xff), "0xff"},
		{uint16(0x100), "0x0100"},
		{uint16(0xffff), "0xffff"},
	}

	encodeUintTests = []marshalTest{
		{uint(0), "0x0"},
		{uint(1), "0x1"},
		{uint(0xff), "0xff"},
		{uint(0x11223344), "0x11223344"},
	}

	decodeBytesTests = []unmarshalTest{
		// invalid
		{input: ``, wantErr: ErrEmptyString},
		{input: `0`, wantErr: ErrMissingPrefix},
		{input: `0x0`, wantErr: ErrOddLength},
		{input: `0x023`, wantErr: ErrOddLength},
		{input: `0xxx`, wantErr: ErrSyntax},
		{input: `0x01zz01`, wantErr: ErrSyntax},
		// valid
		{input: `0x`, want: []byte{}},
		{input: `0X`, want: []byte{}},
		{input: `0x02`, want: []byte{0x02}},
		{input: `0X02`, want: []byte{0x02}},
		{input: `0xffffffffff`, want: []byte{0xff, 0xff, 0xff, 0xff, 0xff}},
		{
			input: `0xffffffffffffffffffffffffffffffffffff`,
			want:  []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
	}

	decodeBigTests = []unmarshalTest{
		// invalid
		{input: `0`, wantErr: ErrMissingPrefix},
		{input: `0x`, wantErr: ErrEmptyNumber},
		{input: `0x01`, wantErr: ErrLeadingZero},
		{input: `0xx`, wantErr: ErrSyntax},
		{input: `0x1zz01`, wantErr: ErrSyntax},
		{
			input:   `0x10000000000000000000000000000000000000000000000000000000000000000`,
			wantErr: ErrBig256Range,
		},
		// valid
		{input: `0x0`, want: big.NewInt(0)},
		{input: `0x2`, want: big.NewInt(0x2)},
		{input: `0x2F2`, want: big.NewInt(0x2f2)},
		{input: `0X2F2`, want: big.NewInt(0x2f2)},
		{input: `0x1122aaff`, want: big.NewInt(0x1122aaff)},
		{input: `0xbBb`, want: big.NewInt(0xbbb)},
		{input: `0xfffffffff`, want: big.NewInt(0xfffffffff)},
		{
			input: `0x112233445566778899aabbccddeeff`,
			want:  bigFromString("112233445566778899aabbccddeeff"),
		},
		{
			input: `0xffffffffffffffffffffffffffffffffffff`,
			want:  bigFromString("ffffffffffffffffffffffffffffffffffff"),
		},
		{
			input: `0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff`,
			want:  bigFromString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
		},
	}

	isValidQtyTests = []unmarshalTest{
		// invalid
		{input: ``, wantErr: ErrEmptyString},
		{input: `0`, wantErr: ErrMissingPrefix},
		{input: `0x`, wantErr: ErrEmptyNumber},
		{input: `0x01`, wantErr: ErrLeadingZero},
		{input: `0x00`, wantErr: ErrLeadingZero},
		{input: `0x0000000000000000000000000000000000000000000000000000000000000001`, wantErr: ErrLeadingZero},
		{input: `0x0000000000000000000000000000000000000000000000000000000000000000`, wantErr: ErrLeadingZero},
		{input: `0x10000000000000000000000000000000000000000000000000000000000000000`, wantErr: ErrTooBigHexString},
		{input: `0x1zz01`, wantErr: ErrHexStringInvalid},
		{input: `0xasdf`, wantErr: ErrHexStringInvalid},

		// valid
		{input: `0x0`, wantErr: nil},
		{input: `0x1`, wantErr: nil},
		{input: `0x2F2`, wantErr: nil},
		{input: `0X2F2`, wantErr: nil},
		{input: `0x1122aaff`, wantErr: nil},
		{input: `0xbbb`, wantErr: nil},
		{input: `0xffffffffffffffff`, wantErr: nil},
		{input: `0x123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0`, wantErr: nil},
	}

	decodeUint64Tests = []unmarshalTest{
		// invalid
		{input: `0`, wantErr: ErrMissingPrefix},
		{input: `0x`, wantErr: ErrEmptyNumber},
		{input: `0x01`, wantErr: ErrLeadingZero},
		{input: `0xfffffffffffffffff`, wantErr: ErrUint64Range},
		{input: `0xx`, wantErr: ErrSyntax},
		{input: `0x1zz01`, wantErr: ErrSyntax},
		// valid
		{input: `0x0`, want: uint64(0)},
		{input: `0x2`, want: uint64(0x2)},
		{input: `0x2F2`, want: uint64(0x2f2)},
		{input: `0X2F2`, want: uint64(0x2f2)},
		{input: `0x1122aaff`, want: uint64(0x1122aaff)},
		{input: `0xbbb`, want: uint64(0xbbb)},
		{input: `0xffffffffffffffff`, want: uint64(0xffffffffffffffff)},
	}
)

func TestDecode(t *testing.T) {
	for idx, test := range decodeBytesTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			dec, err := Decode(test.input)
			checkError(t, test.input, err, test.wantErr)
			if test.want != nil {
				require.EqualValues(t, test.want, dec)
			}
		})
	}
}

func TestEncodeBig(t *testing.T) {
	for idx, test := range encodeBigTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			enc := EncodeBig(test.input.(*big.Int))
			require.Equal(t, test.want, enc)
		})
	}
}

func TestDecodeBig(t *testing.T) {
	for idx, test := range decodeBigTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			dec, err := DecodeBig(test.input)
			checkError(t, test.input, err, test.wantErr)
			if test.want != nil {
				require.Equal(t, test.want.(*big.Int).String(), dec.String())
			}
		})
	}
}

// TestDecodeU256 runs DecodeU256 over DecodeBig's own table: the two must
// accept and reject exactly the same inputs, so that swapping a caller from one
// to the other cannot change which requests are valid.
func TestDecodeU256(t *testing.T) {
	for idx, test := range decodeBigTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			dec, err := DecodeU256(test.input)
			checkError(t, test.input, err, test.wantErr)

			bigDec, bigErr := DecodeBig(test.input)
			require.Equal(t, bigErr == nil, err == nil, "DecodeBig and DecodeU256 must agree on validity")
			if test.want != nil {
				require.Equal(t, test.want.(*big.Int).String(), dec.Dec())
			}
			if bigErr == nil {
				require.Equal(t, bigDec.String(), dec.Dec(), "both decoders must yield the same value")
			}
		})
	}
}

func TestEncodeUint64(t *testing.T) {
	for idx, test := range encodeUint64Tests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			enc := EncodeUint64(test.input.(uint64))
			require.Equal(t, test.want, enc)
		})
	}
}

func TestDecodeUint64(t *testing.T) {
	for idx, test := range decodeUint64Tests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			dec, err := DecodeUint64(test.input)
			checkError(t, test.input, err, test.wantErr)
			if test.want != nil {
				require.EqualValues(t, test.want, dec)
			}
		})
	}
}

func TestEncode(t *testing.T) {
	for _, test := range encodeBytesTests {
		enc := Encode(test.input.([]byte))
		if enc != test.want {
			t.Errorf("input %x: wrong encoding %s", test.input, enc)
		}
	}
}

func TestIsValidQuantity(t *testing.T) {
	for idx, test := range isValidQtyTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			err := IsValidQuantity(test.input)
			checkError(t, test.input, err, test.wantErr)
		})
	}
}

// TestEncodeHexMatchesStdlib compares every hex writer with encoding/hex over lengths that cross
// each vector block boundary and tail size.
func TestEncodeHexMatchesStdlib(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	// The long lengths cross the 64 KiB chunks fed to the assembly.
	lengths := []int{1<<16 - 1, 1 << 16, 1<<16 + 1, 1<<18 + 17}
	for n := range 301 {
		lengths = append(lengths, n)
	}
	for _, n := range lengths {
		src := make([]byte, n)
		for i := range src {
			src[i] = byte(r.Uint32())
		}
		want := hex.EncodeToString(src)
		dst := bytes.Repeat([]byte{0xa5}, 2*n+8)
		encodeHex(dst, src)
		require.Equal(t, want+strings.Repeat("\xa5", 8), string(dst), "len %d", n)
		require.Equal(t, `x"0x`+want+`"`, string(AppendQuoted([]byte("x"), src)), "len %d", n)
		text, _ := Bytes(src).AppendText([]byte("x"))
		require.Equal(t, "x0x"+want, string(text), "len %d", n)
	}
}

// requireDecodeMatchesStdlib compares the whole of an oversized dst, so a write past the decoded
// prefix fails too.
func requireDecodeMatchesStdlib(t *testing.T, src []byte, msgAndArgs ...any) {
	t.Helper()
	want, got := bytes.Repeat([]byte{0xa5}, len(src)/2+8), bytes.Repeat([]byte{0xa5}, len(src)/2+8)
	wn, werr := hex.Decode(want, src)
	gn, gerr := decodeHex(got, src)
	require.Equal(t, werr, gerr, msgAndArgs...)
	require.Equal(t, wn, gn, msgAndArgs...)
	require.Equal(t, want, got, msgAndArgs...)
}

// TestDecodeHexMatchesStdlib covers the SIMD block path and its fallbacks: every pair of digits in
// lower, upper and mixed case at both byte offsets, every length around a block boundary of both
// parities, and a bad character at each position.
func TestDecodeHexMatchesStdlib(t *testing.T) {
	words := make([]byte, 0, 2<<16)
	for v := range 1 << 16 {
		words = binary.BigEndian.AppendUint16(words, uint16(v))
	}
	lower := []byte(hex.EncodeToString(words))
	upper := bytes.ToUpper(lower)
	mixed := slices.Clone(lower)
	for i := 1; i < len(mixed); i += 2 {
		mixed[i] = upper[i]
	}
	for _, src := range [][]byte{lower, upper, mixed} {
		requireDecodeMatchesStdlib(t, src)
		requireDecodeMatchesStdlib(t, append([]byte("00"), src...))
		for n := 0; n <= 401; n++ {
			requireDecodeMatchesStdlib(t, src[:n], "len %d", n)
		}
	}
	for _, n := range []int{64, 65, 66, 67, 200, 201} {
		src := []byte(strings.Repeat("ab", n)[:n])
		for i := range src {
			for b := range 256 {
				bad := slices.Clone(src)
				bad[i] = byte(b)
				requireDecodeMatchesStdlib(t, bad, "len %d, byte %#02x at %d", n, b, i)
			}
		}
	}
	for _, i := range []int{0, 1<<16 - 1, 1 << 16, 1<<16 + 31, 1<<17 + 1, len(mixed) - 1} {
		bad := slices.Clone(mixed)
		bad[i] = 'g'
		requireDecodeMatchesStdlib(t, bad, "bad at %d", i)
		requireDecodeMatchesStdlib(t, bad[:len(bad)-1], "odd length, bad at %d", i)
	}
}

var sinkString string

// TestShortEncodingStaysOnStack checks that a short encoding builds its buffer on the stack:
// Encode allocates only the string it returns, and MarshalText inlined into a caller nothing.
func TestShortEncodingStaysOnStack(t *testing.T) {
	b := bytes.Repeat([]byte{0xab}, 15)
	require.InDelta(t, 1, testing.AllocsPerRun(100, func() { sinkString = Encode(b) }), 0)
	n := 0
	require.InDelta(t, 0, testing.AllocsPerRun(100, func() {
		text, _ := Bytes(b).MarshalText()
		n += len(text)
	}), 0)
}
