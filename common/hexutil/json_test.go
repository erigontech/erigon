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
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func checkError(t *testing.T, input string, got, want error) {
	t.Helper()
	if want == nil {
		require.NoErrorf(t, got, "input %s", input)
		return
	}
	if got == nil {
		require.NoError(t, want, "input %s", input)
		return
	}
	require.Equal(t, want.Error(), got.Error(), "input %s", input)
}

func bigFromString(s string) *big.Int {
	b, ok := new(big.Int).SetString(s, 16)
	if !ok {
		panic("invalid")
	}
	return b
}

var errJSONEOF = errors.New("unexpected end of JSON input")

var unmarshalBigTests = []unmarshalTest{
	// invalid encoding
	{input: "", wantErr: errJSONEOF},
	{input: "null", wantErr: errNonString(bigT)},
	{input: "10", wantErr: errNonString(bigT)},
	{input: `"0"`, wantErr: wrapTypeError(ErrMissingPrefix, bigT)},
	{input: `"0x"`, wantErr: wrapTypeError(ErrEmptyNumber, bigT)},
	{input: `"0x01"`, wantErr: wrapTypeError(ErrLeadingZero, bigT)},
	{input: `"0xx"`, wantErr: wrapTypeError(ErrSyntax, bigT)},
	{input: `"0x1zz01"`, wantErr: wrapTypeError(ErrSyntax, bigT)},
	{
		input:   `"0x10000000000000000000000000000000000000000000000000000000000000000"`,
		wantErr: wrapTypeError(ErrBig256Range, bigT),
	},
	// valid encoding
	{input: `""`, want: big.NewInt(0)},
	{input: `"0x0"`, want: big.NewInt(0)},
	{input: `"0x2"`, want: big.NewInt(0x2)},
	{input: `"0x2F2"`, want: big.NewInt(0x2f2)},
	{input: `"0X2F2"`, want: big.NewInt(0x2f2)},
	{input: `"0x1122aaff"`, want: big.NewInt(0x1122aaff)},
	{input: `"0xbBb"`, want: big.NewInt(0xbbb)},
	{input: `"0xfffffffff"`, want: big.NewInt(0xfffffffff)},
	{
		input: `"0x112233445566778899aabbccddeeff"`,
		want:  bigFromString("112233445566778899aabbccddeeff"),
	},
	{
		input: `"0xffffffffffffffffffffffffffffffffffff"`,
		want:  bigFromString("ffffffffffffffffffffffffffffffffffff"),
	},
	{
		input: `"0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"`,
		want:  bigFromString("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"),
	},
}

func TestUnmarshalBig(t *testing.T) {
	for idx, test := range unmarshalBigTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			var v Big
			err := json.Unmarshal([]byte(test.input), &v)
			checkError(t, test.input, err, test.wantErr)
			if test.want != nil {
				require.Equal(t, test.want.(*big.Int).Bytes(), v.ToInt().Bytes())
			}
		})
	}
}

func BenchmarkUnmarshalBig(b *testing.B) {
	input := []byte(`"0x123456789abcdef123456789abcdef"`)
	for i := 0; i < b.N; i++ {
		var v Big
		if err := v.UnmarshalJSON(input); err != nil {
			b.Fatal(err)
		}
	}
}

func TestMarshalBig(t *testing.T) {
	for idx, test := range encodeBigTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			in := test.input.(*big.Int)
			out, err := json.Marshal((*Big)(in))
			require.NoError(t, err)
			want := `"` + test.want + `"`
			require.Equal(t, want, string(out))
			require.Equal(t, test.want, (*Big)(in).String())
		})
	}
}

var unmarshalUint64Tests = []unmarshalTest{
	// invalid encoding
	{input: "", wantErr: errJSONEOF},
	{input: "null", wantErr: errNonString(uint64T)},
	{input: "10", wantErr: errNonString(uint64T)},
	{input: `"0"`, wantErr: wrapTypeError(ErrMissingPrefix, uint64T)},
	{input: `"0x"`, wantErr: wrapTypeError(ErrEmptyNumber, uint64T)},
	{input: `"0x01"`, wantErr: wrapTypeError(ErrLeadingZero, uint64T)},
	{input: `"0xfffffffffffffffff"`, wantErr: wrapTypeError(ErrUint64Range, uint64T)},
	{input: `"0xx"`, wantErr: wrapTypeError(ErrSyntax, uint64T)},
	{input: `"0x1zz01"`, wantErr: wrapTypeError(ErrSyntax, uint64T)},

	// valid encoding
	{input: `""`, want: uint64(0)},
	{input: `"0x0"`, want: uint64(0)},
	{input: `"0x2"`, want: uint64(0x2)},
	{input: `"0x2F2"`, want: uint64(0x2f2)},
	{input: `"0X2F2"`, want: uint64(0x2f2)},
	{input: `"0x1122aaff"`, want: uint64(0x1122aaff)},
	{input: `"0xbbb"`, want: uint64(0xbbb)},
	{input: `"0xffffffffffffffff"`, want: uint64(0xffffffffffffffff)},
}

func TestUnmarshalUint64(t *testing.T) {
	for idx, test := range unmarshalUint64Tests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			var v Uint64
			err := json.Unmarshal([]byte(test.input), &v)
			checkError(t, test.input, err, test.wantErr)
			if test.want != nil {
				require.EqualValues(t, test.want, v)
			}
		})
	}
}

func BenchmarkUnmarshalUint64(b *testing.B) {
	input := []byte(`"0x123456789abcdf"`)
	for i := 0; i < b.N; i++ {
		var v Uint64
		_ = v.UnmarshalJSON(input)
	}
}

func TestMarshalUint64(t *testing.T) {
	for idx, test := range encodeUint64Tests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			in := test.input.(uint64)
			out, err := json.Marshal(Uint64(in))
			require.NoError(t, err)
			want := `"` + test.want + `"`
			require.Equal(t, want, string(out))
			require.Equal(t, test.want, (Uint64)(in).String())
		})
	}
}

func TestMarshalUint(t *testing.T) {
	for idx, test := range encodeUintTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			in := test.input.(uint)
			out, err := json.Marshal(Uint(in))
			require.NoError(t, err)
			want := `"` + test.want + `"`
			require.Equal(t, want, string(out))
			require.Equal(t, test.want, (Uint)(in).String())
		})
	}
}

var (
	maxUint33bits = uint64(^uint32(0)) + 1
	maxUint64bits = ^uint64(0)
)

var unmarshalUintTests = []unmarshalTest{
	// invalid encoding
	{input: "", wantErr: errJSONEOF},
	{input: "null", wantErr: errNonString(uintT)},
	{input: "10", wantErr: errNonString(uintT)},
	{input: `"0"`, wantErr: wrapTypeError(ErrMissingPrefix, uintT)},
	{input: `"0x"`, wantErr: wrapTypeError(ErrEmptyNumber, uintT)},
	{input: `"0x01"`, wantErr: wrapTypeError(ErrLeadingZero, uintT)},
	{input: `"0x100000000"`, want: uint(maxUint33bits), wantErr32bit: wrapTypeError(ErrUintRange, uintT)},
	{input: `"0xfffffffffffffffff"`, wantErr: wrapTypeError(ErrUintRange, uintT)},
	{input: `"0xx"`, wantErr: wrapTypeError(ErrSyntax, uintT)},
	{input: `"0x1zz01"`, wantErr: wrapTypeError(ErrSyntax, uintT)},

	// valid encoding
	{input: `""`, want: uint(0)},
	{input: `"0x0"`, want: uint(0)},
	{input: `"0x2"`, want: uint(0x2)},
	{input: `"0x2F2"`, want: uint(0x2f2)},
	{input: `"0X2F2"`, want: uint(0x2f2)},
	{input: `"0x1122aaff"`, want: uint(0x1122aaff)},
	{input: `"0xbbb"`, want: uint(0xbbb)},
	{input: `"0xffffffff"`, want: uint(0xffffffff)},
	{input: `"0xffffffffffffffff"`, want: uint(maxUint64bits), wantErr32bit: wrapTypeError(ErrUintRange, uintT)},
}

func TestUnmarshalUint(t *testing.T) {
	for idx, test := range unmarshalUintTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			var v Uint
			err := json.Unmarshal([]byte(test.input), &v)
			if uintBits == 32 && test.wantErr32bit != nil {
				checkError(t, test.input, err, test.wantErr32bit)
				return
			}
			checkError(t, test.input, err, test.wantErr)
			if test.want != nil {
				require.EqualValues(t, test.want, v)
			}

		})
	}
}

func TestUnmarshalFixedUnprefixedText(t *testing.T) {
	tests := []struct {
		input   string
		want    []byte
		wantErr error
	}{
		{input: "0x2", wantErr: ErrOddLength},
		{input: "2", wantErr: ErrOddLength},
		{input: "4444", wantErr: errors.New("hex string has length 4, want 8 for x")},
		{input: "4444", wantErr: errors.New("hex string has length 4, want 8 for x")},
		// check that output is not modified for partially correct input
		{input: "444444gg", wantErr: ErrSyntax, want: []byte{0, 0, 0, 0}},
		{input: "0x444444gg", wantErr: ErrSyntax, want: []byte{0, 0, 0, 0}},
		// valid inputs
		{input: "44444444", want: []byte{0x44, 0x44, 0x44, 0x44}},
		{input: "0x44444444", want: []byte{0x44, 0x44, 0x44, 0x44}},
	}

	for idx, test := range tests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			out := make([]byte, 4)
			err := UnmarshalFixedUnprefixedText("x", []byte(test.input), out)
			checkError(t, test.input, err, test.wantErr)
			if test.want != nil {
				require.Equal(t, test.want, out)
			}
		})
	}
}
