package hexutil

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"math/big"
	"testing"
)

type marshalTest struct {
	input interface{}
	want  string
}

type unmarshalTest struct {
	input        string
	want         interface{}
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
			require.EqualValues(t, test.want, enc)
		})
	}
}

func TestDecodeBig(t *testing.T) {
	for idx, test := range decodeBigTests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			dec, err := DecodeBig(test.input)
			checkError(t, test.input, err, test.wantErr)
			if test.want != nil {
				require.EqualValues(t, test.want.(*big.Int).String(), dec.String())
			}
		})
	}
}

func TestEncodeUint64(t *testing.T) {
	for idx, test := range encodeUint64Tests {
		t.Run(fmt.Sprintf("%d", idx), func(t *testing.T) {
			enc := EncodeUint64(test.input.(uint64))
			require.EqualValues(t, test.want, enc)
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
