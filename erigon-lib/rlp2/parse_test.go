package rlp

import (
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
)

var parseU64Tests = []struct {
	expectErr error
	payload   []byte
	expectPos int
	expectRes uint64
}{
	{payload: hexutility.MustDecodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: hexutility.MustDecodeHex("07"), expectPos: 1, expectRes: 7},
	{payload: hexutility.MustDecodeHex("8107"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutility.MustDecodeHex("B8020004"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutility.MustDecodeHex("C0"), expectErr: fmt.Errorf("%w: uint64 must be a string, not isList", ErrParse)},
	{payload: hexutility.MustDecodeHex("00"), expectErr: fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: 00", ErrParse)},
	{payload: hexutility.MustDecodeHex("8AFFFFFFFFFFFFFFFFFF7C"), expectErr: fmt.Errorf("%w: uint64 must not be more than 8 bytes long, got 10", ErrParse)},
}

var parseU32Tests = []struct {
	expectErr error
	payload   []byte
	expectPos int
	expectRes uint32
}{
	{payload: hexutility.MustDecodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: hexutility.MustDecodeHex("07"), expectPos: 1, expectRes: 7},
	{payload: hexutility.MustDecodeHex("8107"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutility.MustDecodeHex("B8020004"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutility.MustDecodeHex("C0"), expectErr: fmt.Errorf("%w: uint32 must be a string, not isList", ErrParse)},
	{payload: hexutility.MustDecodeHex("00"), expectErr: fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: 00", ErrParse)},
	{payload: hexutility.MustDecodeHex("85FF6738FF7C"), expectErr: fmt.Errorf("%w: uint32 must not be more than 4 bytes long, got 5", ErrParse)},
}

var parseU256Tests = []struct {
	expectErr error
	expectRes *uint256.Int
	payload   []byte
	expectPos int
}{
	{payload: hexutility.MustDecodeHex("8BFFFFFFFFFFFFFFFFFF7C"), expectErr: fmt.Errorf("%w: unexpected end of payload", ErrParse)},
	{payload: hexutility.MustDecodeHex("8AFFFFFFFFFFFFFFFFFF7C"), expectPos: 11, expectRes: new(uint256.Int).SetBytes(hexutility.MustDecodeHex("FFFFFFFFFFFFFFFFFF7C"))},
	{payload: hexutility.MustDecodeHex("85CE05050505"), expectPos: 6, expectRes: new(uint256.Int).SetUint64(0xCE05050505)},
	{payload: hexutility.MustDecodeHex("820400"), expectPos: 3, expectRes: new(uint256.Int).SetUint64(1024)},
	{payload: hexutility.MustDecodeHex("07"), expectPos: 1, expectRes: new(uint256.Int).SetUint64(7)},
	{payload: hexutility.MustDecodeHex("8107"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutility.MustDecodeHex("B8020004"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutility.MustDecodeHex("C0"), expectErr: fmt.Errorf("%w: must be a string, instead of a list", ErrParse)},
	{payload: hexutility.MustDecodeHex("00"), expectErr: fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: 00", ErrParse)},
	{payload: hexutility.MustDecodeHex("A101000000000000000000000000000000000000008B000000000000000000000000"), expectErr: fmt.Errorf("%w: uint256 must not be more than 32 bytes long, got 33", ErrParse)},
}

func TestPrimitives(t *testing.T) {
	for i, tt := range parseU64Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			pos, res, err := U64(tt.payload, 0)
			assert.Equal(tt.expectErr, err)
			assert.Equal(tt.expectPos, pos)
			assert.Equal(tt.expectRes, res)
		})
	}
	for i, tt := range parseU32Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			pos, res, err := U32(tt.payload, 0)
			assert.Equal(tt.expectErr, err)
			assert.Equal(tt.expectPos, pos)
			assert.Equal(tt.expectRes, res)
		})
	}
	for i, tt := range parseU256Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			res := new(uint256.Int)
			pos, err := U256(tt.payload, 0, res)
			assert.Equal(tt.expectErr, err)
			assert.Equal(tt.expectPos, pos)
			if err == nil {
				assert.Equal(tt.expectRes, res)
			}
		})
	}
}
