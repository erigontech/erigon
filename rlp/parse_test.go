package rlp

import (
	"encoding/hex"
	"errors"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/assert"
)

func decodeHex(in string) []byte {
	payload, err := hex.DecodeString(in)
	if err != nil {
		panic(err)
	}
	return payload
}

var parseU64Tests = []struct {
	payload   []byte
	expectPos int
	expectRes uint64
	expectErr error
}{
	{payload: decodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: decodeHex("07"), expectPos: 1, expectRes: 7},
	{payload: decodeHex("8107"), expectErr: errors.New("rlp: non-canonical size information")},
	{payload: decodeHex("B8020004"), expectErr: errors.New("rlp: non-canonical size information")},
	{payload: decodeHex("C0"), expectErr: errors.New("uint64 must be a string, not isList")},
	{payload: decodeHex("00"), expectErr: errors.New("integer encoding for RLP must not have leading zeros: 00")},
	{payload: decodeHex("8AFFFFFFFFFFFFFFFFFF7C"), expectErr: errors.New("uint64 must not be more than 8 bytes long, got 10")},
}

var parseU32Tests = []struct {
	payload   []byte
	expectPos int
	expectRes uint32
	expectErr error
}{
	{payload: decodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: decodeHex("07"), expectPos: 1, expectRes: 7},
	{payload: decodeHex("8107"), expectErr: errors.New("rlp: non-canonical size information")},
	{payload: decodeHex("B8020004"), expectErr: errors.New("rlp: non-canonical size information")},
	{payload: decodeHex("C0"), expectErr: errors.New("uint32 must be a string, not isList")},
	{payload: decodeHex("00"), expectErr: errors.New("integer encoding for RLP must not have leading zeros: 00")},
	{payload: decodeHex("85FF6738FF7C"), expectErr: errors.New("uint32 must not be more than 4 bytes long, got 5")},
}

var parseU256Tests = []struct {
	payload   []byte
	expectPos int
	expectRes *uint256.Int
	expectErr error
}{
	{payload: decodeHex("8BFFFFFFFFFFFFFFFFFF7C"), expectErr: errors.New("unexpected end of payload")},
	{payload: decodeHex("8AFFFFFFFFFFFFFFFFFF7C"), expectPos: 11, expectRes: new(uint256.Int).SetBytes(decodeHex("FFFFFFFFFFFFFFFFFF7C"))},
	{payload: decodeHex("85CE05050505"), expectPos: 6, expectRes: new(uint256.Int).SetUint64(0xCE05050505)},
	{payload: decodeHex("820400"), expectPos: 3, expectRes: new(uint256.Int).SetUint64(1024)},
	{payload: decodeHex("07"), expectPos: 1, expectRes: new(uint256.Int).SetUint64(7)},
	{payload: decodeHex("8107"), expectErr: errors.New("rlp: non-canonical size information")},
	{payload: decodeHex("B8020004"), expectErr: errors.New("rlp: non-canonical size information")},
	{payload: decodeHex("C0"), expectErr: errors.New("must be a string, instead of a list")},
	{payload: decodeHex("00"), expectErr: errors.New("integer encoding for RLP must not have leading zeros: 00")},
	{payload: decodeHex("A101000000000000000000000000000000000000008B000000000000000000000000"), expectErr: errors.New("uint256 must not be more than 32 bytes long, got 33")},
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
