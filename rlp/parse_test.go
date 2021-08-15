package rlp

import (
	"encoding/hex"
	"fmt"
	"testing"

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
}

var parseU32Tests = []struct {
	payload   []byte
	expectPos int
	expectRes uint32
	expectErr error
}{
	{payload: decodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: decodeHex("07"), expectPos: 1, expectRes: 7},
}

func TestPrimitives(t *testing.T) {
	for i, tt := range parseU64Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			pos, res, err := U64(tt.payload, 0)
			assert.NoError(err)
			assert.Equal(tt.expectPos, pos)
			assert.Equal(tt.expectRes, res)
		})
	}
	for i, tt := range parseU32Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			pos, res, err := U32(tt.payload, 0)
			assert.NoError(err)
			assert.Equal(tt.expectPos, pos)
			assert.Equal(tt.expectRes, res)
		})
	}
}
