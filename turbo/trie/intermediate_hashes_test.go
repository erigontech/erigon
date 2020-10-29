package trie

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/stretchr/testify/assert"
)

func TestCompressNibbles(t *testing.T) {
	cases := []struct {
		in     string
		expect string
	}{
		{in: "0000", expect: "00"},
		{in: "0102", expect: "12"},
		{in: "0102030405060708090f", expect: "123456789f"},
		{in: "0f000101", expect: "f011"},
		{in: "", expect: ""},
	}

	compressed := make([]byte, 64)
	decompressed := make([]byte, 64)
	for _, tc := range cases {
		compressed = compressed[:0]
		decompressed = decompressed[:0]

		in := common.Hex2Bytes(tc.in)
		CompressNibbles(in, &compressed)
		msg := "On: " + tc.in + " Len: " + strconv.Itoa(len(compressed))
		assert.Equal(t, tc.expect, fmt.Sprintf("%x", compressed), msg)
		DecompressNibbles(compressed, &decompressed)
		assert.Equal(t, tc.in, fmt.Sprintf("%x", decompressed), msg)
	}
}
