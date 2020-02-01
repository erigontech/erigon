package trie

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/pool"
	"github.com/stretchr/testify/assert"
)

func TestCompressNibbles(t *testing.T) {
	var err error

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

	compressBuf := pool.GetBuffer(64)
	defer pool.PutBuffer(compressBuf)
	decompressBuf := pool.GetBuffer(64)
	defer pool.PutBuffer(decompressBuf)
	for _, tc := range cases {
		compressBuf.Reset()
		decompressBuf.Reset()

		in := common.Hex2Bytes(tc.in)
		CompressNibbles(in, &compressBuf.B)
		compressed := compressBuf.Bytes()
		msg := "On: " + tc.in + " Len: " + strconv.Itoa(len(compressed))
		assert.Equal(t, tc.expect, fmt.Sprintf("%x", compressed), msg)
		DecompressNibbles(compressed, &decompressBuf.B)
		decompressed := decompressBuf.Bytes()
		assert.Equal(t, tc.in, fmt.Sprintf("%x", decompressed), msg)
	}
}
