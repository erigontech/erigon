package trie

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

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

	compressBuf := &bytes.Buffer{}
	decompressBuf := &bytes.Buffer{}
	for _, tc := range cases {
		in := strToNibs(tc.in)
		err := CompressNibbles(in, compressBuf)
		compressed := compressBuf.Bytes()
		assert.Nil(t, err)
		msg := "On: " + tc.in + " Len: " + strconv.Itoa(len(compressed))
		assert.Equal(t, tc.expect, fmt.Sprintf("%x", compressed), msg)
		compressBuf.Reset()

		err = DecompressNibbles(compressed, decompressBuf)
		assert.Nil(t, err)
		decompressed := decompressBuf.Bytes()
		assert.Equal(t, tc.in, fmt.Sprintf("%x", decompressed), msg)
		decompressBuf.Reset()
	}
}

func strToNibs(in string) []uint8 {
	nibs := []byte(in)
	res := make([]uint8, len(in)/2+len(in)%2)
	for i := 0; i < len(nibs)-1; i = i + 2 {
		a := nibToUint8(nibs[i+1 : i+2])
		res[i/2] = a
	}
	return res
}

func nibToUint8(in []byte) uint8 {
	nib, err := strconv.ParseUint(string(in), 16, 4)
	if err != nil {
		panic(err)
	}
	return uint8(nib)
}
