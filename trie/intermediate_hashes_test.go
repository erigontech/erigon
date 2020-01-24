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
		{in: "00", expect: "0000"},
		{in: "0000", expect: "0100"},
		{in: "000000", expect: "020000"},
		{in: "000001", expect: "020010"},
		{in: "01", expect: "0010"},
		{in: "010203040506070809", expect: "081234567890"},
		{in: "0f0000", expect: "02f000"},
		{in: "0f", expect: "00f0"},
		{in: "0f00", expect: "01f0"},
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

		err = DecompressNibbles(compressed, decompressBuf)
		assert.Nil(t, err)
		decompressed := compressBuf.Bytes()
		assert.Equal(t, tc.in, fmt.Sprintf("%x", decompressed), msg)
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
