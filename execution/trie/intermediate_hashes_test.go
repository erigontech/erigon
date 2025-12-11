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

package trie

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/hexutil"

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
		hexutil.CompressNibbles(in, &compressed)
		msg := "On: " + tc.in + " Len: " + strconv.Itoa(len(compressed))
		assert.Equal(t, tc.expect, fmt.Sprintf("%x", compressed), msg)
		hexutil.DecompressNibbles(compressed, &decompressed)
		assert.Equal(t, tc.in, fmt.Sprintf("%x", decompressed), msg)
	}
}
