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

package rlp

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/common/hexutil"
)

var parseU64Tests = []struct {
	expectErr error
	payload   []byte
	expectPos int
	expectRes uint64
}{
	{payload: hexutil.MustDecodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: hexutil.MustDecodeHex("07"), expectPos: 1, expectRes: 7},
	{payload: hexutil.MustDecodeHex("8107"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutil.MustDecodeHex("B8020004"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutil.MustDecodeHex("C0"), expectErr: fmt.Errorf("%w: uint64 must be a string, not isList", ErrParse)},
	{payload: hexutil.MustDecodeHex("00"), expectErr: fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: 00", ErrParse)},
	{payload: hexutil.MustDecodeHex("8AFFFFFFFFFFFFFFFFFF7C"), expectErr: fmt.Errorf("%w: uint64 must not be more than 8 bytes long, got 10", ErrParse)},
}

var parseU32Tests = []struct {
	expectErr error
	payload   []byte
	expectPos int
	expectRes uint32
}{
	{payload: hexutil.MustDecodeHex("820400"), expectPos: 3, expectRes: 1024},
	{payload: hexutil.MustDecodeHex("07"), expectPos: 1, expectRes: 7},
	{payload: hexutil.MustDecodeHex("8107"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutil.MustDecodeHex("B8020004"), expectErr: fmt.Errorf("%w: non-canonical size information", ErrParse)},
	{payload: hexutil.MustDecodeHex("C0"), expectErr: fmt.Errorf("%w: uint32 must be a string, not isList", ErrParse)},
	{payload: hexutil.MustDecodeHex("00"), expectErr: fmt.Errorf("%w: integer encoding for RLP must not have leading zeros: 00", ErrParse)},
	{payload: hexutil.MustDecodeHex("85FF6738FF7C"), expectErr: fmt.Errorf("%w: uint32 must not be more than 4 bytes long, got 5", ErrParse)},
}

func TestPrimitives(t *testing.T) {
	for i, tt := range parseU64Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			pos, res, err := ParseU64(tt.payload, 0)
			assert.Equal(tt.expectErr, err)
			assert.Equal(tt.expectPos, pos)
			assert.Equal(tt.expectRes, res)
		})
	}
	for i, tt := range parseU32Tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			assert := assert.New(t)
			pos, res, err := ParseU32(tt.payload, 0)
			assert.Equal(tt.expectErr, err)
			assert.Equal(tt.expectPos, pos)
			assert.Equal(tt.expectRes, res)
		})
	}
}
