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

package fixedgas

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon/execution/protocol/params"
)

func TestShanghaiIntrinsicGas(t *testing.T) {
	cases := map[string]struct {
		expected          uint64
		dataLen           uint64
		dataNonZeroLen    uint64
		authorizationsLen uint64
		creation          bool
		isShanghai        bool
	}{
		"simple no data": {
			expected:          21000,
			dataLen:           0,
			dataNonZeroLen:    0,
			authorizationsLen: 0,
			creation:          false,
			isShanghai:        false,
		},
		"simple with data": {
			expected:          21512,
			dataLen:           32,
			dataNonZeroLen:    32,
			authorizationsLen: 0,
			creation:          false,
			isShanghai:        false,
		},
		"creation with data no shanghai": {
			expected:          53512,
			dataLen:           32,
			dataNonZeroLen:    32,
			authorizationsLen: 0,
			creation:          true,
			isShanghai:        false,
		},
		"creation with single word and shanghai": {
			expected:          53514, // additional gas for single word
			dataLen:           32,
			dataNonZeroLen:    32,
			authorizationsLen: 0,
			creation:          true,
			isShanghai:        true,
		},
		"creation between word 1 and 2 and shanghai": {
			expected:          53532, // additional gas for going into 2nd word although not filling it
			dataLen:           33,
			dataNonZeroLen:    33,
			authorizationsLen: 0,
			creation:          true,
			isShanghai:        true,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			result, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
				Data:               make([]byte, c.dataLen),
				DataNonZeroLen:     c.dataNonZeroLen,
				AuthorizationsLen:  c.authorizationsLen,
				IsContractCreation: c.creation,
				IsEIP2:             true,
				IsEIP2028:          true,
				IsEIP3860:          c.isShanghai,
			})
			if overflow {
				t.Errorf("expected success but got uint overflow")
			}
			if result.Gas != c.expected {
				t.Errorf("expected %v but got %v", c.expected, result.Gas)
			}
		})
	}
}

func TestZeroDataIntrinsicGas(t *testing.T) {
	assert := assert.New(t)
	result, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
		IsEIP2:    true,
		IsEIP2028: true,
		IsEIP3860: true,
		IsEIP7623: true,
	})
	assert.False(overflow)
	assert.Equal(params.TxGas, result.Gas)
	assert.Equal(params.TxGas, result.FloorGas7623)
}
