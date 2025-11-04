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

	"github.com/erigontech/erigon/execution/chain/params"
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
			// Todo (@somnathb1) - Factor in EIP-7623
			gas, _, overflow := CalcIntrinsicGas(c.dataLen, c.dataNonZeroLen, c.authorizationsLen, 0, 0, c.creation, true, true, c.isShanghai, false, false)
			if overflow != false {
				t.Errorf("expected success but got uint overflow")
			}
			if gas != c.expected {
				t.Errorf("expected %v but got %v", c.expected, gas)
			}
		})
	}
}

func TestZeroDataIntrinsicGas(t *testing.T) {
	assert := assert.New(t)
	gas, floorGas7623, overflow := CalcIntrinsicGas(0, 0, 0, 0, 0, false, true, true, true, true, false)
	assert.False(overflow)
	assert.Equal(params.TxGas, gas)
	assert.Equal(params.TxGas, floorGas7623)
}
