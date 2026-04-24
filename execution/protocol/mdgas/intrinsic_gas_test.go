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

package mdgas

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
			if result.RegularGas != c.expected {
				t.Errorf("expected %v but got %v", c.expected, result.RegularGas)
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
	assert.Equal(params.TxGas, result.RegularGas)
	assert.Equal(params.TxGas, result.FloorGasCost)
}

func TestEIP7976FloorCost(t *testing.T) {
	// EIP-7976 floor: 64 gas per byte (both zero and non-zero),
	// computed as floor_tokens = total_bytes * 4, cost_per_token = 16.
	cases := map[string]struct {
		dataLen        uint64
		dataNonZeroLen uint64
		expectedFloor  uint64
	}{
		"zero data": {
			dataLen:        0,
			dataNonZeroLen: 0,
			expectedFloor:  params.TxGas, // 21000, no floor addition
		},
		"all zero bytes": {
			// 32 zero bytes: floor_tokens = 32*4 = 128, floor = 128*16 = 2048
			dataLen:        32,
			dataNonZeroLen: 0,
			expectedFloor:  params.TxGas + 32*4*params.TxTotalCostFloorPerTokenEIP7976, // 21000 + 2048 = 23048
		},
		"all non-zero bytes": {
			// 32 non-zero bytes: floor_tokens = 32*4 = 128, floor = 128*16 = 2048
			// Key property: same floor as all-zero (byte value doesn't matter)
			dataLen:        32,
			dataNonZeroLen: 32,
			expectedFloor:  params.TxGas + 32*4*params.TxTotalCostFloorPerTokenEIP7976, // 21000 + 2048 = 23048
		},
		"mixed bytes": {
			// 20 zero + 12 non-zero = 32 total: floor_tokens = 32*4 = 128, floor = 128*16 = 2048
			dataLen:        32,
			dataNonZeroLen: 12,
			expectedFloor:  params.TxGas + 32*4*params.TxTotalCostFloorPerTokenEIP7976, // 21000 + 2048 = 23048
		},
		"single byte non-zero": {
			dataLen:        1,
			dataNonZeroLen: 1,
			expectedFloor:  params.TxGas + 1*4*params.TxTotalCostFloorPerTokenEIP7976, // 21000 + 64 = 21064
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)

			result, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
				Data:           make([]byte, c.dataLen),
				DataNonZeroLen: c.dataNonZeroLen,
				IsEIP2:         true,
				IsEIP2028:      true,
				IsEIP3860:      true,
				IsEIP7623:      true,
				IsEIP7976:      true,
			})
			assert.False(overflow)
			assert.Equal(c.expectedFloor, result.FloorGasCost,
				"EIP-7976 floor mismatch")
		})
	}
}

func TestEIP7976VsEIP7623Floor(t *testing.T) {
	// Compare EIP-7976 vs EIP-7623 floor costs for the same data.
	// EIP-7976 is strictly greater than EIP-7623 for any non-empty calldata
	// (64 > 10 for zero bytes, 64 > 40 for non-zero bytes).
	assert := assert.New(t)

	// 32 non-zero bytes:
	// EIP-7623: tokens = 32 + 3*32 = 128, floor = 128*10 = 1280
	// EIP-7976: tokens = 32*4 = 128, floor = 128*16 = 2048
	result7623, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
		Data:           make([]byte, 32),
		DataNonZeroLen: 32,
		IsEIP2:         true,
		IsEIP2028:      true,
		IsEIP7623:      true,
	})
	assert.False(overflow)

	result7976, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
		Data:           make([]byte, 32),
		DataNonZeroLen: 32,
		IsEIP2:         true,
		IsEIP2028:      true,
		IsEIP7623:      true,
		IsEIP7976:      true,
	})
	assert.False(overflow)

	assert.Equal(params.TxGas+128*params.TxTotalCostFloorPerToken, result7623.FloorGasCost)        // 21000+1280=22280
	assert.Equal(params.TxGas+128*params.TxTotalCostFloorPerTokenEIP7976, result7976.FloorGasCost) // 21000+2048=23048
	assert.Greater(result7976.FloorGasCost, result7623.FloorGasCost)

	// 32 zero bytes:
	// EIP-7623: tokens = 32 + 3*0 = 32, floor = 32*10 = 320
	// EIP-7976: tokens = 32*4 = 128, floor = 128*16 = 2048
	result7623z, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
		Data:           make([]byte, 32),
		DataNonZeroLen: 0,
		IsEIP2:         true,
		IsEIP2028:      true,
		IsEIP7623:      true,
	})
	assert.False(overflow)

	result7976z, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
		Data:           make([]byte, 32),
		DataNonZeroLen: 0,
		IsEIP2:         true,
		IsEIP2028:      true,
		IsEIP7623:      true,
		IsEIP7976:      true,
	})
	assert.False(overflow)

	assert.Equal(params.TxGas+32*params.TxTotalCostFloorPerToken, result7623z.FloorGasCost)         // 21000+320=21320
	assert.Equal(params.TxGas+128*params.TxTotalCostFloorPerTokenEIP7976, result7976z.FloorGasCost) // 21000+2048=23048
	assert.Greater(result7976z.FloorGasCost, result7623z.FloorGasCost)

	// Standard gas should be the same regardless of EIP-7976
	assert.Equal(result7623.RegularGas, result7976.RegularGas)
	assert.Equal(result7623z.RegularGas, result7976z.RegularGas)
}
