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

// TestEIP7976FloorCost covers EIP-7976 (Increase Calldata Floor Cost): every
// calldata byte (zero or non-zero) contributes TxStandardTokensPerByte tokens,
// each costing TxTotalCostFloorPerTokenEIP7976 gas.
func TestEIP7976FloorCost(t *testing.T) {
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
			expectedFloor:  params.TxGas + 32*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976, // 21000 + 2048 = 23048
		},
		"all non-zero bytes": {
			// Same floor as all-zero (byte value doesn't matter)
			dataLen:        32,
			dataNonZeroLen: 32,
			expectedFloor:  params.TxGas + 32*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
		},
		"mixed bytes": {
			dataLen:        32,
			dataNonZeroLen: 12,
			expectedFloor:  params.TxGas + 32*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
		},
		"single byte non-zero": {
			dataLen:        1,
			dataNonZeroLen: 1,
			expectedFloor:  params.TxGas + 1*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976, // 21000 + 64 = 21064
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

// TestEIP7976VsEIP7623Floor verifies EIP-7976 floors are strictly greater
// than EIP-7623 floors for the same calldata.
func TestEIP7976VsEIP7623Floor(t *testing.T) {
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

	assert.Equal(params.TxGas+128*params.TxTotalCostFloorPerToken, result7623.FloorGasCost)
	assert.Equal(params.TxGas+128*params.TxTotalCostFloorPerTokenEIP7976, result7976.FloorGasCost)
	assert.Greater(result7976.FloorGasCost, result7623.FloorGasCost)

	// 32 zero bytes:
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

	assert.Equal(params.TxGas+32*params.TxTotalCostFloorPerToken, result7623z.FloorGasCost)
	assert.Equal(params.TxGas+128*params.TxTotalCostFloorPerTokenEIP7976, result7976z.FloorGasCost)
	assert.Greater(result7976z.FloorGasCost, result7623z.FloorGasCost)

	assert.Equal(result7623.RegularGas, result7976.RegularGas)
	assert.Equal(result7623z.RegularGas, result7976z.RegularGas)
}

// TestEIP7981IntrinsicGas covers EIP-7981 (Increase Access List Cost):
// access list data contributes to the floor calculation, and the access
// list data cost is always charged in the standard intrinsic gas path.
func TestEIP7981IntrinsicGas(t *testing.T) {
	cases := map[string]struct {
		dataLen              uint64
		dataNonZeroLen       uint64
		accessListLen        uint64
		storageKeysLen       uint64
		expectedRegularGas   uint64
		expectedFloorGasCost uint64
	}{
		"no data no access list": {
			dataLen:              0,
			dataNonZeroLen:       0,
			accessListLen:        0,
			storageKeysLen:       0,
			expectedRegularGas:   params.TxGas,
			expectedFloorGasCost: params.TxGas,
		},
		"only access list address": {
			dataLen:        0,
			dataNonZeroLen: 0,
			accessListLen:  1,
			storageKeysLen: 0,
			// regular = 21000 + 2400 + (20*4)*16 = 21000 + 2400 + 1280
			expectedRegularGas: params.TxGas +
				params.TxAccessListAddressGas +
				params.TxAccessListAddressBytes*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
			// floor = 21000 + (20*4)*16 = 21000 + 1280
			expectedFloorGasCost: params.TxGas +
				params.TxAccessListAddressBytes*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
		},
		"access list with storage keys": {
			dataLen:        0,
			dataNonZeroLen: 0,
			accessListLen:  1,
			storageKeysLen: 2,
			// access_list_bytes = 1*20 + 2*32 = 84; floor_tokens = 84*4 = 336; data gas = 336*16 = 5376
			// regular = 21000 + 2400 + 2*1900 + 5376 = 32576
			expectedRegularGas: params.TxGas +
				params.TxAccessListAddressGas +
				2*params.TxAccessListStorageKeyGas +
				(params.TxAccessListAddressBytes+2*params.TxAccessListStorageKeyBytes)*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
			expectedFloorGasCost: params.TxGas +
				(params.TxAccessListAddressBytes+2*params.TxAccessListStorageKeyBytes)*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
		},
		"calldata only all non-zero": {
			dataLen:        32,
			dataNonZeroLen: 32,
			accessListLen:  0,
			storageKeysLen: 0,
			// regular = 21000 + 32*16 = 21512
			expectedRegularGas: params.TxGas + 32*params.TxDataNonZeroGasEIP2028,
			// floor = 21000 + (32*4)*16 = 21000 + 2048
			expectedFloorGasCost: params.TxGas + 32*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
		},
		"calldata only all zero bytes": {
			dataLen:        32,
			dataNonZeroLen: 0,
			accessListLen:  0,
			storageKeysLen: 0,
			// regular = 21000 + 32*4 = 21128
			expectedRegularGas: params.TxGas + 32*params.TxDataZeroGas,
			// EIP-7976: zero bytes also cost 4 tokens each for the floor => 21000 + (32*4)*16 = 23048
			expectedFloorGasCost: params.TxGas + 32*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
		},
		"calldata and access list": {
			dataLen:        32,
			dataNonZeroLen: 32,
			accessListLen:  1,
			storageKeysLen: 2,
			// access_list_bytes = 84, access list data gas = 84*4*16 = 5376
			// regular = 21000 + 32*16 + 2400 + 2*1900 + 5376 = 33088
			expectedRegularGas: params.TxGas +
				32*params.TxDataNonZeroGasEIP2028 +
				params.TxAccessListAddressGas +
				2*params.TxAccessListStorageKeyGas +
				(params.TxAccessListAddressBytes+2*params.TxAccessListStorageKeyBytes)*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
			// floor tokens = 32*4 + 84*4 = 128 + 336 = 464; floor = 21000 + 464*16 = 28424
			expectedFloorGasCost: params.TxGas +
				(32+params.TxAccessListAddressBytes+2*params.TxAccessListStorageKeyBytes)*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976,
		},
	}

	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			result, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
				Data:           make([]byte, c.dataLen),
				DataNonZeroLen: c.dataNonZeroLen,
				AccessListLen:  c.accessListLen,
				StorageKeysLen: c.storageKeysLen,
				IsEIP2:         true,
				IsEIP2028:      true,
				IsEIP7623:      true,
				IsEIP7976:      true,
				IsEIP7981:      true,
			})
			assert.False(t, overflow)
			assert.Equal(t, c.expectedRegularGas, result.RegularGas, "RegularGas mismatch")
			assert.Equal(t, c.expectedFloorGasCost, result.FloorGasCost, "FloorGasCost mismatch")
		})
	}
}

// TestEIP7981NotActive verifies that when IsEIP7981 is false (but EIP-7976 is on),
// the EIP-7976 floor formula is used and the access list is NOT included in the
// floor or added to the standard intrinsic gas.
func TestEIP7981NotActive(t *testing.T) {
	result, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
		Data:           make([]byte, 32),
		DataNonZeroLen: 32,
		AccessListLen:  1,
		StorageKeysLen: 2,
		IsEIP2:         true,
		IsEIP2028:      true,
		IsEIP7623:      true,
		IsEIP7976:      true,
		IsEIP7981:      false,
	})
	assert.False(t, overflow)
	// Regular: 21000 + 32*16 + 2400 + 2*1900 = 27712 (no access list data floor charge)
	assert.Equal(t, params.TxGas+32*params.TxDataNonZeroGasEIP2028+params.TxAccessListAddressGas+2*params.TxAccessListStorageKeyGas, result.RegularGas)
	// Floor (EIP-7976, access list not included): 21000 + (32*4)*16 = 23048
	assert.Equal(t, params.TxGas+32*params.TxStandardTokensPerByte*params.TxTotalCostFloorPerTokenEIP7976, result.FloorGasCost)
}
