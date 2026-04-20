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

// TestEIP7981IntrinsicGas covers EIP-7981 (Increase Access List Cost):
// access list data contributes to the floor calculation, and the access
// list data cost is always charged in the standard intrinsic gas path.
func TestEIP7981IntrinsicGas(t *testing.T) {
	const (
		floorCostPerToken = 16
		tokensPerByte     = 4
		addressBytes      = 20
		storageKeyBytes   = 32
	)
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
			expectedRegularGas: params.TxGas + params.TxAccessListAddressGas + addressBytes*tokensPerByte*floorCostPerToken,
			// floor = 21000 + (20*4)*16 = 21000 + 1280
			expectedFloorGasCost: params.TxGas + addressBytes*tokensPerByte*floorCostPerToken,
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
				(addressBytes+2*storageKeyBytes)*tokensPerByte*floorCostPerToken,
			expectedFloorGasCost: params.TxGas + (addressBytes+2*storageKeyBytes)*tokensPerByte*floorCostPerToken,
		},
		"calldata only all non-zero": {
			dataLen:        32,
			dataNonZeroLen: 32,
			accessListLen:  0,
			storageKeysLen: 0,
			// regular = 21000 + 32*16 = 21512
			expectedRegularGas: params.TxGas + 32*params.TxDataNonZeroGasEIP2028,
			// floor = 21000 + (32*4)*16 = 21000 + 2048
			expectedFloorGasCost: params.TxGas + 32*tokensPerByte*floorCostPerToken,
		},
		"calldata only all zero bytes": {
			dataLen:        32,
			dataNonZeroLen: 0,
			accessListLen:  0,
			storageKeysLen: 0,
			// regular = 21000 + 32*4 = 21128
			expectedRegularGas: params.TxGas + 32*params.TxDataZeroGas,
			// EIP-7976: zero bytes also cost 4 tokens each for the floor => 21000 + (32*4)*16 = 23048
			expectedFloorGasCost: params.TxGas + 32*tokensPerByte*floorCostPerToken,
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
				(addressBytes+2*storageKeyBytes)*tokensPerByte*floorCostPerToken,
			// floor tokens = 32*4 + 84*4 = 128 + 336 = 464; floor = 21000 + 464*16 = 28424
			expectedFloorGasCost: params.TxGas + (32+addressBytes+2*storageKeyBytes)*tokensPerByte*floorCostPerToken,
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
				IsEIP7981:      true,
			})
			assert.False(t, overflow)
			assert.Equal(t, c.expectedRegularGas, result.RegularGas, "RegularGas mismatch")
			assert.Equal(t, c.expectedFloorGasCost, result.FloorGasCost, "FloorGasCost mismatch")
		})
	}
}

// TestEIP7981NotActive verifies that when IsEIP7981 is false, the legacy
// EIP-7623 floor calculation is used (zero bytes count as 1 token, floor
// cost per token is 10, access list is not included in the floor).
func TestEIP7981NotActive(t *testing.T) {
	result, overflow := CalcIntrinsicGas(IntrinsicGasCalcArgs{
		Data:           make([]byte, 32),
		DataNonZeroLen: 32,
		AccessListLen:  1,
		StorageKeysLen: 2,
		IsEIP2:         true,
		IsEIP2028:      true,
		IsEIP7623:      true,
		IsEIP7981:      false,
	})
	assert.False(t, overflow)
	// Regular: 21000 + 32*16 + 2400 + 2*1900 = 27712 (no access list data floor charge)
	assert.Equal(t, params.TxGas+32*params.TxDataNonZeroGasEIP2028+params.TxAccessListAddressGas+2*params.TxAccessListStorageKeyGas, result.RegularGas)
	// Floor (EIP-7623): 21000 + (32 + 3*32)*10 = 22280 (access list not in floor)
	assert.Equal(t, params.TxGas+(32+3*32)*params.TxTotalCostFloorPerToken, result.FloorGasCost)
}
