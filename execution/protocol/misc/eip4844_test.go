// Copyright 2022 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

package misc

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
)

func TestFakeExponential(t *testing.T) {
	tests := []struct {
		factor, num, denom uint64
		want               uint64
	}{
		// When num==0 the return value should always equal the value of factor
		{1, 0, 1, 1},
		{38493, 0, 1000, 38493},
		{0, 1234, 2345, 0}, // should be 0
		{1, 2, 1, 6},       // approximate 7.389
		{1, 4, 2, 6},
		{1, 3, 1, 16}, // approximate 20.09
		{1, 6, 2, 18},
		{1, 4, 1, 49}, // approximate 54.60
		{1, 8, 2, 50},
		{10, 8, 2, 542}, // approximate 540.598
		{11, 8, 2, 596}, // approximate 600.58
		{1, 5, 1, 136},  // approximate 148.4
		{1, 5, 2, 11},   // approximate 12.18
		{2, 5, 2, 23},   // approximate 24.36
	}

	for _, tt := range tests {
		factor := uint256.NewInt(tt.factor)
		denom := uint256.NewInt(tt.denom)
		result, overflow := FakeExponential(factor, denom, tt.num)
		if overflow {
			t.Errorf("overflow for %v*e^(%v/%v)", factor, tt.num, denom)
		}
		//t.Logf("%v*e^(%v/%v): %v", factor, num, denom, result)
		if tt.want != result.ToBig().Uint64() {
			t.Errorf("got %v want %v", result, tt.want)
		}
	}
}

// uint256 Sign() reads the top bit as a sign, so a term with that bit set must not end the loop.
func TestFakeExponentialTermWithTopBitSetOverflows(t *testing.T) {
	_, overflow := FakeExponential(uint256.NewInt(1), uint256.NewInt(5007716), 877_300_000)
	require.True(t, overflow)
}

// 811_346_079 is the smallest excess whose 256-bit sum overflows at the Prague update fraction.
func TestFakeExponentialOverflowThreshold(t *testing.T) {
	_, overflow := FakeExponential(uint256.NewInt(1), uint256.NewInt(5007716), 811_346_078)
	require.False(t, overflow)
	_, overflow = FakeExponential(uint256.NewInt(1), uint256.NewInt(5007716), 811_346_079)
	require.True(t, overflow)
}

// GetBlobGasPrice reuses its last result, so a change of any input must not return a stale price.
func TestGetBlobGasPriceAcrossInputs(t *testing.T) {
	pragueTime := uint64(100)
	minPrice := uint64(1_000_000_000)
	base := &chain.Config{CancunTime: new(uint64), PragueTime: &pragueTime}
	highMin := &chain.Config{CancunTime: new(uint64), PragueTime: &pragueTime, MinBlobGasPrice: &minPrice}
	require.NotEqual(t, base.GetBlobGasPriceUpdateFraction(0), base.GetBlobGasPriceUpdateFraction(pragueTime))
	for _, tc := range []struct {
		config *chain.Config
		time   uint64
		excess uint64
	}{
		{base, 0, 0},
		{base, 0, 10_000_000},
		{base, 0, 10_000_000},
		{base, 0, 393_216},
		{highMin, 0, 393_216},
		{base, 0, 10_000_000},
		{base, pragueTime, 10_000_000},
		{base, 0, 10_000_000},
	} {
		want, overflow := FakeExponential(uint256.NewInt(tc.config.GetMinBlobGasPrice()), uint256.NewInt(tc.config.GetBlobGasPriceUpdateFraction(tc.time)), tc.excess)
		require.False(t, overflow)
		got, err := GetBlobGasPrice(tc.config, tc.excess, tc.time)
		require.NoError(t, err)
		require.Equalf(t, want, got, "minPrice %d time %d excess %d", tc.config.GetMinBlobGasPrice(), tc.time, tc.excess)
	}
}

// The second call must not return a cached price for the overflow.
func TestGetBlobGasPriceOverflowIsAnError(t *testing.T) {
	for range 2 {
		_, err := GetBlobGasPrice(chain.TestChainOsakaConfig, 1<<62, 0)
		require.Error(t, err)
	}
}

// A blob base fee whose 256-bit computation overflows is far above the EIP-7918 reserve price, so the EIP-4844 formula applies.
func TestCalcExcessBlobGasWithOverflowingBlobBaseFee(t *testing.T) {
	config := chain.TestChainOsakaConfig
	const headerTime = 12
	excess, used := uint64(1)<<62, 3*params.GasPerBlob
	parent := &types.Header{ExcessBlobGas: &excess, BlobGasUsed: &used, BaseFee: uint256.NewInt(1_000_000_000)}
	target := config.GetTargetBlobsPerBlock(headerTime) * params.GasPerBlob
	require.Equal(t, excess+used-target, CalcExcessBlobGas(config, parent, headerTime))
}
