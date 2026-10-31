// Copyright 2020 The go-ethereum Authors
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

package gasprice_test

import (
	"testing"

	"github.com/holiman/uint256"
)

func BenchmarkHeapPercentile_N20(b *testing.B) {
	testData := make([][]*uint256.Int, iterations)
	for i := range iterations {
		testData[i] = generateUint256Slice(sliceSizeSmall)
	}

	for b.Loop() {
		for j := range iterations {
			values := copyUint256Slice(testData[j])
			_ = heapPercentile(values, percentile)
		}
	}
}

func BenchmarkKthPercentile_N20(b *testing.B) {
	testData := make([][]*uint256.Int, iterations)
	for i := range iterations {
		testData[i] = generateUint256Slice(sliceSizeSmall)
	}

	for b.Loop() {
		for j := range iterations {
			values := copyUint256Slice(testData[j])
			index := (len(values) - 1) * percentile / 100
			_ = findKthUint256(values, index)
		}
	}
}

func BenchmarkHeapPercentile(b *testing.B) {
	testData := generateUint256Slice(sliceSizeLarge)

	for b.Loop() {
		values := copyUint256Slice(testData)
		_ = heapPercentile(values, percentile)
	}
}

func BenchmarkKthPercentile(b *testing.B) {
	testData := generateUint256Slice(sliceSizeLarge)

	for b.Loop() {
		values := copyUint256Slice(testData)
		index := (len(values) - 1) * percentile / 100
		_ = findKthUint256(values, index)
	}
}
