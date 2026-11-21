// Copyright 2026 The Erigon Authors
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

package services

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsGasLimitTargetCompatible(t *testing.T) {
	parentGasLimit := uint64(30_000_000)
	maxGasLimitDifference := parentGasLimit/1024 - 1
	minGasLimit := parentGasLimit - maxGasLimitDifference
	maxGasLimit := parentGasLimit + maxGasLimitDifference

	tests := []struct {
		name           string
		parentGasLimit uint64
		gasLimit       uint64
		targetGasLimit uint64
		want           bool
	}{
		{
			name:           "target in range requires exact gas limit",
			parentGasLimit: parentGasLimit,
			gasLimit:       parentGasLimit,
			targetGasLimit: parentGasLimit,
			want:           true,
		},
		{
			name:           "target in range rejects different gas limit",
			parentGasLimit: parentGasLimit,
			gasLimit:       parentGasLimit + 1,
			targetGasLimit: parentGasLimit,
			want:           false,
		},
		{
			name:           "target above max clamps to max gas limit",
			parentGasLimit: parentGasLimit,
			gasLimit:       maxGasLimit,
			targetGasLimit: maxGasLimit + 1,
			want:           true,
		},
		{
			name:           "target below min clamps to min gas limit",
			parentGasLimit: parentGasLimit,
			gasLimit:       minGasLimit,
			targetGasLimit: minGasLimit - 1,
			want:           true,
		},
		{
			name:           "parent below 1024 has no underflow and clamps to parent",
			parentGasLimit: 1000,
			gasLimit:       1000,
			targetGasLimit: 0,
			want:           true,
		},
		{
			name:           "parent below 1024 rejects non-parent gas limit",
			parentGasLimit: 1000,
			gasLimit:       999,
			targetGasLimit: 0,
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsGasLimitTargetCompatible(tt.parentGasLimit, tt.gasLimit, tt.targetGasLimit))
		})
	}
}
