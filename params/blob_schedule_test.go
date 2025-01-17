// Copyright 2025 The Erigon Authors
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

package params

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/erigontech/erigon-lib/chain"
)

func TestNilBlobSchedule(t *testing.T) {
	var b *chain.BlobSchedule
	assert.Equal(t, uint64(3), b.TargetBlobsPerBlock(false /* isPrague */))
	assert.Equal(t, uint64(6), b.MaxBlobsPerBlock(false /* isPrague */))
	assert.Equal(t, uint64(3338477), b.BaseFeeUpdateFraction(false /* isPrague */))
	assert.Equal(t, uint64(3), b.TargetBlobsPerBlock(true /* isPrague */))
	assert.Equal(t, uint64(6), b.MaxBlobsPerBlock(true /* isPrague */))
	assert.Equal(t, uint64(3338477), b.BaseFeeUpdateFraction(true /* isPrague */))
}

func TestMainnetBlobSchedule(t *testing.T) {
	b := MainnetChainConfig.BlobSchedule
	assert.Equal(t, uint64(3), b.TargetBlobsPerBlock(false /* isPrague */))
	assert.Equal(t, uint64(6), b.MaxBlobsPerBlock(false /* isPrague */))
	assert.Equal(t, uint64(3338477), b.BaseFeeUpdateFraction(false /* isPrague */))
	// EIP-7691: Blob throughput increase
	assert.Equal(t, uint64(6), b.TargetBlobsPerBlock(true /* isPrague */))
	assert.Equal(t, uint64(9), b.MaxBlobsPerBlock(true /* isPrague */))
	assert.Equal(t, uint64(5007716), b.BaseFeeUpdateFraction(true /* isPrague */))
}

func TestGnosisBlobSchedule(t *testing.T) {
	b := GnosisChainConfig.BlobSchedule
	assert.Equal(t, uint64(1), b.TargetBlobsPerBlock(false /* isPrague */))
	assert.Equal(t, uint64(2), b.MaxBlobsPerBlock(false /* isPrague */))
	assert.Equal(t, uint64(1112826), b.BaseFeeUpdateFraction(false /* isPrague */))
	assert.Equal(t, uint64(1), b.TargetBlobsPerBlock(true /* isPrague */))
	assert.Equal(t, uint64(2), b.MaxBlobsPerBlock(true /* isPrague */))
	assert.Equal(t, uint64(1112826), b.BaseFeeUpdateFraction(true /* isPrague */))
}
