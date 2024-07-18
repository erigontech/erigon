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

package utils_test

import (
	"testing"

	"github.com/erigontech/erigon/cl/utils"
)

func TestKeccak256(t *testing.T) {
	data := []byte("test data")
	extras := [][]byte{
		[]byte("extra1"),
		[]byte("extra2"),
	}

	expectedHash := utils.Sha256(data, extras...)
	hashFunc := utils.OptimizedSha256NotThreadSafe()
	expectedOptimizedHash := hashFunc(data, extras...)

	// Test Keccak256 function
	hash := utils.Sha256(data, extras...)
	if hash != expectedHash {
		t.Errorf("Keccak256 returned an incorrect hash. Expected: %x, Got: %x", expectedHash, hash)
	}

	// Test OptimizedKeccak256 function
	optimizedHash := hashFunc(data, extras...)
	if optimizedHash != expectedOptimizedHash {
		t.Errorf("OptimizedKeccak256 returned an incorrect hash. Expected: %x, Got: %x", expectedOptimizedHash, optimizedHash)
	}
}

func TestOptimizedKeccak256NotThreadSafe(t *testing.T) {
	data := []byte("test data")
	extras := [][]byte{
		[]byte("extra1"),
		[]byte("extra2"),
	}

	expectedHash := utils.Sha256(data, extras...)
	hashFunc := utils.OptimizedSha256NotThreadSafe()
	expectedOptimizedHash := hashFunc(data, extras...)

	// Test OptimizedKeccak256NotThreadSafe function
	hash := utils.Sha256(data, extras...)
	if hash != expectedHash {
		t.Errorf("Keccak256 returned an incorrect hash. Expected: %x, Got: %x", expectedHash, hash)
	}

	// Test OptimizedKeccak256NotThreadSafe function
	optimizedHash := hashFunc(data, extras...)
	if optimizedHash != expectedOptimizedHash {
		t.Errorf("OptimizedKeccak256NotThreadSafe returned an incorrect hash. Expected: %x, Got: %x", expectedOptimizedHash, optimizedHash)
	}
}
