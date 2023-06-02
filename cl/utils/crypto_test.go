package utils_test

import (
	"testing"

	"github.com/ledgerwatch/erigon/cl/utils"
)

func TestKeccak256(t *testing.T) {
	data := []byte("test data")
	extras := [][]byte{
		[]byte("extra1"),
		[]byte("extra2"),
	}

	expectedHash := utils.Keccak256(data, extras...)
	hashFunc := utils.OptimizedKeccak256()
	expectedOptimizedHash := hashFunc(data, extras...)

	// Test Keccak256 function
	hash := utils.Keccak256(data, extras...)
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

	expectedHash := utils.Keccak256(data, extras...)
	hashFunc := utils.OptimizedKeccak256NotThreadSafe()
	expectedOptimizedHash := hashFunc(data, extras...)

	// Test OptimizedKeccak256NotThreadSafe function
	hash := utils.Keccak256(data, extras...)
	if hash != expectedHash {
		t.Errorf("Keccak256 returned an incorrect hash. Expected: %x, Got: %x", expectedHash, hash)
	}

	// Test OptimizedKeccak256NotThreadSafe function
	optimizedHash := hashFunc(data, extras...)
	if optimizedHash != expectedOptimizedHash {
		t.Errorf("OptimizedKeccak256NotThreadSafe returned an incorrect hash. Expected: %x, Got: %x", expectedOptimizedHash, optimizedHash)
	}
}
