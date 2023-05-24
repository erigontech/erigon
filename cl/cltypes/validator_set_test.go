package cltypes

import (
	"testing"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

func TestValidatorSet(t *testing.T) {
	// Create sample data
	capacity := 10
	validator1 := &Validator{} // Create a validator object
	validator2 := &Validator{} // Create another validator object

	// Create ValidatorSet
	validatorSet := NewValidatorSet(capacity)
	validatorSet.Append(validator1)
	validatorSet.Append(validator2)

	// Test EncodeSSZ and DecodeSSZ
	encodedData, err := validatorSet.EncodeSSZ(nil)
	assert.NoError(t, err)

	decodedValidatorSet := NewValidatorSet(capacity)
	err = decodedValidatorSet.DecodeSSZ(encodedData, 0)
	assert.NoError(t, err)

	assert.Equal(t, validatorSet.Length(), decodedValidatorSet.Length())

	// Test EncodingSizeSSZ
	expectedEncodingSize := validatorSet.EncodingSizeSSZ()
	encodingSize := validatorSet.EncodingSizeSSZ()
	assert.Equal(t, expectedEncodingSize, encodingSize)

	// Test HashSSZ
	expectedRoot := libcommon.HexToHash("0xc8b99f435835857f2e55c81ad0fe0769ede725d3dd1b3c7ac1e9e4a134a7baf1") // Expected root value
	root, err := validatorSet.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, expectedRoot, libcommon.Hash(root))

	// Test Get
	validator := validatorSet.Get(0)
	assert.Equal(t, validator1, validator)

	// Test Range
	var validatorCount int
	validatorSet.Range(func(index int, value *Validator, length int) bool {
		validatorCount++
		return true
	})
	assert.Equal(t, validatorSet.Length(), validatorCount)

	// Test CopyTo
	otherValidatorSet := NewValidatorSet(capacity)
	validatorSet.CopyTo(otherValidatorSet)
	assert.Equal(t, validatorSet, otherValidatorSet)

	// Test Pop
	assert.Panics(t, func() { validatorSet.Pop() })

	// Test Append
	validatorSet.Append(validator1)
	validatorCount = validatorSet.Length()
	assert.Equal(t, validatorCount, 3)

	// Test Clear
	validatorSet.Clear()
	validatorCount = validatorSet.Length()
	assert.Equal(t, validatorCount, 0)
}
