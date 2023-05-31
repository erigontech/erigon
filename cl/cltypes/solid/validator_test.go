package solid

import (
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
)

func TestValidator(t *testing.T) {
	// Initializing some dummy data
	var pubKey [48]byte
	var withdrawalCred common.Hash
	for i := 0; i < 48; i++ {
		pubKey[i] = byte(i)
	}
	for i := 0; i < 32; i++ {
		withdrawalCred[i] = byte(i + 50)
	}
	effectiveBalance := uint64(123456789)
	slashed := true
	activationEligibilityEpoch := uint64(987654321)
	activationEpoch := uint64(234567890)
	exitEpoch := uint64(345678901)
	withdrawableEpoch := uint64(456789012)

	// Creating a validator from the dummy data
	validator := NewValidatorFromParameters(pubKey, withdrawalCred, effectiveBalance, slashed, activationEligibilityEpoch, activationEpoch, exitEpoch, withdrawableEpoch)

	// Testing the Get methods
	assert.Equal(t, pubKey, validator.PublicKey())
	assert.Equal(t, withdrawalCred, validator.WithdrawalCredentials())
	assert.Equal(t, effectiveBalance, validator.EffectiveBalance())
	assert.Equal(t, slashed, validator.Slashed())
	assert.Equal(t, activationEligibilityEpoch, validator.ActivationEligibilityEpoch())
	assert.Equal(t, activationEpoch, validator.ActivationEpoch())
	assert.Equal(t, exitEpoch, validator.ExitEpoch())
	assert.Equal(t, withdrawableEpoch, validator.WithdrawableEpoch())

	// Testing the SSZ encoding/decoding
	encoded, _ := validator.EncodeSSZ(nil)
	newValidator := NewValidator()
	err := newValidator.DecodeSSZ(encoded, 0)
	assert.NoError(t, err)
	assert.Equal(t, validator, newValidator)

	// Testing CopyTo
	copiedValidator := NewValidator()
	validator.CopyTo(copiedValidator)
	assert.Equal(t, validator, copiedValidator)
}
