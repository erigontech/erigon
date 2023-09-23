package solid

import (
	"encoding/binary"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestValidatorSetTest(t *testing.T) {
	num := 1000
	vset := NewValidatorSet(1000000)
	vset2 := NewValidatorSet(1000000)
	for i := 0; i < num; i++ {
		var pk [48]byte
		var wk [32]byte
		binary.BigEndian.PutUint32(pk[:], uint32(i))
		binary.BigEndian.PutUint32(wk[:], uint32(i))
		vset.Append(NewValidatorFromParameters(
			pk, wk, uint64(1), true, uint64(1), uint64(1), uint64(1), uint64(1),
		))
		vset.HashSSZ()
	}
	firstHash, err := vset.HashSSZ()
	require.NoError(t, err)

	vset.CopyTo(vset2)
	secondHash, err := vset2.HashSSZ()
	require.NoError(t, err)

	require.Equal(t, firstHash, secondHash)
}
