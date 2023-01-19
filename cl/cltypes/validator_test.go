package cltypes_test

import (
	"encoding/hex"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/stretchr/testify/require"
)

/*
{pubkey: '0x227a72a5b99042650eaa52ed66ebf50d31595dba2cbc3da3810378c6fa92c25b93fa0e652a1ac298549cceb6c40d6fc2',
withdrawal_credentials: '0x401ef8ad032de7a3b8a50ae67cd823b0944d2260cd0d018e710eebf8e832b021',
effective_balance: 13619341603830475769, slashed: true, activation_eligibility_epoch: 2719404809456332213,
activation_epoch: 8707665390408467486, exit_epoch: 6929014573432656651, withdrawable_epoch: 3085466968797960434}
*/

func hex2BlsPublicKey(s string) (k [48]byte) {
	bytesKey, err := hex.DecodeString(s)
	if err != nil {
		return [48]byte{}
	}
	copy(k[:], bytesKey)
	return
}

var testValidator = &cltypes.Validator{
	PublicKey:                  hex2BlsPublicKey("227a72a5b99042650eaa52ed66ebf50d31595dba2cbc3da3810378c6fa92c25b93fa0e652a1ac298549cceb6c40d6fc2"),
	WithdrawalCredentials:      common.HexToHash("401ef8ad032de7a3b8a50ae67cd823b0944d2260cd0d018e710eebf8e832b021"),
	EffectiveBalance:           13619341603830475769,
	Slashed:                    true,
	ActivationEligibilityEpoch: 2719404809456332213,
	ActivationEpoch:            8707665390408467486,
	ExitEpoch:                  6929014573432656651,
	WithdrawableEpoch:          3085466968797960434,
}

var testValidatorRoot = common.HexToHash("83e755dbe8b552c628677bcad4d5f28b29f9a24bfe1b3db26f5386ad823a5a67")

func TestValidator(t *testing.T) {
	encoded, _ := testValidator.EncodeSSZ([]byte{0x2})
	decodedValidator := &cltypes.Validator{}
	require.NoError(t, decodedValidator.DecodeSSZ(encoded[1:]))
	root, err := decodedValidator.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), testValidatorRoot)
}
