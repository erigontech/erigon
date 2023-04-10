package cltypes_test

import (
	"encoding/hex"
	"testing"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cl/cltypes"
	"github.com/ledgerwatch/erigon/cl/utils"
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

var testValidator1 = &cltypes.Validator{
	PublicKey:                  hex2BlsPublicKey("227a72a5b99042650eaa52ed66ebf50d31595dba2cbc3da3810378c6fa92c25b93fa0e652a1ac298549cceb6c40d6fc2"),
	WithdrawalCredentials:      common.HexToHash("401ef8ad032de7a3b8a50ae67cd823b0944d2260cd0d018e710eebf8e832b021"),
	EffectiveBalance:           13619341603830475769,
	Slashed:                    true,
	ActivationEligibilityEpoch: 2719404809456332213,
	ActivationEpoch:            8707665390408467486,
	ExitEpoch:                  6929014573432656651,
	WithdrawableEpoch:          3085466968797960434,
}

var testValidator2 = &cltypes.Validator{
	PublicKey:                  hex2BlsPublicKey("bff465728708cccd057fa4be1bf83eb42ec6a70e52cca2d24309de9e2b0cbc1a7110b92e8c7f475625d1e79f86f31a3e"),
	WithdrawalCredentials:      common.HexToHash("3ba9e58ab7d3807a2733e3aa204fdacde3f0d16e7126492c72fa228eae20c5eb"),
	EffectiveBalance:           4748534886993468932,
	Slashed:                    false,
	ActivationEligibilityEpoch: 9720109592954569431,
	ActivationEpoch:            16116572433788512122,
	ExitEpoch:                  12361131058646161796,
	WithdrawableEpoch:          8134858776785446209,
}

var testValidator2Snappified, _ = hex.DecodeString("79f078bff465728708cccd057fa4be1bf83eb42ec6a70e52cca2d24309de9e2b0cbc1a7110b92e8c7f475625d1e79f86f31a3e3ba9e58ab7d3807a2733e3aa204fdacde3f0d16e7126492c72fa228eae20c5eb049ecb26522fe64100d782ece92cc4e4867a27d1403d91a9df8419a7ac348f8bab41e54308f5d2e470")

var testValidatorRoot1 = common.HexToHash("83e755dbe8b552c628677bcad4d5f28b29f9a24bfe1b3db26f5386ad823a5a67")
var testValidatorRoot2 = common.HexToHash("0bcf6f6b165f8ba4a0b59fad23195a83097cdfc62eca06d6219d5699f057aa14")

func TestValidatorSlashed(t *testing.T) {
	encoded, _ := testValidator1.EncodeSSZ([]byte{0x2})
	decodedValidator := &cltypes.Validator{}
	require.NoError(t, decodedValidator.DecodeSSZ(encoded[1:]))
	root, err := decodedValidator.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), testValidatorRoot1)
}

func TestValidatorNonSlashed(t *testing.T) {
	encoded, _ := utils.DecompressSnappy(testValidator2Snappified)
	decodedValidator := &cltypes.Validator{}
	require.NoError(t, decodedValidator.DecodeSSZ(encoded))
	encoded2, _ := decodedValidator.EncodeSSZ(nil)
	require.Equal(t, encoded2, encoded)
	require.Equal(t, decodedValidator, testValidator2)
	root, err := decodedValidator.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), testValidatorRoot2)
}
