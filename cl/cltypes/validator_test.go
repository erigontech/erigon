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

package cltypes_test

import (
	"encoding/hex"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSignedVoluntaryExit(t *testing.T) {
	// Create a sample SignedVoluntaryExit
	voluntaryExit := &cltypes.VoluntaryExit{
		Epoch:          5,
		ValidatorIndex: 10,
	}
	signature := [96]byte{1, 2, 3}

	signedExit := &cltypes.SignedVoluntaryExit{
		VoluntaryExit: voluntaryExit,
		Signature:     signature,
	}

	// Encode SignedVoluntaryExit to SSZ
	encodedExit, err := signedExit.EncodeSSZ(nil)
	require.NoError(t, err, "Failed to encode SignedVoluntaryExit")

	// Decode SSZ to a new SignedVoluntaryExit object
	decodedExit := &cltypes.SignedVoluntaryExit{}
	err = decodedExit.DecodeSSZ(encodedExit, 0)
	require.NoError(t, err, "Failed to decode SSZ to SignedVoluntaryExit")

	// Compare the original and decoded SignedVoluntaryExit
	assert.Equal(t, signedExit.VoluntaryExit.Epoch, decodedExit.VoluntaryExit.Epoch, "Decoded SignedVoluntaryExit has incorrect epoch")
	assert.Equal(t, signedExit.VoluntaryExit.ValidatorIndex, decodedExit.VoluntaryExit.ValidatorIndex, "Decoded SignedVoluntaryExit has incorrect validator index")
	assert.Equal(t, signedExit.Signature, decodedExit.Signature, "Decoded SignedVoluntaryExit has incorrect signature")
}

func TestDepositData(t *testing.T) {
	// Create a sample DepositData
	depositData := &cltypes.DepositData{
		PubKey:                [48]byte{1, 2, 3},
		WithdrawalCredentials: [32]byte{4, 5, 6},
		Amount:                100,
		Signature:             [96]byte{7, 8, 9},
	}

	// Encode DepositData to SSZ
	encodedData, err := depositData.EncodeSSZ(nil)
	require.NoError(t, err, "Failed to encode DepositData")

	// Decode SSZ to a new DepositData object
	decodedData := &cltypes.DepositData{}
	err = decodedData.DecodeSSZ(encodedData, 0)
	require.NoError(t, err, "Failed to decode SSZ to DepositData")

	// Compare the original and decoded DepositData
	assert.Equal(t, depositData.PubKey, decodedData.PubKey, "Decoded DepositData has incorrect public key")
	assert.Equal(t, depositData.WithdrawalCredentials, decodedData.WithdrawalCredentials, "Decoded DepositData has incorrect withdrawal credentials")
	assert.Equal(t, depositData.Amount, decodedData.Amount, "Decoded DepositData has incorrect amount")
	assert.Equal(t, depositData.Signature, decodedData.Signature, "Decoded DepositData has incorrect signature")
}

func hex2BlsPublicKey(s string) (k [48]byte) {
	bytesKey, err := hex.DecodeString(s)
	if err != nil {
		return [48]byte{}
	}
	copy(k[:], bytesKey)
	return
}

var testValidator1 = solid.NewValidatorFromParameters(
	hex2BlsPublicKey("227a72a5b99042650eaa52ed66ebf50d31595dba2cbc3da3810378c6fa92c25b93fa0e652a1ac298549cceb6c40d6fc2"),
	common.HexToHash("401ef8ad032de7a3b8a50ae67cd823b0944d2260cd0d018e710eebf8e832b021"),
	13619341603830475769,
	true,
	2719404809456332213,
	8707665390408467486,
	6929014573432656651,
	3085466968797960434,
)

var testValidator2 = solid.NewValidatorFromParameters(
	hex2BlsPublicKey("bff465728708cccd057fa4be1bf83eb42ec6a70e52cca2d24309de9e2b0cbc1a7110b92e8c7f475625d1e79f86f31a3e"),
	common.HexToHash("3ba9e58ab7d3807a2733e3aa204fdacde3f0d16e7126492c72fa228eae20c5eb"),
	4748534886993468932,
	false,
	9720109592954569431,
	16116572433788512122,
	12361131058646161796,
	8134858776785446209,
)

var testValidator2Snappified, _ = hex.DecodeString("79f078bff465728708cccd057fa4be1bf83eb42ec6a70e52cca2d24309de9e2b0cbc1a7110b92e8c7f475625d1e79f86f31a3e3ba9e58ab7d3807a2733e3aa204fdacde3f0d16e7126492c72fa228eae20c5eb049ecb26522fe64100d782ece92cc4e4867a27d1403d91a9df8419a7ac348f8bab41e54308f5d2e470")

var testValidatorRoot1 = common.HexToHash("83e755dbe8b552c628677bcad4d5f28b29f9a24bfe1b3db26f5386ad823a5a67")
var testValidatorRoot2 = common.HexToHash("0bcf6f6b165f8ba4a0b59fad23195a83097cdfc62eca06d6219d5699f057aa14")

func TestValidatorSlashed(t *testing.T) {
	encoded, _ := testValidator1.EncodeSSZ([]byte{0x2})
	decodedValidator := solid.NewValidator()
	require.NoError(t, decodedValidator.DecodeSSZ(encoded[1:], 0))
	root, err := decodedValidator.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), testValidatorRoot1)
}

func TestValidatorNonSlashed(t *testing.T) {
	encoded, _ := utils.DecompressSnappy(testValidator2Snappified, false)
	decodedValidator := solid.NewValidator()
	require.NoError(t, decodedValidator.DecodeSSZ(encoded, 0))
	encoded2, _ := decodedValidator.EncodeSSZ(nil)
	require.Equal(t, encoded2, encoded)
	require.Equal(t, decodedValidator, testValidator2)
	root, err := decodedValidator.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), testValidatorRoot2)

	assert.False(t, decodedValidator.IsSlashable(1))

}
