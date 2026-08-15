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

package eth2

import (
	"testing"

	blst "github.com/supranational/blst/bindings/go"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/stretchr/testify/require"
)

// blsChangeFixture builds a state holding one validator with BLS withdrawal credentials derived
// from a real keypair, plus a change for it signed with that key.
func blsChangeFixture(t *testing.T) (*state.CachingBeaconState, *cltypes.SignedBLSToExecutionChange) {
	t.Helper()
	cfg := clparams.MainnetBeaconConfig
	s := state.New(&cfg)

	sk, err := bls.GenerateKey()
	require.NoError(t, err)

	var from common.Bytes48
	copy(from[:], (*blst.P1Affine)(sk.PublicKey()).Compress())

	hashedFrom := crypto.Sha256(from[:])
	var creds common.Hash
	creds[0] = byte(cfg.BLSWithdrawalPrefixByte)
	copy(creds[1:], hashedFrom[1:])

	s.AddValidator(solid.NewValidatorFromParameters(
		from, creds, cfg.MaxEffectiveBalance, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch,
	), cfg.MaxEffectiveBalance)

	change := &cltypes.BLSToExecutionChange{ValidatorIndex: 0, From: from, To: common.Address{0xab}}
	domain, err := fork.ComputeDomain(
		cfg.DomainBLSToExecutionChange[:],
		utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		s.GenesisValidatorsRoot(),
	)
	require.NoError(t, err)
	signingRoot, err := fork.ComputeSigningRoot(change, domain)
	require.NoError(t, err)

	var sig common.Bytes96
	copy(sig[:], sk.Sign(signingRoot[:]).Bytes())
	return s, &cltypes.SignedBLSToExecutionChange{Message: change, Signature: sig}
}

func migratedTo(t *testing.T, s *state.CachingBeaconState) bool {
	t.Helper()
	v, err := s.ValidatorForValidatorIndex(0)
	require.NoError(t, err)
	creds := v.WithdrawalCredentials()
	return creds[0] == byte(clparams.MainnetBeaconConfig.ETH1AddressWithdrawalPrefixByte)
}

// Signature verification is the expensive part of this operation and is skipped when the caller
// does not ask for full validation, matching how attestation signatures are handled. Everything
// that determines the resulting state stays unconditional.
func TestProcessBlsToExecutionChangeSignatureGating(t *testing.T) {
	t.Run("valid signature is accepted under full validation", func(t *testing.T) {
		s, signed := blsChangeFixture(t)
		require.NoError(t, (&impl{FullValidation: true}).ProcessBlsToExecutionChange(s, signed))
		require.True(t, migratedTo(t, s))
	})

	t.Run("bad signature is rejected under full validation", func(t *testing.T) {
		s, signed := blsChangeFixture(t)
		signed.Signature[0] ^= 0xff
		require.Error(t, (&impl{FullValidation: true}).ProcessBlsToExecutionChange(s, signed))
		require.False(t, migratedTo(t, s))
	})

	t.Run("signature is not checked without full validation", func(t *testing.T) {
		s, signed := blsChangeFixture(t)
		signed.Signature[0] ^= 0xff
		require.NoError(t, (&impl{}).ProcessBlsToExecutionChange(s, signed))
		require.True(t, migratedTo(t, s))
	})

	// The state-dependent asserts are what block production relies on, so they must hold
	// whether or not signatures are being verified.
	t.Run("credential asserts still apply without full validation", func(t *testing.T) {
		s, signed := blsChangeFixture(t)
		require.NoError(t, (&impl{}).ProcessBlsToExecutionChange(s, signed))
		require.Error(t, (&impl{}).ProcessBlsToExecutionChange(s, signed))
	})
}
