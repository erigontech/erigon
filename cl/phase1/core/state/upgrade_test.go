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

package state

import (
	_ "embed"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed tests/phase0.ssz_snappy
var stateEncoded []byte

func TestUpgradeAndExpectedWithdrawals(t *testing.T) {
	s := New(&clparams.MainnetBeaconConfig)
	utils.DecodeSSZSnappy(s, stateEncoded, int(clparams.Phase0Version))
	require.NoError(t, s.UpgradeToAltair())
	require.NoError(t, s.UpgradeToBellatrix())
	require.NoError(t, s.UpgradeToCapella())
	require.NoError(t, s.UpgradeToDeneb())
	// now WITHDRAWAAALLLLSSSS
	w, err := GetExpectedWithdrawals(s, Epoch(s))
	require.NoError(t, err)
	assert.Empty(t, w.Withdrawals)

}

// TestOnboardBuildersFromPendingDeposits_NewFormatDeposit verifies that a
// pending deposit with PayloadBuilderVersion (0x00) prefix and
// DomainBuilderDeposit signature is onboarded as a builder during the
// GLOAS fork upgrade — not left in the pending queue.
func TestOnboardBuildersFromPendingDeposits_NewFormatDeposit(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := New(&cfg)
	s.SetVersion(clparams.GloasVersion)

	builders := solid.NewStaticListSSZ[*cltypes.Builder](
		int(cfg.BuilderRegistryLimit),
		new(cltypes.Builder).EncodingSizeSSZ(),
	)
	s.SetBuilders(builders)

	// Create a valid new-format builder deposit (0x00 prefix, DomainBuilderDeposit).
	privKey, err := bls.GenerateKey()
	require.NoError(t, err)
	var pubkey common.Bytes48
	copy(pubkey[:], bls.CompressPublicKey(privKey.PublicKey()))

	feeRecipient := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	var creds common.Hash
	creds[0] = clparams.PayloadBuilderVersion
	copy(creds[12:], feeRecipient[:])
	amount := cfg.MinDepositAmount

	dd := &cltypes.DepositData{
		PubKey:                pubkey,
		WithdrawalCredentials: creds,
		Amount:                amount,
	}
	msgHash, err := dd.MessageHash()
	require.NoError(t, err)
	domain, err := fork.ComputeDomain(
		cfg.DomainBuilderDeposit[:],
		utils.Uint32ToBytes4(uint32(cfg.GenesisForkVersion)),
		[32]byte{},
	)
	require.NoError(t, err)
	signingRoot := utils.Sha256(msgHash[:], domain)
	sigObj := privKey.Sign(signingRoot[:])
	var sig common.Bytes96
	copy(sig[:], sigObj.Bytes())

	// Seed the pending deposits with this builder deposit.
	pendingDeposits := solid.NewPendingDepositList(&cfg)
	pendingDeposits.Append(&solid.PendingDeposit{
		PubKey:                pubkey,
		WithdrawalCredentials: creds,
		Amount:                amount,
		Signature:             sig,
		Slot:                  0,
	})
	s.SetPendingDeposits(pendingDeposits)

	require.NoError(t, s.onboardBuildersFromPendingDeposits())

	require.Equal(t, 1, s.GetBuilders().Len(), "new-format builder deposit must be onboarded")
	require.Equal(t, pubkey, s.GetBuilders().Get(0).Pubkey)
	require.Equal(t, 0, s.GetPendingDeposits().Len(), "onboarded deposit must be removed from pending")
}

// TestOnboardBuildersFromPendingDeposits_MalformedDepositDoesNotAbort verifies
// that a malformed pending deposit (undeserializable pubkey/signature) with a
// PayloadBuilderVersion prefix does not abort the fork upgrade. The deposit
// must stay in the pending queue.
func TestOnboardBuildersFromPendingDeposits_MalformedDepositDoesNotAbort(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	s := New(&cfg)
	s.SetVersion(clparams.GloasVersion)

	builders := solid.NewStaticListSSZ[*cltypes.Builder](
		int(cfg.BuilderRegistryLimit),
		new(cltypes.Builder).EncodingSizeSSZ(),
	)
	s.SetBuilders(builders)

	// Malformed deposit: valid PayloadBuilderVersion prefix but garbage pubkey/sig.
	var badPubkey common.Bytes48
	badPubkey[0] = 0xFF
	var creds common.Hash
	creds[0] = clparams.PayloadBuilderVersion
	creds[12] = 0xAA

	pendingDeposits := solid.NewPendingDepositList(&cfg)
	pendingDeposits.Append(&solid.PendingDeposit{
		PubKey:                badPubkey,
		WithdrawalCredentials: creds,
		Amount:                cfg.MinDepositAmount,
		Signature:             common.Bytes96{},
		Slot:                  0,
	})
	s.SetPendingDeposits(pendingDeposits)

	require.NoError(t, s.onboardBuildersFromPendingDeposits(),
		"malformed deposit must not abort fork upgrade")
	require.Equal(t, 0, s.GetBuilders().Len(), "malformed deposit must not create a builder")
	require.Equal(t, 1, s.GetPendingDeposits().Len(), "malformed deposit must stay in pending")
}
