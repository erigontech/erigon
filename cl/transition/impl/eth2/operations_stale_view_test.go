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

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
)

// detachedViews returns validator views that do not alias registry storage.
type detachedViews struct{ *state.CachingBeaconState }

func (d detachedViews) ValidatorForValidatorIndex(i int) (solid.ValidatorView, error) {
	v, err := d.CachingBeaconState.ValidatorForValidatorIndex(i)
	return v.Clone().View(), err
}

func electraStateWith(t *testing.T, cfg *clparams.BeaconChainConfig, vals ...solid.Validator) *state.CachingBeaconState {
	t.Helper()
	s := state.New(cfg)
	s.SetVersion(clparams.ElectraVersion)
	for _, v := range vals {
		require.NoError(t, s.AddValidator(v, 40_000_000_000))
	}
	require.NoError(t, s.SetSlot(cfg.SlotsPerEpoch*(cfg.ShardCommitteePeriod+10)))
	require.NoError(t, s.InitBeaconState())
	return s
}

func eth1Credentials(cfg *clparams.BeaconChainConfig, addr byte) common.Hash {
	var wc common.Hash
	wc[0] = byte(cfg.ETH1AddressWithdrawalPrefixByte)
	wc[12] = addr
	return wc
}

func TestSwitchToCompoundingQueuesNewCredentials(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	v := solid.NewValidatorFromParameters([48]byte{1}, eth1Credentials(&cfg, 0xaa), 32_000_000_000, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch)
	s := electraStateWith(t, &cfg, v)
	require.NoError(t, switchToCompoundingValidator(detachedViews{s}, 0))
	deposits := s.GetPendingDeposits()
	require.Equal(t, 1, deposits.Len())
	wc := deposits.Get(0).WithdrawalCredentials
	require.Equal(t, byte(cfg.CompoundingWithdrawalPrefix), wc[0], "pending deposit must carry the post-switch credentials")
}

func TestConsolidationWithdrawableEpochUsesNewExitEpoch(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	cfg.MinPerEpochChurnLimitElectra = 1_000_000_000_000
	var targetWc common.Hash
	targetWc[0] = byte(cfg.CompoundingWithdrawalPrefix)
	source := solid.NewValidatorFromParameters([48]byte{1}, eth1Credentials(&cfg, 0xaa), 32_000_000_000, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch)
	target := solid.NewValidatorFromParameters([48]byte{2}, targetWc, 32_000_000_000, false, 0, 0, cfg.FarFutureEpoch, cfg.FarFutureEpoch)
	s := electraStateWith(t, &cfg, source, target)
	req := &solid.ConsolidationRequest{SourcePubKey: [48]byte{1}, TargetPubKey: [48]byte{2}}
	req.SourceAddress[0] = 0xaa
	require.NoError(t, (&impl{}).ProcessConsolidationRequest(detachedViews{s}, req))
	exit, err := s.ValidatorExitEpoch(0)
	require.NoError(t, err)
	require.NotEqual(t, cfg.FarFutureEpoch, exit, "consolidation must start")
	withdrawable, err := s.ValidatorWithdrawableEpoch(0)
	require.NoError(t, err)
	require.Equal(t, exit+cfg.MinValidatorWithdrawabilityDelay, withdrawable)
}
