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

package statechange

import (
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestProcessEffectiveBalanceUpdatesWritesOnlyChanges(t *testing.T) {
	for _, tc := range []struct {
		version clparams.StateVersion
		want    []int
	}{
		{version: clparams.DenebVersion, want: []int{1, 3, 6}},
		{version: clparams.FuluVersion, want: []int{1, 3}},
		{version: clparams.GloasVersion, want: []int{1, 3}},
	} {
		t.Run(tc.version.String(), func(t *testing.T) {
			s := state.New(&clparams.MainnetBeaconConfig)
			require.NoError(t, utils.DecodeSSZSnappy(s, startingEffectiveBalancesState, int(clparams.BellatrixVersion)))
			upgradeTo(t, s, tc.version)
			cfg := s.BeaconConfig()
			increment := cfg.EffectiveBalanceIncrement
			downwardThreshold := increment / cfg.HysteresisQuotient * cfg.HysteresisDownwardMultiplier
			upwardThreshold := increment / cfg.HysteresisQuotient * cfg.HysteresisUpwardMultiplier
			for i := range s.ValidatorLength() {
				setEffectiveBalanceCase(t, s, i, cfg.MinActivationBalance, cfg.MinActivationBalance, common.Hash{})
			}

			setEffectiveBalanceCase(t, s, 0, cfg.MinActivationBalance, cfg.MinActivationBalance-downwardThreshold, common.Hash{})
			setEffectiveBalanceCase(t, s, 1, cfg.MinActivationBalance, cfg.MinActivationBalance-downwardThreshold-1, common.Hash{})
			setEffectiveBalanceCase(t, s, 2, cfg.MinActivationBalance-increment, cfg.MinActivationBalance-increment+upwardThreshold, common.Hash{})
			setEffectiveBalanceCase(t, s, 3, cfg.MinActivationBalance-increment, cfg.MinActivationBalance-increment+upwardThreshold+1, common.Hash{})
			setEffectiveBalanceCase(t, s, 4, cfg.MinActivationBalance, cfg.MinActivationBalance+increment/10, common.Hash{})
			setEffectiveBalanceCase(t, s, 5, cfg.MinActivationBalance, cfg.MinActivationBalance+2*increment, common.Hash{})

			compoundingCredentials := common.Hash{}
			compoundingCredentials[0] = byte(cfg.CompoundingWithdrawalPrefix)
			setEffectiveBalanceCase(t, s, 6, cfg.MaxEffectiveBalanceElectra, cfg.MaxEffectiveBalanceElectra+2*increment, compoundingCredentials)

			_, err := s.HashSSZ()
			require.NoError(t, err)

			var written []int
			s.SetEvents(raw.Events{
				OnNewValidatorEffectiveBalance: func(index int, _ uint64) error {
					written = append(written, index)
					return nil
				},
			})
			require.NoError(t, ProcessEffectiveBalanceUpdates(s))
			require.Equal(t, tc.want, written)

			requireWarmRootEqualsColdRoot(t, s)
		})
	}
}

func upgradeTo(t testing.TB, s *state.CachingBeaconState, version clparams.StateVersion) {
	t.Helper()
	if version >= clparams.CapellaVersion {
		require.NoError(t, s.UpgradeToCapella())
	}
	if version >= clparams.DenebVersion {
		require.NoError(t, s.UpgradeToDeneb())
	}
	if version >= clparams.ElectraVersion {
		require.NoError(t, s.UpgradeToElectra())
	}
	if version >= clparams.FuluVersion {
		require.NoError(t, s.UpgradeToFulu())
	}
	if version >= clparams.GloasVersion {
		require.NoError(t, s.UpgradeToGloas())
	}
}

func requireWarmRootEqualsColdRoot(t testing.TB, s *state.CachingBeaconState) {
	t.Helper()
	warmRoot, err := s.HashSSZ()
	require.NoError(t, err)
	encoded, err := s.EncodeSSZ(nil)
	require.NoError(t, err)
	cold := state.New(s.BeaconConfig())
	require.NoError(t, cold.DecodeSSZ(encoded, int(s.Version())))
	coldRoot, err := cold.HashSSZ()
	require.NoError(t, err)
	require.Equal(t, common.Hash(warmRoot), common.Hash(coldRoot))
}

func setEffectiveBalanceCase(t testing.TB, s *state.CachingBeaconState, index int, effectiveBalance, balance uint64, credentials common.Hash) {
	t.Helper()
	require.NoError(t, s.SetEffectiveBalanceForValidatorAtIndex(index, effectiveBalance))
	require.NoError(t, s.SetValidatorBalance(index, balance))
	require.NoError(t, s.SetWithdrawalCredentialForValidatorAtIndex(index, credentials))
}
