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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/require"
)

func TestProcessEpochWarmRootMatchesColdRoot(t *testing.T) {
	for _, version := range []clparams.StateVersion{clparams.FuluVersion, clparams.GloasVersion} {
		t.Run(version.String(), func(t *testing.T) {
			base := newRewardsTestState(t, 2048)
			upgradeTo(t, base, version)
			cfg := base.BeaconConfig()
			require.NoError(t, base.SetSlot(3*cfg.SlotsPerEpoch))
			base.SetFinalizedCheckpoint(solid.Checkpoint{Epoch: state.PreviousEpoch(base)})
			setEffectiveBalanceCase(t, base, 0, cfg.MinActivationBalance, cfg.MinActivationBalance-2*cfg.EffectiveBalanceIncrement, common.Hash{})
			setEffectiveBalanceCase(t, base, 1, cfg.MinActivationBalance-cfg.EffectiveBalanceIncrement, cfg.MinActivationBalance+2*cfg.EffectiveBalanceIncrement, common.Hash{})
			setEffectiveBalanceCase(t, base, 2, cfg.MinActivationBalance, cfg.MinActivationBalance+2*cfg.EffectiveBalanceIncrement, common.Hash{})
			compoundingCredentials := common.Hash{}
			compoundingCredentials[0] = byte(cfg.CompoundingWithdrawalPrefix)
			setEffectiveBalanceCase(t, base, 3, cfg.MaxEffectiveBalanceElectra, cfg.MaxEffectiveBalanceElectra+2*cfg.EffectiveBalanceIncrement, compoundingCredentials)
			require.NoError(t, base.InitBeaconState())

			_, err := base.HashSSZ()
			require.NoError(t, err)

			require.NoError(t, ProcessEpoch(base))

			requireWarmRootEqualsColdRoot(t, base)
		})
	}
}
