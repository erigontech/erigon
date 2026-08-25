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
	"errors"
	"testing"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state/raw"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed tests/phase0.ssz_snappy
var stateEncoded []byte

func TestUpgradeAndExpectedWithdrawals(t *testing.T) {
	s := New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(s, stateEncoded, int(clparams.Phase0Version)))
	require.NoError(t, s.UpgradeToAltair())
	require.NoError(t, s.UpgradeToBellatrix())
	require.NoError(t, s.UpgradeToCapella())
	require.NoError(t, s.UpgradeToDeneb())
	// now WITHDRAWAAALLLLSSSS
	w, err := GetExpectedWithdrawals(s, Epoch(s))
	require.NoError(t, err)
	assert.Empty(t, w.Withdrawals)
}

// TestUpgradeToElectraPropagatesQueueExcessActiveBalanceError pins that a
// failure while queuing a compounding-credential validator's excess active
// balance aborts the Electra upgrade instead of being silently dropped,
// which would leave that validator's excess balance unqueued and diverge
// from other clients' post-upgrade state root.
func TestUpgradeToElectraPropagatesQueueExcessActiveBalanceError(t *testing.T) {
	s := New(&clparams.MainnetBeaconConfig)
	require.NoError(t, utils.DecodeSSZSnappy(s, stateEncoded, int(clparams.Phase0Version)))
	require.NoError(t, s.UpgradeToAltair())
	require.NoError(t, s.UpgradeToBellatrix())
	require.NoError(t, s.UpgradeToCapella())
	require.NoError(t, s.UpgradeToDeneb())

	// Give validator 0 a compounding withdrawal credential and a balance above
	// MinActivationBalance so UpgradeToElectra's QueueExcessActiveBalance path
	// actually calls SetValidatorBalance (and so the OnNewValidatorBalance hook
	// registered below) for it. Activating it also keeps it out of the earlier
	// "not yet active" loop, which writes a balance through the same hook.
	var creds common.Hash
	creds[0] = byte(s.BeaconConfig().CompoundingWithdrawalPrefix)
	require.NoError(t, s.SetWithdrawalCredentialForValidatorAtIndex(0, creds))
	require.NoError(t, s.SetActivationEpochForValidatorAtIndex(0, 0))
	require.NoError(t, s.SetValidatorBalance(0, s.BeaconConfig().MinActivationBalance+1))

	wantErr := errors.New("balance hook failed")
	queued := false
	s.SetEvents(raw.Events{
		OnNewValidatorBalance: func(index int, balance uint64) error {
			// QueueExcessActiveBalance trims down to MinActivationBalance; the
			// "not yet active" loop zeroes instead, so this only matches the path
			// under test.
			if index == 0 && balance == s.BeaconConfig().MinActivationBalance {
				queued = true
				return wantErr
			}
			return nil
		},
	})

	require.ErrorIs(t, s.UpgradeToElectra(), wantErr)
	require.True(t, queued, "error must come from QueueExcessActiveBalance, not another SetValidatorBalance caller")
}
