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

package state_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition"
)

func TestSeededTotalActiveBalanceInvalidatesAtEpochBoundary(t *testing.T) {
	cfg := clparams.MainnetBeaconConfig
	clparams.ApplyMinimalPreset(&cfg)
	source := state.New(&cfg)
	validator := solid.NewValidator()
	validator.SetEffectiveBalance(cfg.MaxEffectiveBalance)
	validator.SetActivationEpoch(0)
	validator.SetExitEpoch(cfg.FarFutureEpoch)
	require.NoError(t, source.AddValidator(validator, cfg.MaxEffectiveBalance))
	require.NoError(t, source.SetSlot(cfg.SlotsPerEpoch-1))
	require.NoError(t, source.InitBeaconState())

	wrongTotal := 2 * cfg.MaxEffectiveBalance
	seeded, err := source.CopyWithTotalActiveBalance(wrongTotal)
	require.NoError(t, err)
	require.Equal(t, wrongTotal, seeded.GetTotalActiveBalance())
	require.NoError(t, transition.DefaultMachine.ProcessSlots(seeded, cfg.SlotsPerEpoch))
	rescanned, err := seeded.Copy()
	require.NoError(t, err)
	require.Equal(t, rescanned.GetTotalActiveBalance(), seeded.GetTotalActiveBalance())
}
