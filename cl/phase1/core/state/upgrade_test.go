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
	"github.com/erigontech/erigon/cl/utils"
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
	w, _ := ExpectedWithdrawals(s, Epoch(s))
	assert.Empty(t, w)

}
