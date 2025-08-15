// Copyright 2022 The Erigon Authors
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

package clparams

import (
	"testing"

	"github.com/stretchr/testify/require"

	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func testConfig(t *testing.T, n NetworkType) {
	network, beacon := GetConfigsByNetwork(n)

	require.Equal(t, *network, NetworkConfigs[n])
	require.Equal(t, *beacon, BeaconConfigs[n])
}

func TestGetConfigsByNetwork(t *testing.T) {
	testConfig(t, chainspec.MainnetChainID)
	testConfig(t, chainspec.SepoliaChainID)
	testConfig(t, chainspec.GnosisChainID)
	testConfig(t, chainspec.ChiadoChainID)
	testConfig(t, chainspec.HoodiChainID)
}
