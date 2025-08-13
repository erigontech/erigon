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

package chain

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestGetBurntContract(t *testing.T) {
	// Ethereum
	assert.Nil(t, chainspec.Mainnet.Config.GetBurntContract(0))
	assert.Nil(t, chainspec.Mainnet.Config.GetBurntContract(10_000_000))

	// Gnosis Chain
	addr := chainspec.Gnosis.Config.GetBurntContract(19_040_000)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x6BBe78ee9e474842Dbd4AB4987b3CeFE88426A92"), *addr)
	addr = chainspec.Gnosis.Config.GetBurntContract(19_040_001)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x6BBe78ee9e474842Dbd4AB4987b3CeFE88426A92"), *addr)

	// Bor Mainnet
	addr = BorMainnet.Config.GetBurntContract(23850000)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = BorMainnet.Config.GetBurntContract(23850000 + 1)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = BorMainnet.Config.GetBurntContract(50523000 - 1)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x70bcA57F4579f58670aB2d18Ef16e02C17553C38"), *addr)
	addr = BorMainnet.Config.GetBurntContract(50523000)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x7A8ed27F4C30512326878652d20fC85727401854"), *addr)
	addr = BorMainnet.Config.GetBurntContract(50523000 + 1)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x7A8ed27F4C30512326878652d20fC85727401854"), *addr)

	// Amoy
	addr = Amoy.Config.GetBurntContract(0)
	require.NotNil(t, addr)
	assert.Equal(t, common.HexToAddress("0x000000000000000000000000000000000000dead"), *addr)
}
