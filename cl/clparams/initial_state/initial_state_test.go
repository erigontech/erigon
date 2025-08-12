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

package initial_state_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/clparams/initial_state"
	chainspec "github.com/erigontech/erigon/execution/chain/spec"
)

func TestMainnet(t *testing.T) {
	state, err := initial_state.GetGenesisState(chainspec.MainnetChainID)
	require.NoError(t, err)
	root, err := state.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, common.Hash(root), common.HexToHash("7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b"))
}

func TestSepolia(t *testing.T) {
	state, err := initial_state.GetGenesisState(chainspec.SepoliaChainID)
	require.NoError(t, err)
	root, err := state.HashSSZ()
	require.NoError(t, err)
	assert.Equal(t, common.Hash(root), common.HexToHash("fb9afe32150fa39f4b346be2519a67e2a4f5efcd50a1dc192c3f6b3d013d2798"))
}
