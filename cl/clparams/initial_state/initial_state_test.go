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

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/v3/cl/clparams"
	"github.com/erigontech/erigon/v3/cl/clparams/initial_state"
	"github.com/stretchr/testify/assert"
)

func TestMainnet(t *testing.T) {
	state, err := initial_state.GetGenesisState(clparams.MainnetNetwork)
	assert.NoError(t, err)
	root, err := state.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.Hash(root), libcommon.HexToHash("7e76880eb67bbdc86250aa578958e9d0675e64e714337855204fb5abaaf82c2b"))
}

func TestSepolia(t *testing.T) {
	state, err := initial_state.GetGenesisState(clparams.SepoliaNetwork)
	assert.NoError(t, err)
	root, err := state.HashSSZ()
	assert.NoError(t, err)
	assert.Equal(t, libcommon.Hash(root), libcommon.HexToHash("fb9afe32150fa39f4b346be2519a67e2a4f5efcd50a1dc192c3f6b3d013d2798"))
}
