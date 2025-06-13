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

package raw

import (
	_ "embed"
	"testing"

	"github.com/erigontech/erigon-lib/common"
	"github.com/stretchr/testify/require"
)

func TestGetters(t *testing.T) {
	state := GetTestState()
	require.NotNil(t, state.BeaconConfig())
	valLength := state.ValidatorLength()
	require.Equal(t, state.balances.Length(), valLength)

	val, err := state.ValidatorBalance(0)
	require.NoError(t, err)
	require.Equal(t, uint64(0x3d5972c17), val)

	root, err := state.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x9f1620db18ee06b9cbdf1b7fa9658701063d2bd05d54b09780f6c0a074b4ce5f"))

	copied, err := state.Copy()
	require.NoError(t, err)

	root, err = copied.BlockRoot()
	require.NoError(t, err)
	require.Equal(t, common.Hash(root), common.HexToHash("0x9f1620db18ee06b9cbdf1b7fa9658701063d2bd05d54b09780f6c0a074b4ce5f"))
}
