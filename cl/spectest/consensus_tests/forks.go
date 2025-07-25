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

package consensus_tests

import (
	"fmt"
	"io/fs"
	"os"
	"testing"

	"github.com/erigontech/erigon/spectest"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ForksFork = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {

	preState, err := spectest.ReadBeaconState(root, c.Version()-1, spectest.PreSsz)
	require.NoError(t, err)

	postState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	expectedError := os.IsNotExist(err)
	if !expectedError {
		require.NoError(t, err)
	}
	switch preState.Version() {
	case clparams.Phase0Version:
		err = preState.UpgradeToAltair()
	case clparams.AltairVersion:
		err = preState.UpgradeToBellatrix()
	case clparams.BellatrixVersion:
		err = preState.UpgradeToCapella()
	case clparams.CapellaVersion:
		err = preState.UpgradeToDeneb()
	case clparams.DenebVersion:
		err = preState.UpgradeToElectra()
	case clparams.ElectraVersion:
		err = preState.UpgradeToFulu()
	default:
		err = spectest.ErrHandlerNotImplemented(fmt.Sprintf("block state %v", preState.Version()))
	}
	if expectedError {
		assert.Error(t, err)
	}

	haveRoot, err := preState.HashSSZ()
	require.NoError(t, err)

	expectedRoot, err := postState.HashSSZ()
	require.NoError(t, err)
	assert.EqualValues(t, expectedRoot, haveRoot, "state root")

	return nil
})
