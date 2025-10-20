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
	"io/fs"
	"os"
	"testing"

	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/spectest"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var SanitySlots = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	// TODO: this is unused, why?
	var slots int
	err = spectest.ReadMeta(root, "slots.yaml", &slots)
	require.NoError(t, err)

	testState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	require.NoError(t, err)

	expectedState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	require.NoError(t, err)

	err = c.Machine.ProcessSlots(testState, expectedState.Slot())
	require.NoError(t, err)

	expectedRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)

	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)

	assert.Equal(t, expectedRoot, haveRoot)
	return nil
})

var SanityBlocks = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {
	var meta struct {
		Description            string `yaml:"description"`
		BlsSetting             int    `yaml:"bls_settings"`
		RevealDeadlinesSetting int    `yaml:"reveal_deadlines_setting"`
		BlocksCount            int    `yaml:"blocks_count"`
	}

	err = spectest.ReadMeta(root, "meta.yaml", &meta)
	require.NoError(t, err)

	testState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	require.NoError(t, err)

	var expectedError bool
	expectedState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	if os.IsNotExist(err) {
		expectedError = true
	} else {
		require.NoError(t, err)
	}

	blocks, err := spectest.ReadBlocks(root, c.Version())
	require.NoError(t, err)

	var block *cltypes.SignedBeaconBlock
	for _, block = range blocks {
		err = machine.TransitionState(c.Machine, testState, block)
		if err != nil {
			break
		}
	}
	// Deal with transition error
	if expectedError {
		require.Error(t, err)
		return nil
	} else {
		require.NoError(t, err)
	}

	finalRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)
	haveRoot, err := testState.HashSSZ()
	require.NoError(t, err)

	assert.Equal(t, finalRoot, haveRoot)

	return nil
})
