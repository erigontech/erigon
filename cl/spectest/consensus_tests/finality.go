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
	"testing"

	"github.com/erigontech/erigon/cl/transition/machine"
	"github.com/erigontech/erigon/spectest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var FinalityFinality = spectest.HandlerFunc(func(t *testing.T, root fs.FS, c spectest.TestCase) (err error) {

	testState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PreSsz)
	require.NoError(t, err)

	expectedState, err := spectest.ReadBeaconState(root, c.Version(), spectest.PostSsz)
	require.NoError(t, err)

	blocks, err := spectest.ReadBlocks(root, c.Version())
	if err != nil {
		return err
	}
	startSlot := testState.Slot()
	for _, block := range blocks {
		if err := machine.TransitionState(c.Machine, testState, block); err != nil {
			require.NoError(t, fmt.Errorf("cannot transition state: %w. slot=%d. start_slot=%d", err, block.Block.Slot, startSlot))
		}
	}
	expectedRoot, err := testState.HashSSZ()
	require.NoError(t, err)

	haveRoot, err := expectedState.HashSSZ()
	require.NoError(t, err)

	assert.EqualValues(t, expectedRoot, haveRoot, "state root")

	return nil
})
