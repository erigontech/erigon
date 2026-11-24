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

package consensus_tests

import (
	"io/fs"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/spectest/spectest"
)

// freezeSibling copies s, so that s shares every registry chunk with the copy and
// must clone a chunk on its first write. When the test ends it fails if the copy
// changed in the meantime.
func freezeSibling(t *testing.T, s *state.CachingBeaconState) {
	t.Helper()
	sibling, err := s.Copy()
	require.NoError(t, err)
	wantRoot, err := sibling.HashSSZ()
	require.NoError(t, err)
	wantSSZ, err := sibling.EncodeSSZ(nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		gotRoot, err := sibling.HashSSZ()
		if !assert.NoError(t, err) {
			return
		}
		gotSSZ, err := sibling.EncodeSSZ(nil)
		if !assert.NoError(t, err) {
			return
		}
		assert.Equal(t, wantRoot, gotRoot, "frozen sibling root changed")
		assert.Equal(t, wantSSZ, gotSSZ, "frozen sibling SSZ changed")
	})
}

func readFrozenPreState(t *testing.T, root fs.FS, version clparams.StateVersion) *state.CachingBeaconState {
	t.Helper()
	preState, err := spectest.ReadBeaconState(root, version, "pre.ssz_snappy")
	require.NoError(t, err)
	freezeSibling(t, preState)
	return preState
}
