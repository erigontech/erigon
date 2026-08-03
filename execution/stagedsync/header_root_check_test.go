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

package stagedsync

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/state/statecfg"
)

// The header state-root check must be on by default and skippable only through
// dbg.CheckHeaderStateRoot, independently of the commitment trie variant.
func TestHeaderRootCheckDefaultOnAndTogglable(t *testing.T) {
	computed := make([]byte, 32)
	expected := make([]byte, 32)
	expected[0] = 0x01

	require.True(t, dbg.CheckHeaderStateRoot, "header root check must default to enabled")

	origBin := statecfg.ExperimentalBinCommitment
	origCheck := dbg.CheckHeaderStateRoot
	t.Cleanup(func() {
		statecfg.ExperimentalBinCommitment = origBin
		dbg.CheckHeaderStateRoot = origCheck
	})

	for _, bin := range []bool{false, true} {
		statecfg.ExperimentalBinCommitment = bin

		dbg.CheckHeaderStateRoot = true
		require.True(t, headerRootMismatch(computed, expected))
		require.False(t, headerRootMismatch(computed, computed))

		dbg.CheckHeaderStateRoot = false
		require.False(t, headerRootMismatch(computed, expected))
	}
}
