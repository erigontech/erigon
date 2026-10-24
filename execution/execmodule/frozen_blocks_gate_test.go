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

package execmodule

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Processing frozen blocks holds the module's semaphore for the whole pass, so
// a consensus layer that inserts during one loses every acquisition. The old
// signal was a channel closed the first time headers opened — an edge, and a
// closed channel never blocks again, so after an unwind sent the module back
// into a frozen-block pass the consensus layer sailed straight through the
// gate it was supposed to wait at.
//
// The question is not "were frozen blocks ever ready" but "is a pass running
// right now", which has to be answerable more than once.
func TestFrozenBlocksGateIsLevelNotEdge(t *testing.T) {
	t.Parallel()
	var g frozenBlocksGate

	require.False(t, g.quiescent(), "before the first pass there is no usable frozen tip to read")

	g.begin()
	require.False(t, g.quiescent(), "a pass is running")
	g.end()
	require.True(t, g.quiescent(), "the first pass finished")

	// An unwind sends it back through: the gate must close again, which is
	// exactly what a one-shot channel cannot do.
	g.begin()
	require.False(t, g.quiescent(), "a later pass must gate again, not inherit the first pass's answer")
	g.end()
	require.True(t, g.quiescent())
}
