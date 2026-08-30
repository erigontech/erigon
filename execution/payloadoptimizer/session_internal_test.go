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

package payloadoptimizer

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewOwnedCandidateTransfersSnapshotOwnership(t *testing.T) {
	buildContext, result := internallyMalformedCandidate(30_000_000, nil)
	owned, err := copyBackendResult(result)
	require.NoError(t, err)
	candidate := newOwnedCandidate(buildContext, owned)

	require.Same(t, owned.Block, candidate.block)
	publicCopy := candidate.copy()
	require.NotSame(t, candidate.block, publicCopy.block)
}
