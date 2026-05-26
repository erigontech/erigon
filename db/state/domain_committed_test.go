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

package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/version"
)

func TestCommitmentBranchReferenced(t *testing.T) {
	t.Parallel()
	const stepSize = uint64(10)
	// minStepsForReferencing == 2, so a range >= 2 steps reaches the threshold.
	const fromAbove = uint64(0)
	const toAbove = uint64(20) // 2 steps -> threshold reached
	const fromBelow = uint64(0)
	const toBelow = uint64(10) // 1 step -> below threshold

	cases := []struct {
		name        string
		fileVersion version.Version
		from, to    uint64
		want        bool
	}{
		{"v2.0 above threshold is referenced", version.V2_0, fromAbove, toAbove, true},
		{"v1.0 above threshold is referenced", version.V1_0, fromAbove, toAbove, true},
		{"v2.1 above threshold is plain", version.V2_1, fromAbove, toAbove, false},
		{"v2.0 below threshold is plain", version.V2_0, fromBelow, toBelow, false},
		{"v2.1 below threshold is plain", version.V2_1, fromBelow, toBelow, false},
		{"hypothetical v2.2 above threshold is plain", version.Version{Major: 2, Minor: 2}, fromAbove, toAbove, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := commitmentBranchReferenced(tc.fileVersion, stepSize, tc.from, tc.to)
			require.Equal(t, tc.want, got)
		})
	}
}
