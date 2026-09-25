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

package antiquary

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/snaptype"
)

// Retirement at the tip covers a single chunk and must stay single-threaded so it does not
// compete with execution. A backlog is the case EL retirement handles with its initial-cycle
// worker bump, and the one that otherwise compresses for days.
func TestIsBlobBacklog(t *testing.T) {
	const limit = uint64(snaptype.CaplinMergeLimit)

	for _, tc := range []struct {
		name string
		span uint64
		want bool
	}{
		{"one chunk, the steady tip case", limit, false},
		{"still short of a second chunk", 2*limit - 1, false},
		{"two chunks", 2 * limit, true},
		{"the 72-chunk backlog measured on sepolia", 72 * limit, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isBlobBacklog(1_000_000, 1_000_000+tc.span))
		})
	}
}

// to can trail from between the guard in antiquateBlobs and this call; an underflowing
// subtraction would otherwise read as an enormous backlog.
func TestIsBlobBacklogHandlesNonAdvancingRange(t *testing.T) {
	require.False(t, isBlobBacklog(1_000_000, 1_000_000))
	require.False(t, isBlobBacklog(1_000_000, 999_999))
}
