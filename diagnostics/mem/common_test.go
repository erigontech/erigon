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

package mem

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRefreshVirtualMemStatsReadsAtMostOncePerMaxAge(t *testing.T) {
	_, readErr := ReadVirtualMemStats()
	_, fresh := RefreshVirtualMemStats(0)
	first := vmStatsRead
	require.False(t, first.IsZero())
	require.Equal(t, readErr == nil, fresh, "fresh only after a successful read")

	_, fresh = RefreshVirtualMemStats(time.Hour)
	require.Equal(t, first, vmStatsRead, "a read younger than maxAge, failed or not, is not repeated")
	require.False(t, fresh, "a cached value is not fresh")

	time.Sleep(time.Millisecond)
	RefreshVirtualMemStats(0)
	require.True(t, vmStatsRead.After(first), "maxAge 0 reads again")
}
