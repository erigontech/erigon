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

package das

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDeferredColumnSyncDue(t *testing.T) {
	slotStart := time.Unix(1000, 0)
	slot := 12 * time.Second
	require.False(t, deferredColumnSyncDue(slotStart, slotStart, slot))
	require.False(t, deferredColumnSyncDue(slotStart.Add(1999*time.Millisecond), slotStart, slot))
	require.True(t, deferredColumnSyncDue(slotStart.Add(2*time.Second), slotStart, slot))
	require.True(t, deferredColumnSyncDue(slotStart.Add(time.Minute), slotStart, slot))
	require.True(t, deferredColumnSyncDue(slotStart.Add(time.Second), slotStart, 6*time.Second))
}
