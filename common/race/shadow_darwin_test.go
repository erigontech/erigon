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

//go:build race && darwin

package race

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeapWindowShadowPreMapped(t *testing.T) {
	require.True(t, enabled, "TSAN Go/darwin shadow layout changed — update mem2shadow/heapWindow")

	// Shadow must be mapped across the whole heap window, so any file mapping
	// the kernel later places between Go arenas is readable by __tsan_read.
	for _, addr := range []uintptr{heapWindowBeg, (heapWindowBeg + heapWindowEnd) / 2, heapWindowEnd - 8} {
		require.True(t, shadowIsMapped(addr), "shadow unmapped for window addr %#x", addr)
	}
}
