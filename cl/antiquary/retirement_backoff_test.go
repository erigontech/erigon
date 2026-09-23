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
)

// ticksUntilReady counts how many ticks are refused before the step runs again.
func ticksUntilReady(t *testing.T, b *retirementBackoff) int {
	t.Helper()
	for skipped := 0; skipped <= maxRetirementSkipTicks*2; skipped++ {
		if b.ready() {
			return skipped
		}
	}
	t.Fatal("backoff never became ready")
	return 0
}

func TestRetirementBackoffRunsUntilSomethingFails(t *testing.T) {
	var b retirementBackoff
	for range 5 {
		require.True(t, b.ready(), "an unfailed step must run on every tick")
	}
}

func TestRetirementBackoffSpacesOutRepeatedFailures(t *testing.T) {
	var b retirementBackoff

	b.failed()
	require.Equal(t, 1, ticksUntilReady(t, &b))

	b.failed()
	require.Equal(t, 2, ticksUntilReady(t, &b))

	b.failed()
	require.Equal(t, 4, ticksUntilReady(t, &b))
}

func TestRetirementBackoffCapsTheGap(t *testing.T) {
	var b retirementBackoff
	for range 64 {
		b.failed()
		ticksUntilReady(t, &b)
	}

	b.failed()
	require.Equal(t, maxRetirementSkipTicks, ticksUntilReady(t, &b))
}

func TestRetirementBackoffResetsOnSuccess(t *testing.T) {
	var b retirementBackoff
	for range 4 {
		b.failed()
		ticksUntilReady(t, &b)
	}

	b.succeeded()
	require.True(t, b.ready())
	require.True(t, b.ready())
}
