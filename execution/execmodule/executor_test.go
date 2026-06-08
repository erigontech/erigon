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

package execmodule_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/execmodule"
)

func TestSetKnownTipHintIgnoresLowerAndZero(t *testing.T) {
	pe := &execmodule.PipelineExecutor{}

	pe.SetKnownTipHint(1000)
	require.Equal(t, uint64(1000), pe.KnownTipHint())

	pe.SetKnownTipHint(500) // lower: ignored
	require.Equal(t, uint64(1000), pe.KnownTipHint())

	pe.SetKnownTipHint(0) // zero: ignored
	require.Equal(t, uint64(1000), pe.KnownTipHint())

	pe.SetKnownTipHint(2000) // higher: applied
	require.Equal(t, uint64(2000), pe.KnownTipHint())
}

// TestSetKnownTipHintConcurrentMonotonic exercises the CAS loop under -race:
// many writers racing with mixed values must leave the global max, never a
// lower value clobbering a higher one.
func TestSetKnownTipHintConcurrentMonotonic(t *testing.T) {
	pe := &execmodule.PipelineExecutor{}

	const goroutines = 32
	const perGoroutine = 100

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range perGoroutine {
				pe.SetKnownTipHint(uint64(g*perGoroutine + i))
			}
		}(g)
	}
	wg.Wait()

	require.Equal(t, uint64(goroutines*perGoroutine-1), pe.KnownTipHint())
}
